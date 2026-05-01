// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Background poller that fetches dual-ship routing from the metadata
//! service and merges it into the shared [`DualShipStore`].
//!
//! Mirrors the cycle in `byoc-dualship-mgr/internal/poller`.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use tracing::{info, warn};

use super::client::DualShipFetcher;
use super::store::{DualShipStore, write_csv_to_disk, write_watermark_to_disk};
use super::types::{ChangeSet, MetricRecord};

pub struct DualShipPollerConfig {
    pub store: Arc<RwLock<DualShipStore>>,
    pub fetcher: DualShipFetcher,
    pub poll_interval: Duration,
    pub csv_path: PathBuf,
}

/// Runs the dual-ship poller until the task is cancelled (e.g. by the
/// tokio runtime shutting down).
///
/// Each cycle:
/// 1. Reads the current watermark from the store.
/// 2. Fetches records from the metadata service since that watermark.
/// 3. Applies them via `replace` (full sync, watermark==0) or `merge`
///    (incremental).
/// 4. Persists the CSV when anything changed and advances the watermark.
///
/// On HTTP failure the cycle logs and retries on the next tick — no
/// in-memory state changes.
pub async fn run_dual_ship_poller(config: DualShipPollerConfig) {
    let DualShipPollerConfig {
        store,
        fetcher,
        poll_interval,
        csv_path,
    } = config;

    info!(
        poll_interval_secs = poll_interval.as_secs(),
        csv_path = %csv_path.display(),
        "starting dual-ship poller"
    );

    // Immediate first poll, then ticker-driven thereafter (matches Go).
    poll_once(&store, &fetcher, &csv_path).await;

    let mut interval = tokio::time::interval(poll_interval);
    // First tick fires immediately — burn it so we don't double-poll
    // right after the initial poll above.
    interval.tick().await;

    loop {
        interval.tick().await;
        poll_once(&store, &fetcher, &csv_path).await;
    }
}

async fn poll_once(
    store: &Arc<RwLock<DualShipStore>>,
    fetcher: &DualShipFetcher,
    csv_path: &std::path::Path,
) {
    let watermark = {
        let guard = store.read().expect("dual-ship store lock poisoned");
        guard.watermark()
    };
    let is_full_sync = watermark == 0;

    let records = match fetcher.fetch(watermark).await {
        Ok(records) => records,
        Err(err) => {
            warn!(%err, full_sync = is_full_sync, "dual-ship fetch failed, will retry");
            return;
        }
    };

    let new_watermark = compute_new_watermark(&records, is_full_sync);

    // Apply all memory mutations under a single brief write lock. The lock
    // is dropped before any disk IO so writers (the next poll cycle) can't
    // be queued behind an fsync.
    let change_set = apply_records_and_set_watermark(store, &records, is_full_sync, new_watermark);

    // Memory state is now authoritative. Disk persistence is best-effort:
    // if it fails the next successful poll will rewrite both files, and at
    // worst startup falls back to a full sync from SaaS.
    //
    // The CSV writer takes a read lock inside the blocking task. Read locks
    // don't block other readers (the metric event hot path), and the only
    // writer is this poller — which is currently awaiting this very task —
    // so contention is impossible.
    if change_set.total() > 0 {
        let store_for_io = Arc::clone(store);
        let csv_path_owned = csv_path.to_path_buf();
        let write_result = tokio::task::spawn_blocking(move || {
            let guard = store_for_io
                .read()
                .expect("dual-ship store lock poisoned");
            write_csv_to_disk(&csv_path_owned, guard.metrics())
        })
        .await;
        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(%err, "failed to persist dual-ship CSV; will retry on next poll");
            }
            Err(err) => {
                warn!(%err, "dual-ship CSV write task panicked");
            }
        }
    }

    // Watermark IO needs no lock at all — `new_watermark` is already an
    // owned local. The in-memory copy was updated above.
    let csv_path_owned = csv_path.to_path_buf();
    let watermark_result = tokio::task::spawn_blocking(move || {
        write_watermark_to_disk(&csv_path_owned, new_watermark)
    })
    .await;
    match watermark_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(%err, "failed to persist dual-ship watermark; will retry on next poll");
        }
        Err(err) => {
            warn!(%err, "dual-ship watermark write task panicked");
        }
    }

    info!(
        added = change_set.added,
        updated = change_set.updated,
        removed = change_set.removed,
        new_watermark,
        full_sync = is_full_sync,
        "dual-ship poll succeeded"
    );
}

/// Applies the fetched records to the in-memory store and updates the
/// watermark under a single brief write lock. No file IO.
fn apply_records_and_set_watermark(
    store: &Arc<RwLock<DualShipStore>>,
    records: &[MetricRecord],
    is_full_sync: bool,
    new_watermark: i64,
) -> ChangeSet {
    let mut guard = store.write().expect("dual-ship store lock poisoned");
    let change_set = if is_full_sync {
        guard.replace(records)
    } else {
        guard.merge(records)
    };
    guard.set_watermark(new_watermark);
    change_set
}

/// Returns the new watermark to persist after a successful fetch.
///
/// - If the response contains records, use the max `last_updated_unix`.
/// - If the response was empty and this was an incremental poll, advance
///   to the current unix time so we don't keep replaying the same window
///   (matches the Go sidecar `pollOnce` behavior).
/// - If the response was empty and this was a full sync, leave the
///   watermark at 0 — we still need to discover any records on the next
///   poll, which is also a full sync.
fn compute_new_watermark(records: &[MetricRecord], is_full_sync: bool) -> i64 {
    if let Some(max) = records.iter().map(|record| record.last_updated_unix).max() {
        return max;
    }
    if is_full_sync {
        return 0;
    }
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::metric_dual_ship::types::Destination;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ENDPOINT_PATH: &str = "/api/unstable/byoc/ingest/metadata/dual-shipped-metrics";

    fn make_fetcher(uri: &str) -> DualShipFetcher {
        DualShipFetcher::new(
            "test-key".to_string(),
            uri.to_string(),
            Duration::from_secs(2),
        )
        .expect("fetcher build should succeed")
    }

    #[test]
    fn compute_new_watermark_uses_max_last_updated_unix() {
        let records = vec![
            MetricRecord {
                name: "a".into(),
                destination: Destination::Saas,
                last_updated_unix: 100,
            },
            MetricRecord {
                name: "b".into(),
                destination: Destination::Dual,
                last_updated_unix: 200,
            },
        ];
        assert_eq!(compute_new_watermark(&records, false), 200);
        assert_eq!(compute_new_watermark(&records, true), 200);
    }

    #[test]
    fn compute_new_watermark_incremental_empty_advances_to_now() {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let advanced = compute_new_watermark(&[], false);
        // Allow a small clock drift between the call and our reference now().
        assert!(advanced >= now - 1);
        assert!(advanced <= now + 5);
    }

    #[test]
    fn compute_new_watermark_full_sync_empty_stays_zero() {
        assert_eq!(compute_new_watermark(&[], true), 0);
    }

    #[tokio::test]
    async fn poll_once_full_sync_writes_csv_and_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "metrics": [
                    { "metric_name": "alpha", "destination": 1, "last_updated_unix": 100 },
                    { "metric_name": "bravo", "destination": 3, "last_updated_unix": 200 },
                    { "metric_name": "charlie", "destination": 2, "last_updated_unix": 300 },
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let store = Arc::new(RwLock::new(DualShipStore::default()));
        let fetcher = make_fetcher(&server.uri());

        poll_once(&store, &fetcher, &csv).await;

        // Reload from disk to verify durable writes.
        let reloaded = DualShipStore::load(&csv).unwrap();
        assert_eq!(reloaded.lookup("alpha"), Some(Destination::Saas));
        assert_eq!(reloaded.lookup("bravo"), Some(Destination::Dual));
        assert!(
            reloaded.lookup("charlie").is_none(),
            "byoc must not be persisted"
        );
        assert_eq!(reloaded.watermark(), 300);
    }

    #[tokio::test]
    async fn poll_once_incremental_merges() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        // Pre-seed the store with watermark > 0 so we hit the merge branch.
        let mut seeded = DualShipStore::default();
        seeded.merge(&[MetricRecord {
            name: "alpha".into(),
            destination: Destination::Saas,
            last_updated_unix: 50,
        }]);
        write_csv_to_disk(&csv, seeded.metrics()).unwrap();
        write_watermark_to_disk(&csv, 50).unwrap();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "metrics": [
                    { "metric_name": "alpha", "destination": 2, "last_updated_unix": 75 },
                    { "metric_name": "delta", "destination": 1, "last_updated_unix": 80 },
                ]
            })))
            .mount(&server)
            .await;

        let store = Arc::new(RwLock::new(DualShipStore::load(&csv).unwrap()));
        let fetcher = make_fetcher(&server.uri());

        poll_once(&store, &fetcher, &csv).await;

        let reloaded = DualShipStore::load(&csv).unwrap();
        // alpha was reclassified to byoc → removed; delta added.
        assert!(reloaded.lookup("alpha").is_none());
        assert_eq!(reloaded.lookup("delta"), Some(Destination::Saas));
        assert_eq!(reloaded.watermark(), 80);
    }

    #[tokio::test]
    async fn poll_once_failed_fetch_does_not_change_state() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let store = Arc::new(RwLock::new(DualShipStore::default()));
        let fetcher = make_fetcher(&server.uri());

        poll_once(&store, &fetcher, &csv).await;

        // Watermark stays at 0; CSV not created.
        let guard = store.read().unwrap();
        assert_eq!(guard.watermark(), 0);
        assert!(guard.is_empty());
        // Watermark file should not exist.
        assert!(std::fs::metadata(super::super::store::watermark_path(&csv)).is_err());
    }

    #[tokio::test]
    async fn poll_once_empty_incremental_advances_watermark_to_now() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("metrics_to_saas.csv");

        // Watermark > 0 so this is an incremental poll.
        let store = Arc::new(RwLock::new(DualShipStore::default()));
        store.write().unwrap().set_watermark(1_000_000_000);
        write_watermark_to_disk(&csv, 1_000_000_000).unwrap();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"metrics": []})))
            .mount(&server)
            .await;

        let fetcher = make_fetcher(&server.uri());
        poll_once(&store, &fetcher, &csv).await;

        let guard = store.read().unwrap();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(guard.watermark() >= now - 1);
    }
}
