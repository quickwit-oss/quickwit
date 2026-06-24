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

//! Slice request coalescer.
//!
//! Object storage requests have a high fixed cost (a network round-trip and, on
//! providers such as S3, a per-request monetary cost) compared to the marginal cost of
//! transferring a few extra bytes. When a search hits a split, it frequently issues several
//! `get_slice` requests for ranges that sit close to each other within the same object (e.g. the
//! term dictionary and the postings of neighboring fields).
//!
//! [`CoalescingStorage`] sits in front of such a storage. It buffers incoming `get_slice` requests
//! for a short window (5ms by default), groups the ones that target nearby ranges of the same
//! object, and issues a single underlying `get_slice` spanning each group. The small gaps between
//! the requested ranges are downloaded wastefully, which trades a bit of bandwidth for fewer
//! round-trips. Each waiter receives a zero-copy slice of the merged buffer.
//!
//! Unlike the [`crate::DebouncedStorage`], which only deduplicates *identical* in-flight requests,
//! the coalescer merges *distinct but nearby* ranges.
//!
//! ## Cascading effect on dependent reads
//!
//! Coalescing one wave of requests is what makes the *next* wave coalescable. Search reads are
//! staged: a term dictionary lookup must complete before the postings list it points to can be
//! read. If the term dictionary `get_slice` requests are coalesced into one GET, they all resolve
//! at the same instant, so the postings list requests they trigger are issued together and land in
//! a single coalescing window — where they, in turn, get coalesced. Conversely, if the term
//! dictionary requests are served as separate GETs they complete at staggered times, scattering the
//! dependent postings list requests across multiple windows and preventing them from merging. In
//! other words, failing to coalesce an upstream stage silently defeats coalescing of every stage
//! that depends on it, so the gap/span thresholds should be generous enough to keep these
//! co-issued term-dictionary reads together.
//!
//! ## Interaction with caching
//!
//! Every read cache (the fast-field [`crate::QuickwitCache`], the per-query
//! [`crate::ByteRangeCache`], the split footer cache, …) sits *above* the coalescer: a cache lookup
//! uses the caller's original range, so coalescing only happens on a miss, and what gets cached is
//! each individual requested slice keyed by its own range — never the merged block, and never the
//! wastefully-fetched gap bytes.
//!
//! There is one subtlety worth recording. The slice handed back to each waiter is a zero-copy
//! [`OwnedBytes::slice`] of the merged buffer, so it keeps that whole buffer alive for as long as
//! the slice lives. If such a slice is then stored in a cache that retains it verbatim (as
//! [`crate::ByteRangeCache`] does for non-overlapping inserts), a small cached slice pins the entire
//! merged block in memory while the cache only accounts for the slice's own length.
//!
//! In practice this is harmless given how Quickwit caches read data:
//! - Term dictionary and postings list reads — the ranges most likely to be coalesced — are only
//!   ever held in the *ephemeral, per-query* [`crate::ByteRangeCache`], which is discarded when the
//!   query finishes, so any over-retained merged block is freed promptly.
//! - The long-lived fast-field cache stores fast fields *whole* rather than as sub-slices, so a
//!   cached entry never pins a larger coalesced allocation than the data it represents.
//!
//! If a future cache were to retain small sub-slices of large coalesced blocks for a long time, the
//! coalescer would need to copy those slices out (detaching them from the merged allocation) before
//! returning them. That is not necessary today.
//!
//! ## Cancellation safety
//!
//! The actual fetch runs in a detached task and communicates results back to callers through
//! `oneshot` channels. A caller dropping its future therefore never aborts an in-flight fetch (the
//! merged request still completes for its remaining waiters), and the internal `std::sync::Mutex`
//! is only ever held for synchronous critical sections, never across an `.await`.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tokio::sync::oneshot;

use crate::metrics::{
    OBJECT_STORAGE_COALESCE_WASTED_BYTES_TOTAL, OBJECT_STORAGE_COALESCED_GETS_TOTAL,
    OBJECT_STORAGE_COALESCED_SUBREQUESTS_TOTAL,
};
use crate::storage::SendableAsync;
use crate::{BulkDeleteError, OwnedBytes, Storage, StorageErrorKind, StorageResult};

/// Default duration the coalescer waits to gather requests before issuing the merged fetch.
const DEFAULT_COALESCE_WINDOW_MS: u64 = 5;
/// Default largest gap (in bytes) tolerated between two ranges for them to be merged. Bytes falling
/// in the gap are downloaded but never requested.
const DEFAULT_COALESCE_MAX_GAP_BYTES: usize = 1 << 20; // 1 MiB
/// Default cap on the span of a single merged request. Prevents a chain of nearby ranges from
/// snowballing into an arbitrarily large download.
const DEFAULT_COALESCE_MAX_SPAN_BYTES: usize = 8 << 20; // 8 MiB

/// Tuning parameters for the slice coalescer, sourced from the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoalesceConfig {
    /// How long to buffer requests before flushing. A zero window disables coalescing entirely.
    pub window: Duration,
    /// Largest gap between two consecutive ranges that may still be merged.
    pub max_gap: usize,
    /// Upper bound on the span of a merged request.
    pub max_span: usize,
}

impl CoalesceConfig {
    /// Reads the configuration from the `QW_S3_COALESCE_*` environment variables, falling back to
    /// the defaults above.
    pub(crate) fn from_env() -> Self {
        let window_ms: u64 = quickwit_common::get_from_env(
            "QW_S3_COALESCE_WINDOW_MS",
            DEFAULT_COALESCE_WINDOW_MS,
            false,
        );
        let max_gap: usize = quickwit_common::get_from_env(
            "QW_S3_COALESCE_MAX_GAP_BYTES",
            DEFAULT_COALESCE_MAX_GAP_BYTES,
            false,
        );
        let max_span: usize = quickwit_common::get_from_env(
            "QW_S3_COALESCE_MAX_SPAN_BYTES",
            DEFAULT_COALESCE_MAX_SPAN_BYTES,
            false,
        );
        CoalesceConfig {
            window: Duration::from_millis(window_ms),
            max_gap,
            max_span,
        }
    }

    fn is_enabled(&self) -> bool {
        !self.window.is_zero()
    }
}

/// A single buffered `get_slice` request: the requested range and the channel used to hand the
/// result back to the waiting caller.
struct PendingRequest {
    range: Range<usize>,
    response_tx: oneshot::Sender<StorageResult<OwnedBytes>>,
}

/// A group of nearby requests that will be served by a single underlying `get_slice`.
struct Cluster {
    /// The merged range spanning every member range.
    range: Range<usize>,
    /// Members, sorted by `range.start`.
    requests: Vec<PendingRequest>,
}

/// The geometry of a cluster, independent of the waiting requests. Computed by [`plan_clusters`]
/// and kept separate so the clustering logic can be unit-tested without channels.
#[derive(Debug, PartialEq, Eq)]
struct ClusterPlan {
    range: Range<usize>,
    /// Indices into the input `ranges` slice, sorted by `range.start`.
    member_indices: Vec<usize>,
}

/// Groups `ranges` into clusters of nearby ranges. The input need not be sorted.
///
/// Ranges are visited in ascending start order and greedily appended to the current cluster while
/// both the gap and span constraints hold; otherwise a new cluster is opened. A range that is
/// larger than `max_span` on its own always forms its own cluster (the constraints only ever
/// prevent *extending* a cluster, never serving a request).
fn plan_clusters(ranges: &[Range<usize>], config: CoalesceConfig) -> Vec<ClusterPlan> {
    let mut order: Vec<usize> = (0..ranges.len()).collect();
    order.sort_by_key(|&idx| ranges[idx].start);

    let mut clusters: Vec<ClusterPlan> = Vec::new();
    for idx in order {
        let candidate = ranges[idx].clone();
        match clusters.last_mut() {
            Some(last) if can_extend(&last.range, &candidate, config) => {
                last.range.end = last.range.end.max(candidate.end);
                last.member_indices.push(idx);
            }
            _ => clusters.push(ClusterPlan {
                range: candidate,
                member_indices: vec![idx],
            }),
        }
    }
    clusters
}

/// Returns whether `candidate` (whose start is `>=` the cluster's start, since ranges are visited
/// in start order) can be merged into `cluster` without violating the gap or span constraints.
fn can_extend(cluster: &Range<usize>, candidate: &Range<usize>, config: CoalesceConfig) -> bool {
    let gap = candidate.start.saturating_sub(cluster.end);
    if gap > config.max_gap {
        return false;
    }
    let merged_end = cluster.end.max(candidate.end);
    let merged_span = merged_end - cluster.start;
    merged_span <= config.max_span
}

/// Storage wrapper that coalesces nearby `get_slice` requests into fewer, larger requests.
///
/// See the [module documentation](self) for the rationale and cancellation-safety guarantees.
pub(crate) struct CoalescingStorage<T> {
    underlying: Arc<T>,
    config: CoalesceConfig,
    /// Requests buffered during the current window, keyed by object path. The first request to
    /// populate a path's buffer is responsible for spawning the flush task; subsequent requests
    /// for the same path simply join the existing buffer.
    pending: Arc<Mutex<HashMap<PathBuf, Vec<PendingRequest>>>>,
}

impl<T> std::fmt::Debug for CoalescingStorage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoalescingStorage")
            .field("config", &self.config)
            .finish()
    }
}

impl<T: Storage> CoalescingStorage<T> {
    /// Wraps `underlying`, reading the coalescing parameters from the environment.
    pub(crate) fn new(underlying: T) -> Self {
        Self::with_config(underlying, CoalesceConfig::from_env())
    }

    pub(crate) fn with_config(underlying: T, config: CoalesceConfig) -> Self {
        Self {
            underlying: Arc::new(underlying),
            config,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Spawns the task that, after the coalescing window elapses, drains the buffer for `path` and
    /// issues the merged fetches.
    fn spawn_flush(&self, path: PathBuf) {
        let pending = self.pending.clone();
        let underlying = self.underlying.clone();
        let config = self.config;
        tokio::spawn(async move {
            tokio::time::sleep(config.window).await;
            let requests = {
                let mut guard = pending.lock().unwrap();
                guard.remove(&path).unwrap_or_default()
            };
            flush_requests(underlying.as_ref(), &path, requests, config).await;
        });
    }
}

/// Drains a buffered set of requests for `path`, grouping them into clusters and fetching each
/// cluster from `underlying`.
async fn flush_requests<T: Storage>(
    underlying: &T,
    path: &Path,
    requests: Vec<PendingRequest>,
    config: CoalesceConfig,
) {
    if requests.is_empty() {
        return;
    }
    let ranges: Vec<Range<usize>> = requests.iter().map(|req| req.range.clone()).collect();
    let plans = plan_clusters(&ranges, config);

    // Move each request into the cluster that claimed its index. Every index appears in exactly one
    // plan, so each slot is taken exactly once.
    let mut slots: Vec<Option<PendingRequest>> = requests.into_iter().map(Some).collect();
    let mut clusters: Vec<Cluster> = Vec::with_capacity(plans.len());
    for plan in plans {
        let mut cluster_requests = Vec::with_capacity(plan.member_indices.len());
        for idx in plan.member_indices {
            let request = slots[idx]
                .take()
                .expect("each request index belongs to exactly one cluster");
            cluster_requests.push(request);
        }
        clusters.push(Cluster {
            range: plan.range,
            requests: cluster_requests,
        });
    }

    let fetches = clusters
        .into_iter()
        .map(|cluster| fetch_cluster(underlying, path, cluster));
    futures::future::join_all(fetches).await;
}

/// Fetches the merged range of a single cluster and dispatches a zero-copy slice to each member.
async fn fetch_cluster<T: Storage>(underlying: &T, path: &Path, cluster: Cluster) {
    record_cluster_metrics(&cluster);

    let merged = cluster.range;
    let result = underlying.get_slice(path, merged.clone()).await;

    for request in cluster.requests {
        let response = match &result {
            Ok(bytes) => {
                debug_assert!(
                    request.range.start >= merged.start && request.range.end <= merged.end,
                    "requested range {:?} must lie within merged range {:?}",
                    request.range,
                    merged,
                );
                let relative_start = request.range.start - merged.start;
                let relative_end = request.range.end - merged.start;
                Ok(bytes.slice(relative_start..relative_end))
            }
            Err(error) => Err(error.clone()),
        };
        // The caller may have been cancelled and dropped its receiver; that is expected and benign.
        let _ = request.response_tx.send(response);
    }
}

/// Records how much work the coalescer performed for a cluster: one merged request standing in for
/// `requests.len()` caller requests, and the number of bytes downloaded that nobody asked for.
fn record_cluster_metrics(cluster: &Cluster) {
    OBJECT_STORAGE_COALESCED_GETS_TOTAL.inc();
    OBJECT_STORAGE_COALESCED_SUBREQUESTS_TOTAL.inc_by(cluster.requests.len() as u64);

    // `cluster.requests` is sorted by start, so we can compute the union of the requested ranges in
    // a single pass. Wasted bytes are the merged span minus that union.
    let mut covered_end = cluster.range.start;
    let mut requested_bytes = 0usize;
    for request in &cluster.requests {
        let start = request.range.start.max(covered_end);
        if request.range.end > start {
            requested_bytes += request.range.end - start;
        }
        covered_end = covered_end.max(request.range.end);
    }
    let wasted_bytes = cluster.range.len() - requested_bytes;
    OBJECT_STORAGE_COALESCE_WASTED_BYTES_TOTAL.inc_by(wasted_bytes as u64);
}

#[async_trait]
impl<T: Storage> Storage for CoalescingStorage<T> {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.underlying.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn crate::PutPayload>) -> StorageResult<()> {
        self.underlying.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.underlying.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        // Empty ranges never warrant a request and would make the gap/span arithmetic meaningless.
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        if !self.config.is_enabled() {
            return self.underlying.get_slice(path, range).await;
        }

        let (response_tx, response_rx) = oneshot::channel();
        let is_first = {
            let mut guard = self.pending.lock().unwrap();
            let buffer = guard.entry(path.to_owned()).or_default();
            buffer.push(PendingRequest { range, response_tx });
            buffer.len() == 1
        };
        if is_first {
            self.spawn_flush(path.to_owned());
        }

        match response_rx.await {
            Ok(result) => result,
            // The flush task dropped the sender without sending: it either panicked or never ran.
            Err(_) => Err(StorageErrorKind::Internal
                .with_error(anyhow!("coalesced get_slice did not produce a response"))),
        }
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // Streaming reads bypass coalescing, just like they bypass the debouncer.
        self.underlying.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.underlying.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.underlying.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.underlying.bulk_delete(paths).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.underlying.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.underlying.uri()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::RamStorage;

    fn test_config() -> CoalesceConfig {
        CoalesceConfig {
            window: Duration::from_millis(5),
            max_gap: 16,
            max_span: 1_000,
        }
    }

    #[test]
    fn test_plan_clusters_merges_nearby_ranges() {
        let config = test_config();
        // 0..10 and 12..20 are 2 bytes apart (<= max_gap), so they merge. 200..210 is far away.
        let ranges = vec![12..20, 0..10, 200..210];
        let plans = plan_clusters(&ranges, config);
        assert_eq!(
            plans,
            vec![
                ClusterPlan {
                    range: 0..20,
                    // sorted by start: index 1 (0..10) then index 0 (12..20)
                    member_indices: vec![1, 0],
                },
                ClusterPlan {
                    range: 200..210,
                    member_indices: vec![2],
                },
            ]
        );
    }

    #[test]
    fn test_plan_clusters_respects_max_gap() {
        let config = test_config();
        // gap of exactly max_gap (16) is allowed; one more is not.
        assert_eq!(plan_clusters(&[0..10, 26..30], config).len(), 1);
        assert_eq!(plan_clusters(&[0..10, 27..30], config).len(), 2);
    }

    #[test]
    fn test_plan_clusters_respects_max_span() {
        let config = CoalesceConfig {
            max_span: 100,
            ..test_config()
        };
        // Three adjacent ranges; the third would push the span past max_span, so it starts a new
        // cluster even though it is adjacent.
        let plans = plan_clusters(&[0..40, 40..80, 80..120], config);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].range, 0..80);
        assert_eq!(plans[1].range, 80..120);
    }

    #[test]
    fn test_plan_clusters_oversized_range_stands_alone() {
        let config = CoalesceConfig {
            max_span: 100,
            ..test_config()
        };
        // A range larger than max_span is still served as its own cluster.
        let ranges = std::iter::once(0..500).collect::<Vec<_>>();
        let plans = plan_clusters(&ranges, config);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].range, 0..500);
    }

    #[test]
    fn test_plan_clusters_handles_overlap_and_containment() {
        let config = test_config();
        // Overlapping (0..10, 5..15) and contained (2..4) ranges all land in one cluster.
        let plans = plan_clusters(&[0..10, 5..15, 2..4], config);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].range, 0..15);
    }

    /// A storage that records every range passed to `get_slice`, delegating to an inner
    /// `RamStorage` for the actual bytes.
    #[derive(Debug)]
    struct RecordingStorage {
        inner: RamStorage,
        calls: Arc<Mutex<Vec<Range<usize>>>>,
        call_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Storage for RecordingStorage {
        async fn check_connectivity(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn put(&self, path: &Path, payload: Box<dyn crate::PutPayload>) -> StorageResult<()> {
            self.inner.put(path, payload).await
        }
        async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
            self.inner.copy_to(path, output).await
        }
        async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.calls.lock().unwrap().push(range.clone());
            self.inner.get_slice(path, range).await
        }
        async fn get_slice_stream(
            &self,
            path: &Path,
            range: Range<usize>,
        ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
            self.inner.get_slice_stream(path, range).await
        }
        async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
            self.inner.get_all(path).await
        }
        async fn delete(&self, path: &Path) -> StorageResult<()> {
            self.inner.delete(path).await
        }
        async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
            self.inner.bulk_delete(paths).await
        }
        async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
            self.inner.file_num_bytes(path).await
        }
        fn uri(&self) -> &Uri {
            self.inner.uri()
        }
    }

    async fn recording_storage_with(content: &[u8]) -> RecordingStorage {
        let inner = RamStorage::default();
        inner
            .put(Path::new("data"), Box::new(content.to_vec()))
            .await
            .unwrap();
        RecordingStorage {
            inner,
            calls: Arc::new(Mutex::new(Vec::new())),
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[tokio::test]
    async fn test_coalesces_nearby_requests_into_single_fetch() {
        let content: Vec<u8> = (0..=255).cycle().take(300).map(|b| b as u8).collect();
        let recording = recording_storage_with(&content).await;
        let calls = recording.calls.clone();
        let call_count = recording.call_count.clone();
        let storage = Arc::new(CoalescingStorage::with_config(recording, test_config()));

        let path = Path::new("data");
        // Two nearby ranges (gap of 2) issued concurrently within the window.
        let storage_a = storage.clone();
        let storage_b = storage.clone();
        let fut_a = async move { storage_a.get_slice(path, 0..10).await.unwrap() };
        let fut_b = async move { storage_b.get_slice(path, 12..20).await.unwrap() };
        let (bytes_a, bytes_b) = tokio::join!(fut_a, fut_b);

        // A single underlying fetch served both requests.
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let expected_calls = std::iter::once(0..20).collect::<Vec<_>>();
        assert_eq!(*calls.lock().unwrap(), expected_calls);
        // Each caller got exactly the bytes it asked for.
        assert_eq!(bytes_a.as_slice(), &content[0..10]);
        assert_eq!(bytes_b.as_slice(), &content[12..20]);
    }

    #[tokio::test]
    async fn test_does_not_coalesce_far_apart_requests() {
        let content: Vec<u8> = (0..2000).map(|b| b as u8).collect();
        let recording = recording_storage_with(&content).await;
        let call_count = recording.call_count.clone();
        let storage = Arc::new(CoalescingStorage::with_config(recording, test_config()));

        let path = Path::new("data");
        let storage_a = storage.clone();
        let storage_b = storage.clone();
        // Gap of 1000 bytes, well beyond max_gap (16) and max_span (1000) once spanned.
        let fut_a = async move { storage_a.get_slice(path, 0..10).await.unwrap() };
        let fut_b = async move { storage_b.get_slice(path, 1010..1020).await.unwrap() };
        let (bytes_a, bytes_b) = tokio::join!(fut_a, fut_b);

        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(bytes_a.as_slice(), &content[0..10]);
        assert_eq!(bytes_b.as_slice(), &content[1010..1020]);
    }

    #[tokio::test]
    async fn test_disabled_window_passes_through() {
        let content: Vec<u8> = (0..100).map(|b| b as u8).collect();
        let recording = recording_storage_with(&content).await;
        let call_count = recording.call_count.clone();
        let config = CoalesceConfig {
            window: Duration::ZERO,
            ..test_config()
        };
        let storage = Arc::new(CoalescingStorage::with_config(recording, config));

        let path = Path::new("data");
        let storage_a = storage.clone();
        let storage_b = storage.clone();
        let fut_a = async move { storage_a.get_slice(path, 0..10).await.unwrap() };
        let fut_b = async move { storage_b.get_slice(path, 12..20).await.unwrap() };
        let (bytes_a, bytes_b) = tokio::join!(fut_a, fut_b);

        // With coalescing disabled, each request goes straight through.
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(bytes_a.as_slice(), &content[0..10]);
        assert_eq!(bytes_b.as_slice(), &content[12..20]);
    }

    #[tokio::test]
    async fn test_empty_range_short_circuits() {
        let recording = recording_storage_with(b"hello world").await;
        let call_count = recording.call_count.clone();
        let storage = CoalescingStorage::with_config(recording, test_config());

        let bytes = storage.get_slice(Path::new("data"), 5..5).await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_different_paths_are_not_merged() {
        let inner = RamStorage::default();
        inner
            .put(Path::new("a"), Box::new(vec![1u8; 100]))
            .await
            .unwrap();
        inner
            .put(Path::new("b"), Box::new(vec![2u8; 100]))
            .await
            .unwrap();
        let recording = RecordingStorage {
            inner,
            calls: Arc::new(Mutex::new(Vec::new())),
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let call_count = recording.call_count.clone();
        let storage = Arc::new(CoalescingStorage::with_config(recording, test_config()));

        let storage_a = storage.clone();
        let storage_b = storage.clone();
        let fut_a = async move { storage_a.get_slice(Path::new("a"), 0..10).await.unwrap() };
        let fut_b = async move { storage_b.get_slice(Path::new("b"), 0..10).await.unwrap() };
        let (bytes_a, bytes_b) = tokio::join!(fut_a, fut_b);

        // Distinct paths are buffered and fetched independently.
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(bytes_a.as_slice(), &[1u8; 10]);
        assert_eq!(bytes_b.as_slice(), &[2u8; 10]);
    }
}
