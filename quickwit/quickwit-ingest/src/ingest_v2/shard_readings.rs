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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::{ShardId, SourceUid};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::metrics::{
    CLOSED_SHARDS, OPEN_SHARDS, SHARD_LT_THROUGHPUT_MIB, SHARD_ST_THROUGHPUT_MIB,
};
use super::state::WeakIngesterState;

const SAMPLE_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ShardThroughputReading {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    pub short_term_ingestion_rate: ByteSize,
    pub long_term_ingestion_rate: ByteSize,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ShardThroughputReadings {
    pub per_source_readings: BTreeMap<SourceUid, Vec<ShardThroughputReading>>,
}

/// The ShardReadingsPublisher is responsible for harvesting shard state and throughput readings
/// from the ingester state, for use in reporting tasks. The legacy, gossip-based LocalShardsUpdate
/// task reads from the data produced here until all indexers are migrated to the new gRPC-based
/// IndexerReportingTask, which communicates shard readings directly with the control plane.
pub(super) struct ShardReadingsPublisher {
    weak_state: WeakIngesterState,
    local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
}

impl ShardReadingsPublisher {
    pub fn spawn(
        weak_state: WeakIngesterState,
        local_shards_tx: watch::Sender<Option<Arc<ShardThroughputReadings>>>,
    ) -> JoinHandle<()> {
        let publisher = Self {
            weak_state,
            local_shards_tx,
        };
        tokio::spawn(publisher.run())
    }

    async fn run(self) {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + SAMPLE_INTERVAL,
            SAMPLE_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(state) = self.weak_state.upgrade() else {
                warn!("stopping ShardReadingsPublisher: failed to upgrade state");
                return;
            };
            match *state.status_rx.borrow() {
                IngesterStatus::Initializing => continue, // WAL loading is async; retry next tick
                IngesterStatus::Failed => return,         // This pod should be killed.
                _ => {}
            }
            let shared_rate_meter_opt = state.shared_rate_meter_rx.borrow().clone();
            let Some(shared_rate_meter) = shared_rate_meter_opt else {
                // possible, but unlikely if we're initialized but haven't sent a reading yet.
                continue;
            };
            let snapshot = shared_rate_meter.harvest();
            report_local_shards_metrics(&snapshot);
            self.local_shards_tx.send_replace(Some(Arc::new(snapshot)));
        }
    }
}

fn report_local_shards_metrics(snapshot: &ShardThroughputReadings) {
    let mut num_open_shards = 0;
    let mut num_closed_shards = 0;

    for shard_readings in snapshot.per_source_readings.values() {
        for shard_reading in shard_readings {
            match shard_reading.shard_state {
                ShardState::Open => num_open_shards += 1,
                ShardState::Closed => num_closed_shards += 1,
                ShardState::Unavailable | ShardState::Unspecified => {}
            }
            SHARD_ST_THROUGHPUT_MIB
                .observe(shard_reading.short_term_ingestion_rate.as_mib().ceil());
            SHARD_LT_THROUGHPUT_MIB.observe(shard_reading.long_term_ingestion_rate.as_mib().ceil());
        }
    }
    OPEN_SHARDS.set(num_open_shards as f64);
    CLOSED_SHARDS.set(num_closed_shards as f64);
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_proto::types::IndexUid;

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::rate_meter::SharedRateMeter;
    use crate::ingest_v2::state::IngesterState;

    async fn state() -> (tempfile::TempDir, IngesterState) {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &["indexer"],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        IngesterState::for_test(cluster).await
    }

    async fn tick() {
        tokio::time::advance(SAMPLE_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_publisher_harvests_while_state_and_wal_are_locked() {
        let (_dir, state) = state().await;
        tokio::time::pause();
        let (sender, mut receiver) = watch::channel(None);
        let publisher = ShardReadingsPublisher::spawn(state.weak(), sender);
        tokio::task::yield_now().await;
        let guard = state.lock_fully("test").await.unwrap();
        tick().await;
        assert!(receiver.borrow_and_update().is_some());
        tick().await;
        assert!(receiver.has_changed().unwrap());
        drop(guard);
        drop(state);
        tick().await;
        publisher.await.unwrap();
    }

    #[tokio::test]
    async fn test_publisher_stops_when_ingester_fails() {
        let (_dir, state) = state().await;
        tokio::time::pause();
        let (sender, _receiver) = watch::channel(None);
        let publisher = ShardReadingsPublisher::spawn(state.weak(), sender);
        state
            .lock_partially("test")
            .await
            .unwrap()
            .set_status(IngesterStatus::Failed)
            .await;
        tick().await;
        publisher.await.unwrap();
    }

    #[test]
    fn test_report_local_shards_metrics() {
        let shared_rate_meter = Arc::new(SharedRateMeter::default());
        let mut shards = Vec::new();
        for (shard_id, shard_state) in [
            (1, ShardState::Open),
            (2, ShardState::Open),
            (3, ShardState::Closed),
        ] {
            let shard = IngesterShard::builder(
                IndexUid::for_test("test-index", 0),
                "test-source".to_string(),
                ShardId::from(shard_id),
            )
            .with_state(shard_state)
            .with_shared_rate_meter(shared_rate_meter.clone())
            .advertisable()
            .build();
            shards.push(shard);
        }
        report_local_shards_metrics(&shared_rate_meter.harvest());
        assert_eq!(OPEN_SHARDS.get(), 2.0);
        assert_eq!(CLOSED_SHARDS.get(), 1.0);

        // Dropped shards are no longer counted.
        drop(shards);
        report_local_shards_metrics(&shared_rate_meter.harvest());
        assert_eq!(OPEN_SHARDS.get(), 0.0);
        assert_eq!(CLOSED_SHARDS.get(), 0.0);
    }
}
