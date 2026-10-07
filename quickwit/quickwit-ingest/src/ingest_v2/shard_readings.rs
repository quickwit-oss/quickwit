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

use std::sync::Arc;
use std::time::Duration;

use quickwit_proto::ingest::ShardState;
use quickwit_proto::ingest::ingester::IngesterStatus;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use super::local_shards_utils::ShardThroughputReadings;
use super::metrics::{
    CLOSED_SHARDS, OPEN_SHARDS, SHARD_LT_THROUGHPUT_MIB, SHARD_ST_THROUGHPUT_MIB,
};
use super::state::WeakIngesterState;

const LOCAL_SHARDS_SAMPLE_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};

/// The ShardReadingsPublisher is responsinble for harvesting shard state and throughput readings
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
            tokio::time::Instant::now() + LOCAL_SHARDS_SAMPLE_INTERVAL,
            LOCAL_SHARDS_SAMPLE_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(state) = self.weak_state.upgrade() else {
                warn!("stopping ShardReadingsPublisher: failed to upgrade state");
                // unexpected: something went wrong upstream. Terminate.
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
            let snapshot = Arc::new(shared_rate_meter.harvest());
            self.local_shards_tx.send_replace(Some(snapshot.clone()));
            report_local_shards_metrics(&snapshot);
        }
    }
}

fn report_local_shards_metrics(snapshot: &ShardThroughputReadings) {
    let mut num_open_shards = 0;
    let mut num_closed_shards = 0;

    for shard_infos in snapshot.per_source_shard_infos.values() {
        for shard_info in shard_infos {
            match shard_info.shard_state {
                ShardState::Open => num_open_shards += 1,
                ShardState::Closed => num_closed_shards += 1,
                ShardState::Unavailable | ShardState::Unspecified => {}
            }
            SHARD_ST_THROUGHPUT_MIB.observe(shard_info.short_term_ingestion_rate.as_mib().ceil());
            SHARD_LT_THROUGHPUT_MIB.observe(shard_info.long_term_ingestion_rate.as_mib().ceil());
        }
    }
    OPEN_SHARDS.set(num_open_shards as f64);
    CLOSED_SHARDS.set(num_closed_shards as f64);
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_proto::types::{IndexUid, ShardId};

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
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
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_publish_local_shards_cadence_and_snapshot() {
        let (_dir, state) = state().await;
        tokio::time::pause();
        let (sender, mut receiver) = watch::channel(None);
        let mut inner = state.lock_partially("test").await.unwrap();
        for (source, id, visible) in [("a", 1, true), ("a", 2, false), ("b", 3, true)] {
            let mut shard = IngesterShard::builder(
                IndexUid::for_test("index", 0),
                source.to_string(),
                ShardId::from(id),
                inner.shared_rate_meter.clone(),
            )
            .build();
            if visible {
                shard.make_advertisable();
            }
            shard.record_persisted_bytes(100);
            inner.shards.insert(shard.queue_id(), shard);
        }
        drop(inner);
        let publisher = ShardReadingsPublisher::spawn(state.weak(), sender);
        tokio::task::yield_now().await;
        assert!(receiver.borrow().is_none());
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL / 2).await;
        tokio::task::yield_now().await;
        assert!(receiver.borrow().is_none());
        tokio::time::advance(LOCAL_SHARDS_SAMPLE_INTERVAL / 2).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        let snapshot = receiver.borrow_and_update().clone().unwrap();
        assert_eq!(snapshot.per_source_shard_infos.len(), 2);
        for shards in snapshot.per_source_shard_infos.values() {
            assert_eq!(shards.len(), 1);
            let shard = shards.first().unwrap();
            assert_ne!(shard.shard_id, ShardId::from(2));
            assert_eq!(shard.short_term_ingestion_rate, ByteSize::b(1_960));
            assert_eq!(shard.long_term_ingestion_rate, ByteSize::b(1_960));
        }
        assert!(!receiver.has_changed().unwrap());
        state.lock_partially("test").await.unwrap().shards.clear();
        tick().await;
        assert!(
            receiver
                .borrow()
                .as_ref()
                .unwrap()
                .per_source_shard_infos
                .is_empty()
        );
        assert_eq!(snapshot.per_source_shard_infos.len(), 2);
        publisher.abort();
        assert!(publisher.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_local_shards_publisher_harvests_while_state_and_wal_are_locked() {
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
    async fn test_publisher_status_lifecycle() {
        let (_dir, mut state) = state().await;
        let meter = state.shared_rate_meter_rx.borrow().clone();
        let (meter_sender, meter_receiver) = watch::channel(None);
        state.shared_rate_meter_rx = meter_receiver;
        tokio::time::pause();
        let (sender, mut receiver) = watch::channel(None);
        let publisher = ShardReadingsPublisher::spawn(state.weak(), sender);
        tokio::task::yield_now().await;
        let mut guard = state.lock_partially("test").await.unwrap();
        guard.set_status(IngesterStatus::Initializing).await;
        tick().await;
        assert!(receiver.borrow().is_none());
        guard.set_status(IngesterStatus::Ready).await;
        tick().await;
        assert!(receiver.borrow().is_none());
        meter_sender.send_replace(meter);
        for status in [
            IngesterStatus::Ready,
            IngesterStatus::Decommissioning,
            IngesterStatus::Decommissioned,
        ] {
            guard.set_status(status).await;
            tick().await;
            assert!(receiver.has_changed().unwrap());
            assert!(receiver.borrow_and_update().is_some());
            assert!(!publisher.is_finished());
        }
        guard.set_status(IngesterStatus::Failed).await;
        tick().await;
        publisher.await.unwrap();
    }

    #[tokio::test]
    async fn test_publisher_metrics_reset() {
        let meter = Arc::new(crate::ingest_v2::SharedRateMeter::default());
        let shards: Vec<_> = [
            ShardState::Open,
            ShardState::Open,
            ShardState::Closed,
            ShardState::Unavailable,
            ShardState::Unspecified,
        ]
        .into_iter()
        .enumerate()
        .map(|(id, status)| {
            IngesterShard::builder(
                IndexUid::for_test("index", 0),
                "source".to_string(),
                ShardId::from(id as u64),
                meter.clone(),
            )
            .with_state(status)
            .advertisable()
            .build()
        })
        .collect();
        tokio::time::sleep(Duration::from_millis(1)).await;
        report_local_shards_metrics(&meter.harvest());
        assert_eq!(OPEN_SHARDS.get(), 2.0);
        assert_eq!(CLOSED_SHARDS.get(), 1.0);
        drop(shards);
        report_local_shards_metrics(&meter.harvest());
        assert_eq!(OPEN_SHARDS.get(), 0.0);
        assert_eq!(CLOSED_SHARDS.get(), 0.0);
    }
}
