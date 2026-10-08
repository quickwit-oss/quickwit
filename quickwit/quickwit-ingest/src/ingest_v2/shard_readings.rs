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
            let snapshot = Arc::new(shared_rate_meter.harvest());
            self.local_shards_tx.send_replace(Some(snapshot));
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};

    use super::*;
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
}
