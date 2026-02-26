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

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_CAPACITY_SCORE_PREFIX;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::{NodeId, SourceUid};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::{BROADCAST_INTERVAL_PERIOD, make_key, parse_key};
use crate::OpenShardCounts;
use crate::ingest_v2::state::WeakIngesterState;
use crate::ingest_v2::wal_capacity_timeseries::{
    WalDiskCapacityTimeSeries, compute_capacity_score,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngesterCapacityScore {
    pub capacity_score: usize,
    pub open_shard_count: usize,
}

/// Periodically snapshots the ingester's WAL memory usage and open shard counts, computes
/// a capacity score, and broadcasts it to other nodes via Chitchat.
pub struct BroadcastIngesterCapacityScoreTask {
    cluster: Cluster,
    weak_state: WeakIngesterState,
    wal_capacity_time_series: Arc<Mutex<WalDiskCapacityTimeSeries>>,
}

impl BroadcastIngesterCapacityScoreTask {
    pub fn spawn(
        cluster: Cluster,
        weak_state: WeakIngesterState,
        wal_capacity_time_series: Arc<Mutex<WalDiskCapacityTimeSeries>>,
    ) -> JoinHandle<()> {
        let mut broadcaster = Self {
            cluster,
            weak_state,
            wal_capacity_time_series,
        };
        tokio::spawn(async move { broadcaster.run().await })
    }

    async fn snapshot(&self) -> Result<Option<(ByteSize, OpenShardCounts)>> {
        let state = self
            .weak_state
            .upgrade()
            .context("ingester state has been dropped")?;

        // lock fully asserts that the ingester is ready. There's a likelihood that this task runs
        // before the WAL is loaded, so we make sure that the ingester is ready just in case.
        if *state.status_rx.borrow() != IngesterStatus::Ready {
            return Ok(None);
        }

        let guard = state
            .lock_fully()
            .await
            .map_err(|_| anyhow::anyhow!("failed to acquire ingester state lock"))?;
        let usage = guard.mrecordlog.resource_usage();
        let disk_used = ByteSize::b(usage.disk_used_bytes as u64);
        let open_shard_counts = guard.get_open_shard_counts();

        Ok(Some((disk_used, open_shard_counts)))
    }

    async fn run(&mut self) {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL_PERIOD);
        let mut previous_sources: BTreeSet<SourceUid> = BTreeSet::new();

        loop {
            interval.tick().await;

            let (disk_used, open_shard_counts) = match self.snapshot().await {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => continue,
                Err(error) => {
                    info!("stopping ingester capacity broadcast: {error}");
                    return;
                }
            };

            let mut ts_guard = self.wal_capacity_time_series.lock().await;
            ts_guard.record(disk_used);
            let remaining_capacity = ts_guard.current().unwrap_or(1.0);
            let capacity_delta = ts_guard.delta().unwrap_or(0.0);
            drop(ts_guard);

            let capacity_score = compute_capacity_score(remaining_capacity, capacity_delta);

            previous_sources = self
                .broadcast_capacity(capacity_score, &open_shard_counts, &previous_sources)
                .await;
        }
    }

    async fn broadcast_capacity(
        &self,
        capacity_score: usize,
        open_shard_counts: &OpenShardCounts,
        previous_sources: &BTreeSet<SourceUid>,
    ) -> BTreeSet<SourceUid> {
        let mut current_sources = BTreeSet::new();

        for (index_uid, source_id, open_shard_count) in open_shard_counts {
            let source_uid = SourceUid {
                index_uid: index_uid.clone(),
                source_id: source_id.clone(),
            };
            let key = make_key(INGESTER_CAPACITY_SCORE_PREFIX, &source_uid);
            let capacity = IngesterCapacityScore {
                capacity_score,
                open_shard_count: *open_shard_count,
            };
            let value = serde_json::to_string(&capacity)
                .expect("`IngesterCapacityScore` should be JSON serializable");
            self.cluster.set_self_key_value(key, value).await;
            current_sources.insert(source_uid);
        }

        for removed_source in previous_sources.difference(&current_sources) {
            let key = make_key(INGESTER_CAPACITY_SCORE_PREFIX, removed_source);
            self.cluster.remove_self_key(&key).await;
        }

        current_sources
    }
}

#[derive(Debug, Clone)]
pub struct IngesterCapacityScoreUpdate {
    pub node_id: NodeId,
    pub source_uid: SourceUid,
    pub capacity_score: usize,
    pub open_shard_count: usize,
}

impl Event for IngesterCapacityScoreUpdate {}

pub async fn setup_ingester_capacity_update_listener(
    cluster: Cluster,
    event_broker: EventBroker,
) -> ListenerHandle {
    cluster
        .subscribe(INGESTER_CAPACITY_SCORE_PREFIX, move |event| {
            let Some(source_uid) = parse_key(event.key) else {
                warn!("failed to parse source UID from key `{}`", event.key);
                return;
            };
            let Ok(ingester_capacity) = serde_json::from_str::<IngesterCapacityScore>(event.value)
            else {
                warn!("failed to parse ingester capacity `{}`", event.value);
                return;
            };
            let node_id: NodeId = event.node.node_id.clone().into();
            event_broker.publish(IngesterCapacityScoreUpdate {
                node_id,
                source_uid,
                capacity_score: ingester_capacity.capacity_score,
                open_shard_count: ingester_capacity.open_shard_count,
            });
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_proto::types::{IndexUid, ShardId, SourceId};

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::state::IngesterState;

    #[tokio::test]
    async fn test_snapshot_state_dropped() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let weak_state = state.weak();
        drop(state);

        let task = BroadcastIngesterCapacityScoreTask {
            cluster,
            weak_state,
            wal_capacity_time_series: Arc::new(Mutex::new(WalDiskCapacityTimeSeries::new(
                ByteSize::mb(1),
            ))),
        };
        assert!(task.snapshot().await.is_err());
    }

    #[tokio::test]
    async fn test_broadcast_ingester_capacity() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let event_broker = EventBroker::default();

        let (_temp_dir, state) = IngesterState::for_test().await;
        let index_uid = IndexUid::for_test("test-index", 0);
        let mut state_guard = state.lock_partially().await.unwrap();
        let shard = IngesterShard::new_solo(
            index_uid.clone(),
            SourceId::from("test-source"),
            ShardId::from(0),
        )
        .advertisable()
        .build();
        state_guard.shards.insert(shard.queue_id(), shard);
        let open_shard_counts = state_guard.get_open_shard_counts();
        drop(state_guard);

        // Simulate 500 of 1000 bytes capacity used => 50% remaining, 0 delta => score = 6
        let ts = Arc::new(Mutex::new(WalDiskCapacityTimeSeries::new(ByteSize::b(
            1000,
        ))));
        let task = BroadcastIngesterCapacityScoreTask {
            cluster: cluster.clone(),
            weak_state: state.weak(),
            wal_capacity_time_series: ts.clone(),
        };
        {
            let mut ts_guard = ts.lock().await;
            ts_guard.record(ByteSize::b(500));
            let remaining = ts_guard.current().unwrap();
            let delta = ts_guard.delta().unwrap();
            assert_eq!(compute_capacity_score(remaining, delta), 6);
        }
        let capacity_score = 6;
        assert_eq!(capacity_score, 6);

        let update_counter = Arc::new(AtomicUsize::new(0));
        let update_counter_clone = update_counter.clone();
        let index_uid_clone = index_uid.clone();
        let _sub = event_broker.subscribe(move |event: IngesterCapacityScoreUpdate| {
            update_counter_clone.fetch_add(1, Ordering::Release);
            assert_eq!(event.source_uid.index_uid, index_uid_clone);
            assert_eq!(event.source_uid.source_id, "test-source");
            assert_eq!(event.capacity_score, 6);
            assert_eq!(event.open_shard_count, 1);
        });

        let _listener =
            setup_ingester_capacity_update_listener(cluster.clone(), event_broker).await;

        let previous_sources = BTreeSet::new();
        task.broadcast_capacity(capacity_score, &open_shard_counts, &previous_sources)
            .await;
        tokio::time::sleep(BROADCAST_INTERVAL_PERIOD * 2).await;

        assert_eq!(update_counter.load(Ordering::Acquire), 1);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: SourceId::from("test-source"),
        };
        let key = make_key(INGESTER_CAPACITY_SCORE_PREFIX, &source_uid);
        let value = cluster.get_self_key_value(&key).await.unwrap();
        let deserialized: IngesterCapacityScore = serde_json::from_str(&value).unwrap();
        assert_eq!(deserialized.capacity_score, 6);
        assert_eq!(deserialized.open_shard_count, 1);
    }
}
