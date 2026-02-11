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

use std::collections::VecDeque;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_AFFINITY_PREFIX;
use quickwit_proto::types::{IndexUid, NodeId, SourceId};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use super::BROADCAST_INTERVAL_PERIOD;
use crate::ingest_v2::state::WeakIngesterState;

pub type OpenShardCounts = Vec<(IndexUid, SourceId, usize)>;

const WAL_CAPACITY_LOOKBACK_WINDOW_LEN: usize = 6;

struct WalCapacityTimeSeries {
    wal_capacity: ByteSize,
    readings: VecDeque<ByteSize>,
}

impl WalCapacityTimeSeries {
    fn new(wal_capacity: ByteSize) -> Self {
        assert!(
            wal_capacity.as_u64() > 0,
            "WAL capacity must be greater than zero"
        );
        Self {
            wal_capacity,
            readings: VecDeque::new(),
        }
    }

    fn record(&mut self, wal_used: ByteSize) {
        let remaining = ByteSize::b(self.wal_capacity.as_u64().saturating_sub(wal_used.as_u64()));
        self.readings.push_front(remaining);
    }

    /// Returns the most recent remaining capacity as a fraction of total WAL capacity,
    /// or `None` if no readings have been recorded yet.
    fn current(&self) -> Option<f64> {
        self.readings
            .front()
            .map(|b| self.as_capacity_usage_pct(*b))
    }

    fn as_capacity_usage_pct(&self, bytes: ByteSize) -> f64 {
        bytes.as_u64() as f64 / self.wal_capacity.as_u64() as f64
    }

    /// How much remaining capacity changed between the oldest and newest readings.
    /// Positive = improving, negative = draining.
    /// At most `WAL_CAPACITY_LOOKBACK_WINDOW_LEN` readings are kept.
    fn delta(&mut self) -> Option<f64> {
        if self.readings.is_empty() {
            return None;
        }
        let oldest = if self.readings.len() > WAL_CAPACITY_LOOKBACK_WINDOW_LEN {
            self.readings.pop_back().unwrap()
        } else {
            *self.readings.back().unwrap()
        };
        let current = *self.readings.front().unwrap();
        Some(self.as_capacity_usage_pct(current) - self.as_capacity_usage_pct(oldest))
    }
}

/// Computes an affinity score from 0 to 100 using a simple PI controller.
///
/// The score has two components:
///
/// - **P (proportional):** How much WAL capacity remains right now. An ingester with 100% free
///   capacity gets 80 points; 50% gets 40; and so on. If remaining capacity drops to 5% or below,
///   the score is immediately 0.
///
/// - **I (integral):** A stability bonus worth up to 20 points. If remaining capacity hasn't
///   changed between the oldest and newest readings, the full 20 points are awarded. As capacity
///   drains faster, the bonus shrinks linearly toward 0. A delta of -10% of total capacity (or
///   worse) zeroes it out.
///
/// Putting it together: a completely idle ingester scores 100 (80 + 20).
/// One that is full but stable scores ~24. One that is draining rapidly scores less.
fn compute_affinity_score(remaining_capacity: f64, capacity_delta: f64) -> u32 {
    if remaining_capacity <= 0.05 {
        return 0;
    }
    let p = 80.0 * remaining_capacity;
    let drain = (-capacity_delta).clamp(0.0, 0.10);
    let i = 20.0 * (1.0 - drain / 0.10);
    (p + i).clamp(0.0, 100.0) as u32
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngesterAffinity {
    pub affinity_score: u32,
    pub open_shard_counts: OpenShardCounts,
}

/// Periodically snapshots the ingester's WAL usage and open shard counts, computes
/// an affinity score, and broadcasts it to other nodes via Chitchat.
pub struct BroadcastIngesterAffinityTask {
    cluster: Cluster,
    weak_state: WeakIngesterState,
    wal_capacity_time_series: WalCapacityTimeSeries,
}

impl BroadcastIngesterAffinityTask {
    pub fn spawn(
        cluster: Cluster,
        weak_state: WeakIngesterState,
        wal_capacity: ByteSize,
    ) -> JoinHandle<()> {
        let mut broadcaster = Self {
            cluster,
            weak_state,
            wal_capacity_time_series: WalCapacityTimeSeries::new(wal_capacity),
        };
        tokio::spawn(async move { broadcaster.run().await })
    }

    async fn snapshot(&self) -> Result<(ByteSize, OpenShardCounts)> {
        let state = self
            .weak_state
            .upgrade()
            .context("ingester state has been dropped")?;

        let wal_lock = state.mrecordlog();
        let wal_guard = wal_lock.read().await;
        let wal = wal_guard.as_ref().context("WAL is not initialized")?;
        let wal_used = ByteSize::b(wal.resource_usage().disk_used_bytes as u64);
        drop(wal_guard);

        let guard = state
            .lock_partially()
            .await
            .map_err(|_| anyhow::anyhow!("failed to acquire ingester state lock"))?;
        let open_shard_counts = guard.get_open_shard_counts();

        Ok((wal_used, open_shard_counts))
    }

    async fn run(&mut self) {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL_PERIOD);

        loop {
            interval.tick().await;

            let (wal_used, open_shard_counts) = match self.snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    error!("failed to snapshot ingester state: {error}");
                    return;
                }
            };

            self.wal_capacity_time_series.record(wal_used);

            let remaining_capacity = self.wal_capacity_time_series.current().unwrap_or(1.0);
            let capacity_delta = self.wal_capacity_time_series.delta().unwrap_or(0.0);

            let affinity = IngesterAffinity {
                affinity_score: compute_affinity_score(remaining_capacity, capacity_delta),
                open_shard_counts,
            };

            self.broadcast_affinity(&affinity).await;
        }
    }

    async fn broadcast_affinity(&self, affinity: &IngesterAffinity) {
        let value = serde_json::to_string(affinity)
            .expect("`IngesterAffinity` should be JSON serializable");
        self.cluster
            .set_self_key_value(INGESTER_AFFINITY_PREFIX, value)
            .await;
    }
}

#[derive(Debug, Clone)]
pub struct IngesterAffinityUpdate {
    pub node_id: NodeId,
    pub affinity_score: u32,
    pub open_shard_counts: OpenShardCounts,
}

impl Event for IngesterAffinityUpdate {}

pub async fn setup_ingester_affinity_update_listener(
    cluster: Cluster,
    event_broker: EventBroker,
) -> ListenerHandle {
    cluster
        .subscribe(INGESTER_AFFINITY_PREFIX, move |event| {
            let Ok(affinity) = serde_json::from_str::<IngesterAffinity>(event.value) else {
                warn!("failed to parse ingester affinity `{}`", event.value);
                return;
            };
            let node_id: NodeId = event.node.node_id.clone().into();
            event_broker.publish(IngesterAffinityUpdate {
                node_id,
                affinity_score: affinity.affinity_score,
                open_shard_counts: affinity.open_shard_counts,
            });
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_proto::types::ShardId;

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::state::IngesterState;

    fn ts(capacity_bytes: u64) -> WalCapacityTimeSeries {
        WalCapacityTimeSeries::new(ByteSize::b(capacity_bytes))
    }

    #[test]
    fn test_wal_capacity_empty() {
        let mut series = ts(100);
        assert!(series.current().is_none());
        assert!(series.delta().is_none());
    }

    #[test]
    fn test_wal_capacity_current_after_record() {
        let mut series = ts(100);
        // 30 bytes used => 70 remaining => 0.70
        series.record(ByteSize::b(30));
        assert_eq!(series.current(), Some(0.70));

        // 90 bytes used => 10 remaining => 0.10
        series.record(ByteSize::b(90));
        assert_eq!(series.current(), Some(0.10));
    }

    #[test]
    fn test_wal_capacity_record_saturates_at_zero() {
        let mut series = ts(100);
        series.record(ByteSize::b(200));
        assert_eq!(series.current(), Some(0.0));
    }

    #[test]
    fn test_wal_capacity_delta_single_reading() {
        let mut series = ts(100);
        series.record(ByteSize::b(50));
        // current == oldest => delta is 0
        assert_eq!(series.delta(), Some(0.0));
    }

    #[test]
    fn test_wal_capacity_delta_growing() {
        let mut series = ts(100);
        // oldest: 60 used => 40 remaining
        series.record(ByteSize::b(60));
        // current: 20 used => 80 remaining
        series.record(ByteSize::b(20));
        // delta = 0.80 - 0.40 = 0.40
        assert_eq!(series.delta(), Some(0.40));
    }

    #[test]
    fn test_wal_capacity_delta_shrinking() {
        let mut series = ts(100);
        // oldest: 20 used => 80 remaining
        series.record(ByteSize::b(20));
        // current: 60 used => 40 remaining
        series.record(ByteSize::b(60));
        // delta = 0.40 - 0.80 = -0.40
        assert_eq!(series.delta(), Some(-0.40));
    }

    #[test]
    fn test_affinity_score_draining_vs_stable() {
        // Node A: capacity draining — usage increases 10, 20, ..., 70 over 7 ticks.
        let mut node_a = ts(100);
        for used in (10..=70).step_by(10) {
            node_a.record(ByteSize::b(used));
        }
        // After 7 readings + delta pop: current = 0.30, delta = -0.50
        let a_remaining = node_a.current().unwrap();
        let a_delta = node_a.delta().unwrap();
        let a_score = compute_affinity_score(a_remaining, a_delta);

        // Node B: steady at 50% usage over 7 ticks.
        let mut node_b = ts(100);
        for _ in 0..7 {
            node_b.record(ByteSize::b(50));
        }
        // After 7 readings + delta pop: current = 0.50, delta = 0.0
        let b_remaining = node_b.current().unwrap();
        let b_delta = node_b.delta().unwrap();
        let b_score = compute_affinity_score(b_remaining, b_delta);

        // p=24, i=0 (max drain) => 24
        assert_eq!(a_score, 24);
        // p=40, i=20 (stable) => 60
        assert_eq!(b_score, 60);
        assert!(b_score > a_score);
    }

    #[tokio::test]
    async fn test_snapshot_state_dropped() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let weak_state = state.weak();
        drop(state);

        let task = BroadcastIngesterAffinityTask {
            cluster,
            weak_state,
            wal_capacity_time_series: WalCapacityTimeSeries::new(ByteSize::mib(256)),
        };
        assert!(task.snapshot().await.is_err());
    }

    #[tokio::test]
    async fn test_broadcast_ingester_affinity() {
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
        .build();
        state_guard.shards.insert(shard.queue_id(), shard);
        let open_shard_counts = state_guard.get_open_shard_counts();
        drop(state_guard);

        // Simulate 500 of 1000 bytes used => 50% remaining, 0 delta => score = 60.0
        let mut task = BroadcastIngesterAffinityTask {
            cluster: cluster.clone(),
            weak_state: state.weak(),
            wal_capacity_time_series: WalCapacityTimeSeries::new(ByteSize::b(1000)),
        };
        task.wal_capacity_time_series.record(ByteSize::b(500));

        let remaining = task.wal_capacity_time_series.current().unwrap();
        let delta = task.wal_capacity_time_series.delta().unwrap();
        let affinity = IngesterAffinity {
            affinity_score: compute_affinity_score(remaining, delta),
            open_shard_counts,
        };
        assert_eq!(affinity.affinity_score, 60);

        let update_counter = Arc::new(AtomicUsize::new(0));
        let update_counter_clone = update_counter.clone();
        let index_uid_clone = index_uid.clone();
        let _sub = event_broker.subscribe(move |event: IngesterAffinityUpdate| {
            update_counter_clone.fetch_add(1, Ordering::Release);
            assert_eq!(event.affinity_score, 60);
            assert_eq!(event.open_shard_counts.len(), 1);
            assert_eq!(event.open_shard_counts[0].0, index_uid_clone);
            assert_eq!(event.open_shard_counts[0].1, "test-source");
            assert_eq!(event.open_shard_counts[0].2, 1);
        });

        let _listener =
            setup_ingester_affinity_update_listener(cluster.clone(), event_broker).await;

        task.broadcast_affinity(&affinity).await;
        tokio::time::sleep(BROADCAST_INTERVAL_PERIOD * 2).await;

        assert_eq!(update_counter.load(Ordering::Acquire), 1);

        let value = cluster
            .get_self_key_value(INGESTER_AFFINITY_PREFIX)
            .await
            .unwrap();
        let deserialized: IngesterAffinity = serde_json::from_str(&value).unwrap();
        assert_eq!(deserialized.affinity_score, 60);
        assert_eq!(deserialized.open_shard_counts.len(), 1);
    }

    #[test]
    fn test_wal_capacity_delta_pops_oldest_beyond_window() {
        let mut series = ts(100);

        // Fill to exactly the window length (6 readings).
        // All use 50 bytes => 50 remaining each.
        for _ in 0..WAL_CAPACITY_LOOKBACK_WINDOW_LEN {
            series.record(ByteSize::b(50));
        }
        assert_eq!(series.readings.len(), WAL_CAPACITY_LOOKBACK_WINDOW_LEN);
        assert_eq!(series.delta(), Some(0.0));
        // At exactly window len, delta does NOT pop.
        assert_eq!(series.readings.len(), WAL_CAPACITY_LOOKBACK_WINDOW_LEN);

        // Push one more (7th): 0 used => 100 remaining => current = 1.0
        series.record(ByteSize::b(0));
        assert_eq!(series.readings.len(), WAL_CAPACITY_LOOKBACK_WINDOW_LEN + 1);

        // delta should pop the oldest (0.50) and return 1.0 - 0.50 = 0.50
        assert_eq!(series.delta(), Some(0.50));
        // After pop, back to window length.
        assert_eq!(series.readings.len(), WAL_CAPACITY_LOOKBACK_WINDOW_LEN);
    }
}
