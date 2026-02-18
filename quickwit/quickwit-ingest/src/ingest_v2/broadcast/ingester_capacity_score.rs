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

use anyhow::{Context, Result};
use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::ring_buffer::RingBuffer;
use quickwit_common::shared_consts::INGESTER_CAPACITY_SCORE_PREFIX;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::{IndexUid, NodeId, SourceId, SourceUid};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::{BROADCAST_INTERVAL_PERIOD, make_key, parse_key};
use crate::ingest_v2::state::WeakIngesterState;

pub type OpenShardCounts = Vec<(IndexUid, SourceId, usize)>;

/// The lookback window length is meant to capture readings far enough back in time to give
/// a rough rate of change estimate. At size 6, with broadcast interval of 5 seconds, this would be
/// 30 seconds of readings.
const WAL_CAPACITY_LOOKBACK_WINDOW_LEN: usize = 6;

/// The ring buffer stores one extra element so that `delta()` can compare the newest reading
/// with the one that is exactly `WAL_CAPACITY_LOOKBACK_WINDOW_LEN` steps ago. Otherwise, that
/// reading would be discarded when the next reading is inserted.
const WAL_CAPACITY_READINGS_LEN: usize = WAL_CAPACITY_LOOKBACK_WINDOW_LEN + 1;

struct WalMemoryCapacityTimeSeries {
    memory_capacity: ByteSize,
    readings: RingBuffer<f64, WAL_CAPACITY_READINGS_LEN>,
}

impl WalMemoryCapacityTimeSeries {
    fn new(memory_capacity: ByteSize) -> Self {
        assert!(memory_capacity.as_u64() > 0);
        Self {
            memory_capacity,
            readings: RingBuffer::default(),
        }
    }

    fn record(&mut self, memory_used: ByteSize) {
        let remaining =
            1.0 - (memory_used.as_u64() as f64 / self.memory_capacity.as_u64() as f64);
        self.readings.push_back(remaining.clamp(0.0, 1.0));
    }

    fn current(&self) -> Option<f64> {
        self.readings.last()
    }

    /// How much remaining capacity changed between the oldest and newest readings.
    /// Positive = improving, negative = draining.
    fn delta(&self) -> Option<f64> {
        let current = self.readings.last()?;
        let oldest = self.readings.front()?;
        Some(current - oldest)
    }
}

/// Computes a capacity score from 0 to 10 using a PD controller.
///
/// The score has two components:
///
/// - **P (proportional):** How much WAL capacity remains right now. An ingester with 100% free
///   capacity gets `PROPORTIONAL_WEIGHT` points; 50% gets half; and so on. If remaining capacity
///   drops to `MIN_PERMISSIBLE_CAPACITY` or below, the score is immediately 0.
///
/// - **D (derivative):** Up to `DERIVATIVE_WEIGHT` bonus points based on how fast remaining
///   capacity is changing over the lookback window. A higher drain rate is worse, so we invert it:
///   `drain / MAX_DRAIN_RATE` normalizes the drain to a 0–1 penalty, and subtracting from 1
///   converts it into a 0–1 bonus. Multiplied by `DERIVATIVE_WEIGHT`, a stable node gets the full
///   bonus and a node draining at `MAX_DRAIN_RATE` or faster gets nothing.
///
/// Putting it together: a completely idle ingester scores 10 (8 + 2).
/// One that is full but stable scores ~2. One that is draining rapidly scores less.
/// A score of 0 means the ingester is at or below minimum permissible capacity.
///
/// Below this remaining capacity fraction, the score is immediately 0.
const MIN_PERMISSIBLE_CAPACITY: f64 = 0.05;
/// Weight of the proportional term (max points from P).
const PROPORTIONAL_WEIGHT: f64 = 8.0;
/// Weight of the derivative term (max points from D).
const DERIVATIVE_WEIGHT: f64 = 2.0;
/// The drain rate (as a fraction of total capacity over the lookback window) at which the
/// derivative penalty is fully applied. Drain rates beyond this are clamped.
const MAX_DRAIN_RATE: f64 = 0.10;

fn compute_capacity_score(remaining_capacity: f64, capacity_delta: f64) -> usize {
    if remaining_capacity <= MIN_PERMISSIBLE_CAPACITY {
        return 0;
    }
    let p = PROPORTIONAL_WEIGHT * remaining_capacity;
    let drain = (-capacity_delta).clamp(0.0, MAX_DRAIN_RATE);
    let d = DERIVATIVE_WEIGHT * (1.0 - drain / MAX_DRAIN_RATE);
    (p + d).clamp(0.0, 10.0) as usize
}

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
    wal_capacity_time_series: WalMemoryCapacityTimeSeries,
}

impl BroadcastIngesterCapacityScoreTask {
    pub fn spawn(
        cluster: Cluster,
        weak_state: WeakIngesterState,
        memory_capacity: ByteSize,
    ) -> JoinHandle<()> {
        let mut broadcaster = Self {
            cluster,
            weak_state,
            wal_capacity_time_series: WalMemoryCapacityTimeSeries::new(memory_capacity),
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
        let memory_used = ByteSize::b(usage.memory_used_bytes as u64);
        let open_shard_counts = guard.get_open_shard_counts();

        Ok(Some((memory_used, open_shard_counts)))
    }

    async fn run(&mut self) {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL_PERIOD);
        let mut previous_sources: BTreeSet<SourceUid> = BTreeSet::new();

        loop {
            interval.tick().await;

            let (memory_used, open_shard_counts) = match self.snapshot().await {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => continue,
                Err(error) => {
                    info!("stopping ingester capacity broadcast: {error}");
                    return;
                }
            };

            self.wal_capacity_time_series.record(memory_used);

            let remaining_capacity = self.wal_capacity_time_series.current().unwrap_or(1.0);
            let capacity_delta = self.wal_capacity_time_series.delta().unwrap_or(0.0);
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
    use quickwit_proto::types::ShardId;

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::state::IngesterState;

    fn ts() -> WalMemoryCapacityTimeSeries {
        WalMemoryCapacityTimeSeries::new(ByteSize::b(100))
    }

    /// Helper: record a reading with `used` bytes against the series' fixed capacity.
    fn record(series: &mut WalMemoryCapacityTimeSeries, used: u64) {
        series.record(ByteSize::b(used));
    }

    #[test]
    fn test_wal_memory_capacity_current_after_record() {
        let mut series = WalMemoryCapacityTimeSeries::new(ByteSize::b(256));
        // 192 of 256 used => 25% remaining
        series.record(ByteSize::b(192));
        assert_eq!(series.current(), Some(0.25));

        // 16 of 256 used => 93.75% remaining
        series.record(ByteSize::b(16));
        assert_eq!(series.current(), Some(0.9375));
    }

    #[test]
    fn test_wal_memory_capacity_record_saturates_at_zero() {
        let mut series = ts();
        // 200 used out of 100 capacity => clamped to 0.0
        record(&mut series, 200);
        assert_eq!(series.current(), Some(0.0));
    }

    #[test]
    fn test_wal_memory_capacity_delta_growing() {
        let mut series = ts();
        // oldest: 60 of 100 used => 40% remaining
        record(&mut series, 60);
        // current: 20 of 100 used => 80% remaining
        record(&mut series, 20);
        // delta = 0.80 - 0.40 = 0.40
        assert_eq!(series.delta(), Some(0.40));
    }

    #[test]
    fn test_wal_memory_capacity_delta_shrinking() {
        let mut series = ts();
        // oldest: 20 of 100 used => 80% remaining
        record(&mut series, 20);
        // current: 60 of 100 used => 40% remaining
        record(&mut series, 60);
        // delta = 0.40 - 0.80 = -0.40
        assert_eq!(series.delta(), Some(-0.40));
    }

    #[test]
    fn test_capacity_score_draining_vs_stable() {
        // Node A: capacity draining — usage increases 10, 20, ..., 70 over 7 ticks.
        let mut node_a = ts();
        for used in (10..=70).step_by(10) {
            record(&mut node_a, used);
        }
        let a_remaining = node_a.current().unwrap();
        let a_delta = node_a.delta().unwrap();
        let a_score = compute_capacity_score(a_remaining, a_delta);

        // Node B: steady at 50% usage over 7 ticks.
        let mut node_b = ts();
        for _ in 0..7 {
            record(&mut node_b, 50);
        }
        let b_remaining = node_b.current().unwrap();
        let b_delta = node_b.delta().unwrap();
        let b_score = compute_capacity_score(b_remaining, b_delta);

        // p=2.4, d=0 (max drain) => 2
        assert_eq!(a_score, 2);
        // p=4, d=2 (stable) => 6
        assert_eq!(b_score, 6);
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

        let task = BroadcastIngesterCapacityScoreTask {
            cluster,
            weak_state,
            wal_capacity_time_series: WalMemoryCapacityTimeSeries::new(ByteSize::mb(1)),
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
        let mut task = BroadcastIngesterCapacityScoreTask {
            cluster: cluster.clone(),
            weak_state: state.weak(),
            wal_capacity_time_series: WalMemoryCapacityTimeSeries::new(ByteSize::b(1000)),
        };
        task.wal_capacity_time_series.record(ByteSize::b(500));

        let remaining = task.wal_capacity_time_series.current().unwrap();
        let delta = task.wal_capacity_time_series.delta().unwrap();
        let capacity_score = compute_capacity_score(remaining, delta);
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

    #[test]
    fn test_wal_memory_capacity_delta_spans_lookback_window() {
        let mut series = ts();

        // Fill to exactly the lookback window length (6 readings), all same value.
        for _ in 0..WAL_CAPACITY_LOOKBACK_WINDOW_LEN {
            record(&mut series, 50);
        }
        assert_eq!(series.delta(), Some(0.0));

        // 7th reading fills the ring buffer. Delta spans 6 intervals.
        record(&mut series, 0);
        assert_eq!(series.delta(), Some(0.50));

        // 8th reading evicts the oldest 50-remaining. Delta still spans 6 intervals.
        record(&mut series, 0);
        assert_eq!(series.delta(), Some(0.50));
    }
}
