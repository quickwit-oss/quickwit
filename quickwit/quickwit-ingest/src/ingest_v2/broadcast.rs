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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
use quickwit_common::sorted_iter::{KeyDiff, SortedByKeyIterator};
use quickwit_common::tower::{ConstantRate, Rate};
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{split_queue_id, NodeId, QueueId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::metrics::INGEST_V2_METRICS;
use super::state::WeakIngesterState;
use crate::RateMibPerSec;

const BROADCAST_INTERVAL_PERIOD: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(5)
};

const ONE_MIB: ByteSize = ByteSize::mib(1);

/// Broadcasted information about a primary shard.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    /// Shard ingestion rate in MiB/s.
    /// Short term ingestion rate. It is measured over a short period of time.
    pub short_term_ingestion_rate: RateMibPerSec,
    /// Long term ingestion rate. It is measured over a larger period of time.
    pub long_term_ingestion_rate: RateMibPerSec,
}

impl Serialize for ShardInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!(
            "{}:{}:{}:{}",
            self.shard_id,
            self.shard_state.as_json_str_name(),
            self.short_term_ingestion_rate.0,
            self.long_term_ingestion_rate.0,
        ))
    }
}

impl<'de> Deserialize<'de> for ShardInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        let mut parts = value.split(':');

        let shard_id: ShardId = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .into();

        let shard_state_str = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?;
        let shard_state = ShardState::from_json_str_name(shard_state_str)
            .ok_or_else(|| serde::de::Error::custom("invalid shard state"))?;

        let short_term_ingestion_rate = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<u16>()
            .map(RateMibPerSec)
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        let long_term_ingestion_rate = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<u16>()
            .map(RateMibPerSec)
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        Ok(Self {
            shard_id,
            shard_state,
            short_term_ingestion_rate,
            long_term_ingestion_rate,
        })
    }
}

/// A set of primary shards belonging to the same source.
pub type ShardInfos = BTreeSet<ShardInfo>;

/// Lists ALL the primary shards hosted by a SINGLE ingester, grouped by source.
#[derive(Debug, Default, Eq, PartialEq)]
struct LocalShardsSnapshot {
    per_source_shard_infos: BTreeMap<SourceUid, ShardInfos>,
}

#[derive(Debug)]
enum ShardInfosChange<'a> {
    Updated {
        source_uid: &'a SourceUid,
        shard_infos: &'a ShardInfos,
    },
    Removed {
        source_uid: &'a SourceUid,
    },
}

impl LocalShardsSnapshot {
    pub fn diff<'a>(&'a self, other: &'a Self) -> impl Iterator<Item = ShardInfosChange<'a>> + 'a {
        self.per_source_shard_infos
            .iter()
            .diff_by_key(other.per_source_shard_infos.iter())
            .filter_map(|key_diff| match key_diff {
                KeyDiff::Added(source_uid, shard_infos) => Some(ShardInfosChange::Updated {
                    source_uid,
                    shard_infos,
                }),
                KeyDiff::Unchanged(source_uid, previous_shard_infos, new_shard_infos) => {
                    if previous_shard_infos != new_shard_infos {
                        Some(ShardInfosChange::Updated {
                            source_uid,
                            shard_infos: new_shard_infos,
                        })
                    } else {
                        None
                    }
                }
                KeyDiff::Removed(source_uid, _shard_infos) => {
                    Some(ShardInfosChange::Removed { source_uid })
                }
            })
    }
}

/// Takes a snapshot of the primary shards hosted by the ingester at regular intervals and
/// broadcasts it to other nodes via Chitchat.
pub(super) struct BroadcastLocalShardsTask {
    cluster: Cluster,
    weak_state: WeakIngesterState,
    shard_throughput_time_series_map: ShardThroughputTimeSeriesMap,
}

const SHARD_THROUGHPUT_LONG_TERM_WINDOW_LEN: usize = 12;

#[derive(Default)]
struct ShardThroughputTimeSeriesMap {
    shard_time_series: HashMap<(SourceUid, ShardId), ShardThroughputTimeSeries>,
}

impl ShardThroughputTimeSeriesMap {
    // Records a list of shard throughputs.
    //
    // A new time series is created for each new shard_ids.
    // If a shard_id had a time series, and it is not present in the
    // `shard_throughput`, the time series will be removed.
    #[allow(clippy::mutable_key_type)]
    pub fn record_shard_throughputs(
        &mut self,
        shard_throughputs: HashMap<(SourceUid, ShardId), (ShardState, ConstantRate)>,
    ) {
        self.shard_time_series
            .retain(|key, _| shard_throughputs.contains_key(key));
        for ((source_uid, shard_id), (shard_state, throughput)) in shard_throughputs {
            let throughput_measurement = throughput.rescale(Duration::from_secs(1)).work_bytes();
            let shard_time_series = self
                .shard_time_series
                .entry((source_uid.clone(), shard_id.clone()))
                .or_default();
            shard_time_series.shard_state = shard_state;
            shard_time_series.record(throughput_measurement);
        }
    }

    pub fn get_per_source_shard_infos(&self) -> BTreeMap<SourceUid, ShardInfos> {
        let mut per_source_shard_infos: BTreeMap<SourceUid, ShardInfos> = BTreeMap::new();
        for ((source_uid, shard_id), shard_time_series) in self.shard_time_series.iter() {
            let shard_state = shard_time_series.shard_state;
            let short_term_ingestion_rate_mib_per_sec_u64: u64 =
                shard_time_series.last().as_u64().div_ceil(ONE_MIB.as_u64());
            let long_term_ingestion_rate_mib_per_sec_u64: u64 = shard_time_series
                .average()
                .as_u64()
                .div_ceil(ONE_MIB.as_u64());
            INGEST_V2_METRICS
                .shard_st_throughput_mib
                .observe(short_term_ingestion_rate_mib_per_sec_u64 as f64);
            INGEST_V2_METRICS
                .shard_lt_throughput_mib
                .observe(long_term_ingestion_rate_mib_per_sec_u64 as f64);

            let short_term_ingestion_rate =
                RateMibPerSec(short_term_ingestion_rate_mib_per_sec_u64 as u16);
            let long_term_ingestion_rate =
                RateMibPerSec(long_term_ingestion_rate_mib_per_sec_u64 as u16);
            let shard_info = ShardInfo {
                shard_id: shard_id.clone(),
                shard_state,
                short_term_ingestion_rate,
                long_term_ingestion_rate,
            };

            per_source_shard_infos
                .entry(source_uid.clone())
                .or_default()
                .insert(shard_info);
        }
        per_source_shard_infos
    }
}

#[derive(Default)]
struct ShardThroughputTimeSeries {
    shard_state: ShardState,
    measurements: [ByteSize; SHARD_THROUGHPUT_LONG_TERM_WINDOW_LEN],
    len: usize,
}

impl ShardThroughputTimeSeries {
    fn last(&self) -> ByteSize {
        self.measurements.last().copied().unwrap_or_default()
    }

    fn average(&self) -> ByteSize {
        if self.len == 0 {
            return ByteSize::default();
        }
        let sum = self
            .measurements
            .iter()
            .rev()
            .take(self.len)
            .map(ByteSize::as_u64)
            .sum::<u64>();
        ByteSize::b(sum / self.len as u64)
    }

    fn record(&mut self, new_throughput_measurement: ByteSize) {
        self.len = (self.len + 1).min(SHARD_THROUGHPUT_LONG_TERM_WINDOW_LEN);
        self.measurements.rotate_left(1);
        let Some(last_measurement) = self.measurements.last_mut() else {
            return;
        };
        *last_measurement = new_throughput_measurement;
    }
}

impl BroadcastLocalShardsTask {
    pub fn spawn(cluster: Cluster, weak_state: WeakIngesterState) -> JoinHandle<()> {
        let mut broadcaster = Self {
            cluster,
            weak_state,
            shard_throughput_time_series_map: Default::default(),
        };
        tokio::spawn(async move { broadcaster.run().await })
    }

    async fn snapshot_local_shards(&mut self) -> Option<LocalShardsSnapshot> {
        let state = self.weak_state.upgrade()?;

        let Ok(mut state_guard) = state.lock_partially().await else {
            return Some(LocalShardsSnapshot::default());
        };

        let queue_ids: Vec<(QueueId, ShardState)> = state_guard
            .shards
            .iter()
            .filter_map(|(queue_id, shard)| {
                if shard.is_advertisable && !shard.is_replica() {
                    Some((queue_id.clone(), shard.shard_state))
                } else {
                    None
                }
            })
            .collect();

        let mut num_open_shards = 0;
        let mut num_closed_shards = 0;

        #[allow(clippy::mutable_key_type)]
        let ingestion_rates: HashMap<(SourceUid, ShardId), (ShardState, ConstantRate)> = queue_ids
            .iter()
            .flat_map(|(queue_id, shard_state)| {
                let Some((_rate_limiter, rate_meter)) = state_guard.rate_trackers.get_mut(queue_id)
                else {
                    warn!(
                        "rate limiter `{queue_id}` not found: this should never happen, please \
                         report"
                    );
                    return None;
                };
                let (index_uid, source_id, shard_id) = split_queue_id(queue_id)?;
                let source_uid = SourceUid {
                    index_uid,
                    source_id,
                };
                // Shard ingestion rate in MiB/s.
                Some(((source_uid, shard_id), (*shard_state, rate_meter.harvest())))
            })
            .collect();

        self.shard_throughput_time_series_map
            .record_shard_throughputs(ingestion_rates);

        let per_source_shard_infos = self
            .shard_throughput_time_series_map
            .get_per_source_shard_infos();

        for shard_infos in per_source_shard_infos.values() {
            for shard_info in shard_infos {
                match shard_info.shard_state {
                    ShardState::Open => num_open_shards += 1,
                    ShardState::Closed => num_closed_shards += 1,
                    ShardState::Unavailable | ShardState::Unspecified => {}
                }
            }
        }
        INGEST_V2_METRICS.open_shards.set(num_open_shards as i64);
        INGEST_V2_METRICS
            .closed_shards
            .set(num_closed_shards as i64);

        let snapshot = LocalShardsSnapshot {
            per_source_shard_infos,
        };
        Some(snapshot)
    }

    async fn broadcast_local_shards(
        &self,
        previous_snapshot: &LocalShardsSnapshot,
        new_snapshot: &LocalShardsSnapshot,
    ) {
        for change in previous_snapshot.diff(new_snapshot) {
            match change {
                ShardInfosChange::Updated {
                    source_uid,
                    shard_infos,
                } => {
                    let key = make_key(source_uid);
                    let value = serde_json::to_string(&shard_infos)
                        .expect("`ShardInfos` should be JSON serializable");
                    self.cluster.set_self_key_value(key, value).await;
                }
                ShardInfosChange::Removed { source_uid } => {
                    let key = make_key(source_uid);
                    self.cluster.remove_self_key(&key).await;
                }
            }
        }
    }

    async fn run(&mut self) {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL_PERIOD);
        let mut previous_snapshot = LocalShardsSnapshot::default();

        loop {
            interval.tick().await;

            let Some(new_snapshot) = self.snapshot_local_shards().await else {
                // The state has been dropped, we can stop the task.
                debug!("stopping local shards broadcast task");
                return;
            };
            self.broadcast_local_shards(&previous_snapshot, &new_snapshot)
                .await;

            previous_snapshot = new_snapshot;
        }
    }
}

fn make_key(source_uid: &SourceUid) -> String {
    format!(
        "{INGESTER_PRIMARY_SHARDS_PREFIX}{}:{}",
        source_uid.index_uid, source_uid.source_id
    )
}

fn parse_key(key: &str) -> Option<SourceUid> {
    let (index_uid_str, source_id_str) = key.rsplit_once(':')?;

    Some(SourceUid {
        index_uid: index_uid_str.parse().ok()?,
        source_id: source_id_str.to_string(),
    })
}

#[derive(Debug, Clone)]
pub struct LocalShardsUpdate {
    pub leader_id: NodeId,
    pub source_uid: SourceUid,
    pub shard_infos: ShardInfos,
    pub is_deletion: bool,
}

impl Event for LocalShardsUpdate {}

pub async fn setup_local_shards_update_listener(
    cluster: Cluster,
    event_broker: EventBroker,
) -> ListenerHandle {
    cluster
        .subscribe(INGESTER_PRIMARY_SHARDS_PREFIX, move |event| {
            let Some(source_uid) = parse_key(event.key) else {
                warn!("failed to parse source UID `{}`", event.key);
                return;
            };
            let Ok(shard_infos) = serde_json::from_str::<ShardInfos>(event.value) else {
                warn!("failed to parse shard infos `{}`", event.value);
                return;
            };
            let leader_id: NodeId = event.node.node_id.clone().into();

            let local_shards_update = LocalShardsUpdate {
                leader_id,
                source_uid,
                shard_infos,
                is_deletion: false,
            };
            event_broker.publish(local_shards_update);
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    use quickwit_cluster::{create_cluster_for_test, ChannelTransport};
    use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{queue_id, IndexUid, Position};

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::rate_meter::RateMeter;
    use crate::ingest_v2::state::IngesterState;

    #[test]
    fn test_shard_info_serde() {
        let shard_info = ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(42),
            long_term_ingestion_rate: RateMibPerSec(40),
        };
        let serialized = serde_json::to_string(&shard_info).unwrap();
        assert_eq!(serialized, r#""00000000000000000001:open:42:40""#);

        let deserialized = serde_json::from_str::<ShardInfo>(&serialized).unwrap();
        assert_eq!(deserialized, shard_info);
    }

    #[test]
    fn test_local_shards_snapshot_diff() {
        let previous_snapshot = LocalShardsSnapshot::default();
        let current_snapshot = LocalShardsSnapshot::default();
        let num_changes = previous_snapshot.diff(&current_snapshot).count();
        assert_eq!(num_changes, 0);

        let previous_snapshot = LocalShardsSnapshot::default();
        let index_uid = IndexUid::from_str("test-index:00000000000000000000000000").unwrap();
        let current_snapshot = LocalShardsSnapshot {
            per_source_shard_infos: vec![(
                SourceUid {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                },
                vec![ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Open,
                    short_term_ingestion_rate: RateMibPerSec(42),
                    long_term_ingestion_rate: RateMibPerSec(42),
                }]
                .into_iter()
                .collect(),
            )]
            .into_iter()
            .collect(),
        };
        let changes = previous_snapshot
            .diff(&current_snapshot)
            .collect::<Vec<_>>();
        assert_eq!(changes.len(), 1);

        let ShardInfosChange::Updated {
            source_uid,
            shard_infos,
        } = &changes[0]
        else {
            panic!(
                "expected `ShardInfosChange::Updated` variant, got {:?}",
                changes[0]
            );
        };
        assert_eq!(source_uid.index_uid, index_uid);
        assert_eq!(source_uid.source_id, "test-source");
        assert_eq!(shard_infos.len(), 1);

        let num_changes = current_snapshot.diff(&current_snapshot).count();
        assert_eq!(num_changes, 0);

        let previous_snapshot = current_snapshot;
        let current_snapshot = LocalShardsSnapshot {
            per_source_shard_infos: vec![(
                SourceUid {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                },
                vec![ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    short_term_ingestion_rate: RateMibPerSec(42),
                    long_term_ingestion_rate: RateMibPerSec(42),
                }]
                .into_iter()
                .collect(),
            )]
            .into_iter()
            .collect(),
        };
        let changes = previous_snapshot
            .diff(&current_snapshot)
            .collect::<Vec<_>>();
        assert_eq!(changes.len(), 1);

        let ShardInfosChange::Updated {
            source_uid,
            shard_infos,
        } = &changes[0]
        else {
            panic!(
                "expected `ShardInfosChange::Updated` variant, got {:?}",
                changes[0]
            );
        };
        assert_eq!(source_uid.index_uid, index_uid);
        assert_eq!(source_uid.source_id, "test-source");
        assert_eq!(shard_infos.len(), 1);

        let previous_snapshot = current_snapshot;
        let current_snapshot = LocalShardsSnapshot::default();

        let changes = previous_snapshot
            .diff(&current_snapshot)
            .collect::<Vec<_>>();
        assert_eq!(changes.len(), 1);

        let ShardInfosChange::Removed { source_uid } = &changes[0] else {
            panic!(
                "expected `ShardInfosChange::Removed` variant, got {:?}",
                changes[0]
            );
        };
        assert_eq!(source_uid.index_uid, index_uid);
        assert_eq!(source_uid.source_id, "test-source");
    }

    #[tokio::test]
    async fn test_broadcast_local_shards_task() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let weak_state = state.weak();
        let mut task = BroadcastLocalShardsTask {
            cluster,
            weak_state,
            shard_throughput_time_series_map: Default::default(),
        };
        let previous_snapshot = task.snapshot_local_shards().await.unwrap();
        assert!(previous_snapshot.per_source_shard_infos.is_empty());

        let mut state_guard = state.lock_partially().await.unwrap();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_00 = queue_id(&index_uid, "test-source", &ShardId::from(0));
        let shard_00 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
        );
        state_guard.shards.insert(queue_id_00.clone(), shard_00);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let mut shard_01 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
        );
        shard_01.is_advertisable = true;
        state_guard.shards.insert(queue_id_01.clone(), shard_01);

        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));
        let mut shard_02 = IngesterShard::new_replica(
            NodeId::from("test-leader"),
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        shard_02.is_advertisable = true;
        state_guard.shards.insert(queue_id_02.clone(), shard_02);

        for queue_id in [queue_id_00, queue_id_01, queue_id_02] {
            let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
            let rate_meter = RateMeter::default();

            state_guard
                .rate_trackers
                .insert(queue_id, (rate_limiter, rate_meter));
        }
        drop(state_guard);

        let new_snapshot = task.snapshot_local_shards().await.unwrap();
        assert_eq!(new_snapshot.per_source_shard_infos.len(), 1);

        task.broadcast_local_shards(&previous_snapshot, &new_snapshot)
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let key = format!(
            "{INGESTER_PRIMARY_SHARDS_PREFIX}{}:{}",
            index_uid, "test-source"
        );
        task.cluster.get_self_key_value(&key).await.unwrap();

        task.broadcast_local_shards(&new_snapshot, &previous_snapshot)
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = task.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
    }

    #[test]
    fn test_make_key() {
        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        let key = make_key(&source_uid);
        assert_eq!(
            key,
            "ingester.primary_shards:test-index:00000000000000000000000000:test-source"
        );
    }

    #[test]
    fn test_parse_key() {
        let key = "test-index:00000000000000000000000000:test-source";
        let source_uid = parse_key(key).unwrap();
        assert_eq!(
            &source_uid.index_uid.to_string(),
            "test-index:00000000000000000000000000"
        );
        assert_eq!(source_uid.source_id, "test-source".to_string());
    }

    #[tokio::test]
    async fn test_local_shards_update_listener() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let event_broker = EventBroker::default();

        let local_shards_update_counter = Arc::new(AtomicUsize::new(0));
        let local_shards_update_counter_clone = local_shards_update_counter.clone();
        let index_uid = IndexUid::from_str("test-index:00000000000000000000000000").unwrap();

        let index_uid_clone = index_uid.clone();
        event_broker
            .subscribe(move |event: LocalShardsUpdate| {
                local_shards_update_counter_clone.fetch_add(1, Ordering::Release);

                assert_eq!(event.source_uid.index_uid, index_uid_clone);
                assert_eq!(event.source_uid.source_id, "test-source");
                assert_eq!(event.shard_infos.len(), 1);

                let shard_info = event.shard_infos.iter().next().unwrap();
                assert_eq!(shard_info.shard_id, ShardId::from(1));
                assert_eq!(shard_info.shard_state, ShardState::Open);
                assert_eq!(shard_info.short_term_ingestion_rate, 42u16);
            })
            .forever();

        setup_local_shards_update_listener(cluster.clone(), event_broker.clone())
            .await
            .forever();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
        };
        let key = make_key(&source_uid);
        let value = serde_json::to_string(&vec![ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: RateMibPerSec(42),
            long_term_ingestion_rate: RateMibPerSec(42),
        }])
        .unwrap();

        cluster.set_self_key_value(key, value).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(local_shards_update_counter.load(Ordering::Acquire), 1);
    }

    #[test]
    fn test_shard_throughput_time_series() {
        let mut time_series = ShardThroughputTimeSeries::default();
        assert_eq!(time_series.last(), ByteSize::mb(0));
        assert_eq!(time_series.average(), ByteSize::mb(0));
        time_series.record(ByteSize::mb(2));
        assert_eq!(time_series.last(), ByteSize::mb(2));
        assert_eq!(time_series.average(), ByteSize::mb(2));
        time_series.record(ByteSize::mb(1));
        assert_eq!(time_series.last(), ByteSize::mb(1));
        assert_eq!(time_series.average(), ByteSize::kb(1500));
        time_series.record(ByteSize::mb(3));
        assert_eq!(time_series.last(), ByteSize::mb(3));
        assert_eq!(time_series.average(), ByteSize::mb(2));
        for _ in 0..SHARD_THROUGHPUT_LONG_TERM_WINDOW_LEN {
            time_series.record(ByteSize::mb(4));
            assert_eq!(time_series.last(), ByteSize::mb(4));
        }

        assert_eq!(time_series.last(), ByteSize::mb(4));
    }
}
