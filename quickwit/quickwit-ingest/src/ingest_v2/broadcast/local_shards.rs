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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ClusterNode, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
use quickwit_common::sorted_iter::{KeyDiff, SortedByKeyIterator};
use quickwit_config::service::QuickwitService;
use quickwit_proto::control_plane;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, warn};

use super::{BROADCAST_INTERVAL_PERIOD, make_key, parse_key};
use crate::RateMibPerSec;
use crate::ingest_v2::shard_readings::{ShardReadingsBySource, ShardThroughputReading};
use crate::ingest_v2::state::WeakIngesterState;

/// Broadcasted information about a shard.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    /// Shard ingestion rate in bytes/s, serialized as rounded-up MiB/s for gossip.
    /// Short term ingestion rate. It is measured over a short period of time.
    pub short_term_ingestion_rate: ByteSize,
    /// Long term ingestion rate. It is measured over a larger period of time.
    pub long_term_ingestion_rate: ByteSize,
}

impl Serialize for ShardInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!(
            "{}:{}:{}:{}",
            self.shard_id,
            self.shard_state.as_json_str_name(),
            RateMibPerSec::from(self.short_term_ingestion_rate).0,
            RateMibPerSec::from(self.long_term_ingestion_rate).0,
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
            .map(ByteSize::from)
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        let long_term_ingestion_rate = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<u16>()
            .map(RateMibPerSec)
            .map(ByteSize::from)
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        Ok(Self {
            shard_id,
            shard_state,
            short_term_ingestion_rate,
            long_term_ingestion_rate,
        })
    }
}

impl From<&ShardThroughputReading> for ShardInfo {
    fn from(reading: &ShardThroughputReading) -> Self {
        Self {
            shard_id: reading.shard_id.clone(),
            shard_state: reading.shard_state,
            short_term_ingestion_rate: reading.short_term_ingestion_rate,
            long_term_ingestion_rate: reading.long_term_ingestion_rate,
        }
    }
}

impl From<&control_plane::ShardInfo> for ShardInfo {
    fn from(shard_info: &control_plane::ShardInfo) -> Self {
        Self {
            shard_id: shard_info.shard_id().clone(),
            shard_state: shard_info.shard_state(),
            short_term_ingestion_rate: ByteSize::b(
                shard_info.short_term_ingestion_rate_bytes_per_sec,
            ),
            long_term_ingestion_rate: ByteSize::b(
                shard_info.long_term_ingestion_rate_bytes_per_sec,
            ),
        }
    }
}

/// A set of shards belonging to the same source.
pub type ShardInfos = BTreeSet<ShardInfo>;

/// Lists all the shards hosted by a single ingester, grouped by source.
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

impl From<&ShardReadingsBySource> for LocalShardsSnapshot {
    fn from(readings: &ShardReadingsBySource) -> Self {
        let mut per_source_shard_infos = BTreeMap::new();
        for (source_uid, shard_readings) in &readings.readings_by_source {
            let shard_infos: ShardInfos = shard_readings.iter().map(ShardInfo::from).collect();
            per_source_shard_infos.insert(source_uid.clone(), shard_infos);
        }
        Self {
            per_source_shard_infos,
        }
    }
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

/// Takes a snapshot of the shards hosted by the ingester at regular intervals and
/// broadcasts it to other nodes via Chitchat.
pub struct BroadcastLocalShardsTask {
    cluster: Cluster,
    weak_state: WeakIngesterState,
    local_shards_rx: watch::Receiver<Option<Arc<ShardReadingsBySource>>>,
    /// Snapshot broadcast on the previous tick. Carried across iterations so
    /// we can diff against the new snapshot and only broadcast changes.
    previous_snapshot: LocalShardsSnapshot,
}

impl BroadcastLocalShardsTask {
    pub fn spawn(
        cluster: Cluster,
        weak_state: WeakIngesterState,
        local_shards_rx: watch::Receiver<Option<Arc<ShardReadingsBySource>>>,
    ) -> JoinHandle<()> {
        let broadcaster = Self {
            cluster,
            weak_state,
            local_shards_rx,
            previous_snapshot: LocalShardsSnapshot::default(),
        };
        tokio::spawn(broadcaster.run())
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
                    let key = make_key(INGESTER_SHARDS_PREFIX, source_uid);
                    let value = serde_json::to_string(&shard_infos)
                        .expect("`ShardInfos` should be JSON serializable");
                    self.cluster.set_self_key_value(key, value).await;
                }
                ShardInfosChange::Removed { source_uid } => {
                    let key = make_key(INGESTER_SHARDS_PREFIX, source_uid);
                    self.cluster.remove_self_key(&key).await;
                }
            }
        }
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(BROADCAST_INTERVAL_PERIOD);

        loop {
            interval.tick().await;

            let all_indexers_enable_shard_scaling_v2 = self
                .cluster
                .all_service_nodes_satisfy(
                    QuickwitService::Indexer,
                    ClusterNode::enable_shard_scaling_v2,
                )
                .await;
            if all_indexers_enable_shard_scaling_v2 {
                info!("cluster is fully migrated, stopping local shards task");
                return;
            }
            if !self.run_once().await {
                // The state has been dropped, we can stop the task.
                debug!("stopping local shards broadcast task");
                return;
            }
        }
    }

    /// Single iteration of the broadcast loop. Returns `false` when the task
    /// should stop.
    #[instrument(name = "broadcast_local_shards.tick", skip_all)]
    async fn run_once(&mut self) -> bool {
        if self.weak_state.upgrade().is_none() {
            return false;
        }
        let Some(readings) = self.local_shards_rx.borrow().clone() else {
            return true;
        };
        let new_snapshot = LocalShardsSnapshot::from(readings.as_ref());
        self.broadcast_local_shards(&self.previous_snapshot, &new_snapshot)
            .await;
        self.previous_snapshot = new_snapshot;
        true
    }
}

#[derive(Debug, Clone)]
pub struct LocalShardsUpdate {
    pub ingester_id: NodeId,
    pub source_uid: SourceUid,
    pub shard_infos: ShardInfos,
}

impl Event for LocalShardsUpdate {}

pub async fn setup_local_shards_update_listener(
    cluster: Cluster,
    event_broker: EventBroker,
) -> ListenerHandle {
    cluster
        .subscribe(INGESTER_SHARDS_PREFIX, move |event| {
            let Some(source_uid) = parse_key(event.key) else {
                warn!("failed to parse source UID `{}`", event.key);
                return;
            };
            let Ok(shard_infos) = serde_json::from_str::<ShardInfos>(event.value) else {
                warn!("failed to parse shard infos `{}`", event.value);
                return;
            };
            let ingester_id: NodeId = NodeId::from_str(&event.node.node_id);

            let local_shards_update = LocalShardsUpdate {
                ingester_id,
                source_uid,
                shard_infos,
            };
            event_broker.publish(local_shards_update);
        })
        .await
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{IndexUid, ShardId, SourceId, SourceUid};

    use super::*;
    use crate::ingest_v2::state::IngesterState;

    #[test]
    fn test_shard_info_serde() {
        let shard_info = ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: ByteSize::mib(42),
            long_term_ingestion_rate: ByteSize::mib(40),
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
        let index_uid = IndexUid::for_test("test-index", 0);
        let current_snapshot = LocalShardsSnapshot {
            per_source_shard_infos: vec![(
                SourceUid {
                    index_uid: index_uid.clone(),
                    source_id: SourceId::from("test-source"),
                },
                vec![ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Open,
                    short_term_ingestion_rate: ByteSize::mib(42),
                    long_term_ingestion_rate: ByteSize::mib(42),
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
                    source_id: SourceId::from("test-source"),
                },
                vec![ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    short_term_ingestion_rate: ByteSize::mib(42),
                    long_term_ingestion_rate: ByteSize::mib(42),
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
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster.clone()).await;
        let (local_shards_tx, local_shards_rx) = watch::channel(None);
        let mut task = BroadcastLocalShardsTask {
            cluster,
            weak_state: state.weak(),
            local_shards_rx,
            previous_snapshot: LocalShardsSnapshot::default(),
        };

        // No readings published yet: nothing to broadcast.
        assert!(task.run_once().await);
        assert!(task.previous_snapshot.per_source_shard_infos.is_empty());

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: SourceId::from("test-source"),
        };
        let reading = ShardThroughputReading {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: ByteSize::b(1),
            long_term_ingestion_rate: ByteSize::mib(2),
        };
        let readings = ShardReadingsBySource {
            readings_by_source: BTreeMap::from([(source_uid, vec![reading])]),
        };
        local_shards_tx.send_replace(Some(Arc::new(readings)));
        assert!(task.run_once().await);

        let key = format!("{INGESTER_SHARDS_PREFIX}{}:{}", index_uid, "test-source");
        let value = task.cluster.get_self_key_value(&key).await.unwrap();
        // Rates are rounded up to the next MiB/s.
        assert_eq!(value, r#"["00000000000000000001:open:1:2"]"#);

        local_shards_tx.send_replace(Some(Arc::new(ShardReadingsBySource::default())));
        assert!(task.run_once().await);
        assert!(task.cluster.get_self_key_value(&key).await.is_none());

        drop(state);
        assert!(!task.run_once().await);
    }

    #[tokio::test]
    async fn test_broadcast_local_shards_task_stops_once_all_indexers_migrated() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster.clone()).await;
        let (_local_shards_tx, local_shards_rx) = watch::channel(None);
        let task_handle =
            BroadcastLocalShardsTask::spawn(cluster.clone(), state.weak(), local_shards_rx);

        tokio::time::sleep(BROADCAST_INTERVAL_PERIOD * 3).await;
        assert!(!task_handle.is_finished());

        cluster.set_self_enable_shard_scaling_v2(true).await;
        tokio::time::timeout(Duration::from_secs(5), task_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_local_shards_update_listener() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let event_broker = EventBroker::default();

        let local_shards_update_counter = Arc::new(AtomicUsize::new(0));
        let local_shards_update_counter_clone = local_shards_update_counter.clone();
        let index_uid = IndexUid::for_test("test-index", 0);

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
                assert_eq!(shard_info.short_term_ingestion_rate, ByteSize::mib(42));
            })
            .forever();

        setup_local_shards_update_listener(cluster.clone(), event_broker.clone())
            .await
            .forever();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: SourceId::from("test-source"),
        };
        let key = make_key(INGESTER_SHARDS_PREFIX, &source_uid);
        let value = serde_json::to_string(&vec![ShardInfo {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Open,
            short_term_ingestion_rate: ByteSize::mib(42),
            long_term_ingestion_rate: ByteSize::mib(42),
        }])
        .unwrap();

        cluster.set_self_key_value(key, value).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(local_shards_update_counter.load(Ordering::Acquire), 1);
    }
}
