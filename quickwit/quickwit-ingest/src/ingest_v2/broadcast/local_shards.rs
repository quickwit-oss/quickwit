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
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
use quickwit_common::sorted_iter::{KeyDiff, SortedByKeyIterator};
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, instrument, warn, info};

use super::{make_key, parse_key};
use crate::ingest_v2::local_shards_utils::ShardThroughputReadings;
use crate::ingest_v2::state::WeakIngesterState;

const LOCAL_SHARDS_BROADCAST_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(5)
};

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

/// A set of shards belonging to the same source.
pub type ShardInfos = BTreeSet<ShardInfo>;

impl Serialize for ShardInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let short_term_ingestion_rate_mib_per_sec =
            self.short_term_ingestion_rate.as_mib().ceil() as u16;
        let long_term_ingestion_rate_mib_per_sec =
            self.long_term_ingestion_rate.as_mib().ceil() as u16;
        serializer.serialize_str(&format!(
            "{}:{}:{}:{}",
            self.shard_id,
            self.shard_state.as_json_str_name(),
            short_term_ingestion_rate_mib_per_sec,
            long_term_ingestion_rate_mib_per_sec,
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
            .map(|mib_per_sec| ByteSize::mib(mib_per_sec.into()))
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        let long_term_ingestion_rate = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<u16>()
            .map(|mib_per_sec| ByteSize::mib(mib_per_sec.into()))
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        Ok(Self {
            shard_id,
            shard_state,
            short_term_ingestion_rate,
            long_term_ingestion_rate,
        })
    }
}

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

impl LocalShardsSnapshot {
    fn from_readings(readings: &ShardThroughputReadings) -> Self {
        let mut per_source_shard_infos = BTreeMap::new();
        for (source_uid, shard_infos) in &readings.per_source_shard_infos {
            let shard_infos: ShardInfos = shard_infos
                .iter()
                .cloned()
                .map(|mut shard_info| {
                    shard_info.short_term_ingestion_rate = ByteSize::mib(
                        (shard_info.short_term_ingestion_rate.as_mib().ceil() as u16).into(),
                    );
                    shard_info.long_term_ingestion_rate = ByteSize::mib(
                        (shard_info.long_term_ingestion_rate.as_mib().ceil() as u16).into(),
                    );
                    shard_info
                })
                .collect();
            per_source_shard_infos.insert(source_uid.clone(), shard_infos);
        }
        Self {
            per_source_shard_infos,
        }
    }

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
    local_shards_rx: watch::Receiver<Option<Arc<ShardThroughputReadings>>>,
    /// Snapshot broadcast on the previous tick. Carried across iterations so
    /// we can diff against the new snapshot and only broadcast changes.
    previous_snapshot: LocalShardsSnapshot,
}

impl BroadcastLocalShardsTask {
    pub fn spawn(
        cluster: Cluster,
        weak_state: WeakIngesterState,
        local_shards_rx: watch::Receiver<Option<Arc<ShardThroughputReadings>>>,
    ) -> JoinHandle<()> {
        let broadcaster = Self {
            cluster,
            weak_state,
            local_shards_rx,
            previous_snapshot: LocalShardsSnapshot::default(),
        };
        tokio::spawn(broadcaster.run())
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(LOCAL_SHARDS_BROADCAST_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if self.cluster.all_indexers_migrated().await {
                info!("Cluster is fully migrated. Stopping local shards task");
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
        let new_snapshot = LocalShardsSnapshot::from_readings(&readings);
        self.broadcast_local_shards(&self.previous_snapshot, &new_snapshot)
            .await;
        self.previous_snapshot = new_snapshot;
        true
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

    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_proto::types::{IndexUid, SourceId};

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::state::IngesterState;

    fn readings(bytes: u64, state: ShardState) -> ShardThroughputReadings {
        ShardThroughputReadings {
            per_source_shard_infos: [(
                SourceUid {
                    index_uid: IndexUid::for_test("index", 0),
                    source_id: "source".to_string(),
                },
                BTreeSet::from([ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: state,
                    short_term_ingestion_rate: ByteSize::b(bytes),
                    long_term_ingestion_rate: ByteSize::b(bytes),
                }]),
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn test_legacy_wire_rate_rounding() {
        for (bytes, mib) in [
            (0, 0),
            (1, 1),
            (bytesize::MIB - 1, 1),
            (bytesize::MIB, 1),
            (bytesize::MIB + 1, 2),
        ] {
            let snapshot = readings(bytes, ShardState::Open);
            let shard = snapshot
                .per_source_shard_infos
                .values()
                .next()
                .unwrap()
                .first()
                .unwrap();
            let json = serde_json::to_string(shard).unwrap();
            assert_eq!(json, format!("\"00000000000000000001:open:{mib}:{mib}\""));
            let decoded: ShardInfo = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.short_term_ingestion_rate, ByteSize::mib(mib));
            assert_eq!(decoded.long_term_ingestion_rate, ByteSize::mib(mib));
        }
    }

    #[tokio::test]
    async fn test_gossip_change_detection_uses_rounded_buckets() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &["indexer"],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_dir, state) = IngesterState::for_test(cluster.clone()).await;
        let (sender, receiver) = watch::channel(None);
        let mut task = BroadcastLocalShardsTask {
            cluster: cluster.clone(),
            weak_state: state.weak(),
            local_shards_rx: receiver,
            previous_snapshot: LocalShardsSnapshot::default(),
        };
        let initial = readings(1, ShardState::Open);
        let source = initial
            .per_source_shard_infos
            .keys()
            .next()
            .unwrap()
            .clone();
        let key = make_key(INGESTER_SHARDS_PREFIX, &source);
        sender.send_replace(Some(Arc::new(initial)));
        assert!(task.run_once().await);
        let original = cluster.get_self_key_value(&key).await.unwrap();
        let same_bucket =
            LocalShardsSnapshot::from_readings(&readings(bytesize::MIB, ShardState::Open));
        assert_eq!(task.previous_snapshot.diff(&same_bucket).count(), 0);
        sender.send_replace(Some(Arc::new(readings(bytesize::MIB, ShardState::Open))));
        assert!(task.run_once().await);
        assert_eq!(cluster.get_self_key_value(&key).await.unwrap(), original);
        for (bytes, status, expected) in [
            (bytesize::MIB + 1, ShardState::Open, "open:2:2"),
            (bytesize::MIB + 1, ShardState::Closed, "closed:2:2"),
        ] {
            sender.send_replace(Some(Arc::new(readings(bytes, status))));
            assert!(task.run_once().await);
            assert!(
                cluster
                    .get_self_key_value(&key)
                    .await
                    .unwrap()
                    .contains(expected)
            );
        }
        sender.send_replace(Some(Arc::new(ShardThroughputReadings::default())));
        assert!(task.run_once().await);
        assert!(cluster.get_self_key_value(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_gossip_cutover_stops_outer_loop_and_retains_keys() {
        for migrated_at_start in [false, true] {
            let cluster = create_cluster_for_test(
                Vec::new(),
                &["indexer"],
                &ChitchatTransport::default(),
                true,
            )
            .await
            .unwrap();
            let (_dir, state) = IngesterState::for_test(cluster.clone()).await;
            cluster
                .wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
                .await
                .unwrap();
            let snapshot = readings(1, ShardState::Open);
            let key = make_key(
                INGESTER_SHARDS_PREFIX,
                snapshot.per_source_shard_infos.keys().next().unwrap(),
            );
            cluster.set_self_key_value(&key, "retained").await;
            if migrated_at_start {
                cluster.set_self_key_value("shard_scaling_v2", "true").await;
                cluster
                    .wait_for_ready_members(
                        |members| members.len() == 1 && members[0].enable_shard_scaling_v2,
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
            }
            let (sender, receiver) = watch::channel(None);
            let handle = BroadcastLocalShardsTask::spawn(cluster.clone(), state.weak(), receiver);
            if !migrated_at_start {
                tokio::task::yield_now().await;
                assert!(!handle.is_finished());
                cluster.set_self_key_value("shard_scaling_v2", "true").await;
                cluster
                    .wait_for_ready_members(
                        |members| members.len() == 1 && members[0].enable_shard_scaling_v2,
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
            }
            sender.send_replace(Some(Arc::new(snapshot)));
            tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                cluster.get_self_key_value(&key).await.as_deref(),
                Some("retained")
            );
        }
    }

    #[tokio::test]
    async fn test_broadcaster_waits_for_snapshots_and_stops_with_state() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &["indexer"],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_dir, state) = IngesterState::for_test(cluster.clone()).await;
        let (sender, receiver) = watch::channel(None);
        let mut task = BroadcastLocalShardsTask {
            cluster: cluster.clone(),
            weak_state: state.weak(),
            local_shards_rx: receiver,
            previous_snapshot: LocalShardsSnapshot::default(),
        };
        let mut guard = state.lock_partially("test").await.unwrap();
        let shard = IngesterShard::builder(
            IndexUid::for_test("index", 0),
            "source".to_string(),
            ShardId::from(1),
            guard.shared_rate_meter.clone(),
        )
        .advertisable()
        .build();
        guard.shards.insert(shard.queue_id(), shard);
        let meter = guard.shared_rate_meter.clone();
        drop(guard);
        assert!(task.run_once().await);
        assert!(task.previous_snapshot.per_source_shard_infos.is_empty());
        tokio::time::sleep(Duration::from_millis(1)).await;
        sender.send_replace(Some(Arc::new(meter.harvest())));
        assert!(task.run_once().await);
        assert_eq!(task.previous_snapshot.per_source_shard_infos.len(), 1);
        drop(state);
        assert!(!task.run_once().await);
    }

    #[tokio::test]
    async fn test_malformed_gossip_does_not_break_listener() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &["indexer"],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let broker = EventBroker::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        broker
            .subscribe(move |event: LocalShardsUpdate| {
                sender.send(event).unwrap();
            })
            .forever();
        setup_local_shards_update_listener(cluster.clone(), broker)
            .await
            .forever();
        let snapshot = readings(1, ShardState::Open);
        let source = snapshot.per_source_shard_infos.keys().next().unwrap();
        let key = make_key(INGESTER_SHARDS_PREFIX, source);
        for (key, value) in [
            (format!("{INGESTER_SHARDS_PREFIX}invalid"), "[]"),
            (key.clone(), "invalid json"),
            (key.clone(), "[\"1:invalid:1:1\"]"),
            (key.clone(), "[\"1:open:bad:1\"]"),
        ] {
            cluster.set_self_key_value(key, value).await;
        }
        cluster
            .set_self_key_value(
                key,
                serde_json::to_string(&snapshot.per_source_shard_infos[source]).unwrap(),
            )
            .await;
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&event.source_uid, source);
        assert_eq!(
            event.shard_infos.first().unwrap().short_term_ingestion_rate,
            ByteSize::mib(1)
        );
        assert!(receiver.try_recv().is_err());
    }

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
        cluster
            .wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
            .await
            .unwrap();
        let (local_shards_tx, local_shards_rx) = watch::channel(None);
        let publisher = crate::ingest_v2::shard_readings::ShardReadingsPublisher::spawn(
            state.weak(),
            local_shards_tx,
        );
        let mut task = BroadcastLocalShardsTask {
            cluster,
            weak_state: state.weak(),
            local_shards_rx,
            previous_snapshot: LocalShardsSnapshot::default(),
        };

        let mut state_guard = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let shard_00 = IngesterShard::builder(
            index_uid.clone(),
            SourceId::from("test-source"),
            ShardId::from(0),
            state_guard.shared_rate_meter.clone(),
        )
        .build();
        state_guard.shards.insert(shard_00.queue_id(), shard_00);

        let shard_01 = IngesterShard::builder(
            index_uid.clone(),
            SourceId::from("test-source"),
            ShardId::from(1),
            state_guard.shared_rate_meter.clone(),
        )
        .advertisable()
        .build();
        let queue_id_01 = shard_01.queue_id();
        state_guard.shards.insert(queue_id_01.clone(), shard_01);

        drop(state_guard);

        tokio::time::timeout(
            Duration::from_secs(5),
            task.local_shards_rx.wait_for(|snapshot| {
                snapshot
                    .as_ref()
                    .is_some_and(|snapshot| !snapshot.per_source_shard_infos.is_empty())
            }),
        )
        .await
        .unwrap()
        .unwrap();
        // First tick: shard_01 (advertisable) is the only one contributing to the snapshot —
        // broadcast publishes it.
        assert!(task.run_once().await);
        assert_eq!(task.previous_snapshot.per_source_shard_infos.len(), 1);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let key = format!("{INGESTER_SHARDS_PREFIX}{}:{}", index_uid, "test-source");
        task.cluster.get_self_key_value(&key).await.unwrap();

        // Remove the only advertisable shard, run again: snapshot empty,
        // broadcast clears the chitchat key.
        let mut state_guard = state.lock_partially("test").await.unwrap();
        state_guard.shards.remove(&queue_id_01);
        drop(state_guard);

        tokio::time::timeout(
            Duration::from_secs(5),
            task.local_shards_rx.wait_for(|snapshot| {
                snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.per_source_shard_infos.is_empty())
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(task.run_once().await);
        assert!(task.previous_snapshot.per_source_shard_infos.is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = task.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
        publisher.abort();
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
