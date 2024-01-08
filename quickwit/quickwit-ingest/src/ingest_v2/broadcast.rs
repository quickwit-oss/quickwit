// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Weak;
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
use quickwit_common::sorted_iter::{KeyDiff, SortedByKeyIterator};
use quickwit_common::tower::Rate;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{split_queue_id, NodeId, QueueId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::ingester::IngesterState;
use super::metrics::INGEST_V2_METRICS;
use crate::RateMibPerSec;

const BROADCAST_INTERVAL_PERIOD: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(5)
};

const ONE_MIB: ByteSize = ByteSize::mib(1);

/// Broadcasted information about a primary shard.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    /// Shard ingestion rate in MiB/s.
    pub ingestion_rate: RateMibPerSec,
}

impl Serialize for ShardInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!(
            "{}:{}:{}",
            self.shard_id,
            self.shard_state.as_json_str_name(),
            self.ingestion_rate.0,
        ))
    }
}

impl<'de> Deserialize<'de> for ShardInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        let mut parts = value.split(':');

        let shard_id = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<ShardId>()
            .map_err(|_| serde::de::Error::custom("invalid shard ID"))?;

        let shard_state_str = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?;
        let shard_state = ShardState::from_json_str_name(shard_state_str)
            .ok_or_else(|| serde::de::Error::custom("invalid shard state"))?;

        let ingestion_rate = parts
            .next()
            .ok_or_else(|| serde::de::Error::custom("invalid shard info"))?
            .parse::<u16>()
            .map(RateMibPerSec)
            .map_err(|_| serde::de::Error::custom("invalid shard ingestion rate"))?;

        Ok(Self {
            shard_id,
            shard_state,
            ingestion_rate,
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
    pub fn diff<'a>(&'a self, other: &'a Self) -> impl Iterator<Item = ShardInfosChange<'a>> + '_ {
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
    weak_state: Weak<RwLock<IngesterState>>,
}

impl BroadcastLocalShardsTask {
    pub fn spawn(cluster: Cluster, weak_state: Weak<RwLock<IngesterState>>) -> JoinHandle<()> {
        let mut broadcaster = Self {
            cluster,
            weak_state,
        };
        tokio::spawn(async move { broadcaster.run().await })
    }

    async fn snapshot_local_shards(&self) -> Option<LocalShardsSnapshot> {
        let state = self.weak_state.upgrade()?;
        let mut state_guard = state.write().await;

        let mut per_source_shard_infos: BTreeMap<SourceUid, ShardInfos> = BTreeMap::new();

        let queue_ids: Vec<(QueueId, ShardState)> = state_guard
            .shards
            .iter()
            .filter_map(|(queue_id, shard)| {
                if !shard.is_replica() {
                    Some((queue_id.clone(), shard.shard_state))
                } else {
                    None
                }
            })
            .collect();

        for (queue_id, shard_state) in queue_ids {
            let Some((_rate_limiter, rate_meter)) = state_guard.rate_trackers.get_mut(&queue_id)
            else {
                warn!("rate limiter `{queue_id}` not found",);
                continue;
            };
            let Some((index_uid, source_id, shard_id)) = split_queue_id(&queue_id) else {
                warn!("failed to parse queue ID `{queue_id}`");
                continue;
            };
            let source_uid = SourceUid {
                index_uid,
                source_id,
            };
            // Shard ingestion rate in MiB/s.
            let ingestion_rate_per_sec = rate_meter.harvest().rescale(Duration::from_secs(1));
            let ingestion_rate_mib_per_sec_u64 = ingestion_rate_per_sec.work() / ONE_MIB.as_u64();
            let ingestion_rate = RateMibPerSec(ingestion_rate_mib_per_sec_u64 as u16);

            let shard_info = ShardInfo {
                shard_id,
                shard_state,
                ingestion_rate,
            };
            per_source_shard_infos
                .entry(source_uid)
                .or_default()
                .insert(shard_info);
        }
        for (source_uid, shard_infos) in &per_source_shard_infos {
            let mut num_open_shards = 0;
            let mut num_closed_shards = 0;

            for shard_info in shard_infos {
                match shard_info.shard_state {
                    ShardState::Open => num_open_shards += 1,
                    ShardState::Closed => num_closed_shards += 1,
                    ShardState::Unavailable | ShardState::Unspecified => {}
                }
            }
            INGEST_V2_METRICS
                .shards
                .with_label_values(["open", source_uid.index_uid.index_id()])
                .set(num_open_shards as i64);
            INGEST_V2_METRICS
                .shards
                .with_label_values(["closed", source_uid.index_uid.index_id()])
                .set(num_closed_shards as i64);
        }
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
        index_uid: index_uid_str.into(),
        source_id: source_id_str.to_string(),
    })
}

#[derive(Debug, Clone)]
pub struct LocalShardsUpdate {
    pub leader_id: NodeId,
    pub source_uid: SourceUid,
    pub shard_infos: ShardInfos,
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
            };
            event_broker.publish(local_shards_update);
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use mrecordlog::MultiRecordLog;
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport};
    use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
    use quickwit_proto::ingest::ingester::{IngesterStatus, ObservationMessage};
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{queue_id, Position};
    use tokio::sync::watch;

    use super::*;
    use crate::ingest_v2::models::IngesterShard;
    use crate::ingest_v2::rate_meter::RateMeter;

    #[test]
    fn test_shard_info_serde() {
        let shard_info = ShardInfo {
            shard_id: 1,
            shard_state: ShardState::Open,
            ingestion_rate: RateMibPerSec(42),
        };
        let serialized = serde_json::to_string(&shard_info).unwrap();
        assert_eq!(serialized, r#""1:open:42""#);

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
        let current_snapshot = LocalShardsSnapshot {
            per_source_shard_infos: vec![(
                SourceUid {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                },
                vec![ShardInfo {
                    shard_id: 1,
                    shard_state: ShardState::Open,
                    ingestion_rate: RateMibPerSec(42),
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
        assert_eq!(source_uid.index_uid, "test-index:0");
        assert_eq!(source_uid.source_id, "test-source");
        assert_eq!(shard_infos.len(), 1);

        let num_changes = current_snapshot.diff(&current_snapshot).count();
        assert_eq!(num_changes, 0);

        let previous_snapshot = current_snapshot;
        let current_snapshot = LocalShardsSnapshot {
            per_source_shard_infos: vec![(
                SourceUid {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                },
                vec![ShardInfo {
                    shard_id: 1,
                    shard_state: ShardState::Closed,
                    ingestion_rate: RateMibPerSec(42),
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
        assert_eq!(source_uid.index_uid, "test-index:0");
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
        assert_eq!(source_uid.index_uid, "test-index:0");
        assert_eq!(source_uid.source_id, "test-source");
    }

    #[tokio::test]
    async fn test_broadcast_local_shards_task() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let (observation_tx, _observation_rx) = watch::channel(Ok(ObservationMessage::default()));
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            rate_trackers: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
            status: IngesterStatus::Ready,
            observation_tx,
        }));
        let weak_state = Arc::downgrade(&state);
        let task = BroadcastLocalShardsTask {
            cluster,
            weak_state,
        };
        let previous_snapshot = task.snapshot_local_shards().await.unwrap();
        assert!(previous_snapshot.per_source_shard_infos.is_empty());

        let mut state_guard = state.write().await;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let shard =
            IngesterShard::new_solo(ShardState::Open, Position::Beginning, Position::Beginning);
        state_guard.shards.insert(queue_id_01.clone(), shard);

        let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
        let rate_meter = RateMeter::default();

        state_guard
            .rate_trackers
            .insert(queue_id_01.clone(), (rate_limiter, rate_meter));

        drop(state_guard);

        let new_snapshot = task.snapshot_local_shards().await.unwrap();
        assert_eq!(new_snapshot.per_source_shard_infos.len(), 1);

        task.broadcast_local_shards(&previous_snapshot, &new_snapshot)
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let key = format!(
            "{INGESTER_PRIMARY_SHARDS_PREFIX}{}:{}",
            "test-index:0", "test-source"
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
            index_uid: "test-index:0".into(),
            source_id: "test-source".to_string(),
        };
        let key = make_key(&source_uid);
        assert_eq!(key, "ingester.primary_shards:test-index:0:test-source");
    }

    #[test]
    fn test_parse_key() {
        let key = "test-index:0:test-source";
        let source_uid = parse_key(key).unwrap();
        assert_eq!(source_uid.index_uid, "test-index:0".to_string(),);
        assert_eq!(source_uid.source_id, "test-source".to_string(),);
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

        event_broker
            .subscribe(move |event: LocalShardsUpdate| {
                local_shards_update_counter_clone.fetch_add(1, Ordering::Release);

                assert_eq!(event.source_uid.index_uid, "test-index:0");
                assert_eq!(event.source_uid.source_id, "test-source");
                assert_eq!(event.shard_infos.len(), 1);

                let shard_info = event.shard_infos.iter().next().unwrap();
                assert_eq!(shard_info.shard_id, 1);
                assert_eq!(shard_info.shard_state, ShardState::Open);
                assert_eq!(shard_info.ingestion_rate, 42u16);
            })
            .forever();

        setup_local_shards_update_listener(cluster.clone(), event_broker.clone())
            .await
            .forever();

        let source_uid = SourceUid {
            index_uid: "test-index:0".into(),
            source_id: "test-source".to_string(),
        };
        let key = make_key(&source_uid);
        let value = serde_json::to_string(&vec![ShardInfo {
            shard_id: 1,
            shard_state: ShardState::Open,
            ingestion_rate: RateMibPerSec(42),
        }])
        .unwrap();

        cluster.set_self_key_value(key, value).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(local_shards_update_counter.load(Ordering::Acquire), 1);
    }
}
