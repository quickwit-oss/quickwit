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

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tracing::warn;

use super::parse_key;
use crate::ingest_v2::local_shards::{ShardInfo, ShardInfos};

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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{IndexUid, ShardId, SourceId, SourceUid};

    use super::*;
    use crate::RateMibPerSec;
    use crate::ingest_v2::models::IngesterShard;
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
                    source_id: SourceId::from("test-source"),
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
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster.clone()).await;
        let mut task = BroadcastLocalShardsTask {
            cluster,
            weak_state: state.weak(),
            shard_throughput_time_series_map: Default::default(),
            previous_snapshot: LocalShardsSnapshot::default(),
        };

        let mut state_guard = state.lock_partially("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let shard_00 = IngesterShard::builder(
            index_uid.clone(),
            SourceId::from("test-source"),
            ShardId::from(0),
        )
        .build();
        state_guard.shards.insert(shard_00.queue_id(), shard_00);

        let shard_01 = IngesterShard::builder(
            index_uid.clone(),
            SourceId::from("test-source"),
            ShardId::from(1),
        )
        .advertisable()
        .build();
        let queue_id_01 = shard_01.queue_id();
        state_guard.shards.insert(queue_id_01.clone(), shard_01);

        drop(state_guard);

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

        assert!(task.run_once().await);
        assert!(task.previous_snapshot.per_source_shard_infos.is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = task.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
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
                assert_eq!(shard_info.short_term_ingestion_rate, 42u16);
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
