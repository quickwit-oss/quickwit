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

use bytesize::ByteSize;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
use quickwit_proto::control_plane as proto;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, ShardId, SourceUid};
use serde::{Deserialize, Serialize, Serializer};
use tracing::warn;

use super::broadcast::parse_key;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    pub short_term_ingestion_rate: ByteSize,
    pub long_term_ingestion_rate: ByteSize,
}

pub type ShardInfos = BTreeSet<ShardInfo>;

#[derive(Debug, Clone, Default)]
pub struct ShardThroughputReadings {
    pub(crate) per_source_shard_infos: BTreeMap<SourceUid, ShardInfos>,
}

pub struct SourceShardReport {
    pub source_uid: SourceUid,
    pub shard_infos: ShardInfos,
}

impl From<ShardInfo> for proto::ShardInfo {
    fn from(shard_info: ShardInfo) -> Self {
        Self {
            shard_id: Some(shard_info.shard_id),
            shard_state: shard_info.shard_state as i32,
            short_term_ingestion_rate_bytes_per_sec: shard_info.short_term_ingestion_rate.as_u64(),
            long_term_ingestion_rate_bytes_per_sec: shard_info.long_term_ingestion_rate.as_u64(),
        }
    }
}

impl From<&proto::ShardInfo> for ShardInfo {
    fn from(shard_info: &proto::ShardInfo) -> Self {
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

impl From<SourceShardReport> for proto::ShardInfosBySource {
    fn from(report: SourceShardReport) -> Self {
        Self {
            index_uid: Some(report.source_uid.index_uid),
            source_id: report.source_uid.source_id,
            shard_infos: report.shard_infos.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<&proto::ShardInfosBySource> for SourceShardReport {
    fn from(report: &proto::ShardInfosBySource) -> Self {
        Self {
            source_uid: SourceUid {
                index_uid: report.index_uid().clone(),
                source_id: report.source_id.clone(),
            },
            shard_infos: report.shard_infos.iter().map(ShardInfo::from).collect(),
        }
    }
}

impl From<ShardThroughputReadings> for proto::ShardsUpdate {
    fn from(snapshot: ShardThroughputReadings) -> Self {
        let shard_infos_by_source = snapshot
            .per_source_shard_infos
            .into_iter()
            .map(|(source_uid, shard_infos)| SourceShardReport {
                source_uid,
                shard_infos,
            })
            .map(Into::into)
            .collect();
        Self {
            shard_infos_by_source,
        }
    }
}

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
    use std::time::Duration;

    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_proto::types::{IndexUid, SourceId};

    use super::*;
    use crate::ingest_v2::broadcast::make_key;

    #[test]
    fn test_snapshot_proto_conversion_preserves_sources_and_bytes() {
        let mut snapshot = ShardThroughputReadings::default();
        for (source_id, state, short_rate, long_rate) in [
            ("source-a", ShardState::Open, 123, 456),
            ("source-b", ShardState::Closed, 987_654_321, 123_456_789),
        ] {
            snapshot.per_source_shard_infos.insert(
                SourceUid {
                    index_uid: IndexUid::for_test("index", 1),
                    source_id: source_id.to_string(),
                },
                BTreeSet::from([ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: state,
                    short_term_ingestion_rate: ByteSize::b(short_rate),
                    long_term_ingestion_rate: ByteSize::b(long_rate),
                }]),
            );
        }
        let update: proto::ShardsUpdate = snapshot.clone().into();
        assert_eq!(update.shard_infos_by_source.len(), 2);
        assert_eq!(update.shard_infos_by_source[0].source_id, "source-a");
        assert_eq!(
            update.shard_infos_by_source[0].shard_infos[0].short_term_ingestion_rate_bytes_per_sec,
            123
        );
        let roundtrip: BTreeMap<_, _> = update
            .shard_infos_by_source
            .iter()
            .map(SourceShardReport::from)
            .map(|report| (report.source_uid, report.shard_infos))
            .collect();
        assert_eq!(roundtrip, snapshot.per_source_shard_infos);
        let empty: proto::ShardsUpdate = ShardThroughputReadings::default().into();
        assert!(empty.shard_infos_by_source.is_empty());
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
                assert_eq!(shard_info.long_term_ingestion_rate, ByteSize::mib(40));
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
        let value = r#"["00000000000000000001:open:42:40"]"#;

        cluster.set_self_key_value(key, value).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(local_shards_update_counter.load(Ordering::Acquire), 1);
    }
}
