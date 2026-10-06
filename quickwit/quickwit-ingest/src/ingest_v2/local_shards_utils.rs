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

use bytesize::ByteSize;
use quickwit_proto::control_plane as proto;
use quickwit_proto::types::SourceUid;

use super::broadcast::{ShardInfo, ShardInfos};

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{IndexUid, ShardId};

    use super::*;

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
}
