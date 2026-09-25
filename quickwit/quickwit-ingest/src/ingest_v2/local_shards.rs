use std::collections::{BTreeMap, BTreeSet};

use bytesize::ByteSize;
use quickwit_proto::control_plane as proto;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{ShardId, SourceUid};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    pub short_term_ingestion_rate: ByteSize,
    pub long_term_ingestion_rate: ByteSize,
}

pub type ShardInfos = BTreeSet<ShardInfo>;

#[derive(Debug, Clone, Default)]
pub struct LocalShardsSnapshot {
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

impl From<LocalShardsSnapshot> for proto::ShardsUpdate {
    fn from(snapshot: LocalShardsSnapshot) -> Self {
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
