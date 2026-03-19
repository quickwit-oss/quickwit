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

#![allow(dead_code)]

use std::convert::TryInto;
use std::str::FromStr;

use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{DeleteQuery, DeleteTask, MetastoreError, MetastoreResult};
use quickwit_proto::types::{DocMappingUid, IndexId, IndexUid, ShardId, SourceId, SplitId};
use sea_query::{Iden, Write};
use tracing::error;

use crate::{IndexMetadata, Split, SplitMetadata, SplitState};

#[derive(Iden, Clone, Copy)]
#[allow(dead_code)]
pub enum Indexes {
    Table,
    IndexUid,
    IndexId,
    IndexMetadataJson,
    CreateTimestamp,
}

/// A model structure for handling index metadata in a database.
#[derive(sqlx::FromRow)]
pub(super) struct MysqlIndex {
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    pub index_id: IndexId,
    pub index_metadata_json: String,
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
}

impl MysqlIndex {
    pub fn index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        let mut index_metadata = serde_json::from_str::<IndexMetadata>(&self.index_metadata_json)
            .map_err(|error| {
            error!(index_id=%self.index_id, error=?error, "failed to deserialize index metadata");

            MetastoreError::JsonDeserializeError {
                struct_name: "IndexMetadata".to_string(),
                message: error.to_string(),
            }
        })?;
        index_metadata.create_timestamp = self.create_timestamp.assume_utc().unix_timestamp();
        Ok(index_metadata)
    }
}

#[derive(Iden, Clone, Copy)]
#[allow(dead_code)]
pub enum Splits {
    Table,
    SplitId,
    SplitState,
    TimeRangeStart,
    TimeRangeEnd,
    CreateTimestamp,
    UpdateTimestamp,
    PublishTimestamp,
    MaturityTimestamp,
    Tags,
    SplitMetadataJson,
    IndexUid,
    NodeId,
    DeleteOpstamp,
}

pub(super) struct FromUnixTimeFunc;

impl Iden for FromUnixTimeFunc {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(s, "FROM_UNIXTIME").unwrap()
    }
}

/// A model structure for handling split metadata in a database.
#[derive(sqlx::FromRow)]
pub(super) struct MysqlSplit {
    pub split_id: SplitId,
    pub split_state: String,
    pub time_range_start: Option<i64>,
    pub time_range_end: Option<i64>,
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
    pub update_timestamp: sqlx::types::time::PrimitiveDateTime,
    pub publish_timestamp: Option<sqlx::types::time::PrimitiveDateTime>,
    pub maturity_timestamp: sqlx::types::time::PrimitiveDateTime,
    pub tags: sqlx::types::Json<Vec<String>>,
    pub split_metadata_json: String,
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    pub delete_opstamp: i64,
}

impl MysqlSplit {
    fn split_metadata(&self) -> MetastoreResult<SplitMetadata> {
        serde_json::from_str::<SplitMetadata>(&self.split_metadata_json).map_err(|error| {
            error!(index_id=%self.index_uid.index_id, split_id=%self.split_id, error=?error, "failed to deserialize split metadata");

            MetastoreError::JsonDeserializeError {
                struct_name: "SplitMetadata".to_string(),
                message: error.to_string(),
            }
        })
    }

    fn split_state(&self) -> MetastoreResult<SplitState> {
        SplitState::from_str(&self.split_state).map_err(|error| {
            error!(index_id=%self.index_uid.index_id, split_id=%self.split_id, split_state=?self.split_state, error=?error, "failed to deserialize split state");
            MetastoreError::JsonDeserializeError {
                struct_name: "SplitState".to_string(),
                message: error,
            }
        })
    }
}

impl TryInto<Split> for MysqlSplit {
    type Error = MetastoreError;

    fn try_into(self) -> Result<Split, Self::Error> {
        let mut split_metadata = self.split_metadata()?;
        split_metadata.create_timestamp = self.create_timestamp.assume_utc().unix_timestamp();
        let split_state = self.split_state()?;
        let update_timestamp = self.update_timestamp.assume_utc().unix_timestamp();
        let publish_timestamp = self
            .publish_timestamp
            .map(|publish_timestamp| publish_timestamp.assume_utc().unix_timestamp());
        split_metadata.index_uid = self.index_uid;
        split_metadata.delete_opstamp = self.delete_opstamp as u64;
        Ok(Split {
            split_metadata,
            split_state,
            update_timestamp,
            publish_timestamp,
        })
    }
}

/// A model structure for handling delete tasks in a database.
#[derive(sqlx::FromRow)]
pub(super) struct MysqlDeleteTask {
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
    pub opstamp: i64,
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    pub delete_query_json: String,
}

impl MysqlDeleteTask {
    fn delete_query(&self) -> MetastoreResult<DeleteQuery> {
        serde_json::from_str::<DeleteQuery>(&self.delete_query_json).map_err(|error| {
            error!(index_id=%self.index_uid.index_id, opstamp=%self.opstamp, error=?error, "failed to deserialize delete query");

            MetastoreError::JsonDeserializeError {
                struct_name: "DeleteQuery".to_string(),
                message: error.to_string(),
            }
        })
    }
}

impl TryInto<DeleteTask> for MysqlDeleteTask {
    type Error = MetastoreError;

    fn try_into(self) -> Result<DeleteTask, Self::Error> {
        let delete_query = self.delete_query()?;
        Ok(DeleteTask {
            create_timestamp: self.create_timestamp.assume_utc().unix_timestamp(),
            opstamp: self.opstamp as u64,
            delete_query: Some(delete_query),
        })
    }
}

#[derive(Iden, Clone, Copy)]
pub(super) enum Shards {
    Table,
    IndexUid,
    SourceId,
    ShardId,
    ShardState,
    LeaderId,
    FollowerId,
    PublishPositionInclusive,
    PublishToken,
}

pub(super) fn shard_state_to_str(shard_state: i32) -> &'static str {
    match ShardState::try_from(shard_state).unwrap_or(ShardState::Unspecified) {
        ShardState::Unspecified => "unspecified",
        ShardState::Open => "open",
        ShardState::Unavailable => "unavailable",
        ShardState::Closed => "closed",
    }
}

pub(super) fn str_to_shard_state(s: &str) -> ShardState {
    match s {
        "open" => ShardState::Open,
        "unavailable" => ShardState::Unavailable,
        "closed" => ShardState::Closed,
        _ => ShardState::Unspecified,
    }
}

#[derive(sqlx::FromRow, Debug)]
pub(super) struct MysqlShard {
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    #[sqlx(try_from = "String")]
    pub source_id: SourceId,
    #[sqlx(try_from = "String")]
    pub shard_id: ShardId,
    pub leader_id: String,
    pub follower_id: Option<String>,
    pub shard_state: String,
    #[sqlx(try_from = "String")]
    pub doc_mapping_uid: DocMappingUid,
    pub publish_position_inclusive: String,
    pub publish_token: Option<String>,
    pub update_timestamp: sqlx::types::time::PrimitiveDateTime,
}

impl From<MysqlShard> for Shard {
    fn from(mysql_shard: MysqlShard) -> Self {
        Shard {
            index_uid: Some(mysql_shard.index_uid),
            source_id: mysql_shard.source_id,
            shard_id: Some(mysql_shard.shard_id),
            shard_state: str_to_shard_state(&mysql_shard.shard_state) as i32,
            leader_id: mysql_shard.leader_id,
            follower_id: mysql_shard.follower_id,
            doc_mapping_uid: Some(mysql_shard.doc_mapping_uid),
            publish_position_inclusive: Some(mysql_shard.publish_position_inclusive.into()),
            publish_token: mysql_shard.publish_token,
            update_timestamp: mysql_shard.update_timestamp.assume_utc().unix_timestamp(),
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
pub(super) struct MysqlIndexTemplate {
    pub index_template_json: String,
}
