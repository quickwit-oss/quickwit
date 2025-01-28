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
pub(super) struct PgIndex {
    /// Index UID. The index UID identifies the index when querying the metastore from the
    /// application.
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    /// Index ID. The index ID is used to resolve user queries.
    pub index_id: IndexId,
    // A JSON string containing all of the IndexMetadata.
    pub index_metadata_json: String,
    /// Timestamp for tracking when the split was created.
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
}

impl PgIndex {
    /// Deserializes index metadata from JSON string stored in column and sets appropriate
    /// timestamps.
    pub fn index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        let mut index_metadata = serde_json::from_str::<IndexMetadata>(&self.index_metadata_json)
            .map_err(|error| {
            error!(index_id=%self.index_id, error=?error, "failed to deserialize index metadata");

            MetastoreError::JsonDeserializeError {
                struct_name: "IndexMetadata".to_string(),
                message: error.to_string(),
            }
        })?;
        // `create_timestamp` and `update_timestamp` are stored in dedicated columns but are also
        // duplicated in [`IndexMetadata`]. We must override the duplicates with the authentic
        // values upon deserialization.
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

pub(super) struct ToTimestampFunc;

impl Iden for ToTimestampFunc {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(s, "TO_TIMESTAMP").unwrap()
    }
}

/// A model structure for handling split metadata in a database.
#[derive(sqlx::FromRow)]
pub(super) struct PgSplit {
    /// Split ID.
    pub split_id: SplitId,
    /// The state of the split. With `update_timestamp`, this is the only mutable attribute of the
    /// split.
    pub split_state: String,
    /// If a timestamp field is available, the min timestamp of the split.
    pub time_range_start: Option<i64>,
    /// If a timestamp field is available, the max timestamp of the split.
    pub time_range_end: Option<i64>,
    /// Timestamp for tracking when the split was created.
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
    /// Timestamp for tracking when the split was last updated.
    pub update_timestamp: sqlx::types::time::PrimitiveDateTime,
    /// Timestamp for tracking when the split was published.
    pub publish_timestamp: Option<sqlx::types::time::PrimitiveDateTime>,
    /// Timestamp for tracking when the split becomes mature.
    /// If a split is already mature, this timestamp is set to 0.
    pub maturity_timestamp: sqlx::types::time::PrimitiveDateTime,
    /// A list of tags for categorizing and searching group of splits.
    pub tags: Vec<String>,
    // The split's metadata serialized as a JSON string.
    pub split_metadata_json: String,
    /// Index UID. It is used as a foreign key in the database.
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    /// Delete opstamp.
    pub delete_opstamp: i64,
}

impl PgSplit {
    /// Deserializes and returns the split's metadata.
    fn split_metadata(&self) -> MetastoreResult<SplitMetadata> {
        serde_json::from_str::<SplitMetadata>(&self.split_metadata_json).map_err(|error| {
            error!(index_id=%self.index_uid.index_id, split_id=%self.split_id, error=?error, "failed to deserialize split metadata");

            MetastoreError::JsonDeserializeError {
                struct_name: "SplitMetadata".to_string(),
                message: error.to_string(),
            }
        })
    }

    /// Deserializes and returns the split's state.
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

impl TryInto<Split> for PgSplit {
    type Error = MetastoreError;

    fn try_into(self) -> Result<Split, Self::Error> {
        let mut split_metadata = self.split_metadata()?;
        // `create_timestamp` and `delete_opstamp` are duplicated in `SplitMetadata` and needs to be
        // overridden with the "true" value stored in a column.
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

/// A model structure for handling split metadata in a database.
#[derive(sqlx::FromRow)]
pub(super) struct PgDeleteTask {
    /// Create timestamp.
    pub create_timestamp: sqlx::types::time::PrimitiveDateTime,
    /// Monotonic increasing unique opstamp.
    pub opstamp: i64,
    /// Index uid.
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    /// Query serialized as a JSON string.
    pub delete_query_json: String,
}

impl PgDeleteTask {
    /// Deserializes and returns the split's metadata.
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

impl TryInto<DeleteTask> for PgDeleteTask {
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

#[derive(sqlx::Type, PartialEq, Debug)]
#[sqlx(type_name = "SHARD_STATE", rename_all = "snake_case")]
pub(super) enum PgShardState {
    Unspecified,
    Open,
    Unavailable,
    Closed,
}

impl From<PgShardState> for ShardState {
    fn from(pg_shard_state: PgShardState) -> Self {
        match pg_shard_state {
            PgShardState::Unspecified => ShardState::Unspecified,
            PgShardState::Open => ShardState::Open,
            PgShardState::Unavailable => ShardState::Unavailable,
            PgShardState::Closed => ShardState::Closed,
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
pub(super) struct PgShard {
    #[sqlx(try_from = "String")]
    pub index_uid: IndexUid,
    #[sqlx(try_from = "String")]
    pub source_id: SourceId,
    #[sqlx(try_from = "String")]
    pub shard_id: ShardId,
    pub leader_id: String,
    pub follower_id: Option<String>,
    pub shard_state: PgShardState,
    #[sqlx(try_from = "String")]
    pub doc_mapping_uid: DocMappingUid,
    pub publish_position_inclusive: String,
    pub publish_token: Option<String>,
    pub update_timestamp: sqlx::types::time::PrimitiveDateTime,
}

impl From<PgShard> for Shard {
    fn from(pg_shard: PgShard) -> Self {
        Shard {
            index_uid: Some(pg_shard.index_uid),
            source_id: pg_shard.source_id,
            shard_id: Some(pg_shard.shard_id),
            shard_state: ShardState::from(pg_shard.shard_state) as i32,
            leader_id: pg_shard.leader_id,
            follower_id: pg_shard.follower_id,
            doc_mapping_uid: Some(pg_shard.doc_mapping_uid),
            publish_position_inclusive: Some(pg_shard.publish_position_inclusive.into()),
            publish_token: pg_shard.publish_token,
            update_timestamp: pg_shard.update_timestamp.assume_utc().unix_timestamp(),
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
pub(super) struct PgIndexTemplate {
    pub index_template_json: String,
}
