// Copyright (C) 2023 Quickwit, Inc.
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

use std::fmt;

use quickwit_common::retry::Retryable;
use serde::{Deserialize, Serialize};

use crate::{queue_id, IndexId, QueueId, ServiceError, ServiceErrorCode, SourceId, SplitId};

pub mod events;

include!("../codegen/quickwit/quickwit.metastore.rs");

pub use metastore_service_client::MetastoreServiceClient;
pub use metastore_service_server::{MetastoreService, MetastoreServiceServer};

pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Lists the object types stored and managed by the metastore.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntityKind {
    /// A checkpoint delta.
    CheckpointDelta {
        /// Index ID.
        index_id: IndexId,
        /// Source ID.
        source_id: SourceId,
    },
    /// An index.
    Index {
        /// Index ID.
        index_id: IndexId,
    },
    /// A set of indexes.
    Indexes {
        /// Index IDs.
        index_ids: Vec<IndexId>,
    },
    /// A source.
    Source {
        /// Index ID.
        index_id: IndexId,
        /// Source ID.
        source_id: SourceId,
    },
    /// A shard.
    Shard {
        /// Shard queue ID: <index_uid>/<source_id>/<shard_id>
        queue_id: QueueId,
    },
    /// A split.
    Split {
        /// Split ID.
        split_id: SplitId,
    },
    /// A set of splits.
    Splits {
        /// Split IDs.
        split_ids: Vec<SplitId>,
    },
}

impl fmt::Display for EntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityKind::CheckpointDelta {
                index_id,
                source_id,
            } => write!(f, "checkpoint delta `{index_id}/{source_id}`"),
            EntityKind::Index { index_id } => write!(f, "index `{}`", index_id),
            EntityKind::Indexes { index_ids } => write!(f, "indexes `{}`", index_ids.join(", ")),
            EntityKind::Shard { queue_id } => write!(f, "shard `{queue_id}`"),
            EntityKind::Source {
                index_id,
                source_id,
            } => write!(f, "source `{index_id}/{source_id}`"),
            EntityKind::Split { split_id } => write!(f, "split `{split_id}`"),
            EntityKind::Splits { split_ids } => write!(f, "splits `{}`", split_ids.join(", ")),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetastoreError {
    #[error("{0} already exist(s)")]
    AlreadyExists(EntityKind),

    #[error("connection error: {message}")]
    Connection { message: String },

    #[error("database error: {message}")]
    Db { message: String },

    #[error("precondition failed for {entity}: {message}")]
    FailedPrecondition { entity: EntityKind, message: String },

    #[error("access forbidden: {message}")]
    Forbidden { message: String },

    #[error("internal error: {message}; cause: `{cause}`")]
    Internal { message: String, cause: String },

    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },

    #[error("IO error: {message}")]
    Io { message: String },

    #[error("failed to deserialize `{struct_name}` from JSON: {message}")]
    JsonDeserializeError {
        struct_name: String,
        message: String,
    },

    #[error("failed to serialize `{struct_name}` to JSON: {message}")]
    JsonSerializeError {
        struct_name: String,
        message: String,
    },

    #[error("{0} do(es) not exist")]
    NotFound(EntityKind),

    #[error("metastore unavailable: {0}")]
    Unavailable(String),
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for MetastoreError {
    fn from(error: sqlx::Error) -> Self {
        MetastoreError::Db {
            message: error.to_string(),
        }
    }
}

impl From<MetastoreError> for tonic::Status {
    fn from(metastore_error: MetastoreError) -> Self {
        let grpc_code = metastore_error.error_code().to_grpc_status_code();
        let error_msg = serde_json::to_string(&metastore_error)
            .unwrap_or_else(|_| format!("raw metastore error: {metastore_error}"));
        tonic::Status::new(grpc_code, error_msg)
    }
}

impl ServiceError for MetastoreError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::AlreadyExists { .. } => ServiceErrorCode::AlreadyExists,
            Self::Connection { .. } => ServiceErrorCode::Internal,
            Self::Db { .. } => ServiceErrorCode::Internal,
            Self::FailedPrecondition { .. } => ServiceErrorCode::BadRequest,
            Self::Forbidden { .. } => ServiceErrorCode::MethodNotAllowed,
            Self::Internal { .. } => ServiceErrorCode::Internal,
            Self::InvalidArgument { .. } => ServiceErrorCode::BadRequest,
            Self::Io { .. } => ServiceErrorCode::Internal,
            Self::JsonDeserializeError { .. } => ServiceErrorCode::Internal,
            Self::JsonSerializeError { .. } => ServiceErrorCode::Internal,
            Self::NotFound { .. } => ServiceErrorCode::NotFound,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl Retryable for MetastoreError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            MetastoreError::Connection { .. }
                | MetastoreError::Db { .. }
                | MetastoreError::Io { .. }
                | MetastoreError::Internal { .. }
        )
    }
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Cli => "ingest-cli",
            SourceType::File => "file",
            SourceType::GcpPubsub => "gcp_pubsub",
            SourceType::IngestV1 => "ingest-api",
            SourceType::IngestV2 => "ingest",
            SourceType::Kafka => "kafka",
            SourceType::Kinesis => "kinesis",
            SourceType::Nats => "nats",
            SourceType::Pulsar => "pulsar",
            SourceType::Vec => "vec",
            SourceType::Void => "void",
        }
    }
}

impl CloseShardsSuccess {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl CloseShardsFailure {
    pub fn queue_id(&self) -> QueueId {
        queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

pub mod serde_utils {
    use serde::{Deserialize, Serialize};

    use super::{MetastoreError, MetastoreResult};

    pub fn from_json_str<'de, T: Deserialize<'de>>(value_str: &'de str) -> MetastoreResult<T> {
        serde_json::from_str(value_str).map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }

    pub fn to_json_str<T: Serialize>(value: &T) -> Result<String, MetastoreError> {
        serde_json::to_string(value).map_err(|error| MetastoreError::JsonSerializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }
}
