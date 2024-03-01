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

use std::fmt;

use quickwit_common::retry::Retryable;
use quickwit_common::service_error::ProutServiceError;
use serde::{Deserialize, Serialize};

use crate::types::{IndexId, IndexUid, QueueId, ShardId, SourceId, SplitId};
use crate::{ServiceError, ServiceErrorCode};

pub mod events;

include!("../codegen/quickwit/quickwit.metastore.rs");

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
    /// An index template.
    IndexTemplate {
        /// Index template ID.
        template_id: String,
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
            EntityKind::IndexTemplate { template_id } => {
                write!(f, "index template `{}`", template_id)
            }
        }
    }
}

// #[derive(Debug, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
// pub enum MetastoreError {
//     #[error("{0} already exist(s)")]
//     AlreadyExists(EntityKind),

//     #[error("connection error: {message}")]
//     Connection { message: String },

//     #[error("database error: {message}")]
//     Db { message: String },

//     #[error("precondition failed for {entity}: {message}")]
//     FailedPrecondition { entity: EntityKind, message: String },

//     #[error("internal error: {message}; cause: `{cause}`")]
//     Internal { message: String, cause: String },

//     #[error("IO error: {message}")]
//     Io { message: String },

// }

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetastoreError {
    #[error("{0} already exists")]
    AlreadyExists(EntityKind),

    #[error("database error: {0}")]
    Db(String),

    #[error("failed precondition: {message}")]
    FailedPrecondition { entity: EntityKind, message: String },

    #[error("access forbidden: {0}")]
    Forbidden(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("{0}")]
    Serde(String),

    #[error("{0} not found")]
    NotFound(EntityKind),

    #[error("request timed out: {0}")]
    Timeout(String),

    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ProutServiceError for MetastoreError {
    fn grpc_status_code(&self) -> tonic::Code {
        match self {
            Self::AlreadyExists(_) => tonic::Code::AlreadyExists,
            Self::Db(_) => tonic::Code::Internal,
            Self::FailedPrecondition { .. } => tonic::Code::FailedPrecondition,
            Self::Forbidden(_) => tonic::Code::PermissionDenied,
            Self::Internal(_) => tonic::Code::Internal,
            Self::InvalidArgument(_) => tonic::Code::InvalidArgument,
            Self::NotFound(_) => tonic::Code::NotFound,
            Self::Serde(_) => tonic::Code::Internal,
            Self::Timeout(_) => tonic::Code::DeadlineExceeded,
            Self::Unavailable(_) => tonic::Code::Unavailable,
        }
    }

    fn new_internal(message: String) -> Self {
        MetastoreError::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        MetastoreError::Timeout(message)
    }

    fn new_unavailable(message: String) -> Self {
        MetastoreError::Unavailable(message)
    }

    fn label_value(&self) -> &'static str {
        match self {
            Self::AlreadyExists(_) => "already_exists",
            Self::Db(_) => "database",
            Self::FailedPrecondition { .. } => "failed_precondition",
            Self::Forbidden(_) => "forbidden",
            Self::Internal(_) => "internal",
            Self::InvalidArgument(_) => "invalid_argument",
            Self::Serde(_) => "serde",
            Self::NotFound(_) => "not_found",
            Self::Timeout(_) => "timeout",
            Self::Unavailable(_) => "unavailable",
        }
    }
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for MetastoreError {
    fn from(error: sqlx::Error) -> Self {
        MetastoreError::Db(error.to_string())
    }
}

// impl Retryable for MetastoreError {
//     fn is_retryable(&self) -> bool {
//         matches!(
//             self,
//             MetastoreError::Connection { .. }
//                 | MetastoreError::Db(_)
//                 | MetastoreError::Io { .. }
//                 | MetastoreError::Internal { .. }
//         )
//     }
// }

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
            SourceType::Unspecified => "unspecified",
            SourceType::Vec => "vec",
            SourceType::Void => "void",
        }
    }
}

impl IndexMetadataRequest {
    pub fn with_index_id(index_id: IndexId) -> Self {
        Self {
            index_uid: None,
            index_id: Some(index_id),
        }
    }

    pub fn with_index_uid(index_uid: IndexUid) -> Self {
        Self {
            index_uid: Some(index_uid),
            index_id: None,
        }
    }

    /// Returns the index id either from the `index_id` or the `index_uid`.
    /// If none of them is set, an error is returned.
    pub fn get_index_id(&self) -> MetastoreResult<IndexId> {
        if let Some(index_id) = &self.index_id {
            Ok(index_id.to_string())
        } else if let Some(index_uid) = &self.index_uid {
            Ok(index_uid.index_id.to_string())
        } else {
            Err(MetastoreError::Internal(
                "`index_id` or `index_uid` must be set".to_string(),
            ))
        }
    }
}

impl MarkSplitsForDeletionRequest {
    pub fn new(index_uid: IndexUid, split_ids: Vec<String>) -> Self {
        Self {
            index_uid: index_uid.into(),
            split_ids,
        }
    }
}

impl LastDeleteOpstampResponse {
    pub fn new(last_delete_opstamp: u64) -> Self {
        Self {
            last_delete_opstamp,
        }
    }
}

impl ListDeleteTasksRequest {
    pub fn new(index_uid: IndexUid, opstamp_start: u64) -> Self {
        Self {
            index_uid: index_uid.into(),
            opstamp_start,
        }
    }
}

pub mod serde_utils {
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};
    use serde_json::Value as JsonValue;

    use super::{MetastoreError, MetastoreResult};

    pub fn from_json_bytes<'de, T: Deserialize<'de>>(value_bytes: &'de [u8]) -> MetastoreResult<T> {
        serde_json::from_slice(value_bytes).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message =
                format!("failed to deserialize object `{struct_name}` from JSON: {error}");
            MetastoreError::Serde(message)
        })
    }

    pub fn from_json_str<'de, T: Deserialize<'de>>(value_str: &'de str) -> MetastoreResult<T> {
        serde_json::from_str(value_str).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message =
                format!("failed to deserialize object `{struct_name}` from JSON: {error}");
            MetastoreError::Serde(message)
        })
    }

    pub fn from_json_value<T: DeserializeOwned>(value: JsonValue) -> MetastoreResult<T> {
        serde_json::from_value(value).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message =
                format!("failed to deserialize object `{struct_name}` from JSON: {error}");
            MetastoreError::Serde(message)
        })
    }

    pub fn to_json_str<T: Serialize>(value: &T) -> Result<String, MetastoreError> {
        serde_json::to_string(value).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message = format!("failed to serialize object `{struct_name}` to JSON: {error}");
            MetastoreError::Serde(message)
        })
    }

    pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, MetastoreError> {
        serde_json::to_vec(value).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message = format!("failed to serialize object `{struct_name}` to JSON: {error}");
            MetastoreError::Serde(message)
        })
    }

    pub fn to_json_bytes_pretty<T: Serialize>(value: &T) -> Result<Vec<u8>, MetastoreError> {
        serde_json::to_vec_pretty(value).map_err(|error| {
            let struct_name = std::any::type_name::<T>();
            let message = format!("failed to serialize object `{struct_name}` to JSON: {error}");
            MetastoreError::Serde(message)
        })
    }
}

impl ListIndexesMetadataRequest {
    pub fn all() -> ListIndexesMetadataRequest {
        ListIndexesMetadataRequest {
            index_id_patterns: vec!["*".to_string()],
        }
    }
}

impl OpenShardsSubrequest {
    pub fn shard_id(&self) -> &ShardId {
        self.shard_id
            .as_ref()
            .expect("`shard_id` should be a required field")
    }
}
