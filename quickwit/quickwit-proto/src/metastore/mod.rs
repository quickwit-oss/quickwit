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

use std::fmt;

use quickwit_common::rate_limited_error;
use quickwit_common::retry::Retryable;
use quickwit_common::tower::{MakeLoadShedError, TimeoutExceeded};
use serde::{Deserialize, Serialize};

use crate::types::{IndexId, IndexUid, QueueId, SourceId, SplitId};
use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

pub mod events;

include!("../codegen/quickwit/quickwit.metastore.rs");

pub const METASTORE_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/metastore_descriptor.bin");

pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Lists the object types stored and managed by the metastore.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
        split_ids: Vec<String>,
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
            EntityKind::Index { index_id } => write!(f, "index `{index_id}`"),
            EntityKind::Indexes { index_ids } => write!(f, "indexes `{}`", index_ids.join(", ")),
            EntityKind::Shard { queue_id } => write!(f, "shard `{queue_id}`"),
            EntityKind::Source {
                index_id,
                source_id,
            } => write!(f, "source `{index_id}/{source_id}`"),
            EntityKind::Split { split_id } => write!(f, "split `{split_id}`"),
            EntityKind::Splits { split_ids } => write!(f, "splits `{}`", split_ids.join(", ")),
            EntityKind::IndexTemplate { template_id } => {
                write!(f, "index template `{template_id}`")
            }
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

    #[error("{0} not found")]
    NotFound(EntityKind),

    #[error("request timed out: {0}")]
    Timeout(String),

    #[error("too many requests")]
    TooManyRequests,

    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl MetastoreError {
    /// Returns `true` if the transaction that emitted this error is "certainly abort".
    /// Returns `false` if we cannot know whether the transaction was successful or not.
    pub fn is_transaction_certainly_aborted(&self) -> bool {
        match self {
            MetastoreError::AlreadyExists(_)
            | MetastoreError::FailedPrecondition { .. }
            | MetastoreError::Forbidden { .. }
            | MetastoreError::InvalidArgument { .. }
            | MetastoreError::JsonDeserializeError { .. }
            | MetastoreError::JsonSerializeError { .. }
            | MetastoreError::NotFound(_)
            | MetastoreError::TooManyRequests => true,
            MetastoreError::Connection { .. }
            | MetastoreError::Db { .. }
            | MetastoreError::Internal { .. }
            | MetastoreError::Io { .. }
            | MetastoreError::Timeout { .. }
            | MetastoreError::Unavailable(_) => false,
        }
    }
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for MetastoreError {
    fn from(error: sqlx::Error) -> Self {
        MetastoreError::Db {
            message: error.to_string(),
        }
    }
}

impl From<TimeoutExceeded> for MetastoreError {
    fn from(_: TimeoutExceeded) -> Self {
        MetastoreError::Timeout("client".to_string())
    }
}

impl ServiceError for MetastoreError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::AlreadyExists(_) => ServiceErrorCode::AlreadyExists,
            Self::Connection { message } => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "metastore/connection internal error: {message}"
                );
                ServiceErrorCode::Internal
            }
            Self::Db { message } => {
                rate_limited_error!(limit_per_min = 6, "metastore/db internal error: {message}");
                ServiceErrorCode::Internal
            }
            Self::FailedPrecondition { .. } => ServiceErrorCode::BadRequest,
            Self::Forbidden { .. } => ServiceErrorCode::Forbidden,
            Self::Internal { message, cause } => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "metastore internal error: {message} cause: {cause}"
                );
                ServiceErrorCode::Internal
            }
            Self::InvalidArgument { .. } => ServiceErrorCode::BadRequest,
            Self::Io { message } => {
                rate_limited_error!(limit_per_min = 6, "metastore/io internal error: {message}");
                ServiceErrorCode::Internal
            }
            Self::JsonDeserializeError {
                struct_name,
                message,
            } => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "metastore/jsondeser internal error: [{struct_name}] {message}"
                );
                ServiceErrorCode::Internal
            }
            Self::JsonSerializeError {
                struct_name,
                message,
            } => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "metastore/jsonser internal error: [{struct_name}]  {message}"
                );
                ServiceErrorCode::Internal
            }
            Self::NotFound(_) => ServiceErrorCode::NotFound,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for MetastoreError {
    fn new_internal(message: String) -> Self {
        quickwit_common::rate_limited_error!(limit_per_min=6, message=%message.as_str(), "metastore error: internal");
        Self::Internal {
            message,
            cause: "".to_string(),
        }
    }

    fn new_timeout(message: String) -> Self {
        quickwit_common::rate_limited_error!(limit_per_min=6, message=%message.as_str(), "metastore error: timeout");
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        quickwit_common::rate_limited_error!(
            limit_per_min = 6,
            "metastore error: too many requests"
        );
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        quickwit_common::rate_limited_error!(limit_per_min=6, message=%message.as_str(), "metastore error: unavailable metastore");
        Self::Unavailable(message)
    }
}

impl Retryable for MetastoreError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connection { .. }
                | Self::Db { .. }
                | Self::Internal { .. }
                | Self::Io { .. }
                | Self::Timeout(_)
                | Self::Unavailable(_)
        )
    }
}

impl MakeLoadShedError for MetastoreError {
    fn make_load_shed_error() -> Self {
        MetastoreError::TooManyRequests
    }
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Cli => "ingest-cli",
            SourceType::File => "file",
            SourceType::IngestV1 => "ingest-api",
            SourceType::IngestV2 => "ingest",
            SourceType::Kafka => "kafka",
            SourceType::Kinesis => "kinesis",
            SourceType::Nats => "nats",
            SourceType::PubSub => "pubsub",
            SourceType::Pulsar => "pulsar",
            SourceType::Stdin => "stdin",
            SourceType::Unspecified => "unspecified",
            SourceType::Vec => "vec",
            SourceType::Void => "void",
        }
    }
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source_type_str = match self {
            SourceType::Cli => "CLI ingest",
            SourceType::File => "file",
            SourceType::IngestV1 => "ingest API v1",
            SourceType::IngestV2 => "ingest API v2",
            SourceType::Kafka => "Apache Kafka",
            SourceType::Kinesis => "Amazon Kinesis",
            SourceType::Nats => "NATS",
            SourceType::PubSub => "Google Cloud Pub/Sub",
            SourceType::Pulsar => "Apache Pulsar",
            SourceType::Stdin => "Stdin",
            SourceType::Unspecified => "unspecified",
            SourceType::Vec => "vec",
            SourceType::Void => "void",
        };
        write!(f, "{source_type_str}")
    }
}

impl IndexMetadataRequest {
    pub fn into_index_id(self) -> Option<IndexId> {
        self.index_uid
            .map(|index_uid| index_uid.index_id)
            .or(self.index_id)
    }

    pub fn for_index_id(index_id: IndexId) -> Self {
        Self {
            index_uid: None,
            index_id: Some(index_id),
        }
    }

    pub fn for_index_uid(index_uid: IndexUid) -> Self {
        Self {
            index_uid: Some(index_uid),
            index_id: None,
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
        serde_json::from_slice(value_bytes).map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }

    pub fn from_json_zstd<T: DeserializeOwned>(value_bytes: &[u8]) -> MetastoreResult<T> {
        let value_json = zstd::decode_all(value_bytes).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: std::any::type_name::<T>().to_string(),
                message: error.to_string(),
            }
        })?;
        serde_json::from_slice(&value_json).map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }

    pub fn from_json_str<'de, T: Deserialize<'de>>(value_str: &'de str) -> MetastoreResult<T> {
        serde_json::from_str(value_str).map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }

    pub fn from_json_value<T: DeserializeOwned>(value: JsonValue) -> MetastoreResult<T> {
        serde_json::from_value(value).map_err(|error| MetastoreError::JsonDeserializeError {
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

    pub fn to_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, MetastoreError> {
        serde_json::to_vec(value).map_err(|error| MetastoreError::JsonSerializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
        })
    }

    pub fn to_json_zstd<T: Serialize>(
        value: &T,
        compression_level: i32,
    ) -> Result<Vec<u8>, MetastoreError> {
        let value_json =
            serde_json::to_vec(value).map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: std::any::type_name::<T>().to_string(),
                message: error.to_string(),
            })?;
        zstd::encode_all(value_json.as_slice(), compression_level).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: std::any::type_name::<T>().to_string(),
                message: error.to_string(),
            }
        })
    }

    pub fn to_json_bytes_pretty<T: Serialize>(value: &T) -> Result<Vec<u8>, MetastoreError> {
        serde_json::to_vec_pretty(value).map_err(|error| MetastoreError::JsonSerializeError {
            struct_name: std::any::type_name::<T>().to_string(),
            message: error.to_string(),
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
