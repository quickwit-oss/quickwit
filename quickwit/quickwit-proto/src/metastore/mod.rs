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

use std::{fmt, io};

use quickwit_common::rate_limited_error;
use quickwit_common::retry::Retryable;
use quickwit_common::tower::{MakeLoadShedError, TimeoutExceeded};
use serde::{Deserialize, Serialize};

use crate::types::{IndexId, IndexUid, QueueId, SourceId};
use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

pub mod events;

include!("../codegen/quickwit/quickwit.metastore.rs");

pub const METASTORE_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/metastore_descriptor.bin");

pub type MetastoreResult<T> = Result<T, MetastoreError>;

const SPLIT_RECOVERY_METADATA_MAGIC: &[u8; 4] = b"QWSM";
const SPLIT_RECOVERY_METADATA_FORMAT_VERSION: u8 = 1;

impl SplitRecoveryMetadata {
    /// Serializes the recovery metadata with a small container header followed by protobuf bytes.
    /// The container version only covers framing; compatible protobuf fields can be added without
    /// changing it.
    pub fn serialize(&self) -> Vec<u8> {
        use prost::Message;

        let mut output = Vec::with_capacity(5 + self.encoded_len());
        output.extend_from_slice(SPLIT_RECOVERY_METADATA_MAGIC);
        output.push(SPLIT_RECOVERY_METADATA_FORMAT_VERSION);
        self.encode(&mut output)
            .expect("encoding a protobuf into a Vec should not fail");
        output
    }

    /// Deserializes split recovery metadata embedded in a split bundle.
    pub fn deserialize(mut bytes: &[u8]) -> io::Result<Self> {
        use prost::Message;

        if bytes.len() < 5 || &bytes[..4] != SPLIT_RECOVERY_METADATA_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid split recovery metadata magic number",
            ));
        }
        let version = bytes[4];
        if version != SPLIT_RECOVERY_METADATA_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported split recovery metadata format version: {version}"),
            ));
        }
        bytes = &bytes[5..];
        Self::decode(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

#[cfg(test)]
mod split_recovery_metadata_tests {
    use super::SplitRecoveryMetadata;
    use crate::types::{DocMappingUid, IndexUid};

    #[test]
    fn test_split_recovery_metadata_roundtrip_and_unknown_fields() {
        let metadata = SplitRecoveryMetadata {
            split_id: "split-a".to_string(),
            index_uid: Some(IndexUid::for_test("index-a", 1)),
            source_id: "source-a".to_string(),
            doc_mapping_uid: Some(DocMappingUid::for_test(2)),
            partition_id: 3,
            num_docs: 4,
            uncompressed_docs_size_bytes: 5,
            time_range_start_inclusive: Some(10),
            time_range_end_inclusive: Some(20),
            create_timestamp: 30,
            mature: true,
            maturation_period_secs: 40,
            tags: vec!["tenant!".to_string(), "tenant:acme".to_string()],
            delete_opstamp: 6,
            num_merge_ops: 7,
            parent_split_ids: vec!["parent-a".to_string(), "parent-b".to_string()],
        };
        let mut serialized = metadata.serialize();
        // An unknown protobuf varint field must be ignored by older readers.
        serialized.extend_from_slice(&[0x98, 0x06, 0x01]);

        let decoded = SplitRecoveryMetadata::deserialize(&serialized).unwrap();
        assert_eq!(decoded, metadata);
    }

    #[test]
    fn test_split_recovery_metadata_rejects_invalid_container_header() {
        let error = SplitRecoveryMetadata::deserialize(b"not-a-manifest").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}

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
        split_id: String,
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

    #[error("invalid publish token for shard `{queue_id}`")]
    InvalidPublishToken { queue_id: QueueId },

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
            | MetastoreError::InvalidPublishToken { .. }
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

impl From<crate::SortFieldsError> for MetastoreError {
    fn from(err: crate::SortFieldsError) -> Self {
        MetastoreError::Internal {
            message: "sort fields error".to_string(),
            cause: err.to_string(),
        }
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
            Self::InvalidPublishToken { .. } => ServiceErrorCode::BadRequest,
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
    pub fn new(
        index_uid: IndexUid,
        split_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            index_uid: index_uid.into(),
            split_ids: split_ids.into_iter().map(Into::into).collect(),
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

impl SplitStats {
    pub fn add_split(&mut self, size_bytes: u64) {
        self.num_splits += 1;
        self.total_size_bytes += size_bytes;
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
