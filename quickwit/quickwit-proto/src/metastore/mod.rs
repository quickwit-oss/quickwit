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

use crate::{IndexId, QueueId, ServiceError, ServiceErrorCode, SourceId, SplitId};

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
        /// Source Id.
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
            EntityKind::Source { source_id } => write!(f, "source `{source_id}`"),
            EntityKind::Split { split_id } => write!(f, "split `{split_id}`"),
            EntityKind::Splits { split_ids } => write!(f, "splits `{}`", split_ids.join(", ")),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetastoreError {
    #[error("Database error: {message}.")]
    Db { message: String },

    #[error("Connection error: {message}.")]
    Connection { message: String },

    #[error("Precondition failed for {entity}: {message}")]
    FailedPrecondition { entity: EntityKind, message: String },

    #[error("{0} do(es) not exist.")]
    NotFound(EntityKind),

    #[error("{0} already exist(s).")]
    AlreadyExists(EntityKind),

    #[error("Access forbidden: {message}.")]
    Forbidden { message: String },

    #[error("Internal error: {message} Cause: `{cause}`.")]
    Internal { message: String, cause: String },

    #[error("IO error: {message}.")]
    Io { message: String },

    #[error("Failed to deserialize `{struct_name}` from JSON: {message}.")]
    JsonDeserializeError {
        struct_name: String,
        message: String,
    },

    #[error("Failed to serialize `{struct_name}` to JSON: {message}.")]
    JsonSerializeError {
        struct_name: String,
        message: String,
    },
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
        let grpc_code = metastore_error.status_code().to_grpc_status_code();
        let error_msg = serde_json::to_string(&metastore_error)
            .unwrap_or_else(|_| format!("Raw metastore error: {metastore_error}"));
        tonic::Status::new(grpc_code, error_msg)
    }
}

impl crate::ServiceError for MetastoreError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::Connection { .. } => ServiceErrorCode::Internal,
            Self::Db { .. } => ServiceErrorCode::Internal,
            Self::FailedPrecondition { .. } => ServiceErrorCode::BadRequest,
            Self::Forbidden { .. } => ServiceErrorCode::MethodNotAllowed,
            Self::AlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::NotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal { .. } => ServiceErrorCode::Internal,
            Self::Io { .. } => ServiceErrorCode::Internal,
            Self::JsonDeserializeError { .. } => ServiceErrorCode::Internal,
            Self::JsonSerializeError { .. } => ServiceErrorCode::Internal,
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
