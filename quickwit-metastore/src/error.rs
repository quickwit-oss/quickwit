// Copyright (C) 2022 Quickwit, Inc.
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

use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};
#[cfg(feature = "postgres")]
use thiserror::Error;

use crate::checkpoint::IncompatibleCheckpointDelta;

/// Metastore error kinds.
#[allow(missing_docs)]
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum MetastoreError {
    #[error("Connection error: `{message}`.")]
    ConnectionError { message: String },

    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },

    #[error("Access forbidden: `{message}`.")]
    Forbidden { message: String },

    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },

    /// Any generic internal error.
    /// The message can be helpful to users, but the detail of the error
    /// are judged uncoverable and not useful for error handling.
    #[error("Internal error: `{message}` Cause: `{cause}`.")]
    InternalError { message: String, cause: String },

    #[error("Failed to deserialize index metadata: `{message}`")]
    InvalidManifest { message: String },

    #[error("IOError `{message}`")]
    Io { message: String },

    #[error("Splits `{split_ids:?}` do not exist.")]
    SplitsDoNotExist { split_ids: Vec<String> },

    #[error("Splits `{split_ids:?}` are not in a deletable state.")]
    SplitsNotDeletable { split_ids: Vec<String> },

    #[error("Splits `{split_ids:?}` are not staged.")]
    SplitsNotStaged { split_ids: Vec<String> },

    #[error("Publish checkpoint delta overlaps with the current checkpoint: {0:?}.")]
    IncompatibleCheckpointDelta(#[from] IncompatibleCheckpointDelta),

    #[error("Source `{source_id}` of type `{source_type}` already exists.")]
    SourceAlreadyExists {
        source_id: String,
        source_type: String,
    },

    #[error("Source `{source_id}` does not exist.")]
    SourceDoesNotExist { source_id: String },

    #[error("Database error: `{message}`.")]
    DbError { message: String },

    #[error("Cannot parse `{name}` from json string: `{message}`.")]
    JsonDeserializeError { name: String, message: String },

    #[error("Cannot serialize in json entiy `{name}`: `{message}`.")]
    JsonSerializeError { name: String, message: String },
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for MetastoreError {
    fn from(error: sqlx::Error) -> Self {
        MetastoreError::DbError {
            message: error.to_string(),
        }
    }
}

impl From<MetastoreError> for quickwit_proto::tonic::Status {
    fn from(metastore_error: MetastoreError) -> Self {
        let grpc_code = metastore_error.status_code().to_grpc_status_code();
        let error_msg = serde_json::to_string(&metastore_error)
            .unwrap_or_else(|_| format!("Raw metastore error: {}", metastore_error));
        quickwit_proto::tonic::Status::new(grpc_code, error_msg)
    }
}

impl ServiceError for MetastoreError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::ConnectionError { .. } => ServiceErrorCode::Internal,
            Self::Forbidden { .. } => ServiceErrorCode::Internal,
            Self::IncompatibleCheckpointDelta(_) => ServiceErrorCode::BadRequest,
            Self::IndexAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::IndexDoesNotExist { .. } => ServiceErrorCode::BadRequest,
            Self::InternalError { .. } => ServiceErrorCode::Internal,
            Self::InvalidManifest { .. } => ServiceErrorCode::Internal,
            Self::Io { .. } => ServiceErrorCode::Internal,
            Self::SourceAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::SourceDoesNotExist { .. } => ServiceErrorCode::BadRequest,
            Self::SplitsDoNotExist { .. } => ServiceErrorCode::BadRequest,
            Self::SplitsNotDeletable { .. } => ServiceErrorCode::BadRequest,
            Self::SplitsNotStaged { .. } => ServiceErrorCode::BadRequest,
            Self::DbError { .. } => ServiceErrorCode::Internal,
            Self::JsonDeserializeError { .. } => ServiceErrorCode::Internal,
            Self::JsonSerializeError { .. } => ServiceErrorCode::Internal,
        }
    }
}

/// Generic Result type for metastore operations.
pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Generic Storage Resolver Error.
#[derive(Error, Debug)]
pub enum MetastoreResolverError {
    /// The input is not a valid URI.
    /// A protocol is required for the URI.
    #[error("Invalid URI format: required: `{0}`")]
    InvalidUri(String),

    /// The protocol is not supported by this resolver.
    #[error("Unsupported protocol: `{0}`")]
    ProtocolUnsupported(String),

    /// The URI is valid, and is meant to be handled by this resolver,
    /// but the resolver failed to actually connect to the storage.
    /// e.g. Connection error, credential error, incompatible version,
    /// internal error in third party, etc.
    #[error("Failed to connect to metastore: `{0}`")]
    FailedToOpenMetastore(MetastoreError),
}
