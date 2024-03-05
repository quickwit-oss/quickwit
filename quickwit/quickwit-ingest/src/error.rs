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

use std::io;

use mrecordlog::error::*;
use quickwit_actors::AskError;
use quickwit_common::tower::BufferError;
pub(crate) use quickwit_proto::error::{grpc_error_to_grpc_status, grpc_status_to_service_error};
use quickwit_proto::ingest::IngestV2Error;
use quickwit_proto::{tonic, GrpcServiceError, ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum IngestServiceError {
    #[error("data corruption: {0}")]
    Corruption(String),
    #[error("index `{index_id}` already exists")]
    IndexAlreadyExists { index_id: String },
    #[error("index `{index_id}` not found")]
    IndexNotFound { index_id: String },
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("invalid position: {0}")]
    InvalidPosition(String),
    #[error("io error {0}")]
    IoError(String),
    #[error("rate limited")]
    RateLimited,
    #[error("ingest service is unavailable")]
    Unavailable,
}

impl From<AskError<IngestServiceError>> for IngestServiceError {
    fn from(error: AskError<IngestServiceError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => IngestServiceError::Unavailable,
            AskError::ProcessMessageError => IngestServiceError::Internal(error.to_string()),
        }
    }
}

impl From<BufferError> for IngestServiceError {
    fn from(error: BufferError) -> Self {
        match error {
            BufferError::Closed => IngestServiceError::Unavailable,
            BufferError::Unknown => IngestServiceError::Internal(error.to_string()),
        }
    }
}

impl From<io::Error> for IngestServiceError {
    fn from(io_error: io::Error) -> Self {
        IngestServiceError::IoError(io_error.to_string())
    }
}

impl From<IngestV2Error> for IngestServiceError {
    fn from(error: IngestV2Error) -> Self {
        match error {
            IngestV2Error::IngesterUnavailable { .. }
            | IngestV2Error::Timeout(_)
            | IngestV2Error::Unavailable(_) => IngestServiceError::Unavailable,
            IngestV2Error::Internal(message) => IngestServiceError::Internal(message),
            IngestV2Error::NotFound(_) => {
                IngestServiceError::Internal("shard not found".to_string())
            }
            IngestV2Error::TooManyRequests => IngestServiceError::RateLimited,
        }
    }
}

impl ServiceError for IngestServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Corruption { .. } => ServiceErrorCode::Internal,
            Self::IndexAlreadyExists { .. } => ServiceErrorCode::AlreadyExists,
            Self::IndexNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidPosition(_) => ServiceErrorCode::BadRequest,
            Self::IoError { .. } => ServiceErrorCode::Internal,
            Self::RateLimited => ServiceErrorCode::TooManyRequests,
            Self::Unavailable => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for IngestServiceError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_unavailable(_: String) -> Self {
        Self::Unavailable
    }
}

#[derive(Debug, thiserror::Error)]
#[error("key should contain 16 bytes, got {0}")]
pub struct CorruptedKey(pub usize);

impl From<CorruptedKey> for IngestServiceError {
    fn from(error: CorruptedKey) -> Self {
        IngestServiceError::Corruption(format!("corrupted key: {error:?}"))
    }
}

impl From<IngestServiceError> for tonic::Status {
    fn from(error: IngestServiceError) -> tonic::Status {
        let code = match &error {
            IngestServiceError::Corruption { .. } => tonic::Code::DataLoss,
            IngestServiceError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestServiceError::IndexNotFound { .. } => tonic::Code::NotFound,
            IngestServiceError::Internal(_) => tonic::Code::Internal,
            IngestServiceError::InvalidPosition(_) => tonic::Code::InvalidArgument,
            IngestServiceError::IoError { .. } => tonic::Code::Internal,
            IngestServiceError::RateLimited => tonic::Code::ResourceExhausted,
            IngestServiceError::Unavailable => tonic::Code::Unavailable,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

impl From<ReadRecordError> for IngestServiceError {
    fn from(error: ReadRecordError) -> IngestServiceError {
        match error {
            ReadRecordError::IoError(io_error) => io_error.into(),
            ReadRecordError::Corruption => {
                IngestServiceError::Corruption("failed to read record".to_string())
            }
        }
    }
}

impl From<AppendError> for IngestServiceError {
    fn from(err: AppendError) -> IngestServiceError {
        match err {
            AppendError::IoError(io_error) => io_error.into(),
            AppendError::MissingQueue(index_id) => IngestServiceError::IndexNotFound { index_id },
            // these errors can't be reached right now
            AppendError::Past => IngestServiceError::InvalidPosition(
                "attempted to append a record in the past".to_string(),
            ),
        }
    }
}

impl From<DeleteQueueError> for IngestServiceError {
    fn from(err: DeleteQueueError) -> IngestServiceError {
        match err {
            DeleteQueueError::IoError(io_error) => io_error.into(),
            DeleteQueueError::MissingQueue(index_id) => {
                IngestServiceError::IndexNotFound { index_id }
            }
        }
    }
}

impl From<TruncateError> for IngestServiceError {
    fn from(err: TruncateError) -> IngestServiceError {
        match err {
            TruncateError::IoError(io_error) => io_error.into(),
            TruncateError::MissingQueue(index_id) => IngestServiceError::IndexNotFound { index_id },
        }
    }
}
