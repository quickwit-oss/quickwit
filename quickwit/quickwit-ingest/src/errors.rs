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

use std::io;

use mrecordlog::error::*;
use quickwit_actors::AskError;
use quickwit_common::tower::BufferError;
use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use serde::Serialize;

#[derive(Debug, Clone, thiserror::Error, Serialize)]
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
    #[error("the ingest service is unavailable")]
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

impl From<tonic::Status> for IngestServiceError {
    fn from(status: tonic::Status) -> Self {
        // TODO: Use status.details() #2859.
        match status.code() {
            tonic::Code::AlreadyExists => IngestServiceError::IndexAlreadyExists {
                index_id: status.message().to_string(),
            },
            tonic::Code::NotFound => IngestServiceError::IndexNotFound {
                index_id: status.message().to_string(),
            },
            tonic::Code::InvalidArgument => {
                IngestServiceError::InvalidPosition(status.message().to_string())
            }
            tonic::Code::ResourceExhausted => IngestServiceError::RateLimited,
            tonic::Code::Unavailable => IngestServiceError::Unavailable,
            _ => IngestServiceError::Internal(status.message().to_string()),
        }
    }
}

impl ServiceError for IngestServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            IngestServiceError::Corruption(_) => ServiceErrorCode::Internal,
            IngestServiceError::IndexAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            IngestServiceError::IndexNotFound { .. } => ServiceErrorCode::NotFound,
            IngestServiceError::Internal { .. } => ServiceErrorCode::Internal,
            IngestServiceError::InvalidPosition(_) => ServiceErrorCode::BadRequest,
            IngestServiceError::IoError { .. } => ServiceErrorCode::Internal,
            IngestServiceError::RateLimited => ServiceErrorCode::RateLimited,
            IngestServiceError::Unavailable => ServiceErrorCode::Internal,
        }
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
