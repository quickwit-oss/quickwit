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

use std::io;

use mrecordlog::error::*;
use quickwit_actors::AskError;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::BufferError;
pub(crate) use quickwit_proto::error::{grpc_error_to_grpc_status, grpc_status_to_service_error};
use quickwit_proto::ingest::router::{IngestFailure, IngestFailureReason};
use quickwit_proto::ingest::{IngestV2Error, RateLimitingCause};
use quickwit_proto::types::IndexId;
use quickwit_proto::{tonic, GrpcServiceError, ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum IngestServiceError {
    #[error("data corruption: {0}")]
    Corruption(String),
    #[error("index `{index_id}` already exists")]
    IndexAlreadyExists { index_id: IndexId },
    #[error("index `{index_id}` not found")]
    IndexNotFound { index_id: IndexId },
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("invalid position: {0}")]
    InvalidPosition(String),
    #[error("io error {0}")]
    IoError(String),
    #[error("rate limited {0}")]
    RateLimited(RateLimitingCause),
    #[error("ingest service is unavailable ({0})")]
    Unavailable(String),
    #[error("bad request ({0})")]
    BadRequest(String),
}

impl From<AskError<IngestServiceError>> for IngestServiceError {
    fn from(error: AskError<IngestServiceError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                IngestServiceError::Unavailable("actor not running".to_string())
            }
            AskError::ProcessMessageError => IngestServiceError::Internal(error.to_string()),
        }
    }
}

impl From<BufferError> for IngestServiceError {
    fn from(error: BufferError) -> Self {
        match error {
            BufferError::Closed => IngestServiceError::Unavailable(error.to_string()),
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
            IngestV2Error::Timeout(error_msg) => {
                IngestServiceError::Unavailable(format!("timeout {error_msg}"))
            }
            IngestV2Error::Unavailable(error_msg) => {
                IngestServiceError::Unavailable(format!("unavailable: {error_msg}"))
            }
            IngestV2Error::Internal(message) => IngestServiceError::Internal(message),
            IngestV2Error::ShardNotFound { .. } => {
                IngestServiceError::Internal("shard not found".to_string())
            }
            IngestV2Error::TooManyRequests(rate_limiting_cause) => {
                IngestServiceError::RateLimited(rate_limiting_cause)
            }
        }
    }
}

impl From<IngestFailure> for IngestServiceError {
    fn from(ingest_failure: IngestFailure) -> Self {
        match ingest_failure.reason() {
            IngestFailureReason::Unspecified => {
                IngestServiceError::Internal("unknown error".to_string())
            }
            IngestFailureReason::IndexNotFound => IngestServiceError::IndexNotFound {
                index_id: ingest_failure.index_id,
            },
            IngestFailureReason::SourceNotFound => IngestServiceError::Internal(format!(
                "Ingest v2 source not found for index {}",
                ingest_failure.index_id
            )),
            IngestFailureReason::Internal => {
                IngestServiceError::Internal("internal error".to_string())
            }
            IngestFailureReason::NoShardsAvailable => {
                IngestServiceError::Unavailable("no shards available".to_string())
            }
            IngestFailureReason::AttemptedShardsRateLimited => {
                IngestServiceError::RateLimited(RateLimitingCause::AttemptedShardsRateLimited)
            }
            IngestFailureReason::AllShardsRateLimited => {
                IngestServiceError::RateLimited(RateLimitingCause::AllShardsRateLimited)
            }
            IngestFailureReason::WalFull => {
                IngestServiceError::RateLimited(RateLimitingCause::WalFull)
            }
            IngestFailureReason::Timeout => {
                IngestServiceError::Internal("request timed out".to_string())
            }
            IngestFailureReason::RouterLoadShedding => {
                IngestServiceError::RateLimited(RateLimitingCause::RouterLoadShedding)
            }
            IngestFailureReason::LoadShedding => {
                IngestServiceError::RateLimited(RateLimitingCause::LoadShedding)
            }
            IngestFailureReason::CircuitBreaker => {
                IngestServiceError::RateLimited(RateLimitingCause::CircuitBreaker)
            }
        }
    }
}

impl ServiceError for IngestServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Corruption(err_msg) => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "ingest/corruption internal error: {err_msg}"
                );
                ServiceErrorCode::Internal
            }
            Self::IndexAlreadyExists { .. } => ServiceErrorCode::AlreadyExists,
            Self::IndexNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::InvalidPosition(_) => ServiceErrorCode::BadRequest,
            Self::IoError(io_err) => {
                rate_limited_error!(limit_per_min = 6, "ingest/io internal error: {io_err}");
                ServiceErrorCode::Internal
            }
            Self::RateLimited(_) => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::BadRequest(_) => ServiceErrorCode::BadRequest,
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

    fn new_too_many_requests() -> Self {
        Self::RateLimited(RateLimitingCause::Unknown)
    }

    fn new_unavailable(error_msg: String) -> Self {
        Self::Unavailable(error_msg)
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
            IngestServiceError::RateLimited(_) => tonic::Code::ResourceExhausted,
            IngestServiceError::Unavailable(_) => tonic::Code::Unavailable,
            IngestServiceError::BadRequest(_) => tonic::Code::InvalidArgument,
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
