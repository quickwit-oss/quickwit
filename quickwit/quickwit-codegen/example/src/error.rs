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

use quickwit_actors::AskError;
use quickwit_common::tower::TimeoutExceeded;
use quickwit_proto::error::GrpcServiceError;
pub use quickwit_proto::error::{grpc_error_to_grpc_status, grpc_status_to_service_error};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};

// Service errors have to be handwritten before codegen.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum HelloError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for HelloError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for HelloError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl<E> From<AskError<E>> for HelloError
where E: fmt::Debug
{
    fn from(error: AskError<E>) -> Self {
        HelloError::Internal(format!("{error:?}"))
    }
}

impl From<TimeoutExceeded> for HelloError {
    fn from(_: TimeoutExceeded) -> Self {
        HelloError::Timeout("client".to_string())
    }
}
