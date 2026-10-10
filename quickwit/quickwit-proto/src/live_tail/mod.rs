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

use quickwit_actors::AskError;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::{MakeLoadShedError, TimeoutExceeded};
use serde::{Deserialize, Serialize};

use crate::GrpcServiceError;
use crate::error::{ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.live_tail.rs");

pub const LIVE_TAIL_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/live_tail_descriptor.bin");

pub type LiveTailResult<T> = std::result::Result<T, LiveTailError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTailError {
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

impl From<TimeoutExceeded> for LiveTailError {
    fn from(_timeout_exceeded: TimeoutExceeded) -> Self {
        Self::Timeout("tower layer timeout".to_string())
    }
}

impl ServiceError for LiveTailError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "live tail error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for LiveTailError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for LiveTailError {
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

impl MakeLoadShedError for LiveTailError {
    fn make_load_shed_error() -> Self {
        Self::TooManyRequests
    }
}

impl From<AskError<LiveTailError>> for LiveTailError {
    fn from(error: AskError<LiveTailError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                Self::new_unavailable("request could not be delivered to actor".to_string())
            }
            AskError::ProcessMessageError => {
                Self::new_internal("an error occurred while processing the request".to_string())
            }
        }
    }
}
