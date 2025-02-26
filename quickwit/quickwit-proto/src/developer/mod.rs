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

use thiserror;

use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.developer.rs");

pub const DEVELOPER_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/developer_descriptor.bin");

pub type DeveloperResult<T> = std::result::Result<T, DeveloperError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperError {
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

impl ServiceError for DeveloperError {
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

impl GrpcServiceError for DeveloperError {
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
