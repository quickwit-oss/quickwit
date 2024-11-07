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

use thiserror;

use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.developer.rs");

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
