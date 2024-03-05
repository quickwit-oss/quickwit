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

use std::fmt;

use quickwit_actors::AskError;
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
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for HelloError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
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
