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

use quickwit_actors::AskError;
use thiserror;

use crate::metastore::MetastoreError;
use crate::{ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.control_plane.rs");

pub type ControlPlaneResult<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("an internal error occurred: {0}")]
    Metastore(#[from] MetastoreError),
    #[error("control plane is unavailable: {0}")]
    Unavailable(String),
}

impl From<ControlPlaneError> for MetastoreError {
    fn from(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Internal(message) => MetastoreError::Internal {
                message: "todo".to_string(),
                cause: message,
            },
            ControlPlaneError::Metastore(error) => error,
            ControlPlaneError::Unavailable(message) => MetastoreError::Unavailable(message),
        }
    }
}

impl From<ControlPlaneError> for tonic::Status {
    fn from(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Internal(message) => tonic::Status::internal(message),
            ControlPlaneError::Metastore(error) => error.into(),
            ControlPlaneError::Unavailable(message) => tonic::Status::unavailable(message),
        }
    }
}

impl From<tonic::Status> for ControlPlaneError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => {
                ControlPlaneError::Unavailable(status.message().to_string())
            }
            _ => ControlPlaneError::Internal(status.message().to_string()),
        }
    }
}

impl From<AskError<ControlPlaneError>> for ControlPlaneError {
    fn from(error: AskError<ControlPlaneError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                ControlPlaneError::Unavailable("request not delivered".to_string())
            }
            AskError::ProcessMessageError => ControlPlaneError::Internal(
                "an error occurred while processing the request".to_string(),
            ),
        }
    }
}

impl ServiceError for ControlPlaneError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal { .. } => ServiceErrorCode::Internal,
            Self::Metastore(error) => error.status_code(),
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}
