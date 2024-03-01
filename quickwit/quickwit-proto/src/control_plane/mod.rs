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

use quickwit_actors::AskError;
use quickwit_common::service_error::ProutServiceError;
use quickwit_common::tower::RpcName;
use thiserror;

use crate::metastore::MetastoreError;

include!("../codegen/quickwit/quickwit.control_plane.rs");

pub type ControlPlaneResult<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControlPlaneError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("metastore error: {0}")]
    Metastore(#[from] MetastoreError),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ProutServiceError for ControlPlaneError {
    fn grpc_status_code(&self) -> tonic::Code {
        match self {
            Self::Internal(_) => tonic::Code::Internal,
            Self::Metastore(error) => error.grpc_status_code(),
            Self::Timeout(_) => tonic::Code::DeadlineExceeded,
            Self::Unavailable(_) => tonic::Code::Unavailable,
        }
    }

    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }

    fn label_value(&self) -> &'static str {
        match self {
            Self::Internal(_) => "internal",
            Self::Metastore(error) => error.label_value(),
            Self::Timeout(_) => "timeout",
            Self::Unavailable(_) => "unavailable",
        }
    }
}

impl From<ControlPlaneError> for MetastoreError {
    fn from(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Internal(message) => MetastoreError::Internal(message),
            ControlPlaneError::Metastore(error) => error,
            ControlPlaneError::Timeout(message) => MetastoreError::Timeout(message),
            ControlPlaneError::Unavailable(message) => MetastoreError::Unavailable(message),
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

impl RpcName for GetOrCreateOpenShardsRequest {
    fn rpc_name() -> &'static str {
        "get_or_create_open_shards"
    }
}

impl RpcName for AdviseResetShardsRequest {
    fn rpc_name() -> &'static str {
        "advise_reset_shards"
    }
}

impl RpcName for GetDebugStateRequest {
    fn rpc_name() -> &'static str {
        "get_debug_state"
    }
}
