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
use quickwit_common::rate_limited_error;
use quickwit_common::tower::{MakeLoadShedError, RpcName};
use thiserror;

use crate::metastore::{MetastoreError, OpenShardSubrequest};
use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.control_plane.rs");

pub type ControlPlaneResult<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("metastore error: {0}")]
    Metastore(#[from] MetastoreError),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl From<quickwit_common::tower::TaskCancelled> for ControlPlaneError {
    fn from(task_cancelled: quickwit_common::tower::TaskCancelled) -> Self {
        ControlPlaneError::Internal(task_cancelled.to_string())
    }
}

impl ServiceError for ControlPlaneError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(error_msg) => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "control plane internal error: {error_msg}"
                );
                ServiceErrorCode::Internal
            }
            Self::Metastore(metastore_error) => metastore_error.error_code(),
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for ControlPlaneError {
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

impl MakeLoadShedError for ControlPlaneError {
    fn make_load_shed_error() -> Self {
        Self::TooManyRequests
    }
}

impl From<ControlPlaneError> for MetastoreError {
    fn from(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Internal(message) => MetastoreError::Internal {
                message: "an internal metastore error occurred".to_string(),
                cause: message,
            },
            ControlPlaneError::Metastore(error) => error,
            ControlPlaneError::Timeout(message) => MetastoreError::Timeout(message),
            ControlPlaneError::TooManyRequests => MetastoreError::TooManyRequests,
            ControlPlaneError::Unavailable(message) => MetastoreError::Unavailable(message),
        }
    }
}

impl From<AskError<ControlPlaneError>> for ControlPlaneError {
    fn from(error: AskError<ControlPlaneError>) -> Self {
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

impl GetOrCreateOpenShardsFailureReason {
    pub fn create_failure(
        &self,
        subrequest: impl Into<GetOrCreateOpenShardsSubrequest>,
    ) -> GetOrCreateOpenShardsFailure {
        let subrequest = subrequest.into();

        GetOrCreateOpenShardsFailure {
            subrequest_id: subrequest.subrequest_id,
            index_id: subrequest.index_id,
            source_id: subrequest.source_id,
            reason: *self as i32,
        }
    }
}

impl From<crate::metastore::OpenShardSubrequest> for GetOrCreateOpenShardsSubrequest {
    fn from(metastore_open_shard_subrequest: OpenShardSubrequest) -> Self {
        let index_id = metastore_open_shard_subrequest.index_uid().index_id.clone();

        Self {
            subrequest_id: metastore_open_shard_subrequest.subrequest_id,
            index_id,
            source_id: metastore_open_shard_subrequest.source_id,
        }
    }
}
