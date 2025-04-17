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

use quickwit_common::rate_limited_error;
use quickwit_common::tower::MakeLoadShedError;
use serde::{Deserialize, Serialize};
use thiserror;

use crate::GrpcServiceError;
use crate::error::{ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.cluster.rs");

pub const CLUSTER_PLANE_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/cluster_descriptor.bin");

pub type ClusterResult<T> = std::result::Result<T, ClusterError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for ClusterError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "cluster internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl GrpcServiceError for ClusterError {
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

impl MakeLoadShedError for ClusterError {
    fn make_load_shed_error() -> Self {
        ClusterError::TooManyRequests
    }
}
