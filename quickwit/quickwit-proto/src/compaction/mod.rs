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

use crate::GrpcServiceError;
use crate::error::{ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.compaction.rs");

pub const COMPACTION_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/compaction_descriptor.bin");

pub type CompactionResult<T> = std::result::Result<T, CompactionError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionError {
    #[error("{0}")]
    Internal(String),
}

impl ServiceError for CompactionError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "compaction error: {err_msg}");
                ServiceErrorCode::Internal
            }
        }
    }
}

// Required by the codegen tower layers. All four constructors are mandatory.
impl GrpcServiceError for CompactionError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }
    fn new_timeout(message: String) -> Self {
        Self::Internal(message)
    }
    fn new_too_many_requests() -> Self {
        Self::Internal("too many requests".to_string())
    }
    fn new_unavailable(message: String) -> Self {
        Self::Internal(message)
    }
}

impl MakeLoadShedError for CompactionError {
    fn make_load_shed_error() -> Self {
        CompactionError::Internal("too many requests".to_string())
    }
}