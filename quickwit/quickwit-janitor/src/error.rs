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
use quickwit_proto::metastore::MetastoreError;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Janitor errors.
#[allow(missing_docs)]
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum JanitorError {
    #[error("internal error: `{0}`")]
    Internal(String),
    #[error("invalid delete query: `{0}`")]
    InvalidDeleteQuery(String),
    #[error("metastore error: `{0}`")]
    Metastore(#[from] MetastoreError),
}

impl ServiceError for JanitorError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "janitor internal error {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::InvalidDeleteQuery(_) => ServiceErrorCode::BadRequest,
            Self::Metastore(metastore_error) => metastore_error.error_code(),
        }
    }
}
