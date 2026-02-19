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

use quickwit_search::SearchError;
use thiserror::Error;

/// Result type for Lambda operations.
pub type LambdaResult<T> = Result<T, LambdaError>;

/// Errors that can occur during Lambda handler operations.
#[derive(Debug, Error)]
pub enum LambdaError {
    /// Error serializing/deserializing protobuf.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Error from the search operation.
    #[error("search error: {0}")]
    Search(#[from] SearchError),
    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
    /// Task was cancelled.
    #[error("cancelled")]
    Cancelled,
}

impl From<prost::DecodeError> for LambdaError {
    fn from(err: prost::DecodeError) -> Self {
        LambdaError::Serialization(format!("protobuf decode error: {}", err))
    }
}

impl From<prost::EncodeError> for LambdaError {
    fn from(err: prost::EncodeError) -> Self {
        LambdaError::Serialization(format!("protobuf encode error: {}", err))
    }
}

impl From<base64::DecodeError> for LambdaError {
    fn from(err: base64::DecodeError) -> Self {
        LambdaError::Serialization(format!("base64 decode error: {}", err))
    }
}

impl From<serde_json::Error> for LambdaError {
    fn from(err: serde_json::Error) -> Self {
        LambdaError::Serialization(format!("json error: {}", err))
    }
}

impl From<LambdaError> for SearchError {
    fn from(err: LambdaError) -> Self {
        match err {
            LambdaError::Search(search_err) => search_err,
            other => SearchError::Internal(other.to_string()),
        }
    }
}
