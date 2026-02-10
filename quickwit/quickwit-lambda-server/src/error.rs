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

use std::fmt;

use quickwit_search::SearchError;

/// Result type for Lambda operations.
pub type LambdaResult<T> = Result<T, LambdaError>;

/// Errors that can occur during Lambda handler operations.
#[derive(Debug)]
pub enum LambdaError {
    /// Error serializing/deserializing protobuf.
    Serialization(String),
    /// Error from the search operation.
    Search(SearchError),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for LambdaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LambdaError::Serialization(msg) => write!(f, "serialization error: {}", msg),
            LambdaError::Search(err) => write!(f, "search error: {}", err),
            LambdaError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for LambdaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LambdaError::Search(err) => Some(err),
            _ => None,
        }
    }
}

impl From<SearchError> for LambdaError {
    fn from(err: SearchError) -> Self {
        LambdaError::Search(err)
    }
}

impl From<prost::DecodeError> for LambdaError {
    fn from(err: prost::DecodeError) -> Self {
        LambdaError::Serialization(format!("Protobuf decode error: {}", err))
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
