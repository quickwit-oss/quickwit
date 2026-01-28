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

/// Errors that can occur during Lambda operations.
#[derive(Debug)]
pub enum LambdaError {
    /// Error during Lambda invocation.
    Invocation(String),
    /// Error serializing/deserializing protobuf.
    Serialization(String),
    /// Error from the search operation.
    Search(SearchError),
    /// Lambda function returned an error.
    FunctionError(String),
    /// Configuration error.
    Configuration(String),
    /// Internal error.
    Internal(String),
    /// Resource conflict (e.g., function already exists during concurrent create).
    ResourceConflict,
    /// Error during Lambda function deployment.
    Deployment(String),
    /// Lambda function not found.
    NotFound(String),
}

impl fmt::Display for LambdaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LambdaError::Invocation(msg) => write!(f, "Lambda invocation error: {}", msg),
            LambdaError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            LambdaError::Search(err) => write!(f, "Search error: {}", err),
            LambdaError::FunctionError(msg) => write!(f, "Lambda function error: {}", msg),
            LambdaError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            LambdaError::Internal(msg) => write!(f, "Internal error: {}", msg),
            LambdaError::ResourceConflict => write!(f, "Resource conflict: function already exists"),
            LambdaError::Deployment(msg) => write!(f, "Deployment error: {}", msg),
            LambdaError::NotFound(name) => write!(f, "Lambda function not found: {}", name),
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
        LambdaError::Serialization(format!("Protobuf encode error: {}", err))
    }
}

impl From<base64::DecodeError> for LambdaError {
    fn from(err: base64::DecodeError) -> Self {
        LambdaError::Serialization(format!("Base64 decode error: {}", err))
    }
}

impl From<serde_json::Error> for LambdaError {
    fn from(err: serde_json::Error) -> Self {
        LambdaError::Serialization(format!("JSON error: {}", err))
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
