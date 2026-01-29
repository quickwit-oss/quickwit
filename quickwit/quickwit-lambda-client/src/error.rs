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

/// Result type for Lambda client operations.
pub type LambdaClientResult<T> = Result<T, LambdaClientError>;

/// Errors that can occur during Lambda client operations.
#[derive(Debug)]
pub enum LambdaClientError {
    /// Error during Lambda invocation.
    Invocation(String),
    /// Error serializing/deserializing protobuf.
    Serialization(String),
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

impl fmt::Display for LambdaClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LambdaClientError::Invocation(msg) => write!(f, "Lambda invocation error: {}", msg),
            LambdaClientError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            LambdaClientError::FunctionError(msg) => write!(f, "Lambda function error: {}", msg),
            LambdaClientError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            LambdaClientError::Internal(msg) => write!(f, "Internal error: {}", msg),
            LambdaClientError::ResourceConflict => {
                write!(f, "Resource conflict: function already exists")
            }
            LambdaClientError::Deployment(msg) => write!(f, "Deployment error: {}", msg),
            LambdaClientError::NotFound(name) => write!(f, "Lambda function not found: {}", name),
        }
    }
}

impl std::error::Error for LambdaClientError {}

impl From<prost::DecodeError> for LambdaClientError {
    fn from(err: prost::DecodeError) -> Self {
        LambdaClientError::Serialization(format!("Protobuf decode error: {}", err))
    }
}

impl From<prost::EncodeError> for LambdaClientError {
    fn from(err: prost::EncodeError) -> Self {
        LambdaClientError::Serialization(format!("Protobuf encode error: {}", err))
    }
}

impl From<base64::DecodeError> for LambdaClientError {
    fn from(err: base64::DecodeError) -> Self {
        LambdaClientError::Serialization(format!("Base64 decode error: {}", err))
    }
}

impl From<serde_json::Error> for LambdaClientError {
    fn from(err: serde_json::Error) -> Self {
        LambdaClientError::Serialization(format!("JSON error: {}", err))
    }
}

impl From<LambdaClientError> for SearchError {
    fn from(err: LambdaClientError) -> Self {
        SearchError::Internal(err.to_string())
    }
}
