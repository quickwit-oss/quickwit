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

/// Result type for Lambda deployment operations.
pub type LambdaDeployResult<T> = Result<T, LambdaDeployError>;

/// Errors that can occur during Lambda function deployment.
#[derive(Debug, Error)]
pub enum LambdaDeployError {
    /// Resource conflict (e.g., function already exists during concurrent create).
    #[error("resource conflict: Lambda function already exists")]
    ResourceConflict,

    /// General deployment error.
    #[error("failed to deploy Lambda function: {0}")]
    Other(String),
}

/// Result type for Lambda invoker operations.
pub type InvokerResult<T> = Result<T, InvokerError>;

/// Errors that can occur during Lambda invoker setup or invocation.
#[derive(Debug, Error)]
pub enum InvokerError {
    /// Configuration or validation error.
    #[error("Lambda configuration error: {0}")]
    Configuration(String),

    /// Error during Lambda invocation.
    #[error("Lambda invocation failed: {0}")]
    Invocation(String),

    /// Error serializing/deserializing data.
    #[error("serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Errors that can occur during serialization/deserialization.
#[derive(Debug, Error)]
pub enum SerializationError {
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("protobuf encode error: {0}")]
    ProtobufEncode(#[from] prost::EncodeError),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<InvokerError> for SearchError {
    fn from(err: InvokerError) -> Self {
        SearchError::Internal(err.to_string())
    }
}
