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

use std::io;

/// Errors encountered while processing an HTTP request
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// io error, usually retryable
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// timeout, usually retryable
    #[error("timeout after {0:?}: {1}")]
    Timeout(std::time::Duration, String),

    /// The request URI could not be parsed or was missing the host/scheme.
    #[error("invalid request URI: {0}")]
    InvalidUri(String),

    /// DNS error before sending the request. It's safe to retry a non idempotent
    /// operation after this error.
    #[error("dns resolution failed for `{host}`: {message}")]
    Dns { host: String, message: String },

    /// A TLS error
    #[error("tls error: {0}")]
    Tls(String),
}

impl HttpError {
    /// Returns `true` if the error represents a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, HttpError::Timeout(..))
    }

    /// Returns `true` when the error is an I/O failure
    pub fn is_io(&self) -> bool {
        matches!(self, HttpError::Io(..))
    }
}
