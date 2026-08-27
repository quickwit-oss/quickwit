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

    /// The response head did not fit within the configured maximum head size.
    /// Not retryable.
    #[error("response head exceeded {0} bytes")]
    HeadTooLarge(usize),

    /// The response could not be parsed as HTTP/1.1.
    #[error("malformed HTTP/1.1 response: {0}")]
    Parse(#[from] httparse::Error),

    /// The response announced a body length we could not interpret
    /// (e.g. a negative `Content-Range`, a malformed `Content-Length`).
    #[error("invalid response length: {0}")]
    InvalidLength(String),

    /// The body ended before the expected number of bytes arrived.
    #[error("unexpected end of response body: read {read} of {expected} bytes")]
    UnexpectedEof { read: usize, expected: usize },

    /// A body frame produced by the request's `http_body::Body` failed.
    #[error("request body error: {0}")]
    Body(String),

    /// DNS error before sending the request. It's safe to retry a non idempotent
    /// operation after this error.
    #[error("dns resolution failed for `{host}`: {message}")]
    Dns { host: String, message: String },

    /// A TLS error
    #[error("tls error: {0}")]
    Tls(String),
}

impl From<std::convert::Infallible> for HttpError {
    fn from(err: std::convert::Infallible) -> Self {
        match err {}
    }
}

// we need this to accept request body from s3 sdk
impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for HttpError {
    fn from(err: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        HttpError::Body(err.to_string())
    }
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
