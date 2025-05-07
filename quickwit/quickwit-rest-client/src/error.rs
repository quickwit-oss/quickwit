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

use reqwest::StatusCode;
use reqwest_middleware::Error as MiddlewareError;
use serde::Deserialize;
use thiserror::Error;

pub static DEFAULT_ADDRESS: &str = "http://127.0.0.1:7280";
pub static DEFAULT_CONTENT_TYPE: &str = "application/json";

#[derive(Error, Debug)]
pub enum Error {
    // Error returned by Quickwit server.
    #[error("API error: {0}")]
    Api(#[from] ApiError),
    // Error returned by reqwest lib.
    #[error("client error: {0:?}")]
    Client(#[from] reqwest::Error),
    // IO Error returned by tokio lib.
    #[error("IO error: {0}")]
    Io(#[from] tokio::io::Error),
    // Internal error returned by quickwit client lib.
    #[error("internal Quickwit client error: {0}")]
    Internal(String),
    // Error returned by reqwest middleware.
    #[error("client middleware error: {0:?}")]
    Middleware(anyhow::Error),
    // Error returned by url lib when parsing a string.
    #[error("URL parsing error: {0}")]
    UrlParse(String),
}

impl Error {
    pub fn status_code(&self) -> Option<StatusCode> {
        match &self {
            Self::Api(error) => Some(error.code),
            Self::Client(error) => error.status(),
            Self::Internal(_) => Some(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Io(_) => Some(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Middleware(_) => Some(StatusCode::INTERNAL_SERVER_ERROR),
            Self::UrlParse(_) => Some(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<MiddlewareError> for Error {
    fn from(error: MiddlewareError) -> Self {
        match error {
            MiddlewareError::Middleware(error) => Error::Middleware(error),
            MiddlewareError::Reqwest(error) => Error::Client(error),
        }
    }
}

#[derive(Debug, Error)]
pub struct ApiError {
    pub message: Option<String>,
    pub code: StatusCode,
}

// Implement `Display` for `ApiError`.
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(error) = &self.message {
            write!(f, "(code={}, message={})", self.code, error)
        } else {
            write!(f, "(code={})", self.code)
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct ErrorResponsePayload {
    pub message: String,
}
