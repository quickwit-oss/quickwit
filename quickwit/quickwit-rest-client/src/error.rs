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
    #[error(transparent)]
    Client(#[from] reqwest::Error),
    // IO Error returned by tokio lib.
    #[error("IO error: {0}")]
    Io(#[from] tokio::io::Error),
    // Internal error returned by quickwit client lib.
    #[error("internal Quickwit client error: {0}")]
    Internal(String),
    // Json serialization/deserialization error.
    #[error("Serde JSON error: {0}")]
    Json(#[from] serde_json::error::Error),
    // Error returned by url lib when parsing a string.
    #[error("URL parsing error: {0}")]
    UrlParse(String),
}

impl Error {
    pub fn status_code(&self) -> Option<StatusCode> {
        match &self {
            Error::Client(err) => err.status(),
            Error::Api(err) => Some(err.code),
            _ => None,
        }
    }
}

#[derive(Error, Debug)]
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
