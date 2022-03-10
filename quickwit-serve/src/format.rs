// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use hyper::header::CONTENT_TYPE;
use hyper::StatusCode;
use serde::{self, Deserialize, Serialize};
use warp::reply::{self, WithHeader, WithStatus};

use crate::error::{ServiceError, ServiceErrorCode};

const JSON_SERIALIZATION_ERROR: &str = "JSON serialization failed.";

/// Output format for the search results.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    PrettyJson,
}

impl Default for Format {
    fn default() -> Self {
        Format::PrettyJson
    }
}

impl ToString for Format {
    fn to_string(&self) -> String {
        match &self {
            Self::Json => "json".to_string(),
            Self::PrettyJson => "prety-json".to_string(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct FormatError {
    #[serde(skip_serializing)]
    pub code: ServiceErrorCode,
    pub error: String,
}

impl ToString for FormatError {
    fn to_string(&self) -> String {
        self.error.clone()
    }
}

impl ServiceError for FormatError {
    fn status_code(&self) -> ServiceErrorCode {
        self.code
    }
}

impl Format {
    fn resp_body<T: serde::Serialize>(self, val: &T) -> serde_json::Result<String> {
        match self {
            Format::Json => serde_json::to_string(val),
            Format::PrettyJson => serde_json::to_string_pretty(&val),
        }
    }

    pub(crate) fn make_reply_for_err<E: crate::error::ServiceError + Serialize>(
        &self,
        err: E,
    ) -> WithStatus<WithHeader<String>> {
        let status_code: StatusCode = err.status_code().to_http_status_code();
        let body_json = self.resp_body(&err).unwrap_or_else(|_| {
            tracing::error!("Error: the response serialization failed.");
            "Error: Failed to serialize response.".to_string()
        });
        let reply_with_header = reply::with_header(body_json, CONTENT_TYPE, "application/json");
        reply::with_status(reply_with_header, status_code)
    }

    pub(crate) fn make_rest_reply<
        T: serde::Serialize,
        E: crate::error::ServiceError + Serialize,
    >(
        self,
        result: Result<T, E>,
    ) -> WithStatus<WithHeader<String>> {
        match result {
            Ok(success) => {
                let body_json_res = self.resp_body(&success);
                match body_json_res {
                    Ok(body_json) => {
                        let reply_with_header =
                            reply::with_header(body_json, CONTENT_TYPE, "application/json");
                        reply::with_status(reply_with_header, StatusCode::OK)
                    }
                    Err(_) => {
                        tracing::error!("Error: the response serialization failed.");
                        self.make_reply_for_err(FormatError {
                            code: ServiceErrorCode::Internal,
                            error: JSON_SERIALIZATION_ERROR.to_string(),
                        })
                    }
                }
            }
            Err(err) => self.make_reply_for_err(err),
        }
    }

    pub(crate) fn make_rest_reply_non_serializable_error<T, E>(
        self,
        result: Result<T, E>,
    ) -> WithStatus<WithHeader<String>>
    where
        T: serde::Serialize,
        E: ServiceError + ToString,
    {
        self.make_rest_reply(result.map_err(|err| FormatError {
            code: err.status_code(),
            error: err.to_string(),
        }))
    }
}
