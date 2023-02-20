// Copyright (C) 2023 Quickwit, Inc.
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

use std::convert::Infallible;

use hyper::header::CONTENT_TYPE;
use hyper::StatusCode;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{self, Deserialize, Serialize, Serializer};
use warp::reply::{self, WithHeader, WithStatus};
use warp::{Filter, Rejection, Reply};

const JSON_SERIALIZATION_ERROR: &str = "JSON serialization failed.";

/// Output format for the search results.
#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Copy, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
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
            Self::PrettyJson => "pretty_json".to_string(),
        }
    }
}

impl Serialize for Format {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

/// This struct represents a QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, Eq, PartialEq, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct FormatQueryString {
    /// The output format requested.
    #[serde(default)]
    pub format: Format,
}

pub(crate) fn qs_format() -> impl Filter<Extract = (Format,), Error = Rejection> + Clone {
    serde_qs::warp::query::<FormatQueryString>(serde_qs::Config::default()).and_then(
        |format_qs: FormatQueryString| async move { Result::<_, Infallible>::Ok(format_qs.format) },
    )
}

pub(crate) fn format_response<T: Serialize, E: ServiceError + ToString>(
    result: Result<T, E>,
    format: Format,
) -> impl Reply {
    format.make_rest_reply(result)
}

#[derive(Serialize)]
pub(crate) struct ApiError {
    #[serde(skip_serializing)]
    pub code: ServiceErrorCode,
    pub message: String,
}

impl ToString for ApiError {
    fn to_string(&self) -> String {
        self.message.clone()
    }
}

impl ServiceError for ApiError {
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

    pub(crate) fn make_reply_for_err<E: ServiceError + Serialize>(
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

    fn internal_make_rest_reply<T: serde::Serialize>(
        self,
        result: Result<T, ApiError>,
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
                        self.make_reply_for_err(ApiError {
                            code: ServiceErrorCode::Internal,
                            message: JSON_SERIALIZATION_ERROR.to_string(),
                        })
                    }
                }
            }
            Err(err) => self.make_reply_for_err(err),
        }
    }

    pub(crate) fn make_rest_reply<T, E>(
        self,
        result: Result<T, E>,
    ) -> WithStatus<WithHeader<String>>
    where
        T: serde::Serialize,
        E: ServiceError + ToString,
    {
        self.internal_make_rest_reply(result.map_err(|err| ApiError {
            code: err.status_code(),
            message: err.to_string(),
        }))
    }
}
