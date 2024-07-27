// Copyright (C) 2024 Quickwit, Inc.
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

use http_serde::http::StatusCode;
use hyper::header::CONTENT_TYPE;
use hyper::http::HeaderValue;
use hyper::{Body, Response};
use quickwit_proto::ServiceError;
use serde::{self, Serialize};
use warp::Reply;

use crate::format::BodyFormat;

const JSON_SERIALIZATION_ERROR: &str = "JSON serialization failed.";

#[derive(Serialize)]
pub(crate) struct RestApiError {
    // For now, we want to keep [`RestApiError`] as simple as possible
    // and return just a message.
    #[serde(skip_serializing)]
    pub status_code: StatusCode,
    pub message: String,
}

/// Makes a JSON API response from a result.
/// The error is wrapped into an [`RestApiError`] to publicly expose
/// a consistent error format.
pub(crate) fn into_rest_api_response<T: serde::Serialize, E: ServiceError>(
    result: Result<T, E>,
    body_format: BodyFormat,
) -> RestApiResponse {
    let rest_api_result = result.map_err(|error| RestApiError {
        status_code: error.error_code().http_status_code(),
        message: error.to_string(),
    });
    let status_code = match &rest_api_result {
        Ok(_) => StatusCode::OK,
        Err(error) => error.status_code,
    };
    RestApiResponse::new(&rest_api_result, status_code, body_format)
}

/// A JSON reply for the REST API.
pub struct RestApiResponse {
    status_code: StatusCode,
    inner: Result<Vec<u8>, ()>,
}

impl RestApiResponse {
    pub fn new<T: serde::Serialize, E: serde::Serialize>(
        result: &Result<T, E>,
        status_code: StatusCode,
        body_format: BodyFormat,
    ) -> Self {
        let inner = body_format.result_to_vec(result);
        RestApiResponse { status_code, inner }
    }
}

impl Reply for RestApiResponse {
    #[inline]
    fn into_response(self) -> Response<Body> {
        match self.inner {
            Ok(body) => {
                let mut response = Response::new(body.into());
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                *response.status_mut() =
                    hyper::StatusCode::from_u16(self.status_code.as_u16()).expect("cannot fail");
                response
            }
            Err(()) => {
                quickwit_common::rate_limited_error!(
                    limit_per_min = 10,
                    "REST body json serialization error."
                );
                warp::reply::json(&RestApiError {
                    status_code: StatusCode::INTERNAL_SERVER_ERROR,
                    message: JSON_SERIALIZATION_ERROR.to_string(),
                })
                .into_response()
            }
        }
    }
}
