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

use hyper::header::CONTENT_TYPE;
use hyper::http::{status, HeaderValue};
use hyper::{Body, Response};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{self, Serialize};
use warp::Reply;

use crate::format::BodyFormat;

const JSON_SERIALIZATION_ERROR: &str = "JSON serialization failed.";

#[derive(Serialize)]
pub(crate) struct RestApiError {
    // For now, we want to keep [`RestApiError`] as simple as possible
    // and return just a message.
    #[serde(skip_serializing)]
    pub service_code: ServiceErrorCode,
    pub message: String,
}

impl ServiceError for RestApiError {
    fn error_code(&self) -> ServiceErrorCode {
        self.service_code
    }
}

impl ToString for RestApiError {
    fn to_string(&self) -> String {
        self.message.clone()
    }
}

/// Makes a JSON API response from a result.
/// The error is wrapped into an [`ApiError`] to publicly expose
/// a consistent error format.
pub(crate) fn into_rest_api_response<T: serde::Serialize, E: ServiceError>(
    result: Result<T, E>,
    body_format: BodyFormat,
) -> RestApiResponse {
    let rest_api_result = result.map_err(|error| RestApiError {
        service_code: error.error_code(),
        message: error.to_string(),
    });
    let status_code = match &rest_api_result {
        Ok(_) => status::StatusCode::OK,
        Err(error) => error.error_code().to_http_status_code(),
    };
    RestApiResponse::new(&rest_api_result, status_code, &body_format)
}

/// A JSON reply for the REST API.
pub struct RestApiResponse {
    status_code: status::StatusCode,
    inner: Result<Vec<u8>, ()>,
}

impl RestApiResponse {
    pub fn new<T: serde::Serialize, E: serde::Serialize>(
        result: &Result<T, E>,
        status_code: status::StatusCode,
        body_format: &BodyFormat,
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
                *response.status_mut() = self.status_code;
                response
            }
            Err(()) => warp::reply::json(&RestApiError {
                service_code: ServiceErrorCode::Internal,
                message: JSON_SERIALIZATION_ERROR.to_string(),
            })
            .into_response(),
        }
    }
}
