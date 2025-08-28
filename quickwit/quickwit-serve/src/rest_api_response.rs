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

use quickwit_proto::ServiceError;
use serde::{self, Serialize};
use warp::Reply;
use warp::hyper::StatusCode;
use warp::hyper::header::CONTENT_TYPE;
use warp::hyper::http::HeaderValue;

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
    fn into_response(self) -> warp::reply::Response {
        match self.inner {
            Ok(body) => {
                let mut response = warp::reply::Response::new(body.into());
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                *response.status_mut() = self.status_code;
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
