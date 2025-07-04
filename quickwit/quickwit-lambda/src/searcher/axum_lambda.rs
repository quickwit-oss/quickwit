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

// Based on https://github.com/aslamplr/warp_lambda under MIT license
// Adapted for axum

use std::collections::HashSet;

use axum::Router;
pub use lambda_http;
use lambda_http::http::HeaderValue;
use lambda_http::{Body as LambdaBody, Error as LambdaError, run};
use mime_guess::{Mime, mime};
use once_cell::sync::Lazy;

#[allow(dead_code)]
static PLAINTEXT_MIMES: Lazy<HashSet<Mime>> = Lazy::new(|| {
    HashSet::from_iter([
        mime::APPLICATION_JAVASCRIPT,
        mime::APPLICATION_JAVASCRIPT_UTF_8,
        mime::APPLICATION_JSON,
    ])
});

pub async fn run_axum(app: Router) -> Result<(), LambdaError> {
    // Note: TraceLayer removed due to version conflicts between axum versions
    run(app).await
}

#[allow(dead_code)]
async fn lambda_body_to_axum_body(
    lambda_body: LambdaBody,
) -> Result<axum::body::Body, LambdaError> {
    let bytes = match lambda_body {
        LambdaBody::Empty => bytes::Bytes::new(),
        LambdaBody::Text(text) => bytes::Bytes::from(text.into_bytes()),
        LambdaBody::Binary(bytes) => bytes::Bytes::from(bytes),
    };
    Ok(axum::body::Body::from(bytes))
}

#[allow(dead_code)]
async fn axum_body_to_lambda_body(
    parts: &lambda_http::http::response::Parts,
    axum_body: axum::body::Body,
) -> Result<LambdaBody, LambdaError> {
    // Concatenate all bytes into a single buffer
    let body_bytes = axum::body::to_bytes(axum_body, usize::MAX).await?.to_vec();

    // Attempt to determine the Content-Type
    let content_type_opt: Option<&HeaderValue> = parts.headers.get("Content-Type");
    let content_encoding_opt: Option<&HeaderValue> = parts.headers.get("Content-Encoding");

    // If Content-Encoding is present, assume compression
    // If Content-Type is not present, don't assume is a string
    if let (Some(content_type), None) = (content_type_opt, content_encoding_opt) {
        let content_type_str = content_type.to_str()?;
        let mime = content_type_str.parse::<Mime>()?;

        if PLAINTEXT_MIMES.contains(&mime) || mime.type_() == mime::TEXT {
            return Ok(LambdaBody::Text(String::from_utf8(body_bytes)?));
        }
    }
    // Not a text response, make binary
    Ok(LambdaBody::Binary(body_bytes))
}
