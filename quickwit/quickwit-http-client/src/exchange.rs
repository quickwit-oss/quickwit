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

//! One HTTP/1.1 exchange: write the request, read the response head, and
//! assemble an `http::Response<ResponseBody>` that streams the body.
//!
//! The connection is moved into the `ResponseBody`, which owns it for the
//! duration of the body read.

use std::time::Duration;

use http_body::Body;

use crate::body::{BufferHint, ResponseBody};
use crate::connection::ConnStream;
use crate::error::HttpError;
use crate::request::write_request;
use crate::response::read_head;

/// Performs one request/response exchange over `conn` and returns the
/// streaming response. The connection is owned by the returned body.
///
/// Timeouts are per read/write call, not total amounts of time spent on either,
/// i.e. they can be relatively low do detect dead stream without tripping on
/// long upload
pub async fn exchange<B>(
    mut conn: ConnStream,
    request: &mut http::Request<B>,
    buffer_hint: BufferHint,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<http::Response<ResponseBody<ConnStream>>, HttpError>
where
    B: Body + Unpin,
    B::Error: Into<HttpError>,
{
    write_request(&mut conn, request, write_timeout).await?;
    let head = read_head(&mut conn, read_timeout).await?;
    let body = ResponseBody::new(conn, head.body, head.leftover, buffer_hint, read_timeout);
    let response = http::Response::from_parts(head.parts, body);
    Ok(response)
}
