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
use crate::endpoint::Endpoint;
use crate::error::HttpError;
use crate::pool::ConnectionPool;
use crate::request::{WriteState, write_request};
use crate::response::read_head;

/// Performs one request/response exchange over `conn` and returns the
/// streaming response.
///
/// `write_state` is updated as the request is written so the caller can decide,
/// on error, whether a retry is safe (the request must be both replayable and
/// uncommitted; see `client::retry_is_safe`).
///
/// Timeouts are per read/write call, not total amounts of time spent on either,
/// i.e. they can be relatively low do detect dead stream without tripping on
/// long upload. If the request allows it and ends cleanly, the connection is
/// passed back to `pool_hook`.
pub(crate) async fn exchange<B>(
    mut conn: ConnStream,
    request: &mut http::Request<B>,
    buffer_hint: BufferHint,
    pool_hook: Option<(ConnectionPool, Endpoint)>,
    write_state: &mut WriteState,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<http::Response<ResponseBody<ConnStream>>, HttpError>
where
    B: Body + Unpin,
    B::Error: Into<HttpError>,
{
    write_request(&mut conn, request, write_timeout, write_state).await?;
    let head = read_head(&mut conn, request.method(), read_timeout).await?;
    let pool_hook = pool_hook.map(|(pool, endpoint)| {
        Box::new(move |conn: ConnStream| {
            pool.release(&endpoint, conn);
        }) as Box<dyn FnOnce(ConnStream) + Send + 'static>
    });
    let body = ResponseBody::new(
        conn,
        head.body,
        head.leftover,
        buffer_hint,
        read_timeout,
        pool_hook,
        head.keep_alive,
    );
    let response = http::Response::from_parts(head.parts, body);
    Ok(response)
}
