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

//! Read-only MCP over stateless, JSON-only Streamable HTTP. No SSE streams or sessions are created.

mod protocol;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tool_tests;
mod tools;

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
use serde_json::Value;
use tools::Tools;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

const BODY_LENGTH_LIMIT: u64 = 1024 * 1024;

#[derive(Clone)]
struct Context {
    tools: Tools,
    allowed_origins: Vec<String>,
}

#[derive(Debug)]
struct TransportError(StatusCode, &'static str);
impl warp::reject::Reject for TransportError {}

pub(crate) fn mcp_api_handlers(
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
    allowed_origins: Vec<String>,
) -> impl Filter<Extract = (Response,), Error = Rejection> + Clone {
    let context = Context {
        tools: Tools {
            search_service,
            metastore,
        },
        allowed_origins,
    };
    // Recover inside the path filter: unrelated REST routes must still get a not-found rejection.
    warp::path!("mcp").and(
        warp::method()
            .and(warp::header::headers_cloned())
            .and(warp::any().map(move || context.clone()))
            .and_then(validate_transport)
            .and(warp::body::content_length_limit(BODY_LENGTH_LIMIT))
            .and(warp::body::bytes())
            .and_then(handle_post)
            .recover(recover)
            .unify(),
    )
}

async fn validate_transport(
    method: Method,
    headers: HeaderMap,
    context: Context,
) -> Result<(HeaderMap, Context), Rejection> {
    if headers.contains_key("origin") {
        let origins: Vec<_> = headers.get_all("origin").iter().collect();
        let origin = origins[0].to_str().unwrap_or("");
        // Unlike general-purpose CORS, MCP requires origin validation against DNS rebinding.
        // A wildcard CORS configuration is NOT an MCP origin allowlist. Non-browser clients
        // normally omit Origin; browser clients require an explicit configured origin.
        if origins.len() != 1
            || origin.is_empty()
            || origin == "null"
            || !context
                .allowed_origins
                .iter()
                .any(|allowed| allowed != "*" && allowed == origin)
        {
            return Err(warp::reject::custom(TransportError(
                StatusCode::FORBIDDEN,
                "Origin not allowed",
            )));
        }
    }
    if method != Method::POST {
        return Err(warp::reject::custom(TransportError(
            StatusCode::METHOD_NOT_ALLOWED,
            "Only POST is supported",
        )));
    }
    let content_type = match headers.get("content-type") {
        Some(value) => value.to_str().unwrap_or(""),
        None => "",
    };
    if !content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case("application/json")
    {
        return Err(warp::reject::custom(TransportError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        )));
    }
    // Streamable HTTP clients must advertise both representations, even though this server
    // chooses JSON for every response and does not offer a GET SSE stream.
    if !accepts(&headers, "application/json") || !accepts(&headers, "text/event-stream") {
        return Err(warp::reject::custom(TransportError(
            StatusCode::NOT_ACCEPTABLE,
            "Accept must include application/json and text/event-stream",
        )));
    }
    if headers.contains_key("mcp-protocol-version") {
        let versions: Vec<_> = headers.get_all("mcp-protocol-version").iter().collect();
        if versions.len() != 1 || versions[0] != protocol::PROTOCOL_VERSION {
            return Err(warp::reject::custom(TransportError(
                StatusCode::BAD_REQUEST,
                "Unsupported MCP-Protocol-Version",
            )));
        }
    }
    Ok((headers, context))
}

fn accepts(headers: &HeaderMap, media_type: &str) -> bool {
    headers.get_all("accept").iter().any(|header| {
        header.to_str().unwrap_or("").split(',').any(|entry| {
            let mut parts = entry.split(';');
            if !parts
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case(media_type)
            {
                return false;
            }
            parts.all(|param| {
                let Some((name, value)) = param.trim().split_once('=') else {
                    return true;
                };
                !name.eq_ignore_ascii_case("q")
                    || matches!(value.trim().parse::<f32>(), Ok(quality) if quality > 0.0 && quality <= 1.0)
            })
        })
    })
}

async fn handle_post(
    (headers, context): (HeaderMap, Context),
    body: Bytes,
) -> Result<Response, Infallible> {
    if body.len() as u64 > BODY_LENGTH_LIMIT {
        return Ok(transport_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Request body too large",
        ));
    }
    let version_opt = match headers.get("mcp-protocol-version") {
        Some(value) => value.to_str().ok(),
        None => None,
    };
    let (status, body_opt) = protocol::handle(&body, version_opt, &context.tools).await;
    let response = match body_opt {
        Some(body) => warp::reply::with_status(warp::reply::json(&body), status).into_response(),
        None => status.into_response(),
    };
    Ok(warp::reply::with_header(response, "cache-control", "no-store").into_response())
}

fn transport_error(status: StatusCode, message: &str) -> Response {
    let body = protocol::error(Value::Null, -32600, message);
    let mut response = warp::reply::with_status(warp::reply::json(&body), status).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert("allow", "POST".parse().unwrap());
    }
    response
}

async fn recover(rejection: Rejection) -> Result<Response, Infallible> {
    let (status, message) = if let Some(error) = rejection.find::<TransportError>() {
        (error.0, error.1)
    } else if rejection.find::<warp::reject::PayloadTooLarge>().is_some() {
        (StatusCode::PAYLOAD_TOO_LARGE, "Request body too large")
    } else if rejection.find::<warp::reject::LengthRequired>().is_some() {
        (StatusCode::LENGTH_REQUIRED, "Content-Length is required")
    } else {
        (StatusCode::BAD_REQUEST, "Invalid HTTP request body")
    };
    Ok(transport_error(status, message))
}
