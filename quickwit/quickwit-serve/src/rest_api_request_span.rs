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

use ::opentelemetry::global;
use ::opentelemetry::propagation::Extractor;
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// `HeaderMap` extracts OpenTelemetry tracing keys from HTTP headers.
struct HeaderMap<'a>(&'a http::HeaderMap);

impl Extractor for HeaderMap<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

/// Extracts an OpenTelemetry context from HTTP [`http::HeaderMap`].
fn extract_context_from_request_headers(headers: &http::HeaderMap) -> ::opentelemetry::Context {
    global::get_text_map_propagator(|prop| prop.extract(&HeaderMap(headers)))
}

/// Create a new OpenTelemetry [`Span`] for an incoming HTTP request.
pub(crate) fn make_http_request_span<B>(request: &http::Request<B>) -> tracing::Span {
    let span = tracing::span!(
        Level::INFO,
        "http_request",
        otel.kind = "Server",
        http.request.method = %request.method(),
        url.path = %request.uri().path()
    );
    if let Some(scheme) = request.uri().scheme_str() {
        span.set_attribute("url.scheme", scheme.to_string());
    };
    if let Some(query) = request.uri().query() {
        span.set_attribute("url.query", query.to_string());
    };
    // Best effort attempt to extract the server address from the incoming HTTP request
    // See: https://opentelemetry.io/docs/specs/semconv/http/http-spans/#setting-serveraddress-and-serverport-attributes
    if let Some(server_address) = request.uri().authority() {
        span.set_attribute("server.address", server_address.host().to_string());
        if let Some(port) = server_address.port_u16() {
            span.set_attribute("server.port", port as i64);
        }
    }
    if let Some(user_agent) = request.headers().get(http::header::USER_AGENT)
        && let Ok(user_agent_str) = user_agent.to_str()
    {
        span.set_attribute("user_agent.original", user_agent_str.to_string());
    };
    let ctx = extract_context_from_request_headers(request.headers());
    let _ = span.set_parent(ctx);
    span
}

pub(crate) fn set_status_code_on_request_span<B>(
    response: &http::Response<B>,
    _latency: std::time::Duration,
    span: &tracing::Span,
) {
    span.set_attribute(
        "http.response.status_code",
        response.status().as_u16() as i64,
    );
}

#[cfg(test)]
mod tests {
    use warp::hyper::{Method, http};

    use super::*;

    #[test]
    fn test_make_http_request_span() {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::INFO)
                .finish(),
        );
        let request = http::Request::builder()
            .method(Method::POST)
            .uri("http://quickwit:7280/api/v1/_elastic/otel-traces-v0_9/_search")
            .header(http::header::USER_AGENT, "test-agent/1.0")
            .body(r#"{"query":{"match_all":{}},"size":1}"#)
            .unwrap();
        let span = make_http_request_span(&request);
        assert!(!span.is_disabled());
    }
}
