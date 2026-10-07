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

use std::sync::Arc;

use http::StatusCode;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::MockSearchService;
use serde_json::{Value, json};
use warp::Filter;
use warp::reply::Response;
use warp::test::RequestBuilder;

use super::{BODY_LENGTH_LIMIT, mcp_api_handlers, protocol};

pub(super) fn request() -> RequestBuilder {
    warp::test::request()
        .method("POST")
        .path("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", protocol::PROTOCOL_VERSION)
}

fn routes(origins: &[&str]) -> impl Filter<Extract = (Response,), Error = warp::Rejection> + Clone {
    mcp_api_handlers(
        Arc::new(MockSearchService::new()),
        MetastoreServiceClient::mocked(),
        origins.iter().map(|origin| origin.to_string()).collect(),
    )
}

#[tokio::test]
async fn test_mcp_initialize_and_discovery() {
    let routes = routes(&[]);
    for offered_version in [protocol::PROTOCOL_VERSION, "unknown-future-version"] {
        let response = warp::test::request()
            .method("POST")
            .path("/mcp")
            .header("accept", "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0", "id": "init", "method": "initialize",
                "params": {"protocolVersion": offered_version, "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"}}
            }))
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key("mcp-session-id"));
        let body: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(body["id"], "init");
        assert_eq!(
            body["result"]["protocolVersion"],
            protocol::PROTOCOL_VERSION
        );
        assert_eq!(
            body["result"]["capabilities"],
            json!({"tools": {"listChanged": false}})
        );
    }
    let response = request()
        .json(&json!({"jsonrpc":"2.0", "id":2, "method":"tools/list"}))
        .reply(&routes)
        .await;
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 4);
    for (tool, name) in tools.iter().zip(super::tools::TOOL_NAMES) {
        assert_eq!(tool["name"], name);
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["inputSchema"]["type"], "object");
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
}

#[tokio::test]
async fn test_mcp_notifications_responses_and_ping() {
    let routes = routes(&[]);
    for message in [
        json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":42}}),
        json!({"jsonrpc":"2.0", "method":"unknown-notification"}),
        // No mock expectations: a tools/call notification must NOT execute the tool.
        json!({"jsonrpc":"2.0", "method":"tools/call", "params":{"name":"count", "arguments":{"index":"logs"}}}),
        json!({"jsonrpc":"2.0", "id":1, "result":{}}),
        json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32601,"message":"Not found"}}),
    ] {
        let response = request().json(&message).reply(&routes).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED, "{message}");
        assert!(response.body().is_empty());
    }
    let response = request()
        .json(&json!({"jsonrpc":"2.0", "id":0, "method":"ping"}))
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body, json!({"jsonrpc":"2.0", "id":0, "result":{}}));
}

#[tokio::test]
async fn test_mcp_invalid_envelopes() {
    let routes = routes(&[]);
    for message in [
        json!([]),
        json!([{"jsonrpc":"2.0","id":1,"method":"ping"}]),
        json!(null),
        json!({"id":1,"method":"ping"}),
        json!({"jsonrpc":"1.0","id":1,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":null,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":true,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":1.5,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":1,"method":1}),
        json!({"jsonrpc":"2.0","id":1,"method":"ping","params":[]}),
        json!({"jsonrpc":"2.0","id":1,"method":"ping","result":{}}),
        json!({"jsonrpc":"2.0","id":1,"error":{"message":"bad"}}),
        json!({"jsonrpc":"2.0","id":1,"result":{},"error":{}}),
    ] {
        let response = request().json(&message).reply(&routes).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{message}");
        let body: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(body["error"]["code"], -32600);
    }
    let response = request().body("{invalid").reply(&routes).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["error"]["code"], -32700);
}

#[tokio::test]
async fn test_mcp_protocol_errors() {
    let routes = routes(&[]);
    for (method, params, code) in [
        ("initialize", json!({}), -32602),
        ("resources/list", json!({}), -32601),
        ("tools/list", json!({"cursor":"unknown"}), -32602),
        ("tools/call", json!({}), -32602),
        ("tools/call", json!({"name":"delete_index"}), -32602),
    ] {
        let response = request()
            .json(&json!({"jsonrpc":"2.0","id":"request","method":method,"params":params}))
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(body["id"], "request");
        assert_eq!(body["error"]["code"], code);
    }
    for version in [None, Some("2025-03-26"), Some("bad")] {
        let mut request = warp::test::request()
            .method("POST")
            .path("/mcp")
            .header("accept", "application/json, text/event-stream");
        if let Some(version) = version {
            request = request.header("mcp-protocol-version", version);
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn test_mcp_transport_guards() {
    let routes = routes(&["https://console.example.com"]);
    let ping = json!({"jsonrpc":"2.0","id":1,"method":"ping"});
    let response = request()
        .header("origin", "https://console.example.com")
        .json(&ping)
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    for origin in [
        "null",
        "https://evil.example",
        "https://console.example.com.evil.example",
    ] {
        let response = request()
            .header("origin", origin)
            .json(&ping)
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for method in ["GET", "DELETE", "PUT", "PATCH", "HEAD"] {
        let response = warp::test::request()
            .method(method)
            .path("/mcp")
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()["allow"], "POST");
    }
    for accept in [
        "application/json",
        "text/event-stream",
        "*/*",
        "application/json, text/event-stream;q=0",
    ] {
        let response = request()
            .header("accept", accept)
            .json(&ping)
            .reply(&routes)
            .await;
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }
    let response = request()
        .header("content-type", "text/plain")
        .body(ping.to_string())
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let response = request()
        .body("x".repeat(BODY_LENGTH_LIMIT as usize + 1))
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response = request()
        .path("/api/v1/another-route")
        .json(&ping)
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_mcp_wildcard_cors_is_not_an_origin_allowlist() {
    let routes = routes(&["*"]);
    let response = request()
        .header("origin", "https://evil.example")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
        .reply(&routes)
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
