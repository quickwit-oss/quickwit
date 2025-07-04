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

use axum::response::Json as AxumJson;
use axum::{Extension, Json};
use quickwit_config::NodeConfig;
use serde_json::{Value as JsonValue, json};

use crate::{BuildInfo, RuntimeInfo};

#[derive(utoipa::OpenApi)]
#[openapi(paths(get_version_axum, get_config_axum,))]
pub struct NodeInfoApi;

/// Create axum router for node info API
pub fn node_info_routes() -> axum::Router {
    axum::Router::new()
        .route("/version", axum::routing::get(get_version_axum))
        .route("/config", axum::routing::get(get_config_axum))
}

#[utoipa::path(
    get,
    tag = "Node Info",
    path = "/version",
    responses(
        (status = 200, description = "Successfully fetched node version information.")
    )
)]
/// Get node version information (axum version).
async fn get_version_axum(
    Extension(build_info): Extension<&'static BuildInfo>,
    Extension(runtime_info): Extension<&'static RuntimeInfo>,
) -> AxumJson<JsonValue> {
    AxumJson(json!({
        "build": build_info,
        "runtime": runtime_info,
    }))
}

#[utoipa::path(
    get,
    tag = "Node Info", 
    path = "/config",
    responses(
        (status = 200, description = "Successfully fetched node configuration.")
    )
)]
/// Get node configuration (axum version).
async fn get_config_axum(Extension(config): Extension<Arc<NodeConfig>>) -> Json<NodeConfig> {
    // We must redact sensitive information such as credentials.
    let mut config = (*config).clone();
    config.redact();
    Json(config)
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;
    use axum_test::TestServer;
    use quickwit_common::uri::Uri;
    use quickwit_config::NodeConfig;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::{BuildInfo, RuntimeInfo};

    #[tokio::test]
    async fn test_node_info_axum_version_handler() {
        let build_info = BuildInfo::get();
        let runtime_info = RuntimeInfo::get();

        // Create the axum app with extensions
        let app = node_info_routes()
            .layer(Extension(build_info))
            .layer(Extension(runtime_info));

        // Create test server
        let server = TestServer::new(app).unwrap();

        // Make a GET request to /version
        let response = server.get("/version").await;

        // Assert the response is successful
        response.assert_status_ok();

        // Check the response content type
        response.assert_header("content-type", "application/json");

        // Parse the response body
        let info_json: JsonValue = response.json();

        // Verify the response structure
        assert!(info_json.is_object());
        assert!(info_json.get("build").is_some());
        assert!(info_json.get("runtime").is_some());

        // Verify build info
        let build_info_json = info_json.get("build").unwrap();
        let expected_build_info_json = serde_json::json!({
            "commit_date": build_info.commit_date,
            "version": build_info.version,
        });
        assert_json_include!(actual: build_info_json, expected: expected_build_info_json);

        // Verify runtime info
        let runtime_info_json = info_json.get("runtime").unwrap();
        let expected_runtime_info_json = serde_json::json!({
            "num_cpus": runtime_info.num_cpus,
        });
        assert_json_include!(
            actual: runtime_info_json,
            expected: expected_runtime_info_json
        );
    }

    #[tokio::test]
    async fn test_node_info_axum_config_handler() {
        let mut config = NodeConfig::for_test();
        config.metastore_uri = Uri::for_test("postgresql://username:password@db");

        // Create the axum app with extensions
        let app = node_info_routes().layer(Extension(Arc::new(config.clone())));

        // Create test server
        let server = TestServer::new(app).unwrap();

        // Make a GET request to /config
        let response = server.get("/config").await;

        // Assert the response is successful
        response.assert_status_ok();

        // Check the response content type
        response.assert_header("content-type", "application/json");

        // Parse the response body
        let resp_json: JsonValue = response.json();

        // Verify the response structure and that sensitive data is redacted
        let expected_response_json = serde_json::json!({
            "node_id": config.node_id,
            "metastore_uri": "postgresql://username:***redacted***@db",
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_node_info_axum_handler_different_methods() {
        let build_info = BuildInfo::get();
        let runtime_info = RuntimeInfo::get();
        let config = Arc::new(NodeConfig::for_test());

        // Create the axum app with all extensions
        let app = node_info_routes()
            .layer(Extension(build_info))
            .layer(Extension(runtime_info))
            .layer(Extension(config));

        // Create test server
        let server = TestServer::new(app).unwrap();

        // Test that GET works for both endpoints
        let version_response = server.get("/version").await;
        version_response.assert_status_ok();

        let config_response = server.get("/config").await;
        config_response.assert_status_ok();

        // Test that POST is not allowed (should return 405 Method Not Allowed)
        let post_version_response = server.post("/version").await;
        post_version_response.assert_status(axum::http::StatusCode::METHOD_NOT_ALLOWED);

        let post_config_response = server.post("/config").await;
        post_config_response.assert_status(axum::http::StatusCode::METHOD_NOT_ALLOWED);

        // Test non-existent endpoint returns 404
        let not_found_response = server.get("/nonexistent").await;
        not_found_response.assert_status(axum::http::StatusCode::NOT_FOUND);
    }
}
