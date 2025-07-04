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

use axum::Router;
use axum::response::IntoResponse;
use axum::routing::get;
use tracing::error;

#[derive(utoipa::OpenApi)]
#[openapi(paths(metrics_handler))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have its own specific API group.
pub struct MetricsApi;

// Axum routes
pub fn metrics_routes() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

#[utoipa::path(
    get,
    tag = "Get Metrics",
    path = "/",
    responses(
        (status = 200, description = "Successfully fetched metrics.", body = String),
        (status = 500, description = "Metrics not available.", body = String),
    ),
)]
async fn metrics_handler() -> impl IntoResponse {
    match quickwit_common::metrics::metrics_text_payload() {
        Ok(metrics) => (axum::http::StatusCode::OK, metrics),
        Err(e) => {
            error!("failed to encode prometheus metrics: {e}");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use axum_test::TestServer;

    use super::*;

    #[tokio::test]
    async fn test_metrics_handler_axum() {
        let app = metrics_routes();
        let server = TestServer::new(app).unwrap();

        let response = server.get("/metrics").await;

        // Should return 200 OK (the metrics system might not be initialized in tests,
        // but the handler should still return a successful response)
        assert_eq!(response.status_code(), axum::http::StatusCode::OK);

        // Should return text content (might be empty if no metrics are registered)
        let body = response.text();
        // Just verify we can get the response body without panicking
        let _ = body.len();
    }
}
