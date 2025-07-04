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
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::get;
use quickwit_common::jemalloc_profiled::{start_profiling, stop_profiling};
use serde::Deserialize;

#[derive(Deserialize)]
struct ProfilerQueryParams {
    min_alloc_size: Option<u64>,
    backtrace_every: Option<u64>,
}

async fn start_profiler_handler(Query(params): Query<ProfilerQueryParams>) -> impl IntoResponse {
    start_profiling(params.min_alloc_size, params.backtrace_every);
    (axum::http::StatusCode::OK, "Heap profiling started")
}

async fn stop_profiler_handler() -> impl IntoResponse {
    stop_profiling();
    (axum::http::StatusCode::OK, "Heap profiling stopped")
}

/// Creates routes for heap profiling endpoints
pub(super) fn heap_prof_routes() -> Router {
    Router::new()
        .route("/heap-prof/start", get(start_profiler_handler))
        .route("/heap-prof/stop", get(stop_profiler_handler))
}

#[cfg(test)]
mod tests {
    use axum_test::TestServer;

    use super::*;

    #[tokio::test]
    async fn test_heap_prof_endpoints() {
        // Create test server with heap profiling routes
        let app = heap_prof_routes();
        let server = TestServer::new(app).unwrap();

        // Test start endpoint without parameters
        let response = server.get("/heap-prof/start").await;
        response.assert_status(axum::http::StatusCode::OK);
        response.assert_text("Heap profiling started");

        // Test start endpoint with parameters
        let response = server
            .get("/heap-prof/start?min_alloc_size=1024&backtrace_every=100")
            .await;
        response.assert_status(axum::http::StatusCode::OK);
        response.assert_text("Heap profiling started");

        // Test stop endpoint
        let response = server.get("/heap-prof/stop").await;
        response.assert_status(axum::http::StatusCode::OK);
        response.assert_text("Heap profiling stopped");
    }
}
