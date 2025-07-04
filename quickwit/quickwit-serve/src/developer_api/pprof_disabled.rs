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

async fn not_implemented_handler() -> impl IntoResponse {
    (
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "Quickwit was compiled without the `pprof` feature",
    )
}

/// pprof/start disabled
/// pprof/flamegraph disabled
pub(super) fn pprof_routes() -> Router {
    Router::new()
        .route("/pprof/start", get(not_implemented_handler))
        .route("/pprof/flamegraph", get(not_implemented_handler))
}
