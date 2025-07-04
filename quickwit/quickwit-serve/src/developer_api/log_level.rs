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

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use serde::Deserialize;
use tracing::{error, info};

use crate::EnvFilterReloadFn;

#[derive(Deserialize)]
struct EnvFilter {
    filter: String,
}

/// Creates routes for log level endpoints
pub(super) fn log_level_routes() -> Router {
    Router::new().route("/log-level", get(log_level_handler).post(log_level_handler))
}

/// Dynamically Quickwit's log level
#[utoipa::path(get, tag = "Debug", path = "/log-level")]
async fn log_level_handler(
    Extension(env_filter_reload_fn): Extension<EnvFilterReloadFn>,
    Query(env_filter): Query<EnvFilter>,
) -> impl IntoResponse {
    match env_filter_reload_fn(&env_filter.filter) {
        Ok(_) => {
            info!(filter = env_filter.filter, "setting log level");
            (
                axum::http::StatusCode::OK,
                format!("set log level to:[{}]", env_filter.filter),
            )
        }
        Err(_) => {
            error!(filter = env_filter.filter, "invalid log level");
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid log level:[{}]", env_filter.filter),
            )
        }
    }
}
