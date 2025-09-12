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

use tracing::error;
use warp::hyper::StatusCode;
use warp::reply::with_status;

#[derive(utoipa::OpenApi)]
#[openapi(paths(metrics_handler))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have its own specific API group.
pub struct MetricsApi;

#[utoipa::path(
    get,
    tag = "Get Metrics",
    path = "/",
    responses(
        (status = 200, description = "Successfully fetched metrics.", body = String),
        (status = 500, description = "Metrics not available.", body = String),
    ),
)]
/// Get Node Metrics
///
/// These are in the form of prometheus metrics.
pub fn metrics_handler() -> impl warp::Reply {
    match quickwit_common::metrics::metrics_text_payload() {
        Ok(metrics) => with_status(metrics, StatusCode::OK),
        Err(e) => {
            error!("failed to encode prometheus metrics: {e}");
            with_status(String::new(), StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
