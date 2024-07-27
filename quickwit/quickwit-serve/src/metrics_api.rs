// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use tracing::error;
use warp::reply::with_status;

#[derive(utoipa::OpenApi)]
#[openapi(paths(metrics_handler))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have it's own specific API group.
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
        Ok(metrics) => with_status(metrics, warp::http::StatusCode::OK),
        Err(e) => {
            error!("failed to encode prometheus metrics: {e}");
            with_status(String::new(), warp::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
