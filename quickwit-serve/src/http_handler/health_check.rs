// Copyright (C) 2021 Quickwit, Inc.
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

use std::fmt;

use serde::Serialize;
use serde_json::json;
use warp::http::header::{HeaderMap, HeaderValue};
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

/// A service status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ServiceStatus {
    /// The service is alive.
    Alive,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Liveness check handler.
pub fn liveness_check_handler() -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone
{
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    let service_status = ServiceStatus::Alive;

    warp::path!("health" / "livez")
        .map(move || make_reply(live_predicate(service_status), service_status))
        .with(warp::reply::with::headers(headers))
}

/// Make an HTTP response based on the given service status.
pub fn make_reply(ok: bool, service_status: ServiceStatus) -> impl warp::Reply {
    let mut status_code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let json = json!({ "service_status": service_status });

    let json_str = match serde_json::to_string(&json) {
        Ok(json) => json,
        Err(err) => {
            status_code = StatusCode::INTERNAL_SERVER_ERROR;
            json!({"error": err.to_string()}).to_string()
        }
    };

    with_status(json_str, status_code)
}

/// Check if the service is alive.
pub fn live_predicate(service_status: ServiceStatus) -> bool {
    matches!(service_status, ServiceStatus::Alive)
}

#[tokio::test]
async fn test_rest_search_api_health_check_livez() {
    let rest_search_api_filter = liveness_check_handler();
    let resp = warp::test::request()
        .path("/health/livez")
        .reply(&rest_search_api_filter)
        .await;
    assert_eq!(resp.status(), 200);
}
