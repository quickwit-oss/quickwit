/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

/// A service status.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ServiceStatus {
    /// The service is alive.
    Alive,

    /// The service is ready.
    Ready,

    /// The service is not ready.
    NotReady,

    /// The service is dead.
    Dead,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Make an HTTP response based on the given service status.
fn make_reply(ok: bool, service_status: ServiceStatus) -> impl warp::Reply {
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

/// The health check service implementation.
#[derive(Clone)]
pub struct HealthService {
    /// Status of the service.
    service_status: Arc<Mutex<ServiceStatus>>,
}

#[allow(dead_code)]
impl HealthService {
    /// Make service status alive.
    pub async fn alive(&self) {
        let mut state = self.service_status.lock().await;
        *state = ServiceStatus::Alive;
    }

    /// Make service status ready.
    pub async fn ready(&self) {
        let mut state = self.service_status.lock().await;
        *state = ServiceStatus::Ready;
    }

    /// Make service status not ready.
    pub async fn not_ready(&self) {
        let mut state = self.service_status.lock().await;
        *state = ServiceStatus::NotReady;
    }

    /// Make service status dead.
    pub async fn dead(&self) {
        let mut state = self.service_status.lock().await;
        *state = ServiceStatus::Dead;
    }
}

/// Check if the service is alive.
fn live_predicate(service_status: ServiceStatus) -> bool {
    match service_status {
        ServiceStatus::Alive => true,
        ServiceStatus::Ready => true,
        ServiceStatus::NotReady => true,
        ServiceStatus::Dead => false,
    }
}

/// Check if the service is ready.
fn ready_predicate(service_status: ServiceStatus) -> bool {
    matches!(service_status, ServiceStatus::Ready)
}

impl HealthService {
    /// Make a health check service.
    pub(crate) fn service(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
        let with_service_status = |service_status: Arc<Mutex<ServiceStatus>>| {
            warp::any().map(move || service_status.clone())
        };
        let health_endpoint = |endpoint: &'static str| {
            warp::path(endpoint)
                .and(warp::get())
                .and(with_service_status(self.service_status.clone()))
                .and_then(|service_status: Arc<Mutex<ServiceStatus>>| async move {
                    let service_status: ServiceStatus = *service_status.lock().await;
                    Ok::<ServiceStatus, Infallible>(service_status)
                })
        };
        let live_service = health_endpoint("livez").map(|service_status: ServiceStatus| {
            make_reply(live_predicate(service_status), service_status)
        });
        let ready_service = health_endpoint("readyz").map(|service_status: ServiceStatus| {
            make_reply(ready_predicate(service_status), service_status)
        });
        live_service.or(ready_service)
    }
}

impl Default for HealthService {
    /// Return default health service.
    fn default() -> HealthService {
        HealthService {
            service_status: Arc::new(Mutex::new(ServiceStatus::Alive)),
        }
    }
}
