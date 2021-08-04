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
use std::sync::Arc;
use std::{env, fmt};

use askama::Template;
use tokio::sync::Mutex;
use warp::hyper::StatusCode;
use warp::reply::{html, with_status};
use warp::{Filter, Rejection, Reply};

/// A service status.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStatus {
    /// The service is starting.
    Starting,

    /// The service is healthy.
    Healthy,

    /// The service is unhealth.
    Unhealthy,

    /// The service is dead.
    Dead,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The health check template.
#[derive(Template)]
#[template(path = "health_check.html")]
struct HealthTemplate<'a> {
    /// Name of the command that is running the service.
    command_name: &'a str,

    /// Arguments of the command that is running the service.
    command_args: Vec<String>,

    /// Status of the service.
    status: ServiceStatus,
}

/// Make an HTTP response based on the given service status.
fn make_reply(ok: bool, service_status: ServiceStatus) -> impl warp::Reply {
    let status_code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(service_status.to_string(), status_code)
}

/// The health check service implementation.
#[derive(Clone)]
pub struct HealthService {
    /// Status of the service.
    service_status: Arc<Mutex<ServiceStatus>>,
}

#[allow(dead_code)]
impl HealthService {
    /// Make service status healthy.
    pub async fn healty(&self) {
        let mut status = self.service_status.lock().await;
        *status = ServiceStatus::Healthy;
    }

    /// Make service status unhealthy.
    pub async fn unhealthy(&self) {
        let mut status = self.service_status.lock().await;
        *status = ServiceStatus::Unhealthy;
    }

    /// Make service status dead.
    pub async fn dead(&self) {
        let mut status = self.service_status.lock().await;
        *status = ServiceStatus::Dead;
    }
}

/// Return as service state HTML.
async fn health_check_handler(data: Arc<Mutex<ServiceStatus>>) -> Result<impl Reply, Infallible> {
    let status: ServiceStatus = *data.lock().await;
    let current_exe = env::current_exe()
        .map(|path| {
            path.file_name()
                .map(|filename| filename.to_string_lossy().to_string())
                .unwrap_or_else(|| "".to_string())
        })
        .unwrap_or_else(|_| "unknown-executable".to_string());
    let health_tmpl = HealthTemplate {
        command_name: &current_exe,
        status,
        command_args: env::args().into_iter().collect(),
    };
    let body = health_tmpl.render().unwrap();
    Ok(html(body))
}

/// Check if the service is ready.
fn ready_predicate(service_status: ServiceStatus) -> bool {
    match service_status {
        ServiceStatus::Starting => false,
        ServiceStatus::Healthy => true,
        ServiceStatus::Unhealthy => false,
        ServiceStatus::Dead => false,
    }
}

/// Check if the service is alive.
fn live_predicate(service_status: ServiceStatus) -> bool {
    match service_status {
        ServiceStatus::Starting => true, //< it should never be called
        ServiceStatus::Healthy => true,
        ServiceStatus::Unhealthy => true,
        ServiceStatus::Dead => false,
    }
}

/// Check if the service has been started.
fn start_predicate(service_status: ServiceStatus) -> bool {
    match service_status {
        ServiceStatus::Starting => true, //< it should never be called
        ServiceStatus::Healthy => true,
        ServiceStatus::Unhealthy => true,
        ServiceStatus::Dead => true, //< dead is started. Kub should not wait for the service startup probe to timeout.
    }
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
        let ready_service = health_endpoint("readyz").map(|service_status: ServiceStatus| {
            make_reply(ready_predicate(service_status), service_status)
        });
        let live_service = health_endpoint("livez").map(|service_status: ServiceStatus| {
            make_reply(live_predicate(service_status), service_status)
        });
        let start_service = health_endpoint("startz").map(|service_status: ServiceStatus| {
            make_reply(start_predicate(service_status), service_status)
        });
        let health_service = warp::path::end()
            .and(warp::get())
            .and(with_service_status(self.service_status.clone()))
            .and_then(health_check_handler);
        ready_service
            .or(live_service)
            .or(start_service)
            .or(health_service)
    }
}

impl Default for HealthService {
    /// Return default health service.
    fn default() -> HealthService {
        HealthService {
            service_status: Arc::new(Mutex::new(ServiceStatus::Starting)),
        }
    }
}
