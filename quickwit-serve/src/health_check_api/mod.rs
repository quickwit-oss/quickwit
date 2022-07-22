// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyper::header::CONTENT_TYPE;
use quickwit_metastore::Metastore;
use quickwit_storage::Storage;
use serde::Serialize;
use serde_json::Value;
use tokio::time::timeout;
use warp::http::header::{HeaderMap, HeaderValue};
use warp::hyper::StatusCode;
use warp::{reply, Filter, Rejection};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[async_trait]
trait Probeable: Send + Sync + 'static {
    fn service_name(&self) -> &str;

    async fn livez(&self) -> bool {
        true
    }

    async fn readyz(&self) -> bool {
        true
    }
}

struct Probeables {
    metastore: Arc<dyn Metastore>,
    storage: Arc<dyn Storage>,
    services: Vec<Arc<dyn Probeable>>,
}

async fn livez_probe(probeables: Arc<Probeables>) -> Result<impl warp::Reply, Infallible> {
    let mut livez_probes = HashMap::new();

    // TODO: Evaluate futures concurrently.
    for service in &probeables.services {
        let livez_probe = timeout(PROBE_TIMEOUT, service.livez())
            .await
            .unwrap_or(false);
        livez_probes.insert(service.service_name(), livez_probe);
    }
    let status_code = if livez_probes.values().all(|livez_probe| *livez_probe) {
        warp::http::StatusCode::OK
    } else {
        warp::http::StatusCode::SERVICE_UNAVAILABLE
    };
    Ok(reply::with_status(reply::json(&livez_probes), status_code))
}

/// Liveness probe handler.
pub(crate) fn liveness_probe_handler(
    probeables: Arc<Probeables>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("health" / "livez")
        .and(warp::get())
        .map(move || probeables.clone())
        .and_then(livez_probe)
}

// TODO
async fn readyz_probe(probeables: Arc<Probeables>) -> Result<impl warp::Reply, Infallible> {
    let mut readyz_probes: HashMap<&str, bool> = HashMap::new();
    let status_code = warp::http::StatusCode::OK;
    Ok(reply::with_status(reply::json(&readyz_probes), status_code))
}

/// Readiness probe handler.
pub(crate) fn readiness_probe_handler(
    probeables: Arc<Probeables>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("health" / "readyz")
        .and(warp::get())
        .map(move || probeables.clone())
        .and_then(readyz_probe)
}
