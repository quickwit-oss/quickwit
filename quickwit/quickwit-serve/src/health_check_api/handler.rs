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

use quickwit_actors::{Healthz, Mailbox};
use quickwit_cluster::Cluster;
use quickwit_indexing::IndexingService;
use quickwit_janitor::JanitorService;
use tracing::error;
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

use crate::rest::recover_fn;
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(paths(get_liveness, get_readiness))]
pub struct HealthCheckApi;

/// Health check handlers.
pub(crate) fn health_check_handlers(
    cluster: Cluster,
    indexer_service_opt: Option<Mailbox<IndexingService>>,
    janitor_service_opt: Option<Mailbox<JanitorService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    liveness_handler(indexer_service_opt, janitor_service_opt).or(readiness_handler(cluster))
}

fn liveness_handler(
    indexer_service_opt: Option<Mailbox<IndexingService>>,
    janitor_service_opt: Option<Mailbox<JanitorService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "livez")
        .and(warp::get())
        .and(with_arg(indexer_service_opt))
        .and(with_arg(janitor_service_opt))
        .then(get_liveness)
        .recover(recover_fn)
}

fn readiness_handler(
    cluster: Cluster,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "readyz")
        .and(warp::get())
        .and(with_arg(cluster))
        .then(get_readiness)
        .recover(recover_fn)
}

#[utoipa::path(
    get,
    tag = "Node Health",
    path = "/livez",
    responses(
        (status = 200, description = "The service is live.", body = bool),
        (status = 503, description = "The service is not live.", body = bool),
    ),
)]
/// Get Node Liveliness
async fn get_liveness(
    indexer_service_opt: Option<Mailbox<IndexingService>>,
    janitor_service_opt: Option<Mailbox<JanitorService>>,
) -> impl warp::Reply {
    let mut is_live = true;

    if let Some(indexer_service) = indexer_service_opt
        && !indexer_service.ask(Healthz).await.unwrap_or(false)
    {
        error!("indexer service is unhealthy");
        is_live = false;
    }
    if let Some(janitor_service) = janitor_service_opt
        && !janitor_service.ask(Healthz).await.unwrap_or(false)
    {
        error!("janitor service is unhealthy");
        is_live = false;
    }
    let status_code = if is_live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&is_live), status_code)
}

#[utoipa::path(
    get,
    tag = "Node Health",
    path = "/readyz",
    responses(
        (status = 200, description = "The service is ready.", body = bool),
        (status = 503, description = "The service is not ready.", body = bool),
    ),
)]
/// Get Node Readiness
async fn get_readiness(cluster: Cluster) -> impl warp::Reply {
    let is_ready = cluster.is_self_node_ready().await;
    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&is_ready), status_code)
}

#[cfg(test)]
mod tests {

    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};

    #[tokio::test]
    async fn test_rest_search_api_health_checks() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let health_check_handler = super::health_check_handlers(cluster.clone(), None, None);
        let resp = warp::test::request()
            .path("/health/livez")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 503);
        cluster.set_self_node_readiness(true).await;
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
