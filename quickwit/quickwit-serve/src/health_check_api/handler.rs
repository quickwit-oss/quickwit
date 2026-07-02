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

use std::sync::Arc;

use quickwit_actors::Healthz;
use quickwit_ingest::try_get_ingester_status;
use quickwit_proto::ingest::ingester::{IngesterService, IngesterStatus};
use quickwit_proto::metastore::MetastoreService;
use tracing::error;
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

use crate::rest::recover_fn;
use crate::{QuickwitServices, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(get_liveness, get_readiness))]
pub struct HealthCheckApi;

/// Health check handlers.
pub(crate) fn health_check_handlers(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    liveness_handler(quickwit_services.clone()).or(readiness_handler(quickwit_services))
}

fn liveness_handler(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "livez")
        .and(warp::get())
        .and(with_arg(quickwit_services))
        .then(get_liveness)
        .recover(recover_fn)
}

fn readiness_handler(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "readyz")
        .and(warp::get())
        .and(with_arg(quickwit_services))
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
async fn get_liveness(quickwit_services: Arc<QuickwitServices>) -> impl warp::Reply {
    let mut is_live = true;

    if let Some(indexer_service) = &quickwit_services.indexing_service_opt
        && !indexer_service.ask(Healthz).await.unwrap_or(false)
    {
        error!("indexer service is unhealthy");
        is_live = false;
    }
    if let Some(janitor_service) = &quickwit_services.janitor_service_opt
        && !janitor_service.ask(Healthz).await.unwrap_or(false)
    {
        error!("janitor service is unhealthy");
        is_live = false;
    }
    if let Some(control_plane_service) = &quickwit_services.control_plane_server_opt
        && !control_plane_service.ask(Healthz).await.unwrap_or(false)
    {
        error!("control plane service is unhealthy");
        is_live = false;
    }
    if !ingester_is_live(&quickwit_services.ingester_opt).await {
        error!("ingester service is unhealthy");
        is_live = false;
    }
    if let Some(metastore_server) = &quickwit_services.metastore_server_opt
        && let Err(error) = metastore_server.check_connectivity().await
    {
        error!(%error, "metastore server is unhealthy");
        is_live = false;
    }
    let status_code = if is_live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&is_live), status_code)
}

/// Returns whether the ingester is live, or `true` if no ingester runs on this node. An ingester
/// is considered dead only when it reports a `Failed` status or its status cannot be retrieved.
async fn ingester_is_live(ingester_opt: &Option<impl IngesterService>) -> bool {
    let Some(ingester) = ingester_opt else {
        return true;
    };
    match try_get_ingester_status(ingester).await {
        Ok(IngesterStatus::Failed) => false,
        Ok(_) => true,
        Err(error) => {
            error!(%error, "failed to get ingester status");
            false
        }
    }
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
async fn get_readiness(quickwit_services: Arc<QuickwitServices>) -> impl warp::Reply {
    let is_ready = quickwit_services.cluster.is_self_node_ready().await;
    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&is_ready), status_code)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::{Actor, Mailbox, Universe};
    use quickwit_cluster::{ChitchatTransport, Cluster, create_cluster_for_test};
    use quickwit_common::ServiceStream;
    use quickwit_config::NodeConfig;
    use quickwit_control_plane::control_plane::ControlPlane;
    use quickwit_index_management::IndexService;
    use quickwit_indexing::IndexingService;
    use quickwit_ingest::{IngestApiService, IngestServiceClient};
    use quickwit_janitor::JanitorService;
    use quickwit_proto::control_plane::ControlPlaneServiceClient;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, IngesterStatus, MockIngesterService, ObservationMessage,
    };
    use quickwit_proto::ingest::router::IngestRouterServiceClient;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_search::MockSearchService;
    use quickwit_storage::StorageResolver;

    use super::{health_check_handlers, ingester_is_live};
    use crate::QuickwitServices;

    async fn test_cluster() -> Cluster {
        let transport = ChitchatTransport::default();
        create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap()
    }

    /// Builds a `QuickwitServices` in which every service is healthy or absent.
    fn healthy_services(cluster: Cluster) -> QuickwitServices {
        let metastore_client = MetastoreServiceClient::mocked();
        let index_manager =
            IndexService::new(metastore_client.clone(), StorageResolver::unconfigured());
        let universe = Universe::new();
        let (ingest_service_mailbox, _ingest_inbox) =
            universe.create_test_mailbox::<IngestApiService>();
        QuickwitServices {
            _report_splits_subscription_handle_opt: None,
            _local_shards_update_listener_handle_opt: None,
            cluster,
            control_plane_server_opt: None,
            control_plane_client: ControlPlaneServiceClient::mocked(),
            indexing_service_opt: None,
            index_manager,
            ingest_service: IngestServiceClient::from_mailbox(ingest_service_mailbox),
            ingest_router_opt: None,
            ingest_router_service: IngestRouterServiceClient::mocked(),
            ingester_opt: None,
            janitor_service_opt: None,
            otlp_logs_service_opt: None,
            otlp_traces_service_opt: None,
            metastore_client,
            metastore_server_opt: None,
            node_config: Arc::new(NodeConfig::for_test()),
            search_service: Arc::new(MockSearchService::new()),
            jaeger_service_opt: None,
            env_filter_reload_fn: crate::do_nothing_env_filter_reload_fn(),
            #[cfg(feature = "datafusion")]
            datafusion_session_builder: None,
        }
    }

    /// A mailbox whose actor never runs and whose inbox is dropped, so `ask` always fails.
    fn dead_mailbox<A: Actor>() -> Mailbox<A> {
        let universe = Universe::new();
        let (mailbox, inbox) = universe.create_test_mailbox::<A>();
        drop(inbox);
        mailbox
    }

    async fn livez_status(services: QuickwitServices) -> u16 {
        let handler = health_check_handlers(Arc::new(services));
        warp::test::request()
            .path("/health/livez")
            .reply(&handler)
            .await
            .status()
            .as_u16()
    }

    /// Builds an ingester whose observation stream reports a single `status` message.
    fn ingester_reporting(status: IngesterStatus) -> IngesterServiceClient {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .returning(move |_| {
                let (observation_tx, observation_stream) = ServiceStream::new_bounded(1);
                let message = ObservationMessage {
                    node_id: "test-ingester".to_string(),
                    status: status as i32,
                };
                observation_tx.try_send(Ok(message)).unwrap();
                Ok(observation_stream)
            });
        IngesterServiceClient::from_mock(mock_ingester)
    }

    #[tokio::test]
    async fn livez_succeeds_when_all_services_healthy() {
        let services = healthy_services(test_cluster().await);
        assert_eq!(livez_status(services).await, 200);
    }

    #[tokio::test]
    async fn livez_fails_when_indexer_unhealthy() {
        let mut services = healthy_services(test_cluster().await);
        services.indexing_service_opt = Some(dead_mailbox::<IndexingService>());
        assert_eq!(livez_status(services).await, 503);
    }

    #[tokio::test]
    async fn livez_fails_when_janitor_unhealthy() {
        let mut services = healthy_services(test_cluster().await);
        services.janitor_service_opt = Some(dead_mailbox::<JanitorService>());
        assert_eq!(livez_status(services).await, 503);
    }

    #[tokio::test]
    async fn livez_fails_when_control_plane_unhealthy() {
        let mut services = healthy_services(test_cluster().await);
        services.control_plane_server_opt = Some(dead_mailbox::<ControlPlane>());
        assert_eq!(livez_status(services).await, 503);
    }

    #[tokio::test]
    async fn livez_fails_when_metastore_unavailable() {
        let mut services = healthy_services(test_cluster().await);
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_check_connectivity()
            .returning(|| Err(anyhow::anyhow!("metastore unreachable")));
        services.metastore_server_opt = Some(MetastoreServiceClient::from_mock(mock_metastore));
        assert_eq!(livez_status(services).await, 503);
    }

    #[tokio::test]
    async fn ingester_is_live_when_status_ready() {
        let ingester = ingester_reporting(IngesterStatus::Ready);
        assert!(ingester_is_live(&Some(ingester)).await);
    }

    #[tokio::test]
    async fn ingester_is_live_when_absent() {
        let ingester_opt: Option<IngesterServiceClient> = None;
        assert!(ingester_is_live(&ingester_opt).await);
    }

    #[tokio::test]
    async fn ingester_is_not_live_when_status_failed() {
        let ingester = ingester_reporting(IngesterStatus::Failed);
        assert!(!ingester_is_live(&Some(ingester)).await);
    }

    #[tokio::test]
    async fn ingester_is_not_live_when_status_unavailable() {
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .returning(|_| {
                // dropping the sender ends the stream before any status is produced
                let (_observation_tx, observation_stream) = ServiceStream::new_bounded(1);
                Ok(observation_stream)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);
        assert!(!ingester_is_live(&Some(ingester)).await);
    }

    #[tokio::test]
    async fn readyz_reflects_cluster_readiness() {
        let cluster = test_cluster().await;
        let services = Arc::new(healthy_services(cluster.clone()));
        let handler = health_check_handlers(services);
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 503);
        cluster.set_self_node_readiness(true).await;
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
