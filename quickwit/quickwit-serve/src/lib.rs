// Copyright (C) 2023 Quickwit, Inc.
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

mod build_info;
mod cluster_api;
mod delete_task_api;
mod elastic_search_api;
mod format;
mod grpc;
mod health_check_api;
mod index_api;
mod indexing_api;
mod ingest_api;
mod jaeger_api;
mod json_api_response;
mod metrics;
mod metrics_api;
mod node_info_handler;
mod openapi;
mod otlp_api;
mod rate_modulator;
mod rest;
mod search_api;
pub(crate) mod simple_list;
mod ui_handler;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytesize::ByteSize;
pub use format::BodyFormat;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, Mailbox, Universe};
use quickwit_cluster::{
    start_cluster_service, Cluster, ClusterChange, ClusterMember, ListenerHandle,
};
use quickwit_common::pubsub::{EventBroker, EventSubscriptionHandle};
use quickwit_common::rate_limiter::RateLimiterSettings;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::tower::{
    BalanceChannel, BoxFutureInfaillible, BufferLayer, Change, ConstantRate, EstimateRateLayer,
    EventListenerLayer, RateLimitLayer, RetryLayer, RetryPolicy, SmaRateEstimator,
};
use quickwit_config::service::QuickwitService;
use quickwit_config::NodeConfig;
use quickwit_control_plane::control_plane::{ControlPlane, ControlPlaneEventSubscriber};
use quickwit_control_plane::{IndexerNodeInfo, IndexerPool};
use quickwit_index_management::{IndexService as IndexManager, IndexServiceError};
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::models::ShardPositionsService;
use quickwit_indexing::start_indexing_service;
use quickwit_ingest::{
    setup_local_shards_update_listener, start_ingest_api_service, wait_for_ingester_decommission,
    GetMemoryCapacity, IngestApiService, IngestRequest, IngestRouter, IngestServiceClient,
    Ingester, IngesterPool, LocalShardsUpdate,
};
use quickwit_jaeger::JaegerService;
use quickwit_janitor::{start_janitor_service, JanitorService};
use quickwit_metastore::{
    ControlPlaneMetastore, ListIndexesMetadataResponseExt, MetastoreResolver,
};
use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
use quickwit_proto::control_plane::ControlPlaneServiceClient;
use quickwit_proto::indexing::{IndexingServiceClient, ShardPositionsUpdate};
use quickwit_proto::ingest::ingester::IngesterServiceClient;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::metastore::{
    EntityKind, ListIndexesMetadataRequest, MetastoreError, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::search::ReportSplitsRequest;
use quickwit_proto::types::NodeId;
use quickwit_search::{
    create_search_client_from_channel, start_searcher_service, SearchJobPlacer, SearchService,
    SearchServiceClient, SearcherContext, SearcherPool,
};
use quickwit_storage::{SplitCache, StorageResolver};
use tokio::sync::oneshot;
use tower::timeout::Timeout;
use tower::ServiceBuilder;
use tracing::{debug, error, info, warn};
use warp::{Filter, Rejection};

pub use crate::build_info::{BuildInfo, RuntimeInfo};
pub use crate::index_api::{ListSplitsQueryParams, ListSplitsResponse};
pub use crate::metrics::SERVE_METRICS;
use crate::rate_modulator::RateModulator;
#[cfg(test)]
use crate::rest::recover_fn;
pub use crate::search_api::{search_request_from_api_request, SearchRequestQueryString, SortBy};

const READINESS_REPORTING_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(25)
} else {
    Duration::from_secs(10)
};

struct QuickwitServices {
    pub node_config: Arc<NodeConfig>,
    pub cluster: Cluster,
    pub metastore_server_opt: Option<MetastoreServiceClient>,
    pub metastore_client: MetastoreServiceClient,
    pub control_plane_service: ControlPlaneServiceClient,
    pub index_manager: IndexManager,
    pub indexing_service_opt: Option<Mailbox<IndexingService>>,
    // Ingest v1
    pub ingest_service: IngestServiceClient,
    // Ingest v2
    pub ingest_router_service: IngestRouterServiceClient,
    pub ingester_service_opt: Option<IngesterServiceClient>,
    pub janitor_service_opt: Option<Mailbox<JanitorService>>,
    pub jaeger_service_opt: Option<JaegerService>,
    pub otlp_logs_service_opt: Option<OtlpGrpcLogsService>,
    pub otlp_traces_service_opt: Option<OtlpGrpcTracesService>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,

    /// The control plane listens to various events.
    /// We must maintain a reference to the subscription handles to continue receiving
    /// notifications. Otherwise, the subscriptions are dropped.
    _local_shards_update_listener_handle_opt: Option<ListenerHandle>,
    _report_splits_subscription_handle_opt: Option<EventSubscriptionHandle>,
}

fn has_node_with_metastore_service(members: &[ClusterMember]) -> bool {
    members.iter().any(|member| {
        member
            .enabled_services
            .contains(&QuickwitService::Metastore)
    })
}

async fn balance_channel_for_service(
    cluster: &Cluster,
    service: QuickwitService,
) -> BalanceChannel<SocketAddr> {
    let cluster_change_stream = cluster.ready_nodes_change_stream().await;
    let service_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) if node.enabled_services().contains(&service) => {
                    Some(Change::Insert(node.grpc_advertise_addr(), node.channel()))
                }
                ClusterChange::Remove(node) => Some(Change::Remove(node.grpc_advertise_addr())),
                _ => None,
            }
        })
    });
    BalanceChannel::from_stream(service_change_stream)
}

async fn start_ingest_client_if_needed(
    node_config: &NodeConfig,
    universe: &Universe,
    cluster: &Cluster,
) -> anyhow::Result<IngestServiceClient> {
    if node_config.is_service_enabled(QuickwitService::Indexer) {
        let ingest_api_service = start_ingest_api_service(
            universe,
            &node_config.data_dir_path,
            &node_config.ingest_api_config,
        )
        .await?;
        let num_buckets = NonZeroUsize::new(60).expect("60 should be non-zero");
        let initial_rate = ConstantRate::new(ByteSize::mib(50).as_u64(), Duration::from_secs(1));
        let rate_estimator = SmaRateEstimator::new(
            num_buckets,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .with_initial_rate(initial_rate);
        let memory_capacity = ingest_api_service.ask(GetMemoryCapacity).await?;
        let min_rate = ConstantRate::new(ByteSize::mib(1).as_u64(), Duration::from_millis(100));
        let rate_modulator = RateModulator::new(rate_estimator.clone(), memory_capacity, min_rate);
        let ingest_service = IngestServiceClient::tower()
            .stack_ingest_layer(
                ServiceBuilder::new()
                    .layer(EstimateRateLayer::<IngestRequest, _>::new(rate_estimator))
                    .layer(BufferLayer::new(100))
                    .layer(RateLimitLayer::new(rate_modulator))
                    .into_inner(),
            )
            .build_from_mailbox(ingest_api_service);
        Ok(ingest_service)
    } else {
        let balance_channel = balance_channel_for_service(cluster, QuickwitService::Indexer).await;
        let ingest_service = IngestServiceClient::from_balance_channel(balance_channel);
        Ok(ingest_service)
    }
}

async fn start_control_plane_if_needed(
    node_config: &NodeConfig,
    cluster: &Cluster,
    event_broker: &EventBroker,
    metastore_client: &MetastoreServiceClient,
    universe: &Universe,
    indexer_pool: &IndexerPool,
    ingester_pool: &IngesterPool,
) -> anyhow::Result<ControlPlaneServiceClient> {
    if node_config.is_service_enabled(QuickwitService::ControlPlane) {
        check_cluster_configuration(
            &node_config.enabled_services,
            &node_config.peer_seeds,
            metastore_client.clone(),
        )
        .await?;

        let cluster_id = cluster.cluster_id().to_string();
        let self_node_id: NodeId = cluster.self_node_id().into();

        let replication_factor = node_config
            .ingest_api_config
            .replication_factor()
            .expect("replication factor should have been validated")
            .get();
        let control_plane_mailbox = setup_control_plane(
            universe,
            event_broker,
            cluster_id,
            self_node_id,
            indexer_pool.clone(),
            ingester_pool.clone(),
            metastore_client.clone(),
            replication_factor,
        )
        .await?;
        Ok(ControlPlaneServiceClient::from_mailbox(
            control_plane_mailbox,
        ))
    } else {
        let balance_channel =
            balance_channel_for_service(cluster, QuickwitService::ControlPlane).await;
        Ok(ControlPlaneServiceClient::from_balance_channel(
            balance_channel,
        ))
    }
}

pub async fn serve_quickwit(
    node_config: NodeConfig,
    runtimes_config: RuntimesConfig,
    metastore_resolver: MetastoreResolver,
    storage_resolver: StorageResolver,
    shutdown_signal: BoxFutureInfaillible<()>,
) -> anyhow::Result<HashMap<String, ActorExitStatus>> {
    let cluster = start_cluster_service(&node_config).await?;

    let event_broker = EventBroker::default();
    let indexer_pool = IndexerPool::default();
    let ingester_pool = IngesterPool::default();
    let universe = Universe::new();

    // Instantiate a metastore "server" if the `metastore` role is enabled on the node.
    let metastore_server_opt: Option<MetastoreServiceClient> =
        if node_config.is_service_enabled(QuickwitService::Metastore) {
            let metastore: MetastoreServiceClient = metastore_resolver
                .resolve(&node_config.metastore_uri)
                .await?;
            let broker_layer = EventListenerLayer::new(event_broker.clone());
            let metastore = MetastoreServiceClient::tower()
                .stack_create_index_layer(broker_layer.clone())
                .stack_delete_index_layer(broker_layer.clone())
                .stack_add_source_layer(broker_layer.clone())
                .stack_delete_source_layer(broker_layer.clone())
                .stack_toggle_source_layer(broker_layer)
                .build(metastore);
            Some(metastore)
        } else {
            None
        };
    // Instantiate a metastore client, either local if available or remote otherwise.
    let metastore_client: MetastoreServiceClient =
        if let Some(metastore_server) = &metastore_server_opt {
            metastore_server.clone()
        } else {
            // Wait for a metastore service to be available for at most 10 seconds.
            if cluster
                .wait_for_ready_members(has_node_with_metastore_service, Duration::from_secs(10))
                .await
                .is_err()
            {
                error!("no metastore service found among cluster members, stopping server");
                anyhow::bail!(
                    "failed to start server: no metastore service was found among cluster \
                     members. try running Quickwit with additional metastore service `quickwit \
                     run --service metastore`"
                )
            }

            let balance_channel =
                balance_channel_for_service(&cluster, QuickwitService::Metastore).await;
            let metastore_client = MetastoreServiceClient::from_balance_channel(balance_channel);
            let retry_layer = RetryLayer::new(RetryPolicy::default());
            MetastoreServiceClient::tower()
                .stack_layer(retry_layer)
                .build(metastore_client)
        };

    // Instantiate a control plane server if the `control-plane` role is enabled on the node.
    // Otherwise, instantiate a control plane client.
    let control_plane_service: ControlPlaneServiceClient = start_control_plane_if_needed(
        &node_config,
        &cluster,
        &event_broker,
        &metastore_client,
        &universe,
        &indexer_pool,
        &ingester_pool,
    )
    .await?;

    // If one of the two following service is enabled, we need to enable the shard position service:
    // - the control plane: as it is in charge of cleaning up shard reach eof.
    // - the indexer: as it hosts ingesters, and ingesters use the shard positions to truncate
    // their the queue associated to shards in mrecordlog, and publish indexers' progress to
    // chitchat.
    if node_config.is_service_enabled(QuickwitService::Indexer)
        || node_config.is_service_enabled(QuickwitService::ControlPlane)
    {
        ShardPositionsService::spawn(universe.spawn_ctx(), event_broker.clone(), cluster.clone());
    }

    // Set up the "control plane proxy" for the metastore.
    let metastore_through_control_plane = MetastoreServiceClient::new(ControlPlaneMetastore::new(
        control_plane_service.clone(),
        metastore_client.clone(),
    ));

    // Setup ingest service v1.
    let ingest_service = start_ingest_client_if_needed(&node_config, &universe, &cluster).await?;

    let indexing_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer) {
        let ingest_api_service: Mailbox<IngestApiService> = universe
            .get_one()
            .context("Ingest API Service should have been started.")?;
        let indexing_service = start_indexing_service(
            &universe,
            &node_config,
            runtimes_config.num_threads_blocking,
            cluster.clone(),
            metastore_through_control_plane.clone(),
            ingest_api_service.clone(),
            ingester_pool.clone(),
            storage_resolver.clone(),
            event_broker.clone(),
        )
        .await?;
        Some(indexing_service)
    } else {
        None
    };

    // Setup indexer pool.
    let cluster_change_stream = cluster.ready_nodes_change_stream().await;
    setup_indexer_pool(
        cluster_change_stream,
        indexer_pool.clone(),
        indexing_service_opt.clone(),
    );

    // Setup ingest service v2.
    let (ingest_router_service, ingester_service_opt) = setup_ingest_v2(
        &node_config,
        &cluster,
        &event_broker,
        control_plane_service.clone(),
        ingester_pool,
    )
    .await?;

    // Any node can serve index management requests (create/update/delete index, add/remove source,
    // etc.), so we always instantiate an index manager.
    let mut index_manager = IndexManager::new(
        metastore_through_control_plane.clone(),
        storage_resolver.clone(),
    );

    if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        {
            let otel_logs_index_config =
                OtlpGrpcLogsService::index_config(&node_config.default_index_root_uri)?;
            let otel_traces_index_config =
                OtlpGrpcTracesService::index_config(&node_config.default_index_root_uri)?;

            for index_config in [otel_logs_index_config, otel_traces_index_config] {
                match index_manager.create_index(index_config, false).await {
                    Ok(_)
                    | Err(IndexServiceError::Metastore(MetastoreError::AlreadyExists(
                        EntityKind::Index { .. },
                    ))) => Ok(()),
                    Err(error) => Err(error),
                }?;
            }
        }
    }

    let cluster_change_stream = cluster.ready_nodes_change_stream().await;

    let split_cache_root_directory: PathBuf =
        node_config.data_dir_path.join("searcher-split-cache");
    let split_cache_opt: Option<Arc<SplitCache>> =
        if let Some(split_cache_config) = node_config.searcher_config.split_cache {
            let split_cache = SplitCache::with_root_path(
                split_cache_root_directory,
                storage_resolver.clone(),
                split_cache_config,
            )
            .context("failed to load searcher split cache")?;
            Some(Arc::new(split_cache))
        } else {
            None
        };

    let searcher_context = Arc::new(SearcherContext::new(
        node_config.searcher_config.clone(),
        split_cache_opt,
    ));

    let (search_job_placer, search_service) = setup_searcher(
        cluster_change_stream,
        metastore_through_control_plane.clone(),
        storage_resolver.clone(),
        searcher_context,
    )
    .await?;

    // The control plane listens for local shards updates to learn about each shard's ingestion
    // throughput. Ingesters (routers) do so to update their shard table.
    let local_shards_update_listener_handle_opt = if node_config
        .is_service_enabled(QuickwitService::ControlPlane)
        || node_config.is_service_enabled(QuickwitService::Indexer)
    {
        Some(setup_local_shards_update_listener(cluster.clone(), event_broker.clone()).await)
    } else {
        None
    };

    let report_splits_subscription_handle_opt =
        // DISCLAIMER: This is quirky here: We base our decision to forward the split report depending
        // on the current searcher configuration.
        if node_config.searcher_config.split_cache.is_some() {
            // The searcher receive hints about new splits to populate their index.
            Some(event_broker.subscribe::<ReportSplitsRequest>(search_job_placer.clone()))
        } else {
            None
        };

    let janitor_service_opt = if node_config.is_service_enabled(QuickwitService::Janitor) {
        let janitor_service = start_janitor_service(
            &universe,
            &node_config,
            metastore_through_control_plane.clone(),
            search_job_placer,
            storage_resolver.clone(),
            event_broker.clone(),
        )
        .await?;
        Some(janitor_service)
    } else {
        None
    };

    let jaeger_service_opt = if node_config.jaeger_config.enable_endpoint
        && node_config.is_service_enabled(QuickwitService::Searcher)
    {
        let search_service = search_service.clone();
        Some(JaegerService::new(
            node_config.jaeger_config.clone(),
            search_service,
        ))
    } else {
        None
    };

    let otlp_logs_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        Some(OtlpGrpcLogsService::new(ingest_service.clone()))
    } else {
        None
    };

    let otlp_traces_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        Some(OtlpGrpcTracesService::new(ingest_service.clone(), None))
    } else {
        None
    };

    let grpc_listen_addr = node_config.grpc_listen_addr;
    let rest_listen_addr = node_config.rest_config.listen_addr;
    let quickwit_services: Arc<QuickwitServices> = Arc::new(QuickwitServices {
        node_config: Arc::new(node_config),
        cluster: cluster.clone(),
        metastore_server_opt,
        metastore_client: metastore_through_control_plane.clone(),
        control_plane_service,
        _local_shards_update_listener_handle_opt: local_shards_update_listener_handle_opt,
        _report_splits_subscription_handle_opt: report_splits_subscription_handle_opt,
        index_manager,
        indexing_service_opt,
        ingest_router_service,
        ingest_service,
        ingester_service_opt: ingester_service_opt.clone(),
        janitor_service_opt,
        jaeger_service_opt,
        otlp_logs_service_opt,
        otlp_traces_service_opt,
        search_service,
    });
    // Setup and start gRPC server.
    let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel::<()>();
    let grpc_readiness_trigger = Box::pin(async move {
        if grpc_readiness_trigger_tx.send(()).is_err() {
            debug!("gRPC server readiness signal receiver was dropped");
        }
    });
    let (grpc_shutdown_trigger_tx, grpc_shutdown_signal_rx) = oneshot::channel::<()>();
    let grpc_shutdown_signal = Box::pin(async move {
        if grpc_shutdown_signal_rx.await.is_err() {
            debug!("gRPC server shutdown trigger sender was dropped");
        }
    });
    let grpc_server = grpc::start_grpc_server(
        grpc_listen_addr,
        quickwit_services.clone(),
        grpc_readiness_trigger,
        grpc_shutdown_signal,
    );
    // Setup and start REST server.
    let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel::<()>();
    let rest_readiness_trigger = Box::pin(async move {
        if rest_readiness_trigger_tx.send(()).is_err() {
            debug!("REST server readiness signal receiver was dropped");
        }
    });
    let (rest_shutdown_trigger_tx, rest_shutdown_signal_rx) = oneshot::channel::<()>();
    let rest_shutdown_signal = Box::pin(async move {
        if rest_shutdown_signal_rx.await.is_err() {
            debug!("REST server shutdown trigger sender was dropped");
        }
    });
    let rest_server = rest::start_rest_server(
        rest_listen_addr,
        quickwit_services,
        rest_readiness_trigger,
        rest_shutdown_signal,
    );

    // Node readiness indicates that the server is ready to receive requests.
    // Thus readiness task is started once gRPC and REST servers are started.
    tokio::spawn(node_readiness_reporting_task(
        cluster,
        metastore_through_control_plane,
        grpc_readiness_signal_rx,
        rest_readiness_signal_rx,
    ));
    let shutdown_handle = tokio::spawn(async move {
        shutdown_signal.await;

        // We must decommission the ingester first before terminating the indexing pipelines that
        // may consume from it. We also need to keep the gRPC server running while doing so.
        wait_for_ingester_decommission(ingester_service_opt).await;
        let actor_exit_statuses = universe.quit().await;

        if grpc_shutdown_trigger_tx.send(()).is_err() {
            debug!("gRPC server shutdown signal receiver was dropped");
        }
        if rest_shutdown_trigger_tx.send(()).is_err() {
            debug!("REST server shutdown signal receiver was dropped");
        }
        actor_exit_statuses
    });
    let grpc_join_handle = tokio::spawn(grpc_server);
    let rest_join_handle = tokio::spawn(rest_server);

    let (grpc_res, rest_res) = tokio::try_join!(grpc_join_handle, rest_join_handle)
        .expect("the tasks running the gRPC and REST servers should not panic or be cancelled");

    if let Err(grpc_err) = grpc_res {
        error!("gRPC server failed: {:?}", grpc_err);
    }
    if let Err(rest_err) = rest_res {
        error!("REST server failed: {:?}", rest_err);
    }
    let actor_exit_statuses = shutdown_handle.await?;
    Ok(actor_exit_statuses)
}

async fn setup_ingest_v2(
    config: &NodeConfig,
    cluster: &Cluster,
    event_broker: &EventBroker,
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
) -> anyhow::Result<(IngestRouterServiceClient, Option<IngesterServiceClient>)> {
    // Instantiate ingest router.
    let self_node_id: NodeId = cluster.self_node_id().into();
    let replication_factor = config
        .ingest_api_config
        .replication_factor()
        .expect("replication factor should have been validated")
        .get();
    let ingest_router = IngestRouter::new(
        self_node_id.clone(),
        control_plane,
        ingester_pool.clone(),
        replication_factor,
    );
    ingest_router.subscribe(event_broker);
    let ingest_router_service = IngestRouterServiceClient::new(ingest_router);

    // We compute the burst limit as something a bit larger than the content length limit, because
    // we actually rewrite the `\n-delimited format into a tiny bit larger buffer, where the
    // line length is prefixed.
    let burst_limit = (config.ingest_api_config.content_length_limit.as_u64() * 3 / 2)
        .clamp(10_000_000, 200_000_000);
    let rate_limiter_settings = RateLimiterSettings {
        burst_limit,
        ..Default::default()
    };
    // Instantiate ingester.
    let ingester_service_opt = if config.is_service_enabled(QuickwitService::Indexer) {
        let wal_dir_path = config.data_dir_path.join("wal");
        fs::create_dir_all(&wal_dir_path)?;
        let ingester = Ingester::try_new(
            cluster.clone(),
            ingester_pool.clone(),
            &wal_dir_path,
            config.ingest_api_config.max_queue_disk_usage,
            config.ingest_api_config.max_queue_memory_usage,
            rate_limiter_settings,
            replication_factor,
        )
        .await?;
        ingester.subscribe(event_broker);
        let ingester_service = IngesterServiceClient::new(ingester);
        Some(ingester_service)
    } else {
        None
    };
    // Setup ingester pool change stream.
    let ingester_service_opt_clone = ingester_service_opt.clone();
    let cluster_change_stream = cluster.ready_nodes_change_stream().await;
    let ingester_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let ingester_service_opt = ingester_service_opt_clone.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node)
                    if node.enabled_services().contains(&QuickwitService::Indexer) =>
                {
                    let node_id: NodeId = node.node_id().into();

                    if node.is_self_node() {
                        let ingester_service = ingester_service_opt
                            .expect("the ingester service should be initialized");
                        Some(Change::Insert(node_id, ingester_service))
                    } else {
                        let ingester_service = IngesterServiceClient::from_channel(
                            node.grpc_advertise_addr(),
                            node.channel(),
                        );
                        Some(Change::Insert(node_id, ingester_service))
                    }
                }
                ClusterChange::Remove(node) => Some(Change::Remove(node.node_id().into())),
                _ => None,
            }
        })
    });
    ingester_pool.listen_for_changes(ingester_change_stream);
    Ok((ingest_router_service, ingester_service_opt))
}

async fn setup_searcher(
    cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    searcher_context: Arc<SearcherContext>,
) -> anyhow::Result<(SearchJobPlacer, Arc<dyn SearchService>)> {
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let search_service = start_searcher_service(
        metastore,
        storage_resolver,
        search_job_placer.clone(),
        searcher_context,
    )
    .await?;
    let search_service_clone = search_service.clone();
    let searcher_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let search_service_clone = search_service_clone.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node)
                    if node.enabled_services().contains(&QuickwitService::Searcher) =>
                {
                    let grpc_addr = node.grpc_advertise_addr();

                    if node.is_self_node() {
                        let search_client =
                            SearchServiceClient::from_service(search_service_clone, grpc_addr);
                        Some(Change::Insert(grpc_addr, search_client))
                    } else {
                        let timeout_channel = Timeout::new(node.channel(), Duration::from_secs(30));
                        let search_client =
                            create_search_client_from_channel(grpc_addr, timeout_channel);
                        Some(Change::Insert(grpc_addr, search_client))
                    }
                }
                ClusterChange::Remove(node) => Some(Change::Remove(node.grpc_advertise_addr())),
                _ => None,
            }
        })
    });
    searcher_pool.listen_for_changes(searcher_change_stream);
    Ok((search_job_placer, search_service))
}

#[allow(clippy::too_many_arguments)]
async fn setup_control_plane(
    universe: &Universe,
    event_broker: &EventBroker,
    cluster_id: String,
    self_node_id: NodeId,
    indexer_pool: IndexerPool,
    ingester_pool: IngesterPool,
    metastore: MetastoreServiceClient,
    replication_factor: usize,
) -> anyhow::Result<Mailbox<ControlPlane>> {
    let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
        universe,
        cluster_id,
        self_node_id,
        indexer_pool,
        ingester_pool,
        metastore,
        replication_factor,
    );
    let subscriber = ControlPlaneEventSubscriber::new(control_plane_mailbox.downgrade());

    event_broker
        .subscribe::<LocalShardsUpdate>(subscriber.clone())
        .forever();
    event_broker
        .subscribe::<ShardPositionsUpdate>(subscriber)
        .forever();

    Ok(control_plane_mailbox)
}

fn setup_indexer_pool(
    cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    indexer_pool: IndexerPool,
    indexing_service_opt: Option<Mailbox<IndexingService>>,
) {
    let indexer_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let indexing_service_clone_opt = indexing_service_opt.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) | ClusterChange::Update(node)
                    if node.enabled_services().contains(&QuickwitService::Indexer) =>
                {
                    let node_id = node.node_id().to_string();
                    let indexing_tasks = node.indexing_tasks().to_vec();
                    let indexing_capacity = node.indexing_capacity();
                    if node.is_self_node() {
                        if let Some(indexing_service_clone) = indexing_service_clone_opt {
                            let client =
                                IndexingServiceClient::from_mailbox(indexing_service_clone);
                            Some(Change::Insert(
                                node_id,
                                IndexerNodeInfo {
                                    client,
                                    indexing_tasks,
                                    indexing_capacity,
                                },
                            ))
                        } else {
                            // That means that cluster thinks we are supposed to have an indexer,
                            // but we actually don't.
                            None
                        }
                    } else {
                        let client = IndexingServiceClient::from_channel(
                            node.grpc_advertise_addr(),
                            node.channel(),
                        );
                        Some(Change::Insert(
                            node_id,
                            IndexerNodeInfo {
                                client,
                                indexing_tasks,
                                indexing_capacity,
                            },
                        ))
                    }
                }
                ClusterChange::Remove(node) => Some(Change::Remove(node.node_id().to_string())),
                _ => None,
            }
        })
    });
    indexer_pool.listen_for_changes(indexer_change_stream);
}

fn require<T: Clone + Send>(
    val_opt: Option<T>,
) -> impl Filter<Extract = (T,), Error = Rejection> + Clone {
    warp::any().and_then(move || {
        let val_opt_clone = val_opt.clone();
        async move {
            if let Some(val) = val_opt_clone {
                Ok(val)
            } else {
                Err(warp::reject())
            }
        }
    })
}

fn with_arg<T: Clone + Send>(arg: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || arg.clone())
}

/// Reports node readiness to chitchat cluster every 10 seconds (25 ms for tests).
async fn node_readiness_reporting_task(
    cluster: Cluster,
    mut metastore: MetastoreServiceClient,
    grpc_readiness_signal_rx: oneshot::Receiver<()>,
    rest_readiness_signal_rx: oneshot::Receiver<()>,
) {
    if grpc_readiness_signal_rx.await.is_err() {
        // the gRPC server failed.
        return;
    };
    info!("gRPC server is ready");

    if rest_readiness_signal_rx.await.is_err() {
        // the REST server failed.
        return;
    };
    info!("REST server is ready");

    let mut interval = tokio::time::interval(READINESS_REPORTING_INTERVAL);

    loop {
        interval.tick().await;

        let node_ready = match metastore.check_connectivity().await {
            Ok(()) => {
                debug!(metastore_endpoints=?metastore.endpoints(), "metastore service is available");
                true
            }
            Err(error) => {
                warn!(metastore_endpoints=?metastore.endpoints(), error=?error, "metastore service is unavailable");
                false
            }
        };
        cluster.set_self_node_readiness(node_ready).await;
    }
}

/// Displays some warnings if the cluster runs a file-backed metastore or serves file-backed
/// indexes.
async fn check_cluster_configuration(
    services: &HashSet<QuickwitService>,
    peer_seeds: &[String],
    mut metastore: MetastoreServiceClient,
) -> anyhow::Result<()> {
    if !services.contains(&QuickwitService::Metastore) || peer_seeds.is_empty() {
        return Ok(());
    }
    if metastore
        .endpoints()
        .iter()
        .any(|uri| !uri.protocol().is_database())
    {
        warn!(
            metastore_endpoints=?metastore.endpoints(),
            "Using a file-backed metastore in cluster mode is not recommended for production use.
            Running multiple file-backed metastores simultaneously can lead to data loss.");
    }
    let file_backed_indexes = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await?
        .deserialize_indexes_metadata()?
        .into_iter()
        .filter(|index_metadata| index_metadata.index_uri().protocol().is_file_storage())
        .collect::<Vec<_>>();
    if !file_backed_indexes.is_empty() {
        let index_ids = file_backed_indexes
            .iter()
            .map(|index_metadata| index_metadata.index_id())
            .join(", ");
        let index_uris = file_backed_indexes
            .iter()
            .map(|index_metadata| index_metadata.index_uri())
            .join(", ");
        warn!(
            index_ids=%index_ids,
            index_uris=%index_uris,
            "Found some file-backed indexes in the metastore. Some nodes in the cluster may not have access to all index files."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport, ClusterNode};
    use quickwit_common::uri::Uri;
    use quickwit_config::SearcherConfig;
    use quickwit_metastore::{metastore_for_test, IndexMetadata};
    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::metastore::ListIndexesMetadataResponse;
    use quickwit_proto::types::PipelineUid;
    use quickwit_search::Job;
    use tokio::sync::{mpsc, watch};
    use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

    use super::*;

    #[tokio::test]
    async fn test_check_cluster_configuration() {
        let services = HashSet::from_iter([QuickwitService::Metastore]);
        let peer_seeds = ["192.168.0.12:7280".to_string()];
        let mut mock_metastore = MetastoreServiceClient::mock();

        mock_metastore
            .expect_endpoints()
            .return_const(vec![Uri::for_test("file:///qwdata/indexes")]);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    IndexMetadata::for_test("test-index", "file:///qwdata/indexes/test-index"),
                ])
                .unwrap())
            });

        check_cluster_configuration(
            &services,
            &peer_seeds,
            MetastoreServiceClient::from(mock_metastore),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_readiness_updates() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let (metastore_readiness_tx, metastore_readiness_rx) = watch::channel(false);
        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_check_connectivity()
            .returning(move || {
                if *metastore_readiness_rx.borrow() {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Metastore not ready"))
                }
            });
        let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel();
        let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel();
        tokio::spawn(node_readiness_reporting_task(
            cluster.clone(),
            MetastoreServiceClient::from(mock_metastore),
            grpc_readiness_signal_rx,
            rest_readiness_signal_rx,
        ));
        assert!(!cluster.is_self_node_ready().await);

        grpc_readiness_trigger_tx.send(()).unwrap();
        rest_readiness_trigger_tx.send(()).unwrap();
        assert!(!cluster.is_self_node_ready().await);

        metastore_readiness_tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(cluster.is_self_node_ready().await);

        metastore_readiness_tx.send(false).unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!cluster.is_self_node_ready().await);
    }

    #[tokio::test]
    async fn test_setup_indexer_pool() {
        let universe = Universe::with_accelerated_time();
        let (indexing_service_mailbox, _indexing_service_inbox) =
            universe.create_test_mailbox::<IndexingService>();

        let (indexer_change_stream_tx, indexer_change_stream_rx) = mpsc::channel(3);
        let indexer_change_stream = ReceiverStream::new(indexer_change_stream_rx);
        let indexer_pool = IndexerPool::default();
        setup_indexer_pool(
            indexer_change_stream,
            indexer_pool.clone(),
            Some(indexing_service_mailbox),
        );

        let new_indexer_node =
            ClusterNode::for_test("test-indexer-node", 1, true, &["indexer"], &[]).await;
        indexer_change_stream_tx
            .send(ClusterChange::Add(new_indexer_node))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(indexer_pool.len(), 1);

        let new_indexer_node_info = indexer_pool.get("test-indexer-node").unwrap();
        assert!(new_indexer_node_info.indexing_tasks.is_empty());

        let new_indexing_task = IndexingTask {
            pipeline_uid: Some(PipelineUid::from_u128(0u128)),
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_ids: Vec::new(),
        };
        let updated_indexer_node = ClusterNode::for_test(
            "test-indexer-node",
            1,
            true,
            &["indexer"],
            &[new_indexing_task.clone()],
        )
        .await;
        indexer_change_stream_tx
            .send(ClusterChange::Update(updated_indexer_node.clone()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let updated_indexer_node_info = indexer_pool.get("test-indexer-node").unwrap();
        assert_eq!(updated_indexer_node_info.indexing_tasks.len(), 1);
        assert_eq!(
            updated_indexer_node_info.indexing_tasks[0],
            new_indexing_task
        );

        indexer_change_stream_tx
            .send(ClusterChange::Remove(updated_indexer_node))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(indexer_pool.is_empty());
    }

    #[tokio::test]
    async fn test_setup_searcher() {
        let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default(), None));
        let metastore = metastore_for_test();
        let (change_stream_tx, change_stream_rx) = mpsc::unbounded_channel();
        let change_stream = UnboundedReceiverStream::new(change_stream_rx);
        let storage_resolver = StorageResolver::unconfigured();
        let (search_job_placer, _searcher_service) =
            setup_searcher(change_stream, metastore, storage_resolver, searcher_context)
                .await
                .unwrap();

        struct DummyJob(String);

        impl Job for DummyJob {
            fn split_id(&self) -> &str {
                &self.0
            }

            fn cost(&self) -> usize {
                1
            }
        }
        search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap_err();

        let self_node = ClusterNode::for_test("node-1", 1337, true, &["searcher"], &[]).await;
        change_stream_tx
            .send(ClusterChange::Add(self_node.clone()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let searcher_client = search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap();
        assert!(searcher_client.is_local());

        change_stream_tx
            .send(ClusterChange::Remove(self_node))
            .unwrap();

        let node = ClusterNode::for_test("node-1", 1337, false, &["searcher"], &[]).await;
        change_stream_tx.send(ClusterChange::Add(node)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let searcher_client = search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap();
        assert!(!searcher_client.is_local());
    }
}
