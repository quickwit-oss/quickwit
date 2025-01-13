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

#![recursion_limit = "256"]

mod build_info;
mod cluster_api;
mod decompression;
mod delete_task_api;
mod developer_api;
mod elasticsearch_api;
mod format;
mod grpc;
mod health_check_api;
mod index_api;
mod indexing_api;
mod ingest_api;
mod jaeger_api;
mod load_shield;
mod metrics;
mod metrics_api;
mod node_info_handler;
mod openapi;
mod otlp_api;
mod rate_modulator;
mod rest;
mod rest_api_response;
mod search_api;
pub(crate) mod simple_list;
pub mod tcp_listener;
mod template_api;
mod ui_handler;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use bytesize::ByteSize;
pub(crate) use decompression::Body;
pub use format::BodyFormat;
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::Lazy;
use quickwit_actors::{ActorExitStatus, Mailbox, SpawnContext, Universe};
use quickwit_cluster::{
    start_cluster_service, Cluster, ClusterChange, ClusterChangeStream, ListenerHandle,
};
use quickwit_common::pubsub::{EventBroker, EventSubscriptionHandle};
use quickwit_common::rate_limiter::RateLimiterSettings;
use quickwit_common::retry::RetryParams;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::tower::{
    BalanceChannel, BoxFutureInfaillible, BufferLayer, Change, CircuitBreakerEvaluator,
    ConstantRate, EstimateRateLayer, EventListenerLayer, GrpcMetricsLayer, LoadShedLayer,
    RateLimitLayer, RetryLayer, RetryPolicy, SmaRateEstimator, TimeoutLayer,
};
use quickwit_common::uri::Uri;
use quickwit_common::{get_bool_from_env, spawn_named_task};
use quickwit_config::service::QuickwitService;
use quickwit_config::{ClusterConfig, IngestApiConfig, NodeConfig};
use quickwit_control_plane::control_plane::{ControlPlane, ControlPlaneEventSubscriber};
use quickwit_control_plane::{IndexerNodeInfo, IndexerPool};
use quickwit_index_management::{IndexService as IndexManager, IndexServiceError};
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::models::ShardPositionsService;
use quickwit_indexing::start_indexing_service;
use quickwit_ingest::{
    get_idle_shard_timeout, setup_local_shards_update_listener, start_ingest_api_service,
    wait_for_ingester_decommission, wait_for_ingester_status, GetMemoryCapacity, IngestRequest,
    IngestRouter, IngestServiceClient, Ingester, IngesterPool, LocalShardsUpdate,
};
use quickwit_jaeger::JaegerService;
use quickwit_janitor::{start_janitor_service, JanitorService};
use quickwit_metastore::{
    ControlPlaneMetastore, ListIndexesMetadataResponseExt, MetastoreResolver,
};
use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
use quickwit_proto::control_plane::ControlPlaneServiceClient;
use quickwit_proto::indexing::{IndexingServiceClient, ShardPositionsUpdate};
use quickwit_proto::ingest::ingester::{
    IngesterService, IngesterServiceClient, IngesterServiceTowerLayerStack, IngesterStatus,
    PersistFailureReason, PersistResponse,
};
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::ingest::{IngestV2Error, RateLimitingCause};
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
use tcp_listener::TcpListenerResolver;
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

const METASTORE_CLIENT_MAX_CONCURRENCY_ENV_KEY: &str = "QW_METASTORE_CLIENT_MAX_CONCURRENCY";
const DEFAULT_METASTORE_CLIENT_MAX_CONCURRENCY: usize = 6;
const DISABLE_DELETE_TASK_SERVICE_ENV_KEY: &str = "QW_DISABLE_DELETE_TASK_SERVICE";

pub type EnvFilterReloadFn = Arc<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync>;

pub fn do_nothing_env_filter_reload_fn() -> EnvFilterReloadFn {
    Arc::new(|_| Ok(()))
}

fn get_metastore_client_max_concurrency() -> usize {
    quickwit_common::get_from_env(
        METASTORE_CLIENT_MAX_CONCURRENCY_ENV_KEY,
        DEFAULT_METASTORE_CLIENT_MAX_CONCURRENCY,
    )
}

static CP_GRPC_CLIENT_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("control_plane", "client"));
static CP_GRPC_SERVER_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("control_plane", "server"));

static INDEXING_GRPC_CLIENT_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("indexing", "client"));
pub(crate) static INDEXING_GRPC_SERVER_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("indexing", "server"));

static INGEST_GRPC_CLIENT_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("ingest", "client"));
static INGEST_GRPC_SERVER_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("ingest", "server"));

static METASTORE_GRPC_CLIENT_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("metastore", "client"));
static METASTORE_GRPC_SERVER_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("metastore", "server"));

static GRPC_TIMEOUT_LAYER: Lazy<TimeoutLayer> =
    Lazy::new(|| TimeoutLayer::new(Duration::from_secs(30)));

struct QuickwitServices {
    pub node_config: Arc<NodeConfig>,
    pub cluster: Cluster,
    pub metastore_server_opt: Option<MetastoreServiceClient>,
    pub metastore_client: MetastoreServiceClient,
    pub control_plane_server_opt: Option<Mailbox<ControlPlane>>,
    pub control_plane_client: ControlPlaneServiceClient,
    pub index_manager: IndexManager,
    pub indexing_service_opt: Option<Mailbox<IndexingService>>,
    // Ingest v1
    pub ingest_service: IngestServiceClient,
    // Ingest v2
    pub ingest_router_opt: Option<IngestRouter>,
    pub ingest_router_service: IngestRouterServiceClient,
    ingester_opt: Option<Ingester>,

    pub janitor_service_opt: Option<Mailbox<JanitorService>>,
    pub jaeger_service_opt: Option<JaegerService>,
    pub otlp_logs_service_opt: Option<OtlpGrpcLogsService>,
    pub otlp_traces_service_opt: Option<OtlpGrpcTracesService>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,

    pub env_filter_reload_fn: EnvFilterReloadFn,

    /// The control plane listens to various events.
    /// We must maintain a reference to the subscription handles to continue receiving
    /// notifications. Otherwise, the subscriptions are dropped.
    _local_shards_update_listener_handle_opt: Option<ListenerHandle>,
    _report_splits_subscription_handle_opt: Option<EventSubscriptionHandle>,
}

impl QuickwitServices {
    /// Client in the type is a bit misleading here.
    ///
    /// The object returned is the implementation of the local ingester service,
    /// with all of the appropriate tower layers.
    pub fn ingester_service(&self) -> Option<IngesterServiceClient> {
        let ingester = self.ingester_opt.clone()?;
        Some(ingester_service_layer_stack(IngesterServiceClient::tower()).build(ingester))
    }
}

async fn balance_channel_for_service(
    cluster: &Cluster,
    service: QuickwitService,
) -> BalanceChannel<SocketAddr> {
    let cluster_change_stream = cluster.change_stream();
    let service_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) if node.enabled_services().contains(&service) => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "adding node `{}` to {} pool",
                        chitchat_id.node_id,
                        service.as_str().replace('_', " "),
                    );
                    Some(Change::Insert(node.grpc_advertise_addr(), node.channel()))
                }
                ClusterChange::Remove(node) if node.enabled_services().contains(&service) => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "removing node `{}` from {} pool",
                        chitchat_id.node_id,
                        service.as_str().replace('_', " "),
                    );
                    Some(Change::Remove(node.grpc_advertise_addr()))
                }
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
        let rate_estimator = SmaRateEstimator::new(
            num_buckets,
            Duration::from_secs(10),
            Duration::from_millis(100),
        );
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
        let ingest_service = IngestServiceClient::from_balance_channel(
            balance_channel,
            node_config.grpc_config.max_message_size,
        );
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
) -> anyhow::Result<(Option<Mailbox<ControlPlane>>, ControlPlaneServiceClient)> {
    if node_config.is_service_enabled(QuickwitService::ControlPlane) {
        check_cluster_configuration(
            &node_config.enabled_services,
            &node_config.peer_seeds,
            metastore_client.clone(),
        )
        .await?;

        let self_node_id: NodeId = cluster.self_node_id().into();

        let control_plane_mailbox = setup_control_plane(
            universe,
            event_broker,
            self_node_id,
            cluster.clone(),
            indexer_pool.clone(),
            ingester_pool.clone(),
            metastore_client.clone(),
            node_config.default_index_root_uri.clone(),
            &node_config.ingest_api_config,
        )
        .await?;

        let control_plane_server_opt = Some(control_plane_mailbox.clone());
        let control_plane_client = ControlPlaneServiceClient::tower()
            .stack_layer(CP_GRPC_SERVER_METRICS_LAYER.clone())
            .stack_layer(LoadShedLayer::new(100))
            .build_from_mailbox(control_plane_mailbox);
        Ok((control_plane_server_opt, control_plane_client))
    } else {
        let balance_channel =
            balance_channel_for_service(cluster, QuickwitService::ControlPlane).await;

        // If the node is a metastore, we skip this check in order to avoid a deadlock.
        // If the node is a searcher, we skip this check because the searcher does not need to.
        if !node_config.is_service_enabled(QuickwitService::Metastore)
            && node_config.enabled_services != HashSet::from([QuickwitService::Searcher])
        {
            info!("connecting to control plane");

            if !balance_channel
                .wait_for(Duration::from_secs(300), |connections| {
                    !connections.is_empty()
                })
                .await
            {
                bail!("could not find control plane in the cluster");
            }
        }
        let control_plane_server_opt = None;
        let control_plane_client = ControlPlaneServiceClient::tower()
            .stack_layer(CP_GRPC_CLIENT_METRICS_LAYER.clone())
            .build_from_balance_channel(balance_channel, node_config.grpc_config.max_message_size);
        Ok((control_plane_server_opt, control_plane_client))
    }
}

fn start_shard_positions_service(
    ingester_opt: Option<Ingester>,
    cluster: Cluster,
    event_broker: EventBroker,
    spawn_ctx: SpawnContext,
) {
    // We spawn a task here, because we need the ingester to be ready before spawning the
    // the `ShardPositionsService`. If we don't, all the events we emit too early will be dismissed.
    tokio::spawn(async move {
        if let Some(ingester) = ingester_opt {
            if wait_for_ingester_status(ingester, IngesterStatus::Ready)
                .await
                .is_err()
            {
                warn!("ingester failed to reach ready status");
            }
        }
        ShardPositionsService::spawn(&spawn_ctx, event_broker, cluster);
    });
}

pub async fn serve_quickwit(
    node_config: NodeConfig,
    runtimes_config: RuntimesConfig,
    metastore_resolver: MetastoreResolver,
    storage_resolver: StorageResolver,
    tcp_listener_resolver: impl TcpListenerResolver,
    shutdown_signal: BoxFutureInfaillible<()>,
    env_filter_reload_fn: EnvFilterReloadFn,
) -> anyhow::Result<HashMap<String, ActorExitStatus>> {
    let cluster = start_cluster_service(&node_config)
        .await
        .context("failed to start cluster service")?;

    let event_broker = EventBroker::default();
    let indexer_pool = IndexerPool::default();
    let ingester_pool = IngesterPool::default();
    let universe = Universe::new();
    let grpc_config = node_config.grpc_config.clone();

    // Instantiate a metastore "server" if the `metastore` role is enabled on the node.
    let metastore_server_opt: Option<MetastoreServiceClient> =
        if node_config.is_service_enabled(QuickwitService::Metastore) {
            let metastore: MetastoreServiceClient = metastore_resolver
                .resolve(&node_config.metastore_uri)
                .await
                .with_context(|| {
                    format!(
                        "failed to resolve metastore uri `{}`",
                        node_config.metastore_uri
                    )
                })?;
            let max_in_flight_requests = if node_config.metastore_uri.protocol().is_database() {
                node_config
                    .metastore_configs
                    .find_postgres()
                    .map(|config| config.max_connections.get() * 2)
                    .unwrap_or_default()
                    .max(100)
            } else {
                100
            };
            // These layers apply to all the RPCs of the metastore.
            let shared_layer = ServiceBuilder::new()
                .layer(METASTORE_GRPC_SERVER_METRICS_LAYER.clone())
                .layer(LoadShedLayer::new(max_in_flight_requests))
                .into_inner();
            let broker_layer = EventListenerLayer::new(event_broker.clone());
            let metastore = MetastoreServiceClient::tower()
                .stack_layer(shared_layer)
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
            info!("connecting to metastore");

            let balance_channel =
                balance_channel_for_service(&cluster, QuickwitService::Metastore).await;

            if !balance_channel
                .wait_for(Duration::from_secs(300), |connections| {
                    !connections.is_empty()
                })
                .await
            {
                bail!("could not find any metastore node in the cluster");
            }
            // These layers applies to all the RPCs of the metastore.
            let shared_layers = ServiceBuilder::new()
                .layer(RetryLayer::new(RetryPolicy::from(RetryParams::standard())))
                .layer(METASTORE_GRPC_CLIENT_METRICS_LAYER.clone())
                .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                    get_metastore_client_max_concurrency(),
                ))
                .into_inner();
            MetastoreServiceClient::tower()
                .stack_layer(shared_layers)
                .build_from_balance_channel(balance_channel, grpc_config.max_message_size)
        };
    // Instantiate a control plane server if the `control-plane` role is enabled on the node.
    // Otherwise, instantiate a control plane client.
    let (control_plane_server_opt, control_plane_client) = start_control_plane_if_needed(
        &node_config,
        &cluster,
        &event_broker,
        &metastore_client,
        &universe,
        &indexer_pool,
        &ingester_pool,
    )
    .await
    .context("failed to start control plane service")?;

    // Set up the "control plane proxy" for the metastore.
    let metastore_through_control_plane = MetastoreServiceClient::new(ControlPlaneMetastore::new(
        control_plane_client.clone(),
        metastore_client,
    ));

    // Setup ingest service v1.
    let ingest_service = start_ingest_client_if_needed(&node_config, &universe, &cluster)
        .await
        .context("failed to start ingest v1 service")?;

    let indexing_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer) {
        let indexing_service = start_indexing_service(
            &universe,
            &node_config,
            runtimes_config.num_threads_blocking,
            cluster.clone(),
            metastore_through_control_plane.clone(),
            ingester_pool.clone(),
            storage_resolver.clone(),
            event_broker.clone(),
        )
        .await
        .context("failed to start indexing service")?;
        Some(indexing_service)
    } else {
        None
    };

    // Setup indexer pool.
    setup_indexer_pool(
        &node_config,
        cluster.change_stream(),
        indexer_pool.clone(),
        indexing_service_opt.clone(),
    );

    // Setup ingest service v2.
    let (ingest_router, ingest_router_service, ingester_opt) = setup_ingest_v2(
        &node_config,
        &cluster,
        &event_broker,
        control_plane_client.clone(),
        ingester_pool,
    )
    .await
    .context("failed to start ingest v2 service")?;

    if node_config.is_service_enabled(QuickwitService::Indexer)
        || node_config.is_service_enabled(QuickwitService::ControlPlane)
    {
        start_shard_positions_service(
            ingester_opt.clone(),
            cluster.clone(),
            event_broker.clone(),
            universe.spawn_ctx().clone(),
        );
    }

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
                OtlpGrpcLogsService::index_config(&node_config.default_index_root_uri)
                    .context("failed to load OTEL logs index config")?;
            let otel_traces_index_config =
                OtlpGrpcTracesService::index_config(&node_config.default_index_root_uri)
                    .context("failed to load OTEL traces index config")?;

            for (index_name, index_config) in [
                ("OTEL logs", otel_logs_index_config),
                ("OTEL traces", otel_traces_index_config),
            ] {
                match index_manager.create_index(index_config, false).await {
                    Ok(_)
                    | Err(IndexServiceError::Metastore(MetastoreError::AlreadyExists(
                        EntityKind::Index { .. },
                    ))) => {}
                    Err(error) => bail!("failed to create {index_name} index: {error}",),
                };
            }
        }
    }
    let split_cache_root_directory: PathBuf =
        node_config.data_dir_path.join("searcher-split-cache");
    let split_cache_opt: Option<Arc<SplitCache>> =
        if let Some(split_cache_limits) = node_config.searcher_config.split_cache {
            let split_cache = SplitCache::with_root_path(
                split_cache_root_directory,
                storage_resolver.clone(),
                split_cache_limits,
            )
            .context("failed to load searcher split cache")?;
            Some(split_cache)
        } else {
            None
        };

    let searcher_context = Arc::new(SearcherContext::new(
        node_config.searcher_config.clone(),
        split_cache_opt,
    ));

    let (search_job_placer, search_service) = setup_searcher(
        &node_config,
        cluster.change_stream(),
        // search remains available without a control plane because not all
        // metastore RPCs are proxied
        metastore_through_control_plane.clone(),
        storage_resolver.clone(),
        searcher_context,
    )
    .await
    .context("failed to start searcher service")?;

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
            !get_bool_from_env(DISABLE_DELETE_TASK_SERVICE_ENV_KEY, false),
        )
        .await
        .context("failed to start janitor service")?;
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
        Some(OtlpGrpcLogsService::new(ingest_router_service.clone()))
    } else {
        None
    };

    let otlp_traces_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        Some(OtlpGrpcTracesService::new(
            ingest_router_service.clone(),
            None,
        ))
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
        control_plane_server_opt,
        control_plane_client,
        _local_shards_update_listener_handle_opt: local_shards_update_listener_handle_opt,
        _report_splits_subscription_handle_opt: report_splits_subscription_handle_opt,
        index_manager,
        indexing_service_opt,
        ingest_router_opt: Some(ingest_router),
        ingest_router_service,
        ingest_service,
        ingester_opt: ingester_opt.clone(),
        janitor_service_opt,
        jaeger_service_opt,
        otlp_logs_service_opt,
        otlp_traces_service_opt,
        search_service,
        env_filter_reload_fn,
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
        tcp_listener_resolver.resolve(grpc_listen_addr).await?,
        grpc_config.max_message_size,
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
        tcp_listener_resolver.resolve(rest_listen_addr).await?,
        quickwit_services,
        rest_readiness_trigger,
        rest_shutdown_signal,
    );

    // Node readiness indicates that the server is ready to receive requests.
    // Thus readiness task is started once gRPC and REST servers are started.
    spawn_named_task(
        node_readiness_reporting_task(
            cluster,
            metastore_through_control_plane,
            ingester_opt.clone(),
            grpc_readiness_signal_rx,
            rest_readiness_signal_rx,
        ),
        "node_readiness_reporting",
    );

    let shutdown_handle = tokio::spawn(async move {
        shutdown_signal.await;

        // We must decommission the ingester first before terminating the indexing pipelines that
        // may consume from it. We also need to keep the gRPC server running while doing so.
        if let Some(ingester) = ingester_opt {
            if let Err(error) = wait_for_ingester_decommission(ingester).await {
                error!("failed to decommission ingester gracefully: {:?}", error);
            }
        }
        let actor_exit_statuses = universe.quit().await;

        if grpc_shutdown_trigger_tx.send(()).is_err() {
            debug!("gRPC server shutdown signal receiver was dropped");
        }
        if rest_shutdown_trigger_tx.send(()).is_err() {
            debug!("REST server shutdown signal receiver was dropped");
        }
        actor_exit_statuses
    });
    let grpc_join_handle = spawn_named_task(grpc_server, "grpc_server");
    let rest_join_handle = spawn_named_task(rest_server, "rest_server");

    let (grpc_res, rest_res) = tokio::try_join!(grpc_join_handle, rest_join_handle)
        .expect("tasks running the gRPC and REST servers should not panic or be cancelled");

    if let Err(grpc_err) = grpc_res {
        error!("gRPC server failed: {:?}", grpc_err);
    }
    if let Err(rest_err) = rest_res {
        error!("REST server failed: {:?}", rest_err);
    }
    let actor_exit_statuses = shutdown_handle
        .await
        .context("failed to gracefully shutdown services")?;
    Ok(actor_exit_statuses)
}

#[derive(Clone, Copy)]
struct PersistCircuitBreakerEvaluator;

impl CircuitBreakerEvaluator for PersistCircuitBreakerEvaluator {
    type Response = PersistResponse;

    type Error = IngestV2Error;

    fn is_circuit_breaker_error(&self, output: &Result<Self::Response, IngestV2Error>) -> bool {
        let Ok(persist_response) = output.as_ref() else {
            return false;
        };
        for persist_failure in &persist_response.failures {
            // This is the error we return when the WAL is full.
            if persist_failure.reason() == PersistFailureReason::WalFull {
                return true;
            }
        }
        false
    }

    fn make_circuit_breaker_output(&self) -> IngestV2Error {
        IngestV2Error::TooManyRequests(RateLimitingCause::CircuitBreaker)
    }
}

/// Stack of layers to use on the server side of the ingester service.
fn ingester_service_layer_stack(
    layer_stack: IngesterServiceTowerLayerStack,
) -> IngesterServiceTowerLayerStack {
    layer_stack
        .stack_layer(INGEST_GRPC_SERVER_METRICS_LAYER.clone())
        .stack_persist_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_persist_layer(
            // "3" may seem a little bit low, but we only consider error caused by a full WAL.
            PersistCircuitBreakerEvaluator.make_layer(
                3,
                Duration::from_millis(500),
                crate::metrics::SERVE_METRICS.circuit_break_total.clone(),
            ),
        )
        .stack_open_replication_stream_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_init_shards_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_retain_shards_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_truncate_shards_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_close_shards_layer(quickwit_common::tower::OneTaskPerCallLayer)
        .stack_decommission_layer(quickwit_common::tower::OneTaskPerCallLayer)
}

async fn setup_ingest_v2(
    node_config: &NodeConfig,
    cluster: &Cluster,
    event_broker: &EventBroker,
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
) -> anyhow::Result<(IngestRouter, IngestRouterServiceClient, Option<Ingester>)> {
    // Instantiate ingest router.
    let self_node_id: NodeId = cluster.self_node_id().into();
    let content_length_limit = node_config.ingest_api_config.content_length_limit;
    let replication_factor = node_config
        .ingest_api_config
        .replication_factor()
        .expect("replication factor should have been validated")
        .get();

    // Any node can serve ingest requests, so we always instantiate an ingest router.
    // TODO: I'm not sure that's such a good idea.
    let ingest_router = IngestRouter::new(
        self_node_id.clone(),
        control_plane.clone(),
        ingester_pool.clone(),
        replication_factor,
        event_broker.clone(),
    );
    ingest_router.subscribe();

    let ingest_router_service = IngestRouterServiceClient::tower()
        .stack_layer(INGEST_GRPC_SERVER_METRICS_LAYER.clone())
        .build(ingest_router.clone());

    // We compute the burst limit as something a bit larger than the content length limit, because
    // we actually rewrite the `\n-delimited format into a tiny bit larger buffer, where the
    // line length is prefixed.
    let burst_limit = (content_length_limit.as_u64() * 3 / 2).clamp(10_000_000, 200_000_000);

    let rate_limit =
        ConstantRate::bytes_per_sec(node_config.ingest_api_config.shard_throughput_limit);
    let rate_limiter_settings = RateLimiterSettings {
        burst_limit,
        rate_limit,
        // Refill every 100ms.
        refill_period: Duration::from_millis(100),
    };

    // Instantiate ingester.
    let ingester_opt: Option<Ingester> = if node_config.is_service_enabled(QuickwitService::Indexer)
    {
        let wal_dir_path = node_config.data_dir_path.join("wal");
        fs::create_dir_all(&wal_dir_path)?;

        let idle_shard_timeout = get_idle_shard_timeout();
        let ingester = Ingester::try_new(
            cluster.clone(),
            control_plane,
            ingester_pool.clone(),
            &wal_dir_path,
            node_config.ingest_api_config.max_queue_disk_usage,
            node_config.ingest_api_config.max_queue_memory_usage,
            rate_limiter_settings,
            replication_factor,
            idle_shard_timeout,
        )
        .await?;
        ingester.subscribe(event_broker);
        // We will now receive all new shard positions update events, from chitchat.
        // Unfortunately at this point, chitchat is already running.
        //
        // We need to make sure the existing positions are loaded too.
        Some(ingester)
    } else {
        None
    };
    // Setup ingester pool change stream.
    let ingester_opt_clone = ingester_opt.clone();
    let max_message_size = node_config.grpc_config.max_message_size;
    let ingester_change_stream = cluster.change_stream().filter_map(move |cluster_change| {
        let ingester_opt_clone_clone = ingester_opt_clone.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) if node.is_indexer() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "adding node `{}` to ingester pool",
                        chitchat_id.node_id,
                    );
                    let node_id: NodeId = node.node_id().into();

                    if node.is_self_node() {
                        // Here, since the service is available locally, we bypass the network stack
                        // and use the instance directly. However, we still want client-side
                        // metrics, so we use both metrics layers.
                        let ingester = ingester_opt_clone_clone
                            .expect("ingester service should be initialized");
                        let ingester_service = ingester_service_layer_stack(
                            IngesterServiceClient::tower()
                                .stack_layer(INGEST_GRPC_CLIENT_METRICS_LAYER.clone()),
                        )
                        .build(ingester);
                        Some(Change::Insert(node_id, ingester_service))
                    } else {
                        let ingester_service = IngesterServiceClient::tower()
                            .stack_layer(INGEST_GRPC_CLIENT_METRICS_LAYER.clone())
                            .stack_layer(GRPC_TIMEOUT_LAYER.clone())
                            .build_from_channel(
                                node.grpc_advertise_addr(),
                                node.channel(),
                                max_message_size,
                            );
                        Some(Change::Insert(node_id, ingester_service))
                    }
                }
                ClusterChange::Remove(node) if node.is_indexer() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "removing node `{}` from ingester pool",
                        chitchat_id.node_id,
                    );
                    Some(Change::Remove(node.node_id().into()))
                }
                _ => None,
            }
        })
    });
    ingester_pool.listen_for_changes(ingester_change_stream);
    Ok((ingest_router, ingest_router_service, ingester_opt))
}

async fn setup_searcher(
    node_config: &NodeConfig,
    cluster_change_stream: ClusterChangeStream,
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
    let max_message_size = node_config.grpc_config.max_message_size;
    let request_timeout = node_config.searcher_config.request_timeout();
    let searcher_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let search_service_clone = search_service_clone.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) if node.is_searcher() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "adding node `{}` to searcher pool",
                        chitchat_id.node_id,
                    );
                    let grpc_addr = node.grpc_advertise_addr();

                    if node.is_self_node() {
                        let search_client =
                            SearchServiceClient::from_service(search_service_clone, grpc_addr);
                        Some(Change::Insert(grpc_addr, search_client))
                    } else {
                        let timeout_channel = Timeout::new(node.channel(), request_timeout);
                        let search_client = create_search_client_from_channel(
                            grpc_addr,
                            timeout_channel,
                            max_message_size,
                        );
                        Some(Change::Insert(grpc_addr, search_client))
                    }
                }
                ClusterChange::Remove(node) if node.is_searcher() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "removing node `{}` from searcher pool",
                        chitchat_id.node_id,
                    );
                    Some(Change::Remove(node.grpc_advertise_addr()))
                }
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
    self_node_id: NodeId,
    cluster: Cluster,
    indexer_pool: IndexerPool,
    ingester_pool: IngesterPool,
    metastore: MetastoreServiceClient,
    default_index_root_uri: Uri,
    ingest_api_config: &IngestApiConfig,
) -> anyhow::Result<Mailbox<ControlPlane>> {
    let cluster_id = cluster.cluster_id().to_string();
    let replication_factor = ingest_api_config
        .replication_factor()
        .expect("replication factor should have been validated")
        .get();
    let cluster_config = ClusterConfig {
        cluster_id,
        auto_create_indexes: true,
        default_index_root_uri,
        replication_factor,
        shard_throughput_limit: ingest_api_config.shard_throughput_limit,
    };
    let (control_plane_mailbox, _control_plane_handle, mut readiness_rx) = ControlPlane::spawn(
        universe,
        cluster_config,
        self_node_id,
        cluster.clone(),
        indexer_pool,
        ingester_pool,
        metastore,
    );
    let subscriber = ControlPlaneEventSubscriber::new(control_plane_mailbox.downgrade());
    event_broker
        .subscribe_without_timeout::<LocalShardsUpdate>(subscriber.clone())
        .forever();
    event_broker
        .subscribe_without_timeout::<ShardPositionsUpdate>(subscriber)
        .forever();

    tokio::time::timeout(
        Duration::from_secs(300),
        readiness_rx.wait_for(|readiness| *readiness),
    )
    .await
    .context("control plane initialization timed out")?
    .context("control plane was killled or quit")?;

    info!("control plane is ready");
    Ok(control_plane_mailbox)
}

fn setup_indexer_pool(
    node_config: &NodeConfig,
    cluster_change_stream: ClusterChangeStream,
    indexer_pool: IndexerPool,
    indexing_service_opt: Option<Mailbox<IndexingService>>,
) {
    let max_message_size = node_config.grpc_config.max_message_size;
    let indexer_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let indexing_service_clone_opt = indexing_service_opt.clone();
        Box::pin(async move {
            match &cluster_change {
                ClusterChange::Add(node) if node.is_indexer() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "adding node `{}` to indexer pool",
                        chitchat_id.node_id,
                    );
                }
                _ => {}
            };
            match cluster_change {
                ClusterChange::Add(node) | ClusterChange::Update(node) if node.is_indexer() => {
                    let node_id = node.node_id().to_owned();
                    let indexing_tasks = node.indexing_tasks().to_vec();
                    let indexing_capacity = node.indexing_capacity();

                    if node.is_self_node() {
                        // Here, since the service is available locally, we bypass the network stack
                        // and use the mailbox directly. However, we still want client-side metrics,
                        // so we use both metrics layers.
                        let indexing_service_mailbox = indexing_service_clone_opt
                            .expect("indexing service should be initialized");
                        // These layers apply to all the RPCs of the indexing service.
                        let shared_layers = ServiceBuilder::new()
                            .layer(INDEXING_GRPC_CLIENT_METRICS_LAYER.clone())
                            .layer(INDEXING_GRPC_SERVER_METRICS_LAYER.clone())
                            .into_inner();
                        let client = IndexingServiceClient::tower()
                            .stack_layer(shared_layers)
                            .build_from_mailbox(indexing_service_mailbox);
                        let change = Change::Insert(
                            node_id.clone(),
                            IndexerNodeInfo {
                                node_id,
                                generation_id: node.chitchat_id().generation_id,
                                client,
                                indexing_tasks,
                                indexing_capacity,
                            },
                        );
                        Some(change)
                    } else {
                        let client = IndexingServiceClient::tower()
                            .stack_layer(INDEXING_GRPC_CLIENT_METRICS_LAYER.clone())
                            .stack_layer(GRPC_TIMEOUT_LAYER.clone())
                            .build_from_channel(
                                node.grpc_advertise_addr(),
                                node.channel(),
                                max_message_size,
                            );
                        let change = Change::Insert(
                            node_id.clone(),
                            IndexerNodeInfo {
                                node_id,
                                generation_id: node.chitchat_id().generation_id,
                                client,
                                indexing_tasks,
                                indexing_capacity,
                            },
                        );
                        Some(change)
                    }
                }
                ClusterChange::Remove(node) if node.is_indexer() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        node_id = chitchat_id.node_id,
                        generation_id = chitchat_id.generation_id,
                        "removing node `{}` from indexer pool",
                        chitchat_id.node_id,
                    );
                    Some(Change::Remove(node.node_id().to_owned()))
                }
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
    metastore: MetastoreServiceClient,
    ingester_opt: Option<impl IngesterService>,
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

    if let Some(ingester) = ingester_opt {
        if let Err(error) = wait_for_ingester_status(ingester, IngesterStatus::Ready).await {
            error!("failed to initialize ingester: {:?}", error);
            info!("shutting down");
            return;
        }
    }
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
    metastore: MetastoreServiceClient,
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
        .deserialize_indexes_metadata()
        .await?
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

pub mod lambda_search_api {
    pub use crate::elasticsearch_api::{
        es_compat_cat_indices_handler, es_compat_index_cat_indices_handler,
        es_compat_index_count_handler, es_compat_index_field_capabilities_handler,
        es_compat_index_multi_search_handler, es_compat_index_search_handler,
        es_compat_index_stats_handler, es_compat_resolve_index_handler, es_compat_scroll_handler,
        es_compat_search_handler, es_compat_stats_handler,
    };
    pub use crate::index_api::get_index_metadata_handler;
    pub use crate::rest::recover_fn;
    pub use crate::search_api::{search_get_handler, search_post_handler};
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport, ClusterNode};
    use quickwit_common::uri::Uri;
    use quickwit_common::ServiceStream;
    use quickwit_config::SearcherConfig;
    use quickwit_metastore::{metastore_for_test, IndexMetadata};
    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::ingest::ingester::{MockIngesterService, ObservationMessage};
    use quickwit_proto::metastore::{ListIndexesMetadataResponse, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, PipelineUid};
    use quickwit_search::Job;
    use tokio::sync::watch;

    use super::*;

    #[tokio::test]
    async fn test_check_cluster_configuration() {
        let services = HashSet::from_iter([QuickwitService::Metastore]);
        let peer_seeds = ["192.168.0.12:7280".to_string()];
        let mut mock_metastore = MockMetastoreService::new();

        mock_metastore
            .expect_endpoints()
            .return_const(vec![Uri::for_test("file:///qwdata/indexes")]);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    IndexMetadata::for_test("test-index", "file:///qwdata/indexes/test-index"),
                ]))
            });

        check_cluster_configuration(
            &services,
            &peer_seeds,
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_check_connectivity()
            .returning(move || {
                if *metastore_readiness_rx.borrow() {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Metastore not ready"))
                }
            });
        let (ingester_status_tx, ingester_status_rx) = watch::channel(IngesterStatus::Initializing);
        let mut mock_ingester = MockIngesterService::new();
        mock_ingester
            .expect_open_observation_stream()
            .returning(move |_| {
                let status_stream = ServiceStream::from(ingester_status_rx.clone());
                let observation_stream = status_stream.map(|status| {
                    let message = ObservationMessage {
                        node_id: "test-node".to_string(),
                        status: status as i32,
                    };
                    Ok(message)
                });
                Ok(observation_stream)
            });
        let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel();
        let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel();
        tokio::spawn(node_readiness_reporting_task(
            cluster.clone(),
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(mock_ingester),
            grpc_readiness_signal_rx,
            rest_readiness_signal_rx,
        ));
        assert!(!cluster.is_self_node_ready().await);

        grpc_readiness_trigger_tx.send(()).unwrap();
        rest_readiness_trigger_tx.send(()).unwrap();
        assert!(!cluster.is_self_node_ready().await);

        metastore_readiness_tx.send(true).unwrap();
        ingester_status_tx.send(IngesterStatus::Ready).unwrap();
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
        let node_config = NodeConfig::for_test();

        let (cluster_change_stream, cluster_change_stream_tx) =
            ClusterChangeStream::new_unbounded();
        let indexer_pool = IndexerPool::default();
        setup_indexer_pool(
            &node_config,
            cluster_change_stream,
            indexer_pool.clone(),
            Some(indexing_service_mailbox),
        );

        let new_indexer_node =
            ClusterNode::for_test("test-indexer-node", 1, true, &["indexer"], &[]).await;
        cluster_change_stream_tx
            .send(ClusterChange::Add(new_indexer_node))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(indexer_pool.len(), 1);

        let new_indexer_node_info = indexer_pool.get("test-indexer-node").unwrap();
        assert!(new_indexer_node_info.indexing_tasks.is_empty());

        let new_indexing_task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(0u128)),
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        let updated_indexer_node = ClusterNode::for_test(
            "test-indexer-node",
            1,
            true,
            &["indexer"],
            &[new_indexing_task.clone()],
        )
        .await;
        cluster_change_stream_tx
            .send(ClusterChange::Update(updated_indexer_node.clone()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let updated_indexer_node_info = indexer_pool.get("test-indexer-node").unwrap();
        assert_eq!(updated_indexer_node_info.indexing_tasks.len(), 1);
        assert_eq!(
            updated_indexer_node_info.indexing_tasks[0],
            new_indexing_task
        );

        cluster_change_stream_tx
            .send(ClusterChange::Remove(updated_indexer_node))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(indexer_pool.is_empty());
    }

    #[tokio::test]
    async fn test_setup_searcher() {
        let node_config = NodeConfig::for_test();
        let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default(), None));
        let metastore = metastore_for_test();
        let (change_stream, change_stream_tx) = ClusterChangeStream::new_unbounded();
        let storage_resolver = StorageResolver::unconfigured();
        let (search_job_placer, _searcher_service) = setup_searcher(
            &node_config,
            change_stream,
            metastore,
            storage_resolver,
            searcher_context,
        )
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
