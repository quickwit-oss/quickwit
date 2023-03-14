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

#![deny(clippy::disallowed_methods)]

mod args;
mod format;
mod metrics;

mod grpc;
mod rest;

mod cluster_api;
mod delete_task_api;
mod elastic_search_api;
mod health_check_api;
mod index_api;
mod indexing_api;
mod ingest_api;
mod node_info_handler;
mod openapi;
mod search_api;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod ui_handler;

use std::collections::HashSet;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use byte_unit::n_mib_bytes;
use format::BodyFormat;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::{Cluster, ClusterMember};
use quickwit_common::pubsub::{EventBroker, EventSubscriptionHandle};
use quickwit_common::tower::{
    BufferLayer, ConstantRate, EstimateRateLayer, Rate, RateLimitLayer, SmaRateEstimator,
};
use quickwit_config::service::QuickwitService;
use quickwit_config::{load_index_config_from_user_config, ConfigFormat, QuickwitConfig};
use quickwit_control_plane::{start_control_plane_service, ControlPlaneServiceClient};
use quickwit_core::{IndexService, IndexServiceError};
use quickwit_grpc_clients::create_balance_channel_from_watched_members;
use quickwit_grpc_clients::service_client_pool::ServiceClientPool;
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::start_indexing_service;
use quickwit_ingest_api::{
    start_ingest_api_service, GetMemoryCapacity, IngestRequest, IngestServiceClient, MemoryCapacity,
};
use quickwit_janitor::{start_janitor_service, JanitorService};
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, Metastore, MetastoreError, MetastoreEvent,
    MetastoreEventPublisher, MetastoreGrpcClient, RetryingMetastore,
};
use quickwit_opentelemetry::otlp::{OTEL_LOGS_INDEX_CONFIG, OTEL_TRACE_INDEX_CONFIG};
use quickwit_search::{start_searcher_service, SearchJobPlacer, SearchService};
use quickwit_storage::quickwit_storage_uri_resolver;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{error, warn};
use warp::{Filter, Rejection};

pub use crate::args::ServeArgs;
pub use crate::index_api::ListSplitsQueryParams;
pub use crate::metrics::SERVE_METRICS;
#[cfg(test)]
use crate::rest::recover_fn;
pub use crate::search_api::{SearchRequestQueryString, SortByField};

const READINESS_REPORTING_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(25)
} else {
    Duration::from_secs(10)
};

struct QuickwitServices {
    pub config: Arc<QuickwitConfig>,
    pub build_info: &'static QuickwitBuildInfo,
    pub cluster: Arc<Cluster>,
    pub metastore: Arc<dyn Metastore>,
    pub control_plane_service: Option<ControlPlaneServiceClient>,
    /// The control plane listens to metastore events.
    /// We need to keep the subscription handle to keep listening. If not, subscription is dropped.
    #[allow(dead_code)]
    pub control_plane_subscription_handle: Option<EventSubscriptionHandle<MetastoreEvent>>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,
    pub indexing_service: Option<Mailbox<IndexingService>>,
    pub janitor_service: Option<Mailbox<JanitorService>>,
    pub ingest_service: IngestServiceClient,
    pub index_service: Arc<IndexService>,
    pub services: HashSet<QuickwitService>,
}

fn has_node_with_metastore_service(members: &[ClusterMember]) -> bool {
    members.iter().any(|member| {
        member
            .enabled_services
            .contains(&QuickwitService::Metastore)
    })
}

pub async fn serve_quickwit(config: QuickwitConfig) -> anyhow::Result<()> {
    let universe = Universe::new();
    let event_broker = EventBroker::default();
    let storage_resolver = quickwit_storage_uri_resolver().clone();
    let cluster =
        quickwit_cluster::start_cluster_service(&config, &config.enabled_services).await?;

    // Instantiate either a file-backed or postgresql [`Metastore`] if the node runs a `Metastore`
    // service, else instantiate a [`MetastoreGrpcClient`].
    let metastore: Arc<dyn Metastore> = if config
        .enabled_services
        .contains(&QuickwitService::Metastore)
    {
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&config.metastore_uri)
            .await?;
        Arc::new(MetastoreEventPublisher::new(
            metastore,
            event_broker.clone(),
        ))
    } else {
        // Wait 10 seconds for nodes running a `Metastore` service.
        cluster
            .wait_for_members(has_node_with_metastore_service, Duration::from_secs(10))
            .await
            .map_err(|_| {
                error!("No metastore service found among cluster members, stopping server.");
                anyhow!(
                    "Failed to start server: no metastore service was found among cluster \
                     members. Try running Quickwit with additional metastore service `quickwit \
                     run --service metastore`."
                )
            })?;
        let grpc_metastore_client = MetastoreGrpcClient::create_and_update_from_members(
            1,
            cluster.ready_member_change_watcher(),
        )
        .await?;
        let metastore_client = RetryingMetastore::new(Box::new(grpc_metastore_client));
        Arc::new(metastore_client)
    };

    check_cluster_configuration(
        &config.enabled_services,
        &config.peer_seeds,
        metastore.clone(),
    )
    .await?;

    // Always instantiate index management service.
    let index_service = Arc::new(IndexService::new(
        metastore.clone(),
        storage_resolver.clone(),
    ));

    // Instantiate the control plane service if enabled.
    // If not and metastore service is enabled, we need to instantiate the control plane client
    // so the metastore can notify the control plane.
    let control_plane_service: Option<ControlPlaneServiceClient> = if config
        .enabled_services
        .contains(&QuickwitService::ControlPlane)
    {
        let control_plane_mailbox =
            start_control_plane_service(&universe, cluster.clone(), metastore.clone()).await?;
        Some(ControlPlaneServiceClient::from_mailbox(
            control_plane_mailbox,
        ))
    } else if config
        .enabled_services
        .contains(&QuickwitService::Metastore)
    {
        let (channel, _) = create_balance_channel_from_watched_members(
            cluster.ready_member_change_watcher(),
            QuickwitService::ControlPlane,
        )
        .await?;
        Some(ControlPlaneServiceClient::from_channel(channel))
    } else {
        None
    };
    let control_plane_subscription_handle =
        control_plane_service.as_ref().map(|scheduler_service| {
            event_broker.subscribe::<MetastoreEvent>(scheduler_service.clone())
        });

    let (ingest_service, indexing_service) = if config
        .enabled_services
        .contains(&QuickwitService::Indexer)
    {
        let ingest_api_service =
            start_ingest_api_service(&universe, &config.data_dir_path, &config.ingest_api_config)
                .await?;
        if config.indexer_config.enable_otlp_endpoint {
            for index_config_content in [OTEL_LOGS_INDEX_CONFIG, OTEL_TRACE_INDEX_CONFIG] {
                let index_config = load_index_config_from_user_config(
                    ConfigFormat::Yaml,
                    index_config_content.as_bytes(),
                    &config.default_index_root_uri,
                )?;
                match index_service.create_index(index_config, false).await {
                    Ok(_)
                    | Err(IndexServiceError::MetastoreError(
                        MetastoreError::IndexAlreadyExists { .. },
                    )) => Ok(()),
                    Err(error) => Err(error),
                }?;
            }
        }
        let indexing_service = start_indexing_service(
            &universe,
            &config,
            cluster.clone(),
            metastore.clone(),
            storage_resolver.clone(),
        )
        .await?;
        let num_buckets = NonZeroUsize::new(60).unwrap();
        let initial_rate = ConstantRate::new(n_mib_bytes!(50), Duration::from_secs(1));
        let rate_estimator = SmaRateEstimator::new(
            num_buckets,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .with_initial_rate(initial_rate);
        let memory_capacity = ingest_api_service.ask(GetMemoryCapacity).await?;
        let min_rate = ConstantRate::new(n_mib_bytes!(1), Duration::from_millis(100));
        let rate_modulator = RateModulator::new(rate_estimator.clone(), memory_capacity, min_rate);
        let ingest_service = IngestServiceClient::tower()
            .ingest_layer(
                ServiceBuilder::new()
                    .layer(EstimateRateLayer::<IngestRequest, _>::new(rate_estimator))
                    .layer(BufferLayer::new(100))
                    .layer(RateLimitLayer::new(rate_modulator))
                    .into(),
            )
            .service(IngestServiceClient::from_mailbox(ingest_api_service));
        (ingest_service, Some(indexing_service))
    } else {
        let (channel, _) = create_balance_channel_from_watched_members(
            cluster.ready_member_change_watcher(),
            QuickwitService::Indexer,
        )
        .await?;
        let ingest_service = IngestServiceClient::from_channel(channel);
        (ingest_service, None)
    };

    let search_job_placer = SearchJobPlacer::new(
        ServiceClientPool::create_and_update_members(cluster.ready_member_change_watcher()).await?,
    );

    let janitor_service = if config.enabled_services.contains(&QuickwitService::Janitor) {
        let janitor_service = start_janitor_service(
            &universe,
            &config,
            metastore.clone(),
            search_job_placer.clone(),
            storage_resolver.clone(),
        )
        .await?;
        Some(janitor_service)
    } else {
        None
    };

    let search_service: Arc<dyn SearchService> = start_searcher_service(
        &config,
        metastore.clone(),
        storage_resolver,
        search_job_placer,
    )
    .await?;

    let grpc_listen_addr = config.grpc_listen_addr;
    let rest_listen_addr = config.rest_listen_addr;
    let services = config.enabled_services.clone();
    let quickwit_services = QuickwitServices {
        config: Arc::new(config),
        build_info: quickwit_build_info(),
        cluster: cluster.clone(),
        metastore: metastore.clone(),
        control_plane_service,
        control_plane_subscription_handle,
        search_service,
        indexing_service,
        janitor_service,
        ingest_service,
        index_service,
        services,
    };
    let grpc_server = grpc::start_grpc_server(grpc_listen_addr, &quickwit_services);
    let rest_server = rest::start_rest_server(rest_listen_addr, &quickwit_services);

    // Node readiness indicates that the server is ready to receive requests.
    // Thus readiness task is started once gRPC and REST servers are started.
    tokio::spawn(node_readiness_reporting_task(cluster, metastore));

    tokio::try_join!(grpc_server, rest_server)?;
    Ok(())
}

#[derive(Clone)]
struct RateModulator<R> {
    rate_estimator: R,
    memory_capacity: MemoryCapacity,
    min_rate: ConstantRate,
}

impl<R> RateModulator<R>
where R: Rate
{
    /// Creates a new `RateModulator` instance.
    ///
    /// # Panics
    ///
    /// Panics if `rate_estimator` and `min_rate` have different periods.
    pub fn new(rate_estimator: R, memory_capacity: MemoryCapacity, min_rate: ConstantRate) -> Self {
        assert_eq!(
            rate_estimator.period(),
            min_rate.period(),
            "Rate estimator and min rate periods must be equal."
        );

        Self {
            rate_estimator,
            memory_capacity,
            min_rate,
        }
    }
}

impl<R> Rate for RateModulator<R>
where R: Rate
{
    fn work(&self) -> u64 {
        let memory_usage_ratio = self.memory_capacity.usage_ratio();
        let work = self.rate_estimator.work().max(self.min_rate.work());

        if memory_usage_ratio < 0.25 {
            work * 2
        } else if memory_usage_ratio > 0.99 {
            work / 32
        } else if memory_usage_ratio > 0.98 {
            work / 16
        } else if memory_usage_ratio > 0.95 {
            work / 8
        } else if memory_usage_ratio > 0.90 {
            work / 4
        } else if memory_usage_ratio > 0.80 {
            work / 2
        } else if memory_usage_ratio > 0.70 {
            work * 2 / 3
        } else {
            work
        }
    }

    fn period(&self) -> Duration {
        self.rate_estimator.period()
    }
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
async fn node_readiness_reporting_task(cluster: Arc<Cluster>, metastore: Arc<dyn Metastore>) {
    let mut interval = tokio::time::interval(READINESS_REPORTING_INTERVAL);
    loop {
        interval.tick().await;
        let node_ready = metastore.check_connectivity().await.is_ok();
        cluster.set_self_node_ready(node_ready).await;
    }
}

/// Displays some warnings if the cluster runs a file-backed metastore or serves file-backed
/// indexes.
async fn check_cluster_configuration(
    services: &HashSet<QuickwitService>,
    peer_seeds: &[String],
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<()> {
    if !services.contains(&QuickwitService::Metastore) || peer_seeds.is_empty() {
        return Ok(());
    }
    if !metastore.uri().protocol().is_database() {
        warn!(
            metastore_uri=%metastore.uri(),
            "Using a file-backed metastore in cluster mode is not recommended for production use. Running multiple file-backed metastores simultaneously can lead to data loss."
        );
    }
    let file_backed_indexes = metastore
        .list_indexes_metadatas()
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

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuickwitBuildInfo {
    pub build_date: &'static str,
    pub build_profile: &'static str,
    pub build_target: &'static str,
    pub cargo_pkg_version: &'static str,
    pub commit_date: &'static str,
    pub commit_hash: &'static str,
    pub commit_short_hash: &'static str,
    pub commit_tags: Vec<String>,
    pub version: String,
}

/// QuickwitBuildInfo from env variables prepopulated by the build script or CI env.
pub fn quickwit_build_info() -> &'static QuickwitBuildInfo {
    const UNKNOWN: &str = "unknown";

    static INSTANCE: OnceCell<QuickwitBuildInfo> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let commit_date = option_env!("QW_COMMIT_DATE")
            .filter(|commit_date| !commit_date.is_empty())
            .unwrap_or(UNKNOWN);
        let commit_hash = option_env!("QW_COMMIT_HASH")
            .filter(|commit_hash| !commit_hash.is_empty())
            .unwrap_or(UNKNOWN);
        let commit_short_hash = option_env!("QW_COMMIT_HASH")
            .filter(|commit_hash| commit_hash.len() >= 7)
            .map(|commit_hash| &commit_hash[..7])
            .unwrap_or(UNKNOWN);
        let commit_tags: Vec<String> = option_env!("QW_COMMIT_TAGS")
            .map(|tags| {
                tags.split(',')
                    .map(|tag| tag.trim().to_string())
                    .filter(|tag| !tag.is_empty())
                    .sorted()
                    .collect()
            })
            .unwrap_or_default();

        let version = commit_tags
            .iter()
            .find(|tag| tag.starts_with('v'))
            .cloned()
            .unwrap_or_else(|| concat!(env!("CARGO_PKG_VERSION"), "-nightly").to_string());

        QuickwitBuildInfo {
            build_date: env!("BUILD_DATE"),
            build_profile: env!("BUILD_PROFILE"),
            build_target: env!("BUILD_TARGET"),
            cargo_pkg_version: env!("CARGO_PKG_VERSION"),
            commit_date,
            commit_hash,
            commit_short_hash,
            commit_tags,
            version,
        }
    })
}
