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

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use bytesize::ByteSize;
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::retry::RetryParams;
use quickwit_common::tower::{
    EventListenerLayer, GrpcMetricsLayer, LoadShedLayer, RetryLayer, RetryPolicy, TimeoutLayer,
};
use quickwit_common::uri::Uri;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_metastore::MetastoreResolver;
use quickwit_proto::metastore::MetastoreServiceClient;
use tower::ServiceBuilder;
use tracing::info;

use crate::balance_channel_for_service;

const METASTORE_CLIENT_MAX_CONCURRENCY_ENV_KEY: &str = "QW_METASTORE_CLIENT_MAX_CONCURRENCY";
const DEFAULT_METASTORE_CLIENT_MAX_CONCURRENCY: usize = 6;
const GRPC_METASTORE_SERVICE_TIMEOUT: Duration = Duration::from_secs(10);

static METASTORE_GRPC_CLIENT_METRICS_LAYER: LazyLock<GrpcMetricsLayer> =
    LazyLock::new(|| GrpcMetricsLayer::new("metastore", "client"));
static METASTORE_GRPC_SERVER_METRICS_LAYER: LazyLock<GrpcMetricsLayer> =
    LazyLock::new(|| GrpcMetricsLayer::new("metastore", "server"));

fn get_metastore_client_max_concurrency() -> usize {
    quickwit_common::get_from_env(
        METASTORE_CLIENT_MAX_CONCURRENCY_ENV_KEY,
        DEFAULT_METASTORE_CLIENT_MAX_CONCURRENCY,
        false,
    )
}

/// The metastore gRPC server this node serves locally — or `NotServed` if it serves none.
///
/// A node serves *either* a writable primary metastore (`metastore` role) *or* a read-only
/// replica (`metastore_read_replica` role), never both: the two roles are mutually exclusive, as
/// enforced by `validate_metastore_read_replica` at config load.
pub(super) enum LocalMetastoreServer {
    /// Writable primary metastore.
    Primary(MetastoreServiceClient),
    /// Read-only metastore replica.
    ReadReplica(MetastoreServiceClient),
    /// This node does not serve a metastore locally (it may still be a client of a remote one).
    NotServed,
}

impl LocalMetastoreServer {
    /// The locally served gRPC metastore client, if this node serves one.
    pub(super) fn client(&self) -> Option<&MetastoreServiceClient> {
        match self {
            LocalMetastoreServer::Primary(client) | LocalMetastoreServer::ReadReplica(client) => {
                Some(client)
            }
            LocalMetastoreServer::NotServed => None,
        }
    }

    /// Resolves the primary (writable) metastore client for this node.
    pub(super) async fn resolve_primary_client(
        &self,
        cluster: &Cluster,
        node_config: &NodeConfig,
    ) -> anyhow::Result<MetastoreServiceClient> {
        match self {
            LocalMetastoreServer::Primary(metastore_server) => Ok(metastore_server.clone()),
            LocalMetastoreServer::ReadReplica(_) | LocalMetastoreServer::NotServed => {
                Self::build_metastore_client(
                    cluster,
                    QuickwitService::Metastore,
                    node_config.grpc_config.max_message_size,
                )
                .await
            }
        }
    }

    /// Resolves the read-only metastore client for this node.
    pub(super) async fn resolve_read_only_client(
        &self,
        cluster: &Cluster,
        node_config: &NodeConfig,
    ) -> anyhow::Result<Option<MetastoreServiceClient>> {
        let should_use_remote_read_replica = node_config
            .is_service_enabled(QuickwitService::Searcher)
            && node_config.searcher_config.use_metastore_read_replica;

        match self {
            LocalMetastoreServer::ReadReplica(metastore_server) => {
                Ok(Some(metastore_server.clone()))
            }
            LocalMetastoreServer::Primary(_) | LocalMetastoreServer::NotServed
                if should_use_remote_read_replica =>
            {
                let read_replica_client = Self::build_metastore_client(
                    cluster,
                    QuickwitService::MetastoreReadReplica,
                    node_config.grpc_config.max_message_size,
                )
                .await?;
                Ok(Some(read_replica_client))
            }
            LocalMetastoreServer::Primary(_) | LocalMetastoreServer::NotServed => Ok(None),
        }
    }

    async fn build_metastore_client(
        cluster: &Cluster,
        service: QuickwitService,
        max_message_size: ByteSize,
    ) -> anyhow::Result<MetastoreServiceClient> {
        info!(%service, "connecting to {service} service");

        let balance_channel = balance_channel_for_service(cluster, service).await;

        ensure!(
            balance_channel
                .wait_for(Duration::from_secs(300), |connections| {
                    !connections.is_empty()
                })
                .await,
            "could not find any `{service}` node in the cluster"
        );
        Ok(MetastoreServiceClient::tower()
            .stack_layer(RetryLayer::new(RetryPolicy::from(RetryParams::standard())))
            .stack_layer(TimeoutLayer::new(GRPC_METASTORE_SERVICE_TIMEOUT))
            .stack_layer(METASTORE_GRPC_CLIENT_METRICS_LAYER.clone())
            .stack_layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                get_metastore_client_max_concurrency(),
            ))
            .build_from_balance_channel(balance_channel, max_message_size, None))
    }

    fn metastore_max_in_flight_requests(node_config: &NodeConfig, uri: &Uri) -> usize {
        if uri.protocol().is_database() {
            node_config
                .metastore_configs
                .find_postgres()
                .map(|config| config.max_connections.get() * 2)
                .unwrap_or_default()
                .max(100)
        } else {
            100
        }
    }
}

pub(super) async fn start_metastore_service_if_needed(
    node_config: &NodeConfig,
    metastore_resolver: &MetastoreResolver,
    event_broker: &EventBroker,
) -> anyhow::Result<LocalMetastoreServer> {
    // Instantiate a primary metastore server if the `metastore` role is enabled on the node.
    if node_config.is_service_enabled(QuickwitService::Metastore) {
        info!(
            metastore_kind = "primary",
            "starting local metastore service"
        );
        let metastore: MetastoreServiceClient = metastore_resolver
            .resolve(&node_config.metastore_uri)
            .await
            .with_context(|| {
                format!(
                    "failed to resolve metastore uri `{}`",
                    node_config.metastore_uri
                )
            })?;
        // These layers apply to all the RPCs of the metastore.
        let shared_layer = ServiceBuilder::new()
            .layer(METASTORE_GRPC_SERVER_METRICS_LAYER.clone())
            .layer(LoadShedLayer::new(
                LocalMetastoreServer::metastore_max_in_flight_requests(
                    node_config,
                    &node_config.metastore_uri,
                ),
            ))
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
        return Ok(LocalMetastoreServer::Primary(metastore));
    }
    // Instantiate a read-only metastore replica server if the `metastore_read_replica` role is
    // enabled on the node.
    if node_config.is_service_enabled(QuickwitService::MetastoreReadReplica) {
        info!(
            metastore_kind = "read_replica",
            "starting local metastore service"
        );
        let Some(read_replica_uri) = &node_config.metastore_read_replica_uri else {
            bail!(
                "`metastore_read_replica_uri` must be set when the `metastore_read_replica` role \
                 is enabled"
            );
        };
        let metastore: MetastoreServiceClient = metastore_resolver
            .resolve_read_only(read_replica_uri)
            .await
            .with_context(|| {
                format!("failed to resolve metastore read replica uri `{read_replica_uri}`")
            })?;
        let shared_layer = ServiceBuilder::new()
            .layer(METASTORE_GRPC_SERVER_METRICS_LAYER.clone())
            .layer(LoadShedLayer::new(
                LocalMetastoreServer::metastore_max_in_flight_requests(
                    node_config,
                    read_replica_uri,
                ),
            ))
            .into_inner();
        let metastore = MetastoreServiceClient::tower()
            .stack_layer(shared_layer)
            .build(metastore);
        return Ok(LocalMetastoreServer::ReadReplica(metastore));
    }
    Ok(LocalMetastoreServer::NotServed)
}
