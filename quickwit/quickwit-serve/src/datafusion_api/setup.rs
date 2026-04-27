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

//! Session construction and gRPC mounting helpers for the DataFusion subsystem.
//!
//! Compiled only when the `datafusion` feature is enabled; [`crate::lib`] and
//! [`crate::grpc`] each have a single `#[cfg(feature = "datafusion")]` call
//! site into this module.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytesize::ByteSize;
use futures::{StreamExt, stream};
use quickwit_cluster::{ClusterChange, ClusterChangeStream, ClusterNode};
use quickwit_common::tower::Change;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::grpc::DataFusionServiceGrpcImpl;
use quickwit_datafusion::proto::data_fusion_service_server::{
    DataFusionServiceServer, SERVICE_NAME as DATAFUSION_SERVICE_NAME,
};
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::{
    DataFusionService, DataFusionSessionBuilder, QuickwitObjectStoreRegistry,
    QuickwitWorkerResolver, build_worker,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::{SearchServiceClient, SearcherPool, create_search_client_from_grpc_addr};
use quickwit_storage::StorageResolver;
use tokio::time::timeout;
use tonic::transport::server::Router;
use tonic_reflection::pb::v1::ServerReflectionRequest;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

use crate::QuickwitServices;

/// Build the generic DataFusion session builder for this node.
///
/// Returns `None` if the searcher role is disabled or the endpoint toggle is
/// unset. The env toggle (`QW_ENABLE_DATAFUSION_ENDPOINT=true`) is kept
/// separate from the Cargo feature flag: the feature gates _whether the code
/// exists_; the env var gates _whether it runs at startup_. This lets a single
/// binary be deployed and have DataFusion toggled per-node.
///
/// Installs a [`QuickwitObjectStoreRegistry`] on the shared `RuntimeEnv` so
/// that any `quickwit://…`, `s3://…`, `file://…`, etc. URL produced by a
/// source resolves to an `ObjectStore` on first read — no startup warmup, no
/// per-query registry refresh.
pub(crate) fn build_datafusion_session_builder(
    node_config: &NodeConfig,
    cluster_change_stream: ClusterChangeStream,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> anyhow::Result<Option<Arc<DataFusionSessionBuilder>>> {
    if !node_config.is_service_enabled(QuickwitService::Searcher) {
        return Ok(None);
    }
    if !quickwit_common::get_bool_from_env("QW_ENABLE_DATAFUSION_ENDPOINT", false) {
        return Ok(None);
    }

    let metrics_source = Arc::new(MetricsDataSource::new(metastore));
    let schema_source = Arc::clone(&metrics_source);
    let datafusion_worker_pool = setup_datafusion_worker_pool(
        cluster_change_stream,
        node_config.grpc_config.max_message_size,
    );
    let worker_resolver = QuickwitWorkerResolver::new(datafusion_worker_pool)
        .with_tls(node_config.grpc_config.tls.is_some());
    let registry = Arc::new(QuickwitObjectStoreRegistry::new(storage_resolver));
    let builder = DataFusionSessionBuilder::new()
        .with_object_store_registry(registry)
        .context("failed to install DataFusion object store registry")?
        .with_runtime_plugin(Arc::clone(&metrics_source) as Arc<_>)
        .with_substrait_consumer(metrics_source as Arc<_>)
        .with_schema_provider_factory("quickwit", "public", move || {
            schema_source.schema_provider()
        })
        .with_worker_resolver(worker_resolver);
    Ok(Some(Arc::new(builder)))
}

fn setup_datafusion_worker_pool(
    cluster_change_stream: ClusterChangeStream,
    max_message_size: ByteSize,
) -> SearcherPool {
    let worker_pool = SearcherPool::default();
    let worker_change_stream = cluster_change_stream
        .then(move |cluster_change| datafusion_worker_changes(cluster_change, max_message_size))
        .flat_map(stream::iter);
    worker_pool.listen_for_changes(worker_change_stream);
    worker_pool
}

async fn datafusion_worker_changes(
    cluster_change: ClusterChange,
    max_message_size: ByteSize,
) -> Vec<Change<SocketAddr, SearchServiceClient>> {
    match cluster_change {
        ClusterChange::Add(node) if is_datafusion_worker_node(&node).await => {
            vec![insert_datafusion_worker(&node, max_message_size)]
        }
        ClusterChange::Remove(node) if node.is_searcher() => {
            vec![Change::Remove(node.grpc_advertise_addr())]
        }
        ClusterChange::Update { previous, updated } => {
            let mut changes = Vec::new();
            if previous.is_searcher() {
                changes.push(Change::Remove(previous.grpc_advertise_addr()));
            }
            if is_datafusion_worker_node(&updated).await {
                changes.push(insert_datafusion_worker(&updated, max_message_size));
            }
            changes
        }
        _ => Vec::new(),
    }
}

async fn is_datafusion_worker_node(node: &ClusterNode) -> bool {
    node.is_searcher() && exposes_datafusion_service(node).await
}

async fn exposes_datafusion_service(node: &ClusterNode) -> bool {
    // DataFusion nodes mount both the query service and distributed worker
    // service together. The query service descriptor is registered with gRPC
    // reflection, so it is the least invasive capability probe for mixed
    // deployments where some searchers were started without the env toggle.
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileContainingSymbol(
            DATAFUSION_SERVICE_NAME.to_string(),
        )),
    };
    let mut client = ServerReflectionClient::new(node.channel());
    let Ok(Ok(response)) = timeout(
        Duration::from_secs(1),
        client.server_reflection_info(stream::iter([request])),
    )
    .await
    else {
        return false;
    };
    let mut response_stream = response.into_inner();
    let Ok(Ok(Some(response))) = timeout(Duration::from_secs(1), response_stream.message()).await
    else {
        return false;
    };
    matches!(
        response.message_response,
        Some(MessageResponse::FileDescriptorResponse(_))
    )
}

fn insert_datafusion_worker(
    node: &ClusterNode,
    max_message_size: ByteSize,
) -> Change<SocketAddr, SearchServiceClient> {
    let grpc_addr = node.grpc_advertise_addr();
    Change::Insert(
        grpc_addr,
        create_search_client_from_grpc_addr(grpc_addr, max_message_size),
    )
}

/// Adapter that appends the DataFusion query and worker gRPC services to the
/// tonic `Router`. Construct via [`build_datafusion_mount`]; [`grpc.rs`] calls
/// [`Self::apply`] on the assembled router.
///
/// Hiding the closure behind a struct avoids exposing the
/// `WorkerServiceServer<Worker>` type publicly (it comes from a
/// `pub(crate)` module inside `datafusion-distributed`).
pub(crate) struct DataFusionMount {
    router_mod: Option<Box<dyn FnOnce(Router) -> Router + Send>>,
}

impl DataFusionMount {
    /// Returns a no-op mount — used when `services.datafusion_session_builder`
    /// is `None` (startup toggle off).
    fn noop() -> Self {
        Self { router_mod: None }
    }

    pub fn apply(self, router: Router) -> Router {
        match self.router_mod {
            Some(f) => f(router),
            None => router,
        }
    }
}

/// Build the mount adapter for the DataFusion gRPC services.
///
/// Side effect: pushes the DataFusion file-descriptor set and the two
/// service names into `file_descriptor_sets` / `enabled` **before** reflection
/// service construction, so `SHOW TABLES` / grpcurl list them correctly.
pub(crate) fn build_datafusion_mount(
    services: &QuickwitServices,
    max_message_size_bytes: usize,
    file_descriptor_sets: &mut Vec<&'static [u8]>,
    enabled: &mut BTreeSet<&'static str>,
) -> DataFusionMount {
    let Some(session_builder) = services.datafusion_session_builder.as_ref() else {
        return DataFusionMount::noop();
    };

    enabled.insert("datafusion");
    enabled.insert("datafusion-worker");
    file_descriptor_sets.push(quickwit_datafusion::proto::DATAFUSION_FILE_DESCRIPTOR_SET);

    let max_size = max_message_size_bytes;
    let session_builder = Arc::clone(session_builder);

    let router_mod = Box::new(move |router: Router| {
        let query_server = DataFusionServiceServer::new(DataFusionServiceGrpcImpl::new(
            DataFusionService::new(Arc::clone(&session_builder)),
        ))
        .max_decoding_message_size(max_size)
        .max_encoding_message_size(max_size);

        let worker = build_worker(Arc::clone(&session_builder));

        router
            .add_service(query_server)
            .add_service(worker.into_worker_server())
    });

    DataFusionMount {
        router_mod: Some(router_mod),
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ingester::IngesterStatus;

    use super::*;

    #[tokio::test]
    async fn datafusion_worker_changes_ignore_non_searcher_adds() {
        let node = ClusterNode::for_test(
            "indexer",
            1,
            false,
            &["indexer"],
            &[],
            IngesterStatus::Ready,
        )
        .await;

        let changes = datafusion_worker_changes(ClusterChange::Add(node), ByteSize::mib(1)).await;

        assert!(changes.is_empty());
    }

    #[tokio::test]
    async fn datafusion_worker_changes_remove_searchers_without_probe() {
        let node = ClusterNode::for_test(
            "searcher",
            1,
            false,
            &["searcher"],
            &[],
            IngesterStatus::Ready,
        )
        .await;
        let grpc_addr = node.grpc_advertise_addr();

        let changes =
            datafusion_worker_changes(ClusterChange::Remove(node), ByteSize::mib(1)).await;

        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], Change::Remove(addr) if *addr == grpc_addr));
    }
}
