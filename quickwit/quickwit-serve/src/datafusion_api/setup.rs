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
use std::sync::Arc;

use anyhow::Context;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::grpc::DataFusionServiceGrpcImpl;
use quickwit_datafusion::proto::data_fusion_service_server::DataFusionServiceServer;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::{
    DataFusionService, DataFusionSessionBuilder, QuickwitObjectStoreRegistry,
    QuickwitWorkerResolver, build_worker,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearcherPool;
use quickwit_storage::StorageResolver;
use tonic::transport::server::Router;

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
    searcher_pool: &SearcherPool,
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
    let worker_resolver = QuickwitWorkerResolver::new(searcher_pool.clone())
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
