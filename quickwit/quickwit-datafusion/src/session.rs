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

//! Generic DataFusion session builder.
//!
//! ## Runtime environment lifecycle
//!
//! `DataFusionSessionBuilder` creates a single `Arc<RuntimeEnv>` at construction
//! time and shares it across every session it builds.  This mirrors the pattern in
//! `dd-datafusion`'s `DDDataFusionRuntime`, where a shared `RuntimeEnv` lets
//! object stores registered at service-startup time be visible to all queries
//! without any per-query re-registration.
//!
//! ## Memory limits
//!
//! By default the shared `RuntimeEnv` uses DataFusion's `UnboundedMemoryPool`,
//! which imposes no cap on query memory.  For production deployments use
//! `with_memory_limit(bytes)` to install a `GreedyMemoryPool` that will error
//! queries exceeding the limit before they OOM the process.  This mirrors
//! `DDDataFusionRuntime::DDDataFusionRuntimeConfig::greedy_memory_pool_size`.
//!
//! ## Result materialization
//!
//! `execute_substrait` collects all result batches into memory before returning.
//! For large rollup queries this is unsuitable for production use — a streaming
//! variant returning `SendableRecordBatchStream` is the correct interface for
//! a real `DataFusionService` gRPC handler.  That handler is intentionally
//! deferred: Pomsky owns the `CloudPremService.SubstraitSearch` path; the OSS
//! `DataFusionService.ExecuteSubstrait` gRPC endpoint is a follow-up.
//!
//! ## Worker TLS
//!
//! `with_tls(true)` makes `QuickwitWorkerResolver` emit `https://` URLs.
//! However `datafusion-distributed`'s `DefaultChannelResolver` uses plain
//! `tonic::transport::Channel::from_shared(url)` — no certificate configuration.
//! In TLS clusters the worker connections will fail at the TLS handshake unless
//! a custom `ChannelResolver` that loads the right certs is registered via
//! `builder.set_distributed_channel_resolver(...)`.  This is currently unresolved.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use quickwit_search::SearcherPool;

use crate::catalog::QuickwitSchemaProvider;
use crate::data_source::QuickwitDataSource;
use crate::resolver::QuickwitWorkerResolver;
use crate::task_estimator::QuickwitTaskEstimator;

/// Builds `SessionContext`s for DataFusion queries over Quickwit data.
///
/// Holds a single `Arc<RuntimeEnv>` shared across all sessions it creates.
pub struct DataFusionSessionBuilder {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    searcher_pool: Option<SearcherPool>,
    use_tls: bool,
    /// Shared runtime environment — one instance for the lifetime of this builder.
    /// All sessions produced by `build_session()` use this env, so object stores
    /// registered by `source.init()` or lazily by `MetricsTableProvider::scan()`
    /// are immediately visible to every concurrent and subsequent session.
    runtime: Arc<RuntimeEnv>,
}

impl std::fmt::Debug for DataFusionSessionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionSessionBuilder")
            .field("num_sources", &self.sources.len())
            .field("has_searcher_pool", &self.searcher_pool.is_some())
            .finish()
    }
}

impl DataFusionSessionBuilder {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            searcher_pool: None,
            use_tls: false,
            runtime: Arc::new(RuntimeEnv::default()),
        }
    }

    /// Set a hard memory limit (bytes) for all queries built by this session builder.
    ///
    /// Installs a `GreedyMemoryPool` on the shared `RuntimeEnv`.  DataFusion will
    /// return an error from any query that attempts to allocate beyond this limit,
    /// preventing unbounded memory growth on large rollup queries.
    ///
    /// Without this, `RuntimeEnv::default()` uses `UnboundedMemoryPool` and
    /// large aggregations will OOM the process before DataFusion can intervene.
    ///
    /// Must be called before `with_source()` — sources call `init(&self.runtime)`
    /// on registration and expect the pool to be in place.
    pub fn with_memory_limit(mut self, bytes: usize) -> DFResult<Self> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)))
            .build_arc()?;
        self.runtime = runtime;
        Ok(self)
    }

    pub fn with_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Register a data source and call its `init` hook immediately.
    ///
    /// `init` receives the shared `RuntimeEnv` so sources that know their
    /// object-store URLs at construction time (e.g. a blob-store connector with
    /// a fixed bucket URI) can register them once here rather than on every scan.
    pub fn with_source(mut self, source: Arc<dyn QuickwitDataSource>) -> Self {
        source.init(&self.runtime);
        self.sources.push(source);
        self
    }

    pub fn with_searcher_pool(mut self, pool: SearcherPool) -> Self {
        self.searcher_pool = Some(pool);
        self
    }

    /// Returns the shared `RuntimeEnv`.
    ///
    /// Pass this to `build_quickwit_worker` so workers share the same object-store
    /// registry as the coordinator.
    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    /// Returns a slice of all registered data sources.
    pub fn sources(&self) -> &[Arc<dyn QuickwitDataSource>] {
        &self.sources
    }

    /// Validate that no two sources register conflicting UDF or UDAF names.
    ///
    /// Called automatically by `build_session()`.  You can also call it after
    /// `with_source()` to catch conflicts at registration time.
    pub fn check_invariants(&self) -> DFResult<()> {
        let mut seen_udfs: HashSet<String> = HashSet::new();
        let mut seen_udafs: HashSet<String> = HashSet::new();
        for source in &self.sources {
            let contribs = source.contributions();
            for name in contribs.udf_names() {
                if !seen_udfs.insert(name.clone()) {
                    return Err(datafusion::error::DataFusionError::Configuration(format!(
                        "two data sources both register a scalar UDF named '{name}'"
                    )));
                }
            }
            for name in contribs.udaf_names() {
                if !seen_udafs.insert(name.clone()) {
                    return Err(datafusion::error::DataFusionError::Configuration(format!(
                        "two data sources both register an aggregate UDAF named '{name}'"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Execute a Substrait plan (protobuf bytes) and return the results.
    ///
    /// # Memory and streaming caveat
    ///
    /// This method collects all result batches into memory before returning.
    /// For large rollup queries covering many splits this is unsuitable for
    /// production use.  The correct production interface is a streaming handler
    /// (returning `SendableRecordBatchStream`) wired into a `DataFusionService`
    /// gRPC endpoint.  That endpoint is deferred; Pomsky wraps this via
    /// `CloudPremService.SubstraitSearch`.  Use `with_memory_limit()` to bound
    /// memory usage until streaming is in place.
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
    ) -> DFResult<Vec<arrow::array::RecordBatch>> {
        use prost::Message;
        use datafusion_substrait::substrait::proto::Plan;

        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let ctx = self.build_session()?;
        crate::substrait::execute_substrait_plan(&plan, &ctx, &self.sources).await
    }

    /// Build a `SessionContext` backed by the shared `RuntimeEnv`.
    ///
    /// Calls `check_invariants()` first — returns an error if two registered
    /// sources contribute conflicting UDF or UDAF names.
    pub fn build_session(&self) -> DFResult<SessionContext> {
        self.check_invariants()?;

        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
        // We register our own catalog; skip the default "datafusion" one.
        config.options_mut().catalog.create_default_catalog_and_schema = false;

        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            // All sessions share the same RuntimeEnv so object stores registered
            // at startup (via init) or lazily (via scan) are globally visible.
            .with_runtime_env(Arc::clone(&self.runtime));

        if let Some(ref pool) = self.searcher_pool {
            let worker_resolver =
                QuickwitWorkerResolver::new(pool.clone()).with_tls(self.use_tls);
            builder = builder
                .with_distributed_worker_resolver(worker_resolver)
                .with_distributed_task_estimator(QuickwitTaskEstimator)
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        let mut combined = crate::data_source::DataSourceContributions::default();
        for source in &self.sources {
            combined.merge(source.contributions());
        }
        builder = combined.apply_to_builder(builder);

        let mut state = builder.build();

        for source in &self.sources {
            let Some((ft, factory)) = source.ddl_registration() else {
                continue;
            };
            state
                .table_factories_mut()
                .insert(ft.clone(), Arc::clone(&factory));
            state
                .table_factories_mut()
                .insert(ft.to_uppercase(), Arc::clone(&factory));
        }

        let ctx = SessionContext::new_with_state(state);

        let schema_provider = Arc::new(QuickwitSchemaProvider::new(self.sources.clone()));
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", schema_provider)
            .expect("register quickwit schema");
        ctx.register_catalog("quickwit", catalog);

        Ok(ctx)
    }
}
