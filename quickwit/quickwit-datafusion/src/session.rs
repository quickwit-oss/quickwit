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
//! `with_memory_limit(bytes)` to install a `GreedyMemoryPool`.
//!
//! ## Worker URL resolution
//!
//! The default path uses `with_searcher_pool(pool)` which wraps the pool in a
//! `QuickwitWorkerResolver`.  For deployments that don't use `SearcherPool` for
//! service discovery (e.g., a downstream caller using custom service discovery, Consul, or a Chitchat
//! variant), use `with_worker_resolver(resolver)` to supply any type that
//! implements `datafusion_distributed::WorkerResolver`.
//!
//! ## Result materialization
//!
//! `execute_substrait` collects all result batches into memory before returning.
//! For large rollup queries this is unsuitable for production use.  A streaming
//! variant is deferred; A downstream caller can wrap this via its own gRPC handler.
//! Use `with_memory_limit()` to bound memory usage until streaming is in place.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, WorkerResolver,
};
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
    /// Pluggable worker URL resolver.  `None` = single-node execution.
    /// Set via `with_searcher_pool` (default impl) or `with_worker_resolver`
    /// (custom impl for other service discovery).
    worker_resolver: Option<Arc<dyn WorkerResolver + Send + Sync>>,
    /// Shared runtime environment — one instance for the lifetime of this builder.
    runtime: Arc<RuntimeEnv>,
}

impl std::fmt::Debug for DataFusionSessionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionSessionBuilder")
            .field("num_sources", &self.sources.len())
            .field("distributed", &self.worker_resolver.is_some())
            .finish()
    }
}

impl Default for DataFusionSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFusionSessionBuilder {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            worker_resolver: None,
            runtime: Arc::new(RuntimeEnv::default()),
        }
    }

    /// Set a hard memory limit (bytes) for all queries built by this session builder.
    ///
    /// Installs a `GreedyMemoryPool` on the shared `RuntimeEnv`.  DataFusion will
    /// return an error from any query that attempts to allocate beyond this limit,
    /// preventing unbounded memory growth on large rollup queries.
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

    /// Register a data source and call its `init` hook immediately.
    ///
    /// `init` receives the shared `RuntimeEnv` so sources that know their
    /// object-store URLs at construction time can register them once here.
    pub fn with_source(mut self, source: Arc<dyn QuickwitDataSource>) -> Self {
        source.init(&self.runtime);
        self.sources.push(source);
        self
    }

    /// Enable distributed execution using the default `SearcherPool`-backed
    /// resolver.
    ///
    /// Worker URLs are derived from the pool's socket-address keys using plain
    /// `http://` (or `https://` if you have separately configured TLS on the
    /// `QuickwitWorkerResolver`).  For non-`SearcherPool` deployments, use
    /// `with_worker_resolver` instead.
    pub fn with_searcher_pool(self, pool: SearcherPool) -> Self {
        self.with_worker_resolver(QuickwitWorkerResolver::new(pool))
    }

    /// Enable distributed execution with a custom worker URL resolver.
    ///
    /// Use this when `SearcherPool` is not the right abstraction — for example:
    /// - A downstream caller using custom service discovery or topology.
    /// - Tests use a fixed list of mock worker addresses.
    /// - TLS deployments need `QuickwitWorkerResolver::new(pool).with_tls(true)`.
    ///
    /// Any type implementing `datafusion_distributed::WorkerResolver` is accepted.
    pub fn with_worker_resolver(
        mut self,
        resolver: impl WorkerResolver + Send + Sync + 'static,
    ) -> Self {
        self.worker_resolver = Some(Arc::new(resolver));
        self
    }

    /// Returns the shared `RuntimeEnv`.
    ///
    /// Pass this to `build_quickwit_worker` so workers share the same
    /// object-store registry as the coordinator.
    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    /// Returns a slice of all registered data sources.
    pub fn sources(&self) -> &[Arc<dyn QuickwitDataSource>] {
        &self.sources
    }

    /// Validate that no two sources register conflicting UDF or UDAF names.
    ///
    /// This is a development-time sanity check — call it once at service startup
    /// after all sources are registered, not on every query.  It is not called
    /// automatically by `build_session()`.
    ///
    /// ```ignore
    /// let builder = DataFusionSessionBuilder::new()
    ///     .with_source(source_a)
    ///     .with_source(source_b);
    /// builder.check_invariants()?;  // fail fast at startup
    /// // ... serve queries
    /// ```
    pub fn check_invariants(&self) -> DFResult<()> {
        let mut seen_udfs: HashSet<String> = HashSet::new();
        for source in &self.sources {
            let contribs = source.contributions();
            for name in contribs.udf_names() {
                if !seen_udfs.insert(name.clone()) {
                    return Err(datafusion::error::DataFusionError::Configuration(format!(
                        "two data sources both register a scalar UDF named '{name}'"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Execute a Substrait plan (protobuf bytes) and return the results.
    ///
    /// Builds a fresh session, converts the plan via `QuickwitSubstraitConsumer`,
    /// and collects all results into memory.  See the module-level doc on
    /// materialization limits.
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
    ) -> DFResult<Vec<arrow::array::RecordBatch>> {
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let ctx = self.build_session()?;
        crate::substrait::execute_substrait_plan(&plan, &ctx, &self.sources).await
    }

    /// Build a `SessionContext` backed by the shared `RuntimeEnv`.
    ///
    /// Does NOT call `check_invariants()` — callers should invoke that once at
    /// startup, not on every query.
    pub fn build_session(&self) -> DFResult<SessionContext> {
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

        if let Some(resolver) = &self.worker_resolver {
            // Clone the Arc so ownership passes into the distributed extension.
            // `Arc<dyn WorkerResolver>` implements `WorkerResolver` via deref,
            // so the forwarding wrapper is not needed.
            builder = builder
                .with_distributed_worker_resolver(ArcWorkerResolver(Arc::clone(resolver)))
                .with_distributed_task_estimator(QuickwitTaskEstimator)
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        // Accumulate contributions from all sources and apply them at once.
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
            .map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "failed to register 'public' schema: {e}"
                ))
            })?;
        ctx.register_catalog("quickwit", catalog);

        Ok(ctx)
    }
}

/// Newtype wrapper so `Arc<dyn WorkerResolver>` can be passed to
/// `with_distributed_worker_resolver`, which requires an owned `impl WorkerResolver`.
///
/// `Arc<dyn WorkerResolver + Send + Sync>` cannot be passed directly because
/// the trait bound requires `Sized`.  This wrapper is `'static` and satisfies
/// the `WorkerResolver + Send + Sync + 'static` bound.
struct ArcWorkerResolver(Arc<dyn WorkerResolver + Send + Sync>);

impl WorkerResolver for ArcWorkerResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, datafusion::error::DataFusionError> {
        self.0.get_urls()
    }
}
