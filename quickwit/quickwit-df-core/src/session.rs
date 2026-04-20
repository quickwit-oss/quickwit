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
//! time and shares it across every session it builds. A shared `RuntimeEnv` lets
//! object stores registered at service-startup time be visible to all queries
//! without any per-query re-registration.
//!
//! ## Memory limits
//!
//! By default the shared `RuntimeEnv` uses DataFusion's default memory pool,
//! which imposes no cap on query memory. For production deployments use
//! `with_memory_limit(bytes)` to install a `GreedyMemoryPool`.
//!
//! ## Worker URL resolution
//!
//! Enable distributed execution by calling `with_worker_resolver(resolver)` with
//! any type that implements `datafusion_distributed::WorkerResolver`. Downstream
//! crates (e.g. `quickwit-datafusion`) provide concrete resolvers backed by
//! Quickwit's `SearcherPool`.
//!
//! ## Result streaming
//!
//! `execute_substrait` returns a `SendableRecordBatchStream` backed by the
//! DataFusion streaming executor — no intermediate materialization occurs.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, TaskEstimator, WorkerResolver,
};

use crate::catalog::QuickwitSchemaProvider;
use crate::data_source::QuickwitDataSource;
use crate::task_estimator::DataSourceExecPartitionEstimator;

/// Builds `SessionContext`s for DataFusion queries over registered data sources.
///
/// Holds a single `Arc<RuntimeEnv>` shared across all sessions it creates.
pub struct DataFusionSessionBuilder {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    /// Pluggable worker URL resolver.  `None` = single-node execution.
    worker_resolver: Option<Arc<dyn WorkerResolver + Send + Sync>>,
    /// Task estimator applied when distributed execution is enabled. Defaults to
    /// [`DataSourceExecPartitionEstimator`], which works for any `DataSourceExec`
    /// plan (parquet, tantivy, custom). Override via [`with_task_estimator`] for
    /// source-specific heuristics.
    task_estimator: Arc<dyn TaskEstimator + Send + Sync>,
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
            task_estimator: Arc::new(DataSourceExecPartitionEstimator),
            runtime: Arc::new(RuntimeEnv::default()),
        }
    }

    /// Set a hard memory limit (bytes) for all queries built by this session builder.
    ///
    /// Installs a `GreedyMemoryPool` on the shared `RuntimeEnv`.  DataFusion will
    /// return an error from any query that attempts to allocate beyond this limit.
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

    /// Install a custom `ObjectStoreRegistry` on the shared `RuntimeEnv`.
    ///
    /// Use this when the glue layer wants to resolve object stores on
    /// demand — for example, `quickwit_datafusion::QuickwitObjectStoreRegistry`
    /// builds lazy wrappers over a `StorageResolver` on miss.
    ///
    /// Must be called before `with_source()` so that any source-side
    /// registration through the `init` hook uses the intended registry.
    pub fn with_object_store_registry(
        mut self,
        registry: Arc<dyn ObjectStoreRegistry>,
    ) -> DFResult<Self> {
        self.runtime = RuntimeEnvBuilder::new()
            .with_object_store_registry(registry)
            .build_arc()?;
        Ok(self)
    }

    /// Register a data source and call its `init` hook immediately.
    pub fn with_source(mut self, source: Arc<dyn QuickwitDataSource>) -> Self {
        source.init(&self.runtime);
        self.sources.push(source);
        self
    }

    /// Enable distributed execution with the given worker URL resolver.
    pub fn with_worker_resolver(
        mut self,
        resolver: impl WorkerResolver + Send + Sync + 'static,
    ) -> Self {
        self.worker_resolver = Some(Arc::new(resolver));
        self
    }

    /// Override the task estimator used for distributed execution.
    ///
    /// The default [`DataSourceExecPartitionEstimator`] is sufficient for any
    /// source that produces a `DataSourceExec` with accurate output partitioning.
    pub fn with_task_estimator(
        mut self,
        estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Self {
        self.task_estimator = Arc::new(estimator);
        self
    }

    /// Returns the shared `RuntimeEnv`.
    ///
    /// Pass this to [`crate::worker::build_worker`] so workers share the same
    /// object-store registry as the coordinator.
    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    /// Returns a slice of all registered data sources.
    pub fn sources(&self) -> &[Arc<dyn QuickwitDataSource>] {
        &self.sources
    }

    /// Validate that no two sources register conflicting UDF names.
    ///
    /// Development-time sanity check — call once at service startup after all
    /// sources are registered, not on every query. Not called automatically.
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

    /// Execute a Substrait plan (protobuf bytes) and return a streaming result.
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let ctx = self.build_session()?;
        crate::substrait::execute_substrait_plan_streaming(&plan, &ctx, &self.sources).await
    }

    /// Build a `SessionContext` backed by the shared `RuntimeEnv`.
    pub fn build_session(&self) -> DFResult<SessionContext> {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
        // We register our own catalog; skip the default "datafusion" one.
        config
            .options_mut()
            .catalog
            .create_default_catalog_and_schema = false;

        // Let each source install its config extensions (split runtime factory,
        // sync execution pool, ...) before `SessionStateBuilder::new()` runs.
        for source in &self.sources {
            source.configure_session(&mut config);
        }

        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_runtime_env(Arc::clone(&self.runtime));

        // Accumulate contributions from all sources and apply them first, so
        // source-specific physical optimizer rules run before the distributed
        // rule inspects the fully-optimized plan.
        let mut combined = crate::data_source::DataSourceContributions::default();
        for source in &self.sources {
            combined.merge(source.contributions());
        }
        builder = combined.apply_to_builder(builder);

        if let Some(resolver) = &self.worker_resolver {
            // DistributedPhysicalOptimizerRule is added LAST so it sees the
            // fully source-optimized plan.
            builder = builder
                .with_distributed_worker_resolver(ArcWorkerResolver(Arc::clone(resolver)))
                .with_distributed_task_estimator(ArcTaskEstimator(Arc::clone(&self.task_estimator)))
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

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
struct ArcWorkerResolver(Arc<dyn WorkerResolver + Send + Sync>);

impl WorkerResolver for ArcWorkerResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, datafusion::error::DataFusionError> {
        self.0.get_urls()
    }
}

/// Newtype wrapper so `Arc<dyn TaskEstimator>` can be passed to
/// `with_distributed_task_estimator`, which requires an owned `impl TaskEstimator`.
struct ArcTaskEstimator(Arc<dyn TaskEstimator + Send + Sync>);

impl std::fmt::Debug for ArcTaskEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcTaskEstimator").finish_non_exhaustive()
    }
}

impl TaskEstimator for ArcTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        cfg: &datafusion::config::ConfigOptions,
    ) -> Option<datafusion_distributed::TaskEstimation> {
        self.0.task_estimation(plan, cfg)
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        task_count: usize,
        cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.scale_up_leaf_node(plan, task_count, cfg)
    }
}
