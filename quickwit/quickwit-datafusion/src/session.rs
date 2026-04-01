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
//! When `with_source(source)` is called, `source.init(&self.runtime)` fires
//! immediately so that sources with known object-store URLs (e.g. a blob-store
//! connector) can register them once at startup.  Sources whose URLs are only
//! discoverable at query time (e.g. metrics, where indexes are listed from the
//! metastore) do lazy registration from `scan()` — which is safe because `scan()`
//! writes into the same shared `RuntimeEnv`, so the first scan for an index pays
//! the registration cost and every subsequent scan across any session is a no-op.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
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
    pub fn check_invariants(&self) -> datafusion::error::Result<()> {
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

    /// Build a `SessionContext` backed by the shared `RuntimeEnv`.
    pub fn build_session(&self) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
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

        ctx
    }
}
