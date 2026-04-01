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
//! `DataFusionSessionBuilder` knows nothing about metrics, logs, or traces.
//! Data sources are registered via `with_source()` and provide their own
//! `TableProviderFactory` and default table resolution.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use quickwit_search::SearcherPool;

use crate::catalog::QuickwitSchemaProvider;
use crate::data_source::QuickwitDataSource;
use crate::resolver::QuickwitWorkerResolver;
use crate::task_estimator::QuickwitTaskEstimator;

/// Builds a `SessionContext` for DataFusion queries over Quickwit data.
///
/// Data sources (metrics, logs, traces, …) are registered with `with_source()`.
/// The session is fully generic — no source-specific code lives here.
///
/// In Pomsky, the `CloudPremServiceImpl.substrait_search()` handler calls
/// `build_session()` and delegates execution to the OSS `execute_sql_statements()`.
pub struct DataFusionSessionBuilder {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    searcher_pool: Option<SearcherPool>,
    use_tls: bool,
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
        }
    }

    pub fn with_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    pub fn with_source(mut self, source: Arc<dyn QuickwitDataSource>) -> Self {
        self.sources.push(source);
        self
    }

    pub fn with_searcher_pool(mut self, pool: SearcherPool) -> Self {
        self.searcher_pool = Some(pool);
        self
    }

    /// Returns a slice of all registered data sources.
    ///
    /// Used by the Flight worker builder to call `register_for_worker()` on each.
    pub fn sources(&self) -> &[Arc<dyn QuickwitDataSource>] {
        &self.sources
    }

    /// Validate that no two sources register conflicting UDF or UDAF names.
    ///
    /// Returns an error if two sources contribute a scalar UDF or aggregate
    /// UDAF with the same name.  Call this before `build_session()` to surface
    /// conflicts early rather than getting an opaque DataFusion error at query time.
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

    /// Build a configured `SessionContext` ready for multi-statement SQL execution.
    ///
    /// Sets up:
    /// - Distributed execution if a searcher pool is present
    /// - One `TableProviderFactory` per registered source (both lowercase and
    ///   uppercase keys, since DataFusion uppercases the `STORED AS` token)
    /// - A `QuickwitSchemaProvider` in the `quickwit.public` catalog
    pub fn build_session(&self) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
        // We register our own catalog; skip the default "datafusion" one.
        config.options_mut().catalog.create_default_catalog_and_schema = false;

        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();

        if let Some(ref pool) = self.searcher_pool {
            let worker_resolver =
                QuickwitWorkerResolver::new(pool.clone()).with_tls(self.use_tls);
            builder = builder
                .with_distributed_worker_resolver(worker_resolver)
                .with_distributed_task_estimator(QuickwitTaskEstimator)
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        // Collect contributions from all sources (rules, codecs, UDFs, UDAFs,
        // extension planners) and apply them all into the builder before build().
        let mut combined = crate::data_source::DataSourceContributions::default();
        for source in &self.sources {
            combined.merge(source.contributions());
        }
        builder = combined.apply_to_builder(builder);

        let mut state = builder.build();

        // Register each source's TableProviderFactory for `STORED AS <token>` DDL.
        // Using ddl_registration() guarantees the same factory instance handles
        // both the lowercase and uppercase keys — no mismatch possible.
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
