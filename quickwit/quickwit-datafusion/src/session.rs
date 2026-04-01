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

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::Result as DFResult;
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
}

impl DataFusionSessionBuilder {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            searcher_pool: None,
        }
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

    /// Build a configured `SessionContext` ready for multi-statement SQL execution.
    ///
    /// Sets up:
    /// - Distributed execution if a searcher pool is present
    /// - One `TableProviderFactory` per registered source (both lowercase and
    ///   uppercase keys, since DataFusion uppercases the `STORED AS` token)
    /// - A `QuickwitSchemaProvider` in the `quickwit.public` catalog
    pub fn build_session(&self) -> DFResult<SessionContext> {
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
            builder = builder
                .with_distributed_worker_resolver(QuickwitWorkerResolver::new(pool.clone()))
                .with_distributed_task_estimator(QuickwitTaskEstimator)
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        // Let each source configure the builder before it is built.
        // This is where sources register codecs, UDFs, opener factories, etc.
        for source in &self.sources {
            builder = source.configure_session_state_builder(builder);
        }

        let mut state = builder.build();

        // Register each source's TableProviderFactory for `STORED AS <file_type>` DDL.
        // Only done for sources that opt into DDL support via file_type() = Some(_).
        for source in &self.sources {
            let Some(ft) = source.file_type() else {
                continue;
            };
            let factory: Arc<dyn datafusion::catalog::TableProviderFactory> =
                source.create_table_provider_factory();
            state
                .table_factories_mut()
                .insert(ft.to_string(), Arc::clone(&factory));
            state
                .table_factories_mut()
                .insert(ft.to_uppercase(), source.create_table_provider_factory());
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
