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

//! Session builder for DataFusion metrics query execution.

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use quickwit_search::SearcherPool;

use crate::catalog::{MetricsIndexResolver, MetricsSchemaProvider};
use crate::resolver::MetricsWorkerResolver;
use crate::task_estimator::MetricsTaskEstimator;

/// Everything needed to build a DataFusion session for metrics queries.
pub struct MetricsSessionBuilder {
    index_resolver: Arc<dyn MetricsIndexResolver>,
    searcher_pool: Option<SearcherPool>,
}

impl MetricsSessionBuilder {
    pub fn new(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self {
            index_resolver,
            searcher_pool: None,
        }
    }

    pub fn with_searcher_pool(mut self, pool: SearcherPool) -> Self {
        self.searcher_pool = Some(pool);
        self
    }

    pub fn index_resolver(&self) -> &Arc<dyn MetricsIndexResolver> {
        &self.index_resolver
    }

    /// Build a `SessionContext` configured for metrics queries.
    pub fn build_session(&self) -> DFResult<SessionContext> {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
        config.options_mut().catalog.create_default_catalog_and_schema = false;

        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();

        if let Some(ref pool) = self.searcher_pool {
            let worker_resolver = MetricsWorkerResolver::new(pool.clone());
            builder = builder
                .with_distributed_worker_resolver(worker_resolver)
                .with_distributed_task_estimator(MetricsTaskEstimator)
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        let mut state = builder.build();

        // DataFusion uppercases the file type string, so register both to be safe
        let make_factory = || -> Arc<dyn datafusion::catalog::TableProviderFactory> {
            Arc::new(crate::table_factory::MetricsTableProviderFactory::new(
                Arc::clone(&self.index_resolver),
            ))
        };
        state.table_factories_mut().insert(
            crate::table_factory::METRICS_FILE_TYPE.to_string(),
            make_factory(),
        );
        state.table_factories_mut().insert(
            crate::table_factory::METRICS_FILE_TYPE.to_uppercase(),
            make_factory(),
        );

        let ctx = SessionContext::new_with_state(state);

        let schema_provider = Arc::new(MetricsSchemaProvider::new(
            Arc::clone(&self.index_resolver),
        ));
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", schema_provider)
            .expect("register metrics schema");
        ctx.register_catalog("quickwit", catalog);

        Ok(ctx)
    }
}
