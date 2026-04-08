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

//! Distributed DataFusion worker session setup.
//!
//! This module is named `worker` because the distributed protocol uses a
//! custom `WorkerService` gRPC (from datafusion-distributed PR #375), not
//! Arrow Flight.  The name `flight` would be misleading.
//!
//! `QuickwitWorkerSessionBuilder` prepares each worker session:
//! 1. Applies source contributions (optimizer rules, extension planners, UDFs,
//!    UDAFs, codecs) before `SessionStateBuilder::build()`.
//! 2. Injects the shared `RuntimeEnv` from the coordinator's
//!    `DataFusionSessionBuilder` so that object stores registered at startup
//!    are visible on workers without any per-session re-registration.
//! 3. Registers the `QuickwitSchemaProvider` so table references in
//!    deserialized plan fragments resolve correctly.
//! 4. Calls `register_for_worker()` for any post-build runtime state.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_distributed::{Worker, WorkerQueryContext, WorkerSessionBuilder};
use tracing::debug;

use crate::catalog::QuickwitSchemaProvider;
use crate::data_source::QuickwitDataSource;

/// `WorkerSessionBuilder` that shares the coordinator's `RuntimeEnv` and
/// applies all source contributions on every new worker session.
#[derive(Clone)]
pub struct QuickwitWorkerSessionBuilder {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    /// Shared with the coordinator's `DataFusionSessionBuilder`.
    /// Object stores registered at startup (via `init`) or lazily (via `scan`)
    /// are immediately visible to workers without any re-registration.
    runtime: Arc<RuntimeEnv>,
}

impl QuickwitWorkerSessionBuilder {
    pub fn new(sources: Vec<Arc<dyn QuickwitDataSource>>, runtime: Arc<RuntimeEnv>) -> Self {
        Self { sources, runtime }
    }
}

#[async_trait]
impl WorkerSessionBuilder for QuickwitWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        // Phase 1: contributions (rules, planners, UDFs, UDAFs, codecs) + shared env.
        let mut combined = crate::data_source::DataSourceContributions::default();
        for source in &self.sources {
            combined.merge(source.contributions());
        }
        let state = combined
            .apply_to_builder(ctx.builder)
            .with_runtime_env(Arc::clone(&self.runtime))
            .build();

        // Phase 2: catalog for table-reference resolution in plan fragments.
        // `register_schema` only fails if "public" is already registered, which
        // cannot happen here since the catalog is freshly created above.
        let schema_provider = Arc::new(QuickwitSchemaProvider::new(self.sources.clone()));
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", schema_provider)
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to register 'public' schema on worker: {e}"
                ))
            })?;
        state
            .catalog_list()
            .register_catalog("quickwit".to_string(), catalog);

        // Phase 3: post-build runtime registration (rare — most stores are already
        // in the shared RuntimeEnv from startup or lazy scan registration).
        for source in &self.sources {
            if let Err(err) = source.register_for_worker(&state).await {
                debug!(
                    error = %err,
                    "data source register_for_worker failed (non-fatal)"
                );
            }
        }

        Ok(state)
    }
}

/// Build a `Worker` that shares the coordinator's `RuntimeEnv`.
///
/// Pass `session_builder.runtime()` from the coordinator's
/// `DataFusionSessionBuilder` so that object stores registered at service
/// startup are available to workers without re-registration.
pub fn build_quickwit_worker(
    sources: &[Arc<dyn QuickwitDataSource>],
    runtime: Arc<RuntimeEnv>,
) -> Worker {
    let session_builder = QuickwitWorkerSessionBuilder::new(sources.to_vec(), runtime);
    Worker::from_session_builder(session_builder)
}
