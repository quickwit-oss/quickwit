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

//! Worker service for distributed DataFusion query execution.
//!
//! `QuickwitWorkerSessionBuilder` is the generic worker session setup:
//! it iterates over all registered `QuickwitDataSource` implementations
//! and calls `register_for_worker()` on each, so a single worker can
//! execute plan fragments for any registered data source.
//!
//! `build_quickwit_worker(sources)` is the top-level entry point called
//! by `quickwit-serve/src/grpc.rs`, which then calls `worker.into_worker_server()`
//! to obtain a tonic service that can be added to the gRPC server.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{Worker, WorkerQueryContext, WorkerSessionBuilder};
use tracing::debug;

use crate::catalog::QuickwitSchemaProvider;
use crate::data_source::QuickwitDataSource;

/// `WorkerSessionBuilder` that prepares each worker session for all registered
/// data sources.
///
/// On each new worker request:
/// 1. Registers the `QuickwitSchemaProvider` (needed to resolve table references
///    in deserialized plan fragments)
/// 2. Calls `register_for_worker()` on every source so that object stores and
///    other runtime state are in place before plan execution begins
#[derive(Clone)]
pub struct QuickwitWorkerSessionBuilder {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
}

impl QuickwitWorkerSessionBuilder {
    pub fn new(sources: Vec<Arc<dyn QuickwitDataSource>>) -> Self {
        Self { sources }
    }
}

#[async_trait]
impl WorkerSessionBuilder for QuickwitWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        // Phase 1: let each source configure the builder before build().
        // Codecs, UDFs, and opener factories must be registered here.
        let mut builder = ctx.builder;
        for source in &self.sources {
            builder = source.configure_session_state_builder(builder);
        }
        let state = builder.build();

        // Phase 2: register catalog so plan fragments can resolve table refs.
        let schema_provider =
            Arc::new(QuickwitSchemaProvider::new(self.sources.clone()));
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", schema_provider)
            .expect("register quickwit schema on worker");
        state
            .catalog_list()
            .register_catalog("quickwit".to_string(), catalog);

        // Phase 3: post-build runtime registration (e.g., object stores).
        for source in &self.sources {
            if let Err(err) = source.register_for_worker(&state).await {
                debug!(
                    file_type = ?source.file_type(),
                    error = %err,
                    "data source register_for_worker failed (non-fatal)"
                );
            }
        }

        Ok(state)
    }
}

/// Build a `Worker` configured for the given data sources.
///
/// Called by `quickwit-serve/src/grpc.rs`. Callers obtain the gRPC service
/// via `worker.into_worker_server()` and add it to the tonic server.
pub fn build_quickwit_worker(sources: &[Arc<dyn QuickwitDataSource>]) -> Worker {
    let session_builder = QuickwitWorkerSessionBuilder::new(sources.to_vec());
    Worker::from_session_builder(session_builder)
}
