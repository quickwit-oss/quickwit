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

//! Arrow Flight service for distributed metrics query execution.

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_distributed::{Worker, WorkerQueryContext, WorkerSessionBuilder};
use tracing::debug;

use crate::catalog::{MetricsIndexResolver, MetricsSchemaProvider};

/// `WorkerSessionBuilder` that registers the metrics catalog on each
/// worker session so that plan fragments referencing metrics tables
/// can resolve them.
#[derive(Clone)]
pub struct MetricsWorkerSessionBuilder {
    index_resolver: Arc<dyn MetricsIndexResolver>,
}

impl MetricsWorkerSessionBuilder {
    pub fn new(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self { index_resolver }
    }
}

#[async_trait]
impl WorkerSessionBuilder for MetricsWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        let state = ctx.builder.build();

        let schema_provider = Arc::new(MetricsSchemaProvider::new(
            Arc::clone(&self.index_resolver),
        ));
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", schema_provider)
            .expect("register metrics schema on worker");
        state
            .catalog_list()
            .register_catalog("quickwit".to_string(), catalog);

        let index_names: Vec<String> = ctx
            .headers
            .get("x-metrics-indexes")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();

        for index_name in &index_names {
            if let Ok((_, object_store, object_store_url)) =
                self.index_resolver.resolve(index_name).await
            {
                state
                    .runtime_env()
                    .register_object_store(object_store_url.as_ref(), object_store);
                debug!(index_name, "registered object store on worker");
            }
        }

        if index_names.is_empty() {
            if let Ok((_, object_store, object_store_url)) =
                self.index_resolver.resolve("metrics").await
            {
                state
                    .runtime_env()
                    .register_object_store(object_store_url.as_ref(), object_store);
                debug!("registered default 'metrics' object store on worker");
            }
        }

        Ok(state)
    }
}

/// Build an Arrow Flight `Worker` configured for metrics queries.
pub fn build_metrics_worker(index_resolver: Arc<dyn MetricsIndexResolver>) -> Worker {
    let session_builder = MetricsWorkerSessionBuilder::new(index_resolver);
    Worker::from_session_builder(session_builder)
}

/// Build the Arrow Flight service for metrics queries.
pub fn build_flight_service(worker: Worker) -> FlightServiceServer<Worker> {
    FlightServiceServer::new(worker)
}
