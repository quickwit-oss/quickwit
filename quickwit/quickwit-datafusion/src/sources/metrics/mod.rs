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

//! Metrics data source for DataFusion.
//!
//! `MetricsDataSource` implements `QuickwitDataSource` and encapsulates all
//! metrics-specific logic: split providers, index resolution, filter pushdown,
//! and object-store pre-registration for Flight workers.
//!
//! All metrics-specific code lives in this module; none leaks into the generic
//! session / catalog / worker layer.

pub mod factory;
pub mod index_resolver;
pub mod metastore_provider;
pub mod predicate;
pub mod table_provider;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::debug;

use crate::data_source::QuickwitDataSource;
use self::factory::{MetricsTableProviderFactory, METRICS_FILE_TYPE};
use self::index_resolver::{MetastoreIndexResolver, MetricsIndexResolver};
use self::table_provider::MetricsTableProvider;

/// `QuickwitDataSource` implementation for OSS parquet metrics.
///
/// Backed by the Quickwit metastore for split discovery and `StorageResolver`
/// for object-store access.  Registers object stores on Flight workers via
/// `register_for_worker()`.
#[derive(Debug)]
pub struct MetricsDataSource {
    index_resolver: Arc<dyn MetricsIndexResolver>,
}

impl MetricsDataSource {
    /// Create a production `MetricsDataSource` backed by the metastore.
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
    ) -> Self {
        let resolver = MetastoreIndexResolver::new(metastore, storage_resolver);
        Self {
            index_resolver: Arc::new(resolver),
        }
    }

    /// Create with a custom resolver (for tests).
    pub fn with_resolver(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self { index_resolver }
    }
}

/// Minimal 4-column schema — always present in every OSS metrics parquet file.
fn minimal_base_schema() -> SchemaRef {
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

#[async_trait]
impl QuickwitDataSource for MetricsDataSource {
    fn file_type(&self) -> &str {
        METRICS_FILE_TYPE
    }

    fn create_table_provider_factory(&self) -> Arc<dyn TableProviderFactory> {
        Arc::new(MetricsTableProviderFactory::new(
            Arc::clone(&self.index_resolver),
        ))
    }

    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.index_resolver.resolve(index_name).await {
            Ok((split_provider, object_store, object_store_url)) => {
                let provider = MetricsTableProvider::new(
                    minimal_base_schema(),
                    split_provider,
                    object_store,
                    object_store_url,
                );
                Ok(Some(Arc::new(provider)))
            }
            // Index not found in this source — let the next source try
            Err(_) => Ok(None),
        }
    }

    async fn register_for_worker(&self, state: &SessionState) -> DFResult<()> {
        let index_names = self.index_resolver.list_index_names().await?;
        for index_name in &index_names {
            match self.index_resolver.resolve(index_name).await {
                Ok((_, object_store, object_store_url)) => {
                    state
                        .runtime_env()
                        .register_object_store(object_store_url.as_ref(), object_store);
                    debug!(index_name, "registered object store for metrics worker");
                }
                Err(err) => {
                    debug!(
                        index_name,
                        error = %err,
                        "skipping metrics index in worker registration (non-fatal)"
                    );
                }
            }
        }
        Ok(())
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        self.index_resolver.list_index_names().await
    }
}
