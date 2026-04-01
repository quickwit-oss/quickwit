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

//! Catalog and schema providers for metrics indexes.
//!
//! `MetricsSchemaProvider` resolves index names to `MetricsTableProvider`
//! instances. The schema is declared by the caller via CREATE EXTERNAL TABLE
//! DDL; when no DDL is present, a minimal base schema (4 required fields) is
//! used.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

use crate::table_provider::{MetricsSplitProvider, MetricsTableProvider};

/// Resolves per-index resources at query time.
///
/// Production uses `MetastoreIndexResolver` (queries metastore for index
/// metadata, resolves storage URI). Tests use `SimpleIndexResolver` (wraps
/// a single split provider + store for all names).
#[async_trait]
pub trait MetricsIndexResolver: Send + Sync + std::fmt::Debug {
    /// Given an index name, return its split provider, object store, and
    /// the ObjectStoreUrl to register with the DataFusion runtime.
    async fn resolve(
        &self,
        index_name: &str,
    ) -> DFResult<(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)>;

    /// List all available metrics index names.
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}

/// Simple resolver that returns the same split provider + store for every index.
///
/// Useful for tests and single-index setups where there is only one backing store.
#[derive(Debug)]
pub struct SimpleIndexResolver {
    split_provider: Arc<dyn MetricsSplitProvider>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    index_names: Vec<String>,
}

impl SimpleIndexResolver {
    pub fn new(
        split_provider: Arc<dyn MetricsSplitProvider>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            split_provider,
            object_store,
            object_store_url,
            index_names: vec!["metrics".to_string()],
        }
    }

    pub fn with_index_names(mut self, names: Vec<String>) -> Self {
        self.index_names = names;
        self
    }
}

#[async_trait]
impl MetricsIndexResolver for SimpleIndexResolver {
    async fn resolve(
        &self,
        _index_name: &str,
    ) -> DFResult<(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)> {
        Ok((
            Arc::clone(&self.split_provider),
            Arc::clone(&self.object_store),
            self.object_store_url.clone(),
        ))
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        Ok(self.index_names.clone())
    }
}

/// Returns a minimal base Arrow schema with the 4 guaranteed columns.
///
/// Used when no DDL schema is available. The 4 guaranteed columns are always
/// present in every OSS metrics parquet file.
pub fn minimal_base_schema() -> SchemaRef {
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Schema provider that resolves index names to metrics table providers.
///
/// Uses a `MetricsIndexResolver` to get per-index resources at query time,
/// so a single catalog can serve queries across multiple indexes.
///
/// Explicitly registered tables (via `CREATE EXTERNAL TABLE`) are stored in
/// memory and take precedence over lazily-resolved metastore tables.
pub struct MetricsSchemaProvider {
    index_resolver: Arc<dyn MetricsIndexResolver>,
    /// Default arrow schema used when no DDL schema is provided.
    arrow_schema: SchemaRef,
    /// Tables explicitly registered via DDL (CREATE OR REPLACE EXTERNAL TABLE).
    registered_tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl MetricsSchemaProvider {
    pub fn new(index_resolver: Arc<dyn MetricsIndexResolver>) -> Self {
        Self {
            index_resolver,
            arrow_schema: minimal_base_schema(),
            registered_tables: Mutex::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for MetricsSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsSchemaProvider")
            .field("num_fields", &self.arrow_schema.fields().len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for MetricsSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // table_names() is sync but listing indexes is async.
        // Use block_on since this is only called for SHOW TABLES / metadata queries.
        let resolver = Arc::clone(&self.index_resolver);
        std::thread::scope(|_| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(resolver.list_index_names())
                    .unwrap_or_default()
            })
        })
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        // First check explicitly registered tables (from DDL)
        if let Some(provider) = self.registered_tables.lock().unwrap().get(name).cloned() {
            return Ok(Some(provider));
        }

        // Fall back to lazily resolving from the metastore
        let (split_provider, object_store, object_store_url) =
            match self.index_resolver.resolve(name).await {
                Ok(result) => result,
                Err(_) => return Ok(None),
            };
        let provider = MetricsTableProvider::new(
            Arc::clone(&self.arrow_schema),
            split_provider,
            object_store,
            object_store_url,
        );
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        if self.registered_tables.lock().unwrap().contains_key(name) {
            return true;
        }
        self.table_names().contains(&name.to_string())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.registered_tables.lock().unwrap();
        let old = tables.insert(name, table);
        Ok(old)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.registered_tables.lock().unwrap();
        Ok(tables.remove(name))
    }
}
