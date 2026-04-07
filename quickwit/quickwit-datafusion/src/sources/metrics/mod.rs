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

pub(crate) mod factory;
pub(crate) mod index_resolver;
pub(crate) mod metastore_provider;
pub(crate) mod predicate;
pub(crate) mod table_provider;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use quickwit_proto::metastore::{MetastoreError, MetastoreServiceClient};
use quickwit_storage::StorageResolver;

use crate::data_source::{DataSourceContributions, QuickwitDataSource};
use self::factory::{MetricsTableProviderFactory, METRICS_FILE_TYPE};
use self::index_resolver::{MetastoreIndexResolver, MetricsIndexResolver};
use self::table_provider::MetricsTableProvider;

/// Returns `true` when `err` wraps a [`MetastoreError::NotFound`].
///
/// Used to distinguish "this data source does not own that index" (caller
/// should try the next source) from a genuine metastore failure that should
/// be surfaced to the user.
fn is_index_not_found(err: &datafusion::error::DataFusionError) -> bool {
    match err {
        datafusion::error::DataFusionError::External(boxed) => boxed
            .downcast_ref::<MetastoreError>()
            .map(|me| matches!(me, MetastoreError::NotFound(_)))
            .unwrap_or(false),
        _ => false,
    }
}

/// `QuickwitDataSource` implementation for OSS parquet metrics.
///
/// Backed by the Quickwit metastore for split discovery and `StorageResolver`
/// for object-store access.  Object stores are registered lazily inside
/// `MetricsTableProvider::scan()` on each use.
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
    fn contributions(&self) -> DataSourceContributions {
        DataSourceContributions::default()
    }

    /// Handle `ReadRel` nodes in incoming Substrait plans.
    ///
    /// ## OSS path — `NamedTable`
    ///
    /// When the read type is `NamedTable { names: [index_name] }` and the index
    /// exists in the metastore, returns a `MetricsTableProvider` using the
    /// schema from `schema_hint` (derived from `ReadRel.base_schema` by the
    /// caller).  Returning `None` for an unknown index lets the standard catalog
    /// path take over.
    ///
    /// ## Extension path — custom protos (downstream callers)
    ///
    /// A downstream caller registers its own `QuickwitDataSource` that handles
    /// `ExtensionTable<MetricRead>`.  This default implementation only handles
    /// `NamedTable` — `ExtensionTable` always returns `Ok(None)` here.
    async fn try_consume_read_rel(
        &self,
        rel: &datafusion_substrait::substrait::proto::ReadRel,
        schema_hint: Option<arrow::datatypes::SchemaRef>,
    ) -> DFResult<Option<(String, Arc<dyn TableProvider>)>> {
        use datafusion_substrait::substrait::proto::read_rel::ReadType;

        // Only handle NamedTable reads.  ExtensionTable (downstream callers) returns None.
        let Some(ReadType::NamedTable(nt)) = &rel.read_type else {
            return Ok(None);
        };
        // `NamedTable::names` is a path like ["catalog", "schema", "table"];
        // the last element is the effective table name.  An empty list is a
        // malformed plan — skip rather than silently resolving to index "".
        let Some(index_name) = nt.names.last() else {
            return Ok(None);
        };
        let index_name = index_name.as_str();

        // Use the producer-declared schema if available; fall back to minimal base schema.
        let schema = schema_hint.unwrap_or_else(minimal_base_schema);

        match self.index_resolver.resolve(index_name).await {
            Ok((split_provider, object_store, object_store_url)) => {
                let provider = MetricsTableProvider::new(
                    schema,
                    split_provider,
                    object_store,
                    object_store_url,
                );
                Ok(Some((index_name.to_string(), Arc::new(provider))))
            }
            Err(err) => {
                // Not-found means this source doesn't own the index; let others try.
                if is_index_not_found(&err) { Ok(None) } else { Err(err) }
            }
        }
    }

    fn ddl_registration(&self) -> Option<(String, Arc<dyn TableProviderFactory>)> {
        let factory: Arc<dyn TableProviderFactory> = Arc::new(MetricsTableProviderFactory::new(
            Arc::clone(&self.index_resolver),
        ));
        Some((METRICS_FILE_TYPE.to_string(), factory))
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
            Err(err) => {
                // Only swallow "index not found" — propagate everything else so the
                // caller gets an actionable error (e.g. metastore unavailable).
                if is_index_not_found(&err) { Ok(None) } else { Err(err) }
            }
        }
    }

    async fn register_for_worker(&self, _state: &SessionState) -> DFResult<()> {
        // No-op: object stores are registered lazily and idempotently inside
        // `MetricsTableProvider::scan()`, so eager pre-registration on every
        // worker task startup is unnecessary.
        Ok(())
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        self.index_resolver.list_index_names().await
    }
}
