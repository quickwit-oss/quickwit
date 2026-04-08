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

//! `MetricsTableProvider` — DataFusion TableProvider for a metrics index.
//!
//! Queries the metastore for published splits, prunes via Postgres filters,
//! and returns a standard `ParquetSource`-backed `DataSourceExec`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DFResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::expressions::Column;
use object_store::ObjectStore;
use quickwit_parquet_engine::schema::SORT_ORDER;
use quickwit_parquet_engine::split::MetricsSplitMetadata;
use tracing::debug;

use super::predicate;

/// Provides split metadata for a metrics index.
#[async_trait]
pub trait MetricsSplitProvider: Send + Sync + fmt::Debug {
    async fn list_splits(
        &self,
        query: &predicate::MetricsSplitQuery,
    ) -> DFResult<Vec<MetricsSplitMetadata>>;
}

/// TableProvider for a single metrics index.
///
/// On `scan()`, queries the metastore for published splits matching the
/// pushed-down predicates, then returns a standard `ParquetSource`-backed
/// `DataSourceExec` with one file group per split.
#[derive(Debug)]
pub struct MetricsTableProvider {
    schema: SchemaRef,
    split_provider: Arc<dyn MetricsSplitProvider>,
    object_store: Arc<dyn ObjectStore>,
    /// URL scheme for the object store (e.g. "file:///tmp/data" or "memory://").
    object_store_url: ObjectStoreUrl,
}

impl MetricsTableProvider {
    pub fn new(
        schema: SchemaRef,
        split_provider: Arc<dyn MetricsSplitProvider>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            schema,
            split_provider,
            object_store,
            object_store_url,
        }
    }
}

#[async_trait]
impl TableProvider for MetricsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|expr| classify_filter(expr)).collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Extract split-level filters for metastore pruning
        let (split_query, _remaining) = predicate::extract_split_filters(filters);

        debug!(
            metric_names = ?split_query.metric_names,
            time_start = ?split_query.time_range_start,
            time_end = ?split_query.time_range_end,
            "querying metastore for matching splits"
        );

        let splits = self.split_provider.list_splits(&split_query).await?;

        debug!(num_splits = splits.len(), "found matching splits");

        // Register our object store with the runtime so ParquetSource can use it
        // Register on every scan to handle sessions where register_for_worker
        // was not called (single-node non-distributed mode). The call is idempotent
        // but acquires a write-lock on RuntimeEnv's object-store map; for the
        // distributed path register_for_worker pre-registers stores so this is a
        // no-op. A future improvement: skip if already registered.
        state
            .runtime_env()
            .register_object_store(self.object_store_url.as_ref(), Arc::clone(&self.object_store));

        // Build file groups — one PartitionedFile per split
        let file_groups: Vec<PartitionedFile> = splits
            .iter()
            .map(|split| PartitionedFile::new(split.parquet_filename(), split.size_bytes))
            .collect();

        // Configure ParquetSource with bloom filters + pushdown enabled
        let table_schema: datafusion_datasource::TableSchema = self.schema.clone().into();
        let parquet_source = ParquetSource::new(table_schema)
            .with_bloom_filter_on_read(true)
            .with_pushdown_filters(true)
            .with_reorder_filters(true)
            .with_enable_page_index(true);

        // Build the FileScanConfig
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            Arc::new(parquet_source),
        );

        // Add each split as its own file group (one file per partition)
        for file in file_groups {
            builder = builder.with_file(file);
        }

        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()))?;
        }

        if let Some(lim) = limit {
            builder = builder.with_limit(Some(lim));
        }

        // Declare the full parquet sort order to DataFusion so it can skip
        // redundant sort operators. Matches the writer's SORT_ORDER exactly:
        // [metric_name, service, env, datacenter, region, host, timestamp_secs].
        // Columns absent from the projected schema are skipped; nulls_first=false
        // matches the writer's SortOptions.
        let sort_options = SortOptions { descending: false, nulls_first: false };
        let sort_exprs: Vec<PhysicalSortExpr> = SORT_ORDER
            .iter()
            .filter_map(|col_name| {
                self.schema.index_of(col_name).ok().map(|idx| {
                    PhysicalSortExpr::new(
                        Arc::new(Column::new(col_name, idx)),
                        sort_options,
                    )
                })
            })
            .collect();
        if let Some(ordering) = LexOrdering::new(sort_exprs) {
            builder = builder.with_output_ordering(vec![ordering]);
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

fn classify_filter(expr: &Expr) -> TableProviderFilterPushDown {
    match expr {
        Expr::BinaryExpr(binary) => {
            if let Some(col_name) = column_name_from_expr(&binary.left)
                .or_else(|| column_name_from_expr(&binary.right))
            {
                // OSS uses bare column names (no tag_ prefix)
                match col_name.as_str() {
                    "metric_name" | "timestamp_secs" | "service" | "env"
                    | "datacenter" | "region" | "host" => {
                        TableProviderFilterPushDown::Inexact
                    }
                    _ => TableProviderFilterPushDown::Unsupported,
                }
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }
        Expr::InList(in_list) => {
            if let Some(col_name) = column_name_from_expr(&in_list.expr) {
                match col_name.as_str() {
                    "metric_name" | "service" | "env" | "datacenter"
                    | "region" | "host" => TableProviderFilterPushDown::Inexact,
                    _ => TableProviderFilterPushDown::Unsupported,
                }
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }
        _ => TableProviderFilterPushDown::Unsupported,
    }
}

fn column_name_from_expr(expr: &Expr) -> Option<String> {
    predicate::column_name(expr)
}
