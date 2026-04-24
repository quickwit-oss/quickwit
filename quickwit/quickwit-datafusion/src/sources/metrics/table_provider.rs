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
use datafusion::arrow;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_plan::expressions::Column;
use quickwit_common::uri::Uri;
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use tracing::debug;

use super::predicate;

const METRICS_SORT_ORDER: &[&str] = &[
    "metric_name",
    "service",
    "env",
    "datacenter",
    "region",
    "host",
    "timeseries_id",
    "timestamp_secs",
];

/// Provides split metadata for a metrics index.
#[async_trait]
pub trait MetricsSplitProvider: Send + Sync + fmt::Debug {
    async fn list_splits(
        &self,
        query: &predicate::MetricsSplitQuery,
    ) -> DFResult<Vec<ParquetSplitMetadata>>;
}

/// TableProvider for a single metrics index.
///
/// On `scan()`, queries the metastore for published splits matching the
/// pushed-down predicates, then returns a standard `ParquetSource`-backed
/// `DataSourceExec` with one file group per split.
///
/// `ObjectStoreUrl` only accepts a scheme + authority, so the full index
/// URI is split:
/// - `scheme://authority` goes into the `ObjectStoreUrl`.
/// - The path component (e.g. the `my-index/` part of `s3://bucket/my-index/`) is prepended to each
///   split's filename in the emitted `PartitionedFile`s.
///
/// Neither this type nor `scan()` constructs an `Arc<dyn ObjectStore>` —
/// the session's custom
/// [`crate::object_store_registry::QuickwitObjectStoreRegistry`] builds a
/// lazy wrapper the first time DataFusion reads from the URL.
#[derive(Debug)]
pub struct MetricsTableProvider {
    schema: SchemaRef,
    split_provider: Arc<dyn MetricsSplitProvider>,
    /// `scheme://authority` portion of the index URI, used as the
    /// `ObjectStoreUrl`. All splits of this index route to the same
    /// `ObjectStore` (the registry returns one lazy wrapper per authority).
    object_store_url: ObjectStoreUrl,
    /// Path component of the index URI (e.g. `my-index/` for
    /// `s3://bucket/my-index/`). Prepended to each split's filename when
    /// building `PartitionedFile`s so DataFusion reads the correct key.
    /// Never starts with `/`; always ends with `/` (or is empty).
    path_prefix: String,
}

impl MetricsTableProvider {
    pub fn new(
        schema: SchemaRef,
        split_provider: Arc<dyn MetricsSplitProvider>,
        index_uri: Uri,
    ) -> DFResult<Self> {
        let (object_store_url, path_prefix) = split_uri(&index_uri)?;
        Ok(Self {
            schema,
            split_provider,
            object_store_url,
            path_prefix,
        })
    }
}

/// Split an index URI into the `scheme://authority` portion (for
/// `ObjectStoreUrl`) and the path portion (prefix for split filenames).
///
/// The path prefix is normalised: no leading `/`, trailing `/` if non-empty.
/// An empty prefix means splits live directly at the authority root.
fn split_uri(index_uri: &Uri) -> DFResult<(ObjectStoreUrl, String)> {
    let parsed = url::Url::parse(index_uri.as_str()).map_err(|err| {
        DataFusionError::Internal(format!(
            "failed to parse index URI `{}` as URL: {err}",
            index_uri.as_str()
        ))
    })?;
    let authority = match parsed.host_str() {
        Some(host) => format!("{}://{host}", parsed.scheme()),
        None => format!("{}://", parsed.scheme()),
    };
    let object_store_url = ObjectStoreUrl::parse(&authority).map_err(|err| {
        DataFusionError::Internal(format!(
            "failed to build ObjectStoreUrl from `{authority}`: {err}"
        ))
    })?;
    let mut path_prefix = parsed.path().trim_start_matches('/').to_string();
    if !path_prefix.is_empty() && !path_prefix.ends_with('/') {
        path_prefix.push('/');
    }
    Ok((object_store_url, path_prefix))
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
        _state: &dyn Session,
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

        // The `ObjectStore` for this URL is lazily built by
        // `QuickwitObjectStoreRegistry` on first read — see
        // `crate::object_store_registry`. No registration needed here.

        // Build file groups — one PartitionedFile per split. The path
        // prepends the index URI's path component so the `ObjectStore`
        // (which is scoped to `scheme://authority`) finds the right file.
        let file_groups: Vec<PartitionedFile> = splits
            .iter()
            .map(|split| {
                let path = format!("{}{}", self.path_prefix, split.parquet_filename());
                PartitionedFile::new(path, split.size_bytes)
            })
            .collect();

        // Configure ParquetSource with bloom filters + pushdown enabled
        let table_schema: datafusion_datasource::TableSchema = self.schema.clone().into();
        let parquet_source = ParquetSource::new(table_schema)
            .with_bloom_filter_on_read(true)
            .with_pushdown_filters(true)
            .with_reorder_filters(true)
            .with_enable_page_index(true);

        // Build the FileScanConfig
        let mut builder =
            FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(parquet_source));

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

        // Advertise only the contiguous prefix of the writer sort order present
        // in the table schema. Later keys are not globally ordered if an
        // earlier discriminator is hidden by the declared schema.
        if let Some(ordering) = metrics_output_ordering(&self.schema) {
            builder = builder.with_output_ordering(vec![ordering]);
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

fn classify_filter(expr: &Expr) -> TableProviderFilterPushDown {
    match expr {
        Expr::BinaryExpr(binary) => {
            if let Some(col_name) =
                column_name_from_expr(&binary.left).or_else(|| column_name_from_expr(&binary.right))
            {
                // OSS uses bare column names (no tag_ prefix)
                match col_name.as_str() {
                    "metric_name" | "timestamp_secs" => TableProviderFilterPushDown::Inexact,
                    _ => TableProviderFilterPushDown::Unsupported,
                }
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }
        Expr::InList(in_list) => {
            if let Some(col_name) = column_name_from_expr(&in_list.expr) {
                match col_name.as_str() {
                    "metric_name" => TableProviderFilterPushDown::Inexact,
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

fn metrics_output_ordering(schema: &SchemaRef) -> Option<LexOrdering> {
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let sort_exprs: Vec<PhysicalSortExpr> = METRICS_SORT_ORDER
        .iter()
        .map_while(|col_name| {
            schema.index_of(col_name).ok().map(|idx| {
                PhysicalSortExpr::new(Arc::new(Column::new(col_name, idx)), sort_options)
            })
        })
        .collect();
    LexOrdering::new(sort_exprs)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn schema_with_columns(columns: &[&str]) -> SchemaRef {
        let fields = columns
            .iter()
            .map(|column| Field::new(*column, DataType::Utf8, true))
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields))
    }

    fn ordering_column_names(ordering: &LexOrdering) -> Vec<String> {
        ordering
            .iter()
            .map(|expr| {
                expr.expr
                    .as_any()
                    .downcast_ref::<Column>()
                    .expect("metrics ordering should contain column expressions")
                    .name()
                    .to_string()
            })
            .collect()
    }

    fn expected_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn metrics_output_ordering_stops_at_first_missing_sort_key() {
        let schema = schema_with_columns(&["metric_name", "service", "timestamp_secs"]);

        let ordering = metrics_output_ordering(&schema).unwrap();

        assert_eq!(
            ordering_column_names(&ordering),
            expected_names(&["metric_name", "service"])
        );
    }

    #[test]
    fn metrics_output_ordering_keeps_timestamp_after_all_discriminators() {
        let schema = schema_with_columns(METRICS_SORT_ORDER);

        let ordering = metrics_output_ordering(&schema).unwrap();

        assert_eq!(
            ordering_column_names(&ordering),
            expected_names(METRICS_SORT_ORDER)
        );
    }
}
