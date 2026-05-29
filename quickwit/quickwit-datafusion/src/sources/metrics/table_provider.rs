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
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, LazyLock};

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
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_plan::expressions::Column;
use mini_moka::sync::Cache;
use quickwit_common::uri::Uri;
use quickwit_parquet_engine::sorted_series::SORTED_SERIES_COLUMN;
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_parquet_engine::table_config::ProductType;
use regex::{Regex, RegexBuilder};
use regex_automata::dfa::{Automaton, dense};
use regex_automata::{Anchored, Input};
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
// Per-regex compile-time cap for dense DFA determinization. This limits how
// much memory we are willing to spend building one prefix-pruning automaton for
// a split zonemap regex. If a pathological regex exceeds this limit, DFA
// compilation fails and pruning falls back to conservative keep-split behavior.
const ZONEMAP_DFA_SIZE_LIMIT_BYTES: usize = 1_000_000;
const ZONEMAP_REGEX_CACHE_MAX_ENTRIES: usize = 4096;
// Cache up to roughly 256 worst-case DFAs. The cache is weighted by the actual
// compiled DFA memory usage, so small/common zonemap regexes occupy less of the
// budget than regexes close to `ZONEMAP_DFA_SIZE_LIMIT_BYTES`.
const ZONEMAP_DFA_CACHE_MAX_BYTES: u64 = 256 * ZONEMAP_DFA_SIZE_LIMIT_BYTES as u64;
// Account for the regex key and cache bookkeeping when weighting entries. This
// also keeps invalid regex entries bounded even though they do not store a DFA.
const ZONEMAP_CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;

type DenseDfa = dense::DFA<Vec<u32>>;

static ZONEMAP_DFA_CONFIG: LazyLock<dense::Config> = LazyLock::new(|| {
    dense::Config::new()
        .dfa_size_limit(Some(ZONEMAP_DFA_SIZE_LIMIT_BYTES))
        .determinize_size_limit(Some(ZONEMAP_DFA_SIZE_LIMIT_BYTES))
});
static ZONEMAP_SYNTAX_CONFIG: LazyLock<regex_automata::util::syntax::Config> =
    LazyLock::new(|| regex_automata::util::syntax::Config::new().dot_matches_new_line(true));
static ZONEMAP_REGEX_CACHE: LazyLock<Cache<String, CachedCompiled<Regex>>> =
    LazyLock::new(|| Cache::new(ZONEMAP_REGEX_CACHE_MAX_ENTRIES as u64));
static ZONEMAP_DFA_CACHE: LazyLock<Cache<String, CachedCompiled<DenseDfa>>> = LazyLock::new(|| {
    Cache::<String, CachedCompiled<DenseDfa>>::builder()
        .max_capacity(ZONEMAP_DFA_CACHE_MAX_BYTES)
        .weigher(|regex, compiled| zonemap_dfa_cache_weight(regex, compiled))
        .build()
});

#[derive(Clone)]
enum CachedCompiled<T> {
    Valid(Arc<T>),
    Invalid,
}

impl<T> CachedCompiled<T> {
    fn valid(&self) -> Option<Arc<T>> {
        match self {
            Self::Valid(compiled) => Some(Arc::clone(compiled)),
            Self::Invalid => None,
        }
    }
}

fn zonemap_dfa_cache_weight(regex: &str, compiled: &CachedCompiled<DenseDfa>) -> u32 {
    let compiled_size = match compiled {
        CachedCompiled::Valid(dfa) => dfa.memory_usage(),
        CachedCompiled::Invalid => 0,
    };
    regex
        .len()
        .saturating_add(ZONEMAP_CACHE_ENTRY_OVERHEAD_BYTES)
        .saturating_add(compiled_size)
        .min(u32::MAX as usize) as u32
}

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
        Ok(filters
            .iter()
            .map(|expr| classify_filter(expr, &self.schema))
            .collect())
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
        let num_splits_before_metadata_pruning = splits.len();
        let splits = prune_splits_with_metadata(splits, filters);

        debug!(
            num_splits = splits.len(),
            num_pruned_by_metadata =
                num_splits_before_metadata_pruning.saturating_sub(splits.len()),
            "found matching splits"
        );

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

        // Add each split as its own file group (one file per partition). Empty
        // scans still need one empty partition so single-partition operators
        // such as COUNT(*) can satisfy their distribution requirement.
        if file_groups.is_empty() {
            builder = builder.with_file_group(FileGroup::new(vec![]));
        } else {
            for file in file_groups {
                builder = builder.with_file(file);
            }
        }

        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()))?;
        }

        if let Some(lim) = limit {
            builder = builder.with_limit(Some(lim));
        }

        // Advertise every ordering the writer physically guarantees for each
        // split. `sorted_series` is a materialized composite prefix of the
        // writer sort schema, so it is the preferred single-column series key
        // for streaming rollups. The expanded sort schema remains useful for
        // queries that group/filter on the raw tag columns.
        let output_orderings = metrics_output_orderings(&self.schema, &splits);
        if !output_orderings.is_empty() {
            builder = builder.with_output_ordering(output_orderings);
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

fn classify_filter(expr: &Expr, schema: &SchemaRef) -> TableProviderFilterPushDown {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            let left = classify_filter(&binary.left, schema);
            let right = classify_filter(&binary.right, schema);
            if left == TableProviderFilterPushDown::Inexact
                || right == TableProviderFilterPushDown::Inexact
            {
                TableProviderFilterPushDown::Inexact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }
        Expr::BinaryExpr(binary) => {
            let column_name = column_name_from_expr(&binary.left)
                .or_else(|| column_name_from_expr(&binary.right));
            classify_declared_column_filter(column_name, schema)
        }
        Expr::InList(in_list) => {
            classify_declared_column_filter(column_name_from_expr(&in_list.expr), schema)
        }
        Expr::Like(like) if !like.negated => {
            classify_declared_column_filter(column_name_from_expr(&like.expr), schema)
        }
        _ => TableProviderFilterPushDown::Unsupported,
    }
}

fn classify_declared_column_filter(
    column_name: Option<String>,
    schema: &SchemaRef,
) -> TableProviderFilterPushDown {
    if matches!(
        column_name.as_deref(),
        Some(column_name) if schema.index_of(column_name).is_ok()
    ) {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

fn column_name_from_expr(expr: &Expr) -> Option<String> {
    predicate::column_name(expr)
}

fn prune_splits_with_metadata(
    splits: Vec<ParquetSplitMetadata>,
    filters: &[Expr],
) -> Vec<ParquetSplitMetadata> {
    let string_filters = predicate::extract_string_filters(filters);
    let string_prefix_filters = predicate::extract_string_prefix_filters(filters);
    if string_filters.is_empty() && string_prefix_filters.is_empty() {
        return splits;
    }

    splits
        .into_iter()
        .filter(|split| {
            split_may_match_string_filters(split, &string_filters)
                && split_may_match_string_prefix_filters(split, &string_prefix_filters)
        })
        .collect()
}

fn split_may_match_string_filters(
    split: &ParquetSplitMetadata,
    string_filters: &[predicate::StringFilter],
) -> bool {
    string_filters
        .iter()
        .all(|string_filter| split_may_match_string_filter(split, string_filter))
}

fn split_may_match_string_filter(
    split: &ParquetSplitMetadata,
    string_filter: &predicate::StringFilter,
) -> bool {
    if string_filter.values.is_empty() {
        return false;
    }

    if string_filter.column == "metric_name" && !split.metric_names.is_empty() {
        return string_filter
            .values
            .iter()
            .any(|value| split.metric_names.contains(value));
    }

    if let Some(split_values) = split.low_cardinality_tags.get(&string_filter.column) {
        return string_filter
            .values
            .iter()
            .any(|value| split_values.contains(value));
    }

    if let Some(superset_regex) = split.zonemap_regexes.get(&string_filter.column) {
        return zonemap_may_match_any_value(superset_regex, &string_filter.values);
    }

    true
}

fn split_may_match_string_prefix_filters(
    split: &ParquetSplitMetadata,
    string_prefix_filters: &[predicate::StringPrefixFilter],
) -> bool {
    string_prefix_filters.iter().all(|string_prefix_filter| {
        split_may_match_string_prefix_filter(split, string_prefix_filter)
    })
}

fn split_may_match_string_prefix_filter(
    split: &ParquetSplitMetadata,
    string_prefix_filter: &predicate::StringPrefixFilter,
) -> bool {
    if string_prefix_filter.prefixes.is_empty() {
        return false;
    }

    if string_prefix_filter.column == "metric_name" && !split.metric_names.is_empty() {
        return string_prefix_filter.prefixes.iter().any(|prefix| {
            split
                .metric_names
                .iter()
                .any(|metric_name| metric_name.starts_with(prefix))
        });
    }

    if let Some(split_values) = split.low_cardinality_tags.get(&string_prefix_filter.column) {
        return string_prefix_filter
            .prefixes
            .iter()
            .any(|prefix| split_values.iter().any(|value| value.starts_with(prefix)));
    }

    if let Some(superset_regex) = split.zonemap_regexes.get(&string_prefix_filter.column) {
        return zonemap_may_match_any_prefix(superset_regex, &string_prefix_filter.prefixes);
    }

    true
}

fn zonemap_may_match_any_value(superset_regex: &str, values: &[String]) -> bool {
    let Some(regex) = cached_zonemap_regex(superset_regex) else {
        return true;
    };
    values.iter().any(|value| regex.is_match(value))
}

fn zonemap_may_match_any_prefix(superset_regex: &str, prefixes: &[String]) -> bool {
    let Some(dfa) = cached_zonemap_dfa(superset_regex) else {
        return true;
    };

    prefixes.iter().any(
        |prefix| match zonemap_dfa_may_match_prefix(dfa.as_ref(), prefix) {
            Ok(may_match) => may_match,
            Err(error) => {
                debug!(
                    %error,
                    superset_regex,
                    prefix,
                    "failed to evaluate split zonemap regex prefix"
                );
                true
            }
        },
    )
}

fn cached_zonemap_regex(superset_regex: &str) -> Option<Arc<Regex>> {
    let cache_key = superset_regex.to_string();
    if let Some(cached) = ZONEMAP_REGEX_CACHE.get(&cache_key) {
        return cached.valid();
    }

    let compiled = match RegexBuilder::new(superset_regex)
        .dot_matches_new_line(true)
        .build()
    {
        Ok(regex) => CachedCompiled::Valid(Arc::new(regex)),
        Err(error) => {
            debug!(
                %error,
                superset_regex,
                "ignoring invalid split zonemap regex"
            );
            CachedCompiled::Invalid
        }
    };
    let result = compiled.valid();
    ZONEMAP_REGEX_CACHE.insert(cache_key, compiled);
    result
}

fn cached_zonemap_dfa(superset_regex: &str) -> Option<Arc<DenseDfa>> {
    let cache_key = superset_regex.to_string();
    if let Some(cached) = ZONEMAP_DFA_CACHE.get(&cache_key) {
        return cached.valid();
    }

    let compiled = match dense::Builder::new()
        .configure(ZONEMAP_DFA_CONFIG.clone())
        .syntax(*ZONEMAP_SYNTAX_CONFIG)
        .build(superset_regex)
    {
        Ok(dfa) => CachedCompiled::Valid(Arc::new(dfa)),
        Err(error) => {
            debug!(
                %error,
                superset_regex,
                "ignoring invalid split zonemap regex"
            );
            CachedCompiled::Invalid
        }
    };
    let result = compiled.valid();
    ZONEMAP_DFA_CACHE.insert(cache_key, compiled);
    result
}

fn zonemap_dfa_may_match_prefix<A: Automaton>(
    dfa: &A,
    prefix: &str,
) -> Result<bool, regex_automata::MatchError> {
    let empty = Input::new("").anchored(Anchored::Yes);
    let mut state = dfa.start_state_forward(&empty)?;
    for byte in prefix.bytes() {
        state = dfa.next_state(state, byte);
        if dfa.is_dead_state(state) {
            return Ok(false);
        }
    }

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    visited.insert(state);
    queue.push_back(state);

    while let Some(state) = queue.pop_front() {
        let end_state = dfa.next_eoi_state(state);
        if dfa.is_match_state(end_state) {
            return Ok(true);
        }
        if dfa.is_dead_state(state) {
            continue;
        }
        for byte in 0..=u8::MAX {
            let next_state = dfa.next_state(state, byte);
            if !dfa.is_dead_state(next_state) && visited.insert(next_state) {
                queue.push_back(next_state);
            }
        }
    }

    Ok(false)
}

fn sort_expr(
    schema: &SchemaRef,
    col_name: &str,
    sort_options: SortOptions,
) -> Option<PhysicalSortExpr> {
    schema
        .index_of(col_name)
        .ok()
        .map(|idx| PhysicalSortExpr::new(Arc::new(Column::new(col_name, idx)), sort_options))
}

fn metrics_output_orderings(
    schema: &SchemaRef,
    splits: &[ParquetSplitMetadata],
) -> Vec<LexOrdering> {
    if !splits_have_default_metrics_sort(splits) {
        return Vec::new();
    }

    let ascending = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let timestamp_descending = SortOptions {
        descending: true,
        nulls_first: false,
    };

    let mut orderings = Vec::new();

    if let Some(sorted_series) = sort_expr(schema, SORTED_SERIES_COLUMN, ascending) {
        let mut sort_exprs = vec![sorted_series];
        if let Some(timestamp) = sort_expr(schema, "timestamp_secs", timestamp_descending) {
            sort_exprs.push(timestamp);
        }
        if let Some(ordering) = LexOrdering::new(sort_exprs) {
            orderings.push(ordering);
        }
    }

    // Advertise only the contiguous prefix of the expanded writer sort order
    // present in the table schema. Later keys are not globally ordered if an
    // earlier discriminator is hidden by the declared schema.
    let expanded_sort_exprs: Vec<PhysicalSortExpr> = METRICS_SORT_ORDER
        .iter()
        .map_while(|col_name| {
            let sort_options = if *col_name == "timestamp_secs" {
                timestamp_descending
            } else {
                ascending
            };
            sort_expr(schema, col_name, sort_options)
        })
        .collect();
    if let Some(ordering) = LexOrdering::new(expanded_sort_exprs) {
        orderings.push(ordering);
    }

    orderings
}

fn splits_have_default_metrics_sort(splits: &[ParquetSplitMetadata]) -> bool {
    splits.is_empty()
        || splits
            .iter()
            .all(|split| split.sort_fields.as_str() == ProductType::Metrics.default_sort_fields())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::prelude::*;
    use quickwit_parquet_engine::split::{
        ParquetSplitId, TAG_ENV, TAG_HOST, TAG_SERVICE, TimeRange,
    };

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
                    .downcast_ref::<datafusion_physical_plan::expressions::Column>()
                    .expect("metrics ordering should contain column expressions")
                    .name()
                    .to_string()
            })
            .collect()
    }

    fn ordering_column_options(ordering: &LexOrdering) -> Vec<SortOptions> {
        ordering.iter().map(|expr| expr.options).collect()
    }

    fn expected_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn split_with_sort_fields(sort_fields: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .index_uid("test-index")
            .time_range(TimeRange::new(0, 1))
            .sort_fields(sort_fields)
            .build()
    }

    fn test_split(split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("idx:00000000000000000000000000")
            .time_range(TimeRange::new(100, 200))
            .num_rows(10)
            .size_bytes(1024)
            .add_metric_name("cpu.usage")
            .build()
    }

    fn test_split_with_low_cardinality_tag(
        split_id: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("idx:00000000000000000000000000")
            .time_range(TimeRange::new(100, 200))
            .num_rows(10)
            .size_bytes(1024)
            .add_metric_name("cpu.usage")
            .add_low_cardinality_tag(tag_key, tag_value)
            .build()
    }

    fn test_split_with_zonemap(
        split_id: &str,
        column: &str,
        superset_regex: &str,
    ) -> ParquetSplitMetadata {
        let mut split = test_split(split_id);
        split
            .zonemap_regexes
            .insert(column.to_string(), superset_regex.to_string());
        split
    }

    fn metadata_pruned_split_ids(
        splits: Vec<ParquetSplitMetadata>,
        filters: Vec<Expr>,
    ) -> Vec<String> {
        prune_splits_with_metadata(splits, &filters)
            .into_iter()
            .map(|split| split.split_id.as_str().to_string())
            .collect()
    }

    fn assert_metadata_pruned_split_ids(
        splits: Vec<ParquetSplitMetadata>,
        filters: Vec<Expr>,
        expected_split_ids: &[&str],
    ) {
        assert_eq!(
            metadata_pruned_split_ids(splits, filters),
            expected_names(expected_split_ids)
        );
    }

    fn assert_metadata_prunes_all(splits: Vec<ParquetSplitMetadata>, filters: Vec<Expr>) {
        assert_metadata_pruned_split_ids(splits, filters, &[]);
    }

    #[test]
    fn metrics_output_ordering_stops_at_first_missing_sort_key() {
        let schema = schema_with_columns(&["metric_name", "service", "timestamp_secs"]);

        let orderings = metrics_output_orderings(&schema, &[]);

        assert_eq!(
            ordering_column_names(&orderings[0]),
            expected_names(&["metric_name", "service"])
        );
    }

    #[test]
    fn metrics_output_ordering_keeps_descending_timestamp_after_all_discriminators() {
        let schema = schema_with_columns(METRICS_SORT_ORDER);

        let orderings = metrics_output_orderings(&schema, &[]);

        assert_eq!(
            ordering_column_names(&orderings[0]),
            expected_names(METRICS_SORT_ORDER)
        );
        let options = ordering_column_options(&orderings[0]);
        assert!(
            !options[..options.len() - 1]
                .iter()
                .any(|option| option.descending),
            "non-timestamp metrics sort columns should be ascending"
        );
        assert!(
            options.last().unwrap().descending,
            "timestamp_secs must be advertised as descending to match the writer"
        );
    }

    #[test]
    fn metrics_output_ordering_prefers_sorted_series_when_declared() {
        let schema = schema_with_columns(&[
            "metric_name",
            "timestamp_secs",
            SORTED_SERIES_COLUMN,
            "service",
        ]);

        let orderings = metrics_output_orderings(&schema, &[]);

        assert_eq!(
            ordering_column_names(&orderings[0]),
            expected_names(&[SORTED_SERIES_COLUMN, "timestamp_secs"])
        );
        assert!(
            ordering_column_options(&orderings[0])[1].descending,
            "timestamp_secs must remain descending within sorted_series"
        );
        assert_eq!(
            ordering_column_names(&orderings[1]),
            expected_names(&["metric_name", "service"])
        );
    }

    #[test]
    fn metrics_output_ordering_requires_known_default_sort_fields() {
        let schema = schema_with_columns(&[
            "metric_name",
            "timestamp_secs",
            SORTED_SERIES_COLUMN,
            "service",
        ]);
        let unknown_sort_split = split_with_sort_fields("");
        let descending_tag_split =
            split_with_sort_fields("metric_name|-service|timeseries_id|timestamp_secs/V2");
        let default_sort_split = split_with_sort_fields(ProductType::Metrics.default_sort_fields());

        assert!(
            metrics_output_orderings(&schema, &[unknown_sort_split]).is_empty(),
            "must not advertise ordering for old or unknown sort metadata"
        );
        assert!(
            metrics_output_orderings(&schema, &[descending_tag_split]).is_empty(),
            "must not advertise sorted_series ordering for non-default sort metadata"
        );
        assert!(
            !metrics_output_orderings(&schema, &[default_sort_split]).is_empty(),
            "default metrics sort metadata should enable the advertised ordering"
        );
    }

    #[test]
    fn metadata_pruning_uses_low_cardinality_tags() {
        assert_metadata_pruned_split_ids(
            vec![
                test_split_with_low_cardinality_tag("web", TAG_SERVICE, "web"),
                test_split_with_low_cardinality_tag("api", TAG_SERVICE, "api"),
            ],
            vec![col(TAG_SERVICE).eq(lit("web"))],
            &["web"],
        );
    }

    #[test]
    fn metadata_pruning_treats_low_cardinality_in_list_as_any_match() {
        assert_metadata_pruned_split_ids(
            vec![
                test_split_with_low_cardinality_tag("web", TAG_SERVICE, "web"),
                test_split_with_low_cardinality_tag("api", TAG_SERVICE, "api"),
                test_split_with_low_cardinality_tag("db", TAG_SERVICE, "db"),
            ],
            vec![col(TAG_SERVICE).in_list(vec![lit("web"), lit("api")], false)],
            &["web", "api"],
        );
    }

    #[test]
    fn metadata_pruning_uses_zonemap_regexes() {
        let mut prod_split = test_split("prod");
        prod_split
            .zonemap_regexes
            .insert(TAG_ENV.to_string(), "^prod$".to_string());
        let mut staging_split = test_split("staging");
        staging_split
            .zonemap_regexes
            .insert(TAG_ENV.to_string(), "^staging$".to_string());

        assert_metadata_pruned_split_ids(
            vec![prod_split, staging_split],
            vec![col(TAG_ENV).eq(lit("prod"))],
            &["prod"],
        );
    }

    #[test]
    fn metadata_pruning_uses_zonemap_regexes_for_declared_custom_columns() {
        let mut matching_split = test_split("matching");
        matching_split.zonemap_regexes.insert(
            "availability_zone".to_string(),
            "^us\\-east\\-1a$".to_string(),
        );
        let mut nonmatching_split = test_split("nonmatching");
        nonmatching_split.zonemap_regexes.insert(
            "availability_zone".to_string(),
            "^us\\-east\\-1b$".to_string(),
        );

        assert_metadata_pruned_split_ids(
            vec![matching_split, nonmatching_split],
            vec![col("availability_zone").eq(lit("us-east-1a"))],
            &["matching"],
        );
    }

    #[test]
    fn metadata_pruning_matches_zonemap_equality_case_sensitively() {
        let exact = test_split_with_zonemap("exact", TAG_ENV, "^v1$");

        assert_metadata_pruned_split_ids(
            vec![exact.clone()],
            vec![col(TAG_ENV).eq(lit("v1"))],
            &["exact"],
        );
        assert_metadata_prunes_all(vec![exact.clone()], vec![col(TAG_ENV).eq(lit("V1"))]);
        assert_metadata_prunes_all(vec![exact], vec![col(TAG_ENV).eq(lit("v2"))]);
    }

    #[test]
    fn metadata_pruning_keeps_zonemap_superset_equality_matches() {
        let superset = test_split_with_zonemap("superset", TAG_ENV, "^v.*$");

        assert_metadata_pruned_split_ids(
            vec![superset.clone()],
            vec![col(TAG_ENV).eq(lit("v1"))],
            &["superset"],
        );
        assert_metadata_pruned_split_ids(
            vec![superset.clone()],
            vec![col(TAG_ENV).eq(lit("v2"))],
            &["superset"],
        );
        assert_metadata_prunes_all(vec![superset], vec![col(TAG_ENV).eq(lit("w3"))]);
    }

    #[test]
    fn metadata_pruning_requires_all_zonemap_conjuncts() {
        let mut multi_column = test_split_with_zonemap("multi-column", TAG_ENV, "^v.*$");
        multi_column
            .zonemap_regexes
            .insert(TAG_HOST.to_string(), "^x$".to_string());
        assert_metadata_pruned_split_ids(
            vec![multi_column.clone()],
            vec![col(TAG_ENV).eq(lit("v1")).and(col(TAG_HOST).eq(lit("x")))],
            &["multi-column"],
        );
        assert_metadata_prunes_all(
            vec![multi_column.clone()],
            vec![col(TAG_ENV).eq(lit("w3")).and(col(TAG_HOST).eq(lit("x")))],
        );
        assert_metadata_prunes_all(
            vec![multi_column],
            vec![col(TAG_ENV).eq(lit("v1")).and(col(TAG_HOST).eq(lit("y")))],
        );
    }

    #[test]
    fn metadata_pruning_keeps_or_predicates_conservative() {
        let mut multi_column = test_split_with_zonemap("multi-column", TAG_ENV, "^v.*$");
        multi_column
            .zonemap_regexes
            .insert(TAG_HOST.to_string(), "^x$".to_string());

        assert_metadata_pruned_split_ids(
            vec![multi_column],
            vec![col(TAG_ENV).eq(lit("w3")).or(col(TAG_HOST).eq(lit("y")))],
            &["multi-column"],
        );
    }

    #[test]
    fn metadata_pruning_treats_zonemap_in_list_as_any_match() {
        let exact = test_split_with_zonemap("exact", TAG_ENV, "^v1$");
        assert_metadata_pruned_split_ids(
            vec![exact.clone()],
            vec![col(TAG_ENV).in_list(vec![lit("a"), lit("v1"), lit("c")], false)],
            &["exact"],
        );
        assert_metadata_prunes_all(
            vec![exact],
            vec![col(TAG_ENV).in_list(vec![lit("a"), lit("V1"), lit("c")], false)],
        );

        let superset = test_split_with_zonemap("superset", TAG_ENV, "^v.*$");
        assert_metadata_pruned_split_ids(
            vec![superset.clone()],
            vec![col(TAG_ENV).in_list(vec![lit("v1"), lit("v2"), lit("abc")], false)],
            &["superset"],
        );
        assert_metadata_prunes_all(
            vec![superset],
            vec![col(TAG_ENV).in_list(vec![lit("a"), lit("b"), lit("c")], false)],
        );
    }

    #[test]
    fn metadata_pruning_keeps_unsupported_string_predicates_conservative() {
        let split = test_split_with_zonemap("prod", TAG_ENV, "^prod$");

        assert_metadata_pruned_split_ids(
            vec![split.clone()],
            vec![col(TAG_ENV).gt(lit("prod"))],
            &["prod"],
        );
        assert_metadata_pruned_split_ids(
            vec![split.clone()],
            vec![col(TAG_ENV).eq(lit(1i64))],
            &["prod"],
        );
        assert_metadata_pruned_split_ids(
            vec![split.clone()],
            vec![col(TAG_ENV).ilike(lit("PROD%"))],
            &["prod"],
        );
        assert_metadata_pruned_split_ids(
            vec![split],
            vec![col(TAG_ENV).not_like(lit("prod%"))],
            &["prod"],
        );
    }

    #[test]
    fn metadata_pruning_uses_low_cardinality_tag_prefixes() {
        assert_metadata_pruned_split_ids(
            vec![
                test_split_with_low_cardinality_tag("host-07", TAG_HOST, "ID-0701"),
                test_split_with_low_cardinality_tag("host-08", TAG_HOST, "ID-0801"),
            ],
            vec![col(TAG_HOST).like(lit("ID-07%"))],
            &["host-07"],
        );
    }

    #[test]
    fn metadata_pruning_prunes_zonemap_regexes_for_like_prefixes() {
        let mut host_07_split = test_split("host-07");
        host_07_split
            .zonemap_regexes
            .insert(TAG_HOST.to_string(), "^ID\\-0701$".to_string());
        let mut host_08_split = test_split("host-08");
        host_08_split
            .zonemap_regexes
            .insert(TAG_HOST.to_string(), "^ID\\-0801$".to_string());

        assert_metadata_pruned_split_ids(
            vec![host_07_split, host_08_split],
            vec![col(TAG_HOST).like(lit("ID-07%"))],
            &["host-07"],
        );
    }

    #[test]
    fn zonemap_prefix_matching_accepts_possible_suffixes() {
        assert!(zonemap_may_match_any_prefix(
            "^[I][\\s\\S]+$",
            &["ID-07".to_string()]
        ));
        assert!(!zonemap_may_match_any_prefix(
            "^host\\-[\\s\\S]+$",
            &["ID-07".to_string()]
        ));
    }

    #[test]
    fn metadata_pruning_evaluates_legacy_dotall_zonemap_regexes() {
        assert!(zonemap_may_match_any_value(
            "^foo.+$",
            &["foo\nbar".to_string()]
        ));
        assert!(zonemap_may_match_any_prefix(
            "^foo.+$",
            &["foo\n".to_string()]
        ));
    }

    #[test]
    fn classify_filter_pushes_down_like_for_metrics_tags() {
        let schema = schema_with_columns(&[TAG_HOST]);
        assert_eq!(
            classify_filter(&col(TAG_HOST).like(lit("ID-07%")), &schema),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn classify_filter_uses_declared_schema_columns() {
        let schema = schema_with_columns(&["metric_name", "availability_zone"]);

        assert_eq!(
            classify_filter(&col("availability_zone").eq(lit("us-east-1a")), &schema),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_filter(
                &col("availability_zone").in_list(vec![lit("us-east-1a")], false),
                &schema
            ),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_filter(&col("availability_zone").like(lit("us-east-%")), &schema),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_filter(&col("undeclared_tag").eq(lit("value")), &schema),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn metadata_pruning_keeps_splits_without_relevant_metadata() {
        let split = test_split("unknown");

        assert_metadata_pruned_split_ids(
            vec![split],
            vec![col(TAG_ENV).eq(lit("prod"))],
            &["unknown"],
        );
    }

    #[test]
    fn metadata_pruning_keeps_splits_with_invalid_zonemap_regex() {
        let mut split = test_split("invalid-regex");
        split
            .zonemap_regexes
            .insert(TAG_ENV.to_string(), "(".to_string());

        assert_metadata_pruned_split_ids(
            vec![split],
            vec![col(TAG_ENV).eq(lit("prod"))],
            &["invalid-regex"],
        );
    }
}
