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

//! Predicate extraction for Postgres split pruning.
//!
//! Extracts metric_name, time_range, and tag filters from DataFusion
//! filter expressions to build a query for the `metrics_splits` table.
//!
//! OSS column names: `service`, `env`, `datacenter`, `region`, `host`
//! (no `tag_` prefix — the parquet files use bare column names).

use std::collections::HashMap;

use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};
use datafusion::scalar::ScalarValue;

/// Extracted filters for querying the metrics_splits table.
///
/// Split-level filters for metastore pruning.
///
/// These fields are safe to pass to the metastore for coarse split discovery.
/// More granular string filters are extracted separately for conservative
/// client-side metadata pruning after the split metadata has been fetched.
#[derive(Debug, Default, Clone)]
pub struct MetricsSplitQuery {
    pub metric_names: Option<Vec<String>>,
    pub time_range_start: Option<u64>,
    pub time_range_end: Option<u64>,
}

/// Equality/IN filter over string-valued columns.
///
/// This is used for split metadata pruning. Values are interpreted as an OR
/// list for a column; multiple filters on the same column are intersected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StringFilter {
    pub column: String,
    pub values: Vec<String>,
}

/// Prefix filter over string-valued columns extracted from simple LIKE
/// predicates.
///
/// Each prefix represents `column LIKE 'prefix%'`. More general LIKE patterns
/// are intentionally ignored because split pruning needs to stay conservative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StringPrefixFilter {
    pub column: String,
    pub prefixes: Vec<String>,
}

/// Analyzes pushed-down filter expressions and extracts split-level filters.
///
/// Returns a `MetricsSplitQuery` for Postgres pruning plus any remaining
/// filter expressions that must be applied at the parquet reader level.
pub fn extract_split_filters(filters: &[Expr]) -> (MetricsSplitQuery, Vec<Expr>) {
    let mut query = MetricsSplitQuery::default();
    let mut remaining = Vec::new();

    for filter in filters {
        if !try_extract_filter(filter, &mut query) {
            remaining.push(filter.clone());
        }
    }

    (query, remaining)
}

/// Extract string equality/IN predicates that are safe to evaluate against
/// exact split metadata or zonemap superset regexes.
pub(crate) fn extract_string_filters(filters: &[Expr]) -> Vec<StringFilter> {
    let mut by_column: HashMap<String, Vec<String>> = HashMap::new();

    for filter in filters {
        collect_string_filters(filter, &mut by_column);
    }

    by_column
        .into_iter()
        .map(|(column, values)| StringFilter { column, values })
        .collect()
}

/// Extract simple string prefix predicates of the form `column LIKE 'prefix%'`.
pub(crate) fn extract_string_prefix_filters(filters: &[Expr]) -> Vec<StringPrefixFilter> {
    let mut by_column: HashMap<String, Vec<String>> = HashMap::new();

    for filter in filters {
        collect_string_prefix_filters(filter, &mut by_column);
    }

    by_column
        .into_iter()
        .map(|(column, prefixes)| StringPrefixFilter { column, prefixes })
        .collect()
}

fn collect_string_filters(expr: &Expr, by_column: &mut HashMap<String, Vec<String>>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => {
                let Some((column, value)) = string_eq_filter(left, right) else {
                    return;
                };
                tighten_string_values(by_column, column, vec![value]);
            }
            Operator::And => {
                collect_string_filters(left, by_column);
                collect_string_filters(right, by_column);
            }
            _ => {}
        },
        Expr::InList(in_list) if !in_list.negated => {
            let Some(column) = column_name(&in_list.expr) else {
                return;
            };
            let values: Vec<String> = in_list.list.iter().filter_map(scalar_utf8).collect();
            if values.len() == in_list.list.len() {
                tighten_string_values(by_column, column, values);
            }
        }
        _ => {}
    }
}

fn collect_string_prefix_filters(expr: &Expr, by_column: &mut HashMap<String, Vec<String>>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            collect_string_prefix_filters(left, by_column);
            collect_string_prefix_filters(right, by_column);
        }
        Expr::Like(like) => {
            let Some((column, prefix)) = string_prefix_filter(like) else {
                return;
            };
            by_column.entry(column).or_default().push(prefix);
        }
        _ => {}
    }
}

fn string_eq_filter(left: &Expr, right: &Expr) -> Option<(String, String)> {
    match (column_name(left), scalar_utf8(right)) {
        (Some(column), Some(value)) => Some((column, value)),
        _ => match (scalar_utf8(left), column_name(right)) {
            (Some(value), Some(column)) => Some((column, value)),
            _ => None,
        },
    }
}

fn string_prefix_filter(like: &Like) -> Option<(String, String)> {
    if like.negated || like.case_insensitive || like.escape_char.is_some() {
        return None;
    }

    let column = column_name(&like.expr)?;
    let pattern = scalar_utf8(&like.pattern)?;
    let prefix = simple_like_prefix(&pattern)?;
    Some((column, prefix))
}

fn simple_like_prefix(pattern: &str) -> Option<String> {
    let prefix = pattern.strip_suffix('%')?;
    if prefix.is_empty() || prefix.contains(['%', '_']) {
        return None;
    }
    Some(prefix.to_string())
}

fn tighten_string_values(
    by_column: &mut HashMap<String, Vec<String>>,
    column: String,
    values: Vec<String>,
) {
    let mut values = dedup_strings(values);
    match by_column.get_mut(&column) {
        Some(existing) => {
            existing.retain(|value| values.contains(value));
        }
        None => {
            by_column.insert(column, std::mem::take(&mut values));
        }
    }
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::with_capacity(values.len());
    for value in values {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }
    deduped
}

fn try_extract_filter(expr: &Expr, query: &mut MetricsSplitQuery) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => try_extract_eq(left, right, query),
            Operator::GtEq => try_extract_ts_gte(left, right, query),
            Operator::Gt => try_extract_ts_gt(left, right, query),
            Operator::Lt => try_extract_ts_lt(left, right, query),
            Operator::LtEq => try_extract_ts_lte(left, right, query),
            Operator::And => {
                let l = try_extract_filter(left, query);
                let r = try_extract_filter(right, query);
                l && r
            }
            _ => false,
        },
        Expr::InList(in_list) if !in_list.negated => {
            try_extract_in_list(&in_list.expr, &in_list.list, query)
        }
        _ => false,
    }
}

fn try_extract_eq(left: &Expr, right: &Expr, query: &mut MetricsSplitQuery) -> bool {
    let (col, val) = match (column_name(left), scalar_utf8(right)) {
        (Some(c), Some(v)) => (c, v),
        _ => match (scalar_utf8(left), column_name(right)) {
            (Some(v), Some(c)) => (c, v),
            _ => return false,
        },
    };
    set_tag_values(&col, vec![val], query)
}

fn try_extract_in_list(expr: &Expr, list: &[Expr], query: &mut MetricsSplitQuery) -> bool {
    let col = match column_name(expr) {
        Some(n) => n,
        None => return false,
    };
    let values: Vec<String> = list.iter().filter_map(scalar_utf8).collect();
    if values.is_empty() || values.len() != list.len() {
        return false;
    }
    set_tag_values(&col, values, query)
}

fn try_extract_ts_gte(left: &Expr, right: &Expr, q: &mut MetricsSplitQuery) -> bool {
    // column >= literal
    if let (Some(c), Some(v)) = (column_name(left), scalar_u64(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_start(q, v);
        return true;
    }
    // literal >= column  →  column <= literal  →  time_range_end
    if let (Some(v), Some(c)) = (scalar_u64(left), column_name(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_end(q, v + 1);
        return true;
    }
    false
}

fn try_extract_ts_gt(left: &Expr, right: &Expr, q: &mut MetricsSplitQuery) -> bool {
    // column > literal
    if let (Some(c), Some(v)) = (column_name(left), scalar_u64(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_start(q, v + 1);
        return true;
    }
    // literal > column  →  column < literal  →  time_range_end
    if let (Some(v), Some(c)) = (scalar_u64(left), column_name(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_end(q, v);
        return true;
    }
    false
}

fn try_extract_ts_lt(left: &Expr, right: &Expr, q: &mut MetricsSplitQuery) -> bool {
    // column < literal
    if let (Some(c), Some(v)) = (column_name(left), scalar_u64(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_end(q, v);
        return true;
    }
    // literal < column  →  column > literal  →  time_range_start
    if let (Some(v), Some(c)) = (scalar_u64(left), column_name(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_start(q, v + 1);
        return true;
    }
    false
}

fn try_extract_ts_lte(left: &Expr, right: &Expr, q: &mut MetricsSplitQuery) -> bool {
    // column <= literal
    if let (Some(c), Some(v)) = (column_name(left), scalar_u64(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_end(q, v + 1);
        return true;
    }
    // literal <= column  →  column >= literal  →  time_range_start
    if let (Some(v), Some(c)) = (scalar_u64(left), column_name(right))
        && c == "timestamp_secs"
    {
        tighten_time_range_start(q, v);
        return true;
    }
    false
}

fn tighten_time_range_start(q: &mut MetricsSplitQuery, start: u64) {
    q.time_range_start = Some(match q.time_range_start {
        Some(prev) => prev.max(start),
        None => start,
    });
}

fn tighten_time_range_end(q: &mut MetricsSplitQuery, end: u64) {
    q.time_range_end = Some(match q.time_range_end {
        Some(prev) => prev.min(end),
        None => end,
    });
}

/// Map OSS column names (no `tag_` prefix) to MetricsSplitQuery tag fields.
fn set_tag_values(col: &str, values: Vec<String>, q: &mut MetricsSplitQuery) -> bool {
    match col {
        "metric_name" => {
            q.metric_names = Some(values);
            true
        }
        _ => false,
    }
}

pub(crate) fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name().to_string()),
        // DataFusion inserts CASTs when comparing UInt64 columns with Int64 literals.
        // Unwrap the cast to find the underlying column name.
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => column_name(expr),
        _ => None,
    }
}

fn scalar_utf8(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        // DF auto-casts string literals to Dict(Int32, Utf8) to match dict-encoded columns
        Expr::Literal(ScalarValue::Dictionary(_, inner), _) => scalar_utf8_from_scalar(inner),
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => scalar_utf8(expr),
        _ => None,
    }
}

fn scalar_utf8_from_scalar(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(s)) => Some(s.clone()),
        ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn scalar_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0 => Some(*v as u64),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(*v as u64),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) if *v >= 0 => Some(*v as u64),
        // Unwrap casts inserted by DataFusion type coercion.
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. })
        | Expr::TryCast(datafusion::logical_expr::TryCast { expr, .. }) => scalar_u64(expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::*;

    use super::*;

    #[test]
    fn test_extract_metric_name_eq() {
        let filters = vec![col("metric_name").eq(lit("cpu.usage"))];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.metric_names, Some(vec!["cpu.usage".to_string()]));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_extract_timestamp_range() {
        let filters = vec![
            col("timestamp_secs").gt_eq(lit(1000u64)),
            col("timestamp_secs").lt(lit(2000u64)),
        ];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.time_range_start, Some(1000));
        assert_eq!(query.time_range_end, Some(2000));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_tag_filters_stay_as_remaining() {
        // Tag filters are not pushed to the metastore; they remain for parquet-level filtering.
        let filters = vec![
            col("metric_name").eq(lit("cpu.usage")),
            col("service").eq(lit("web")),
            col("env").eq(lit("prod")),
        ];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.metric_names, Some(vec!["cpu.usage".to_string()]));
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn test_extract_string_filters_for_tags() {
        let filters = vec![
            col("service").eq(lit("web")),
            col("env").in_list(vec![lit("prod"), lit("staging")], false),
            col("value").gt(lit(42.0)),
        ];

        let mut string_filters = extract_string_filters(&filters);
        string_filters.sort_by(|left, right| left.column.cmp(&right.column));

        assert_eq!(
            string_filters,
            vec![
                StringFilter {
                    column: "env".to_string(),
                    values: vec!["prod".to_string(), "staging".to_string()],
                },
                StringFilter {
                    column: "service".to_string(),
                    values: vec!["web".to_string()],
                },
            ]
        );
    }

    #[test]
    fn test_extract_string_filters_intersects_repeated_column() {
        let filters = vec![
            col("service").in_list(vec![lit("web"), lit("api")], false),
            col("service").eq(lit("api")),
        ];

        let string_filters = extract_string_filters(&filters);

        assert_eq!(
            string_filters,
            vec![StringFilter {
                column: "service".to_string(),
                values: vec!["api".to_string()],
            }]
        );
    }

    #[test]
    fn test_extract_string_prefix_filters_for_simple_like() {
        let filters = vec![
            col("host").like(lit("ID-07%")),
            col("env").like(lit("prod")),
            col("service").ilike(lit("web%")),
            col("region").like(lit("us_%")),
        ];

        let prefix_filters = extract_string_prefix_filters(&filters);

        assert_eq!(
            prefix_filters,
            vec![StringPrefixFilter {
                column: "host".to_string(),
                prefixes: vec!["ID-07".to_string()],
            }]
        );
    }

    #[test]
    fn test_unknown_column_left_as_remaining() {
        let filters = vec![
            col("metric_name").eq(lit("cpu.usage")),
            col("value").gt(lit(42.0)),
        ];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.metric_names, Some(vec!["cpu.usage".to_string()]));
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_in_list_extraction() {
        let filters =
            vec![col("metric_name").in_list(vec![lit("cpu.usage"), lit("memory.used")], false)];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(
            query.metric_names,
            Some(vec!["cpu.usage".to_string(), "memory.used".to_string()])
        );
        assert!(remaining.is_empty());
    }

    // ── CAST unwrapping (DataFusion type coercion) ─────────────

    #[test]
    fn test_timestamp_gte_with_cast_column() {
        // DataFusion rewrites `timestamp_secs >= 1000` (UInt64 col vs Int64 lit) as
        // CAST(timestamp_secs AS Int64) >= 1000
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Cast(datafusion::logical_expr::Cast {
                expr: Box::new(col("timestamp_secs")),
                data_type: datafusion::arrow::datatypes::DataType::Int64,
            })),
            op: Operator::GtEq,
            right: Box::new(lit(1000i64)),
        })];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.time_range_start, Some(1000));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_timestamp_lt_with_cast_column() {
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Cast(datafusion::logical_expr::Cast {
                expr: Box::new(col("timestamp_secs")),
                data_type: datafusion::arrow::datatypes::DataType::Int64,
            })),
            op: Operator::Lt,
            right: Box::new(lit(2000i64)),
        })];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.time_range_end, Some(2000));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_timestamp_gt_with_cast_literal() {
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("timestamp_secs")),
            op: Operator::Gt,
            right: Box::new(Expr::Cast(datafusion::logical_expr::Cast {
                expr: Box::new(lit(500i64)),
                data_type: datafusion::arrow::datatypes::DataType::UInt64,
            })),
        })];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.time_range_start, Some(501));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_metric_name_eq_with_dict_cast() {
        let dict_lit = Expr::Literal(
            ScalarValue::Dictionary(
                Box::new(datafusion::arrow::datatypes::DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("cpu.usage".to_string()))),
            ),
            None,
        );
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("metric_name")),
            op: Operator::Eq,
            right: Box::new(dict_lit),
        })];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.metric_names, Some(vec!["cpu.usage".to_string()]));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_tag_filters_all_stay_as_remaining() {
        let filters = vec![
            col("service").eq(lit("web")),
            col("env").eq(lit("prod")),
            col("datacenter").eq(lit("dc1")),
            col("region").eq(lit("us-east-1")),
            col("host").eq(lit("host-01")),
        ];
        let (query, remaining) = extract_split_filters(&filters);
        assert!(query.metric_names.is_none());
        assert_eq!(remaining.len(), 5);
    }

    #[test]
    fn test_combined_metric_time_tags_pushdown() {
        let filters = vec![
            col("metric_name").eq(lit("cpu.usage")),
            col("timestamp_secs").gt_eq(lit(1000u64)),
            col("timestamp_secs").lt(lit(2000u64)),
            col("env").eq(lit("prod")),
            col("value").gt(lit(0.5)), // not pushable
        ];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.metric_names, Some(vec!["cpu.usage".to_string()]));
        assert_eq!(query.time_range_start, Some(1000));
        assert_eq!(query.time_range_end, Some(2000));
        assert_eq!(remaining.len(), 2, "env and value > 0.5 should remain");
    }

    #[test]
    fn test_timestamp_lte_pushdown() {
        let filters = vec![col("timestamp_secs").lt_eq(lit(5000u64))];
        let (query, remaining) = extract_split_filters(&filters);
        assert_eq!(query.time_range_end, Some(5001));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_tag_in_list_stays_as_remaining() {
        let filters = vec![col("service").in_list(vec![lit("web"), lit("api")], false)];
        let (query, remaining) = extract_split_filters(&filters);
        assert!(query.metric_names.is_none());
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_no_filters_returns_empty_query() {
        let (query, remaining) = extract_split_filters(&[]);
        assert!(query.metric_names.is_none());
        assert!(query.time_range_start.is_none());
        assert!(query.time_range_end.is_none());
        assert!(remaining.is_empty());
    }

    // ── Extraction → pruning pipeline (Fix #22) ───────────────────────

    /// Verifies that `extract_split_filters` prunes at the SPLIT level, not just
    /// at the row level. This test would fail if metric_name equality extraction
    /// were removed — `count_matching` would return 2 instead of 1.
    #[test]
    fn test_metric_name_pruning_prunes_splits_not_just_rows() {
        use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};

        use crate::sources::metrics::test_utils::TestSplitProvider;

        let cpu_split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("cpu"))
            .index_uid("idx:0000")
            .time_range(TimeRange::new(100, 300))
            .num_rows(2)
            .size_bytes(1024)
            .add_metric_name("cpu.usage")
            .build();
        let mem_split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("mem"))
            .index_uid("idx:0000")
            .time_range(TimeRange::new(100, 300))
            .num_rows(2)
            .size_bytes(1024)
            .add_metric_name("memory.used")
            .build();

        let provider = TestSplitProvider::new(vec![cpu_split, mem_split]);

        let filters = vec![col("metric_name").eq(lit("cpu.usage"))];
        let (query, remaining) = extract_split_filters(&filters);
        assert!(
            remaining.is_empty(),
            "metric_name = 'cpu.usage' must be fully extracted"
        );

        let matching = provider.count_matching(&query);
        assert_eq!(
            matching, 1,
            "predicate extractor must prune to 1 split for metric_name = 'cpu.usage', got \
             {matching}"
        );
    }

    // ── TestSplitProvider multi-value IN list (Fix #23) ───────────────

    #[test]
    fn test_metric_name_in_list_prunes_splits() {
        use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};

        use crate::sources::metrics::test_utils::TestSplitProvider;

        let cpu_split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("cpu"))
            .index_uid("idx:0000")
            .time_range(TimeRange::new(100, 300))
            .num_rows(2)
            .size_bytes(1024)
            .add_metric_name("cpu.usage")
            .build();
        let mem_split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("mem"))
            .index_uid("idx:0000")
            .time_range(TimeRange::new(100, 300))
            .num_rows(2)
            .size_bytes(1024)
            .add_metric_name("memory.used")
            .build();
        let disk_split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("disk"))
            .index_uid("idx:0000")
            .time_range(TimeRange::new(100, 300))
            .num_rows(2)
            .size_bytes(1024)
            .add_metric_name("disk.io")
            .build();

        let provider = TestSplitProvider::new(vec![cpu_split, mem_split, disk_split]);

        let filters =
            vec![col("metric_name").in_list(vec![lit("cpu.usage"), lit("memory.used")], false)];
        let (query, remaining) = extract_split_filters(&filters);
        assert!(remaining.is_empty());
        assert_eq!(provider.count_matching(&query), 2);
    }
}
