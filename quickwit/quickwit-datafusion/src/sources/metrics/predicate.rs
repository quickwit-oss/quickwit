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

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Extracted filters for querying the metrics_splits table.
///
/// Split-level filters for metastore pruning.
///
/// Only metric name and time range are used — the only fields the metastore
/// reliably populates today. Tag-based pruning will be added when the
/// zonemap/bloom-filter mechanism lands.
#[derive(Debug, Default, Clone)]
pub struct MetricsSplitQuery {
    pub metric_names: Option<Vec<String>>,
    pub time_range_start: Option<u64>,
    pub time_range_end: Option<u64>,
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
