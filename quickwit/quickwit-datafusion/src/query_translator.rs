//! Translates a Quickwit [`SearchRequest`] into a DataFusion [`DataFrame`].
//!
//! The translation builds a per-split join plan (inv ⋈ f), unions them
//! across splits, then applies aggregation / sort / limit on top.
//! Everything uses the DataFrame API — no SQL strings.

use std::ops::Bound;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{col, lit, Expr, JoinType, SortExpr};
use datafusion::prelude::{DataFrame, SessionContext};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::search::{SearchRequest, SortField, SortOrder};
use quickwit_query::query_ast::{BoolQuery, FullTextQuery, QueryAst, RangeQuery, TermQuery};
use quickwit_query::JsonLiteral;
use tantivy_datafusion::{
    IndexOpener, TantivyInvertedIndexProvider, TantivyTableProvider, full_text_udf,
};

use crate::table_provider::OpenerFactory;

// ── QueryAst → Expr ─────────────────────────────────────────────────

/// Convert a [`QueryAst`] into a DataFusion filter [`Expr`].
///
/// Full-text queries become `full_text(field, text)` UDF calls.
/// Term/range/bool queries become standard comparison expressions.
pub fn query_ast_to_expr(ast: &QueryAst) -> Result<Expr> {
    match ast {
        QueryAst::MatchAll => Ok(lit(true)),
        QueryAst::MatchNone => Ok(lit(false)),
        QueryAst::FullText(ft) => Ok(full_text_expr(ft)),
        QueryAst::Term(t) => Ok(term_expr(t)),
        QueryAst::Range(r) => range_expr(r),
        QueryAst::Bool(b) => bool_expr(b),

        QueryAst::TermSet(ts) => {
            let mut exprs = Vec::new();
            for (field, values) in &ts.terms_per_field {
                let literals: Vec<Expr> = values.iter().map(|v| lit(v.as_str())).collect();
                exprs.push(col(field.as_str()).in_list(literals, false));
            }
            Ok(and_all(exprs))
        }

        QueryAst::Boost { underlying, .. } => query_ast_to_expr(underlying),

        QueryAst::Wildcard(w) => {
            let pattern = w
                .value
                .replace('%', "\\%")
                .replace('_', "\\_")
                .replace('*', "%")
                .replace('?', "_");
            Ok(col(w.field.as_str()).like(lit(pattern)))
        }

        QueryAst::UserInput(_) => Err(DataFusionError::Plan(
            "UserInput queries must be parsed before translation; \
             call query_ast.parse_user_query() first"
                .to_string(),
        )),

        QueryAst::Regex(r) => Ok(col(r.field.as_str()).like(lit(format!("%{}%", r.regex)))),

        QueryAst::FieldPresence(fp) => Ok(col(fp.field.as_str()).is_not_null()),

        QueryAst::PhrasePrefix(pp) => Ok(Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(full_text_udf()),
                vec![col(pp.field.as_str()), lit(pp.phrase.as_str())],
            ),
        )),

        QueryAst::Cache(c) => query_ast_to_expr(&c.inner),
    }
}

fn full_text_expr(ft: &FullTextQuery) -> Expr {
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        Arc::new(full_text_udf()),
        vec![col(ft.field.as_str()), lit(ft.text.as_str())],
    ))
}

fn term_expr(t: &TermQuery) -> Expr {
    col(t.field.as_str()).eq(lit(t.value.as_str()))
}

fn range_expr(r: &RangeQuery) -> Result<Expr> {
    let field = col(r.field.as_str());
    let mut exprs = Vec::new();
    match &r.lower_bound {
        Bound::Included(v) => exprs.push(field.clone().gt_eq(json_literal_to_expr(v))),
        Bound::Excluded(v) => exprs.push(field.clone().gt(json_literal_to_expr(v))),
        Bound::Unbounded => {}
    }
    match &r.upper_bound {
        Bound::Included(v) => exprs.push(field.clone().lt_eq(json_literal_to_expr(v))),
        Bound::Excluded(v) => exprs.push(field.lt(json_literal_to_expr(v))),
        Bound::Unbounded => {}
    }
    Ok(and_all(exprs))
}

fn bool_expr(b: &BoolQuery) -> Result<Expr> {
    let mut parts = Vec::new();
    for ast in &b.must {
        parts.push(query_ast_to_expr(ast)?);
    }
    for ast in &b.filter {
        parts.push(query_ast_to_expr(ast)?);
    }
    for ast in &b.must_not {
        parts.push(Expr::Not(Box::new(query_ast_to_expr(ast)?)));
    }
    if !b.should.is_empty() {
        let should_exprs: Vec<Expr> = b
            .should
            .iter()
            .map(query_ast_to_expr)
            .collect::<Result<Vec<_>>>()?;
        parts.push(or_all(should_exprs));
    }
    Ok(and_all(parts))
}

fn json_literal_to_expr(literal: &JsonLiteral) -> Expr {
    match literal {
        JsonLiteral::String(s) => lit(s.as_str()),
        JsonLiteral::Number(n) => {
            if let Some(i) = n.as_i64() {
                lit(i)
            } else if let Some(u) = n.as_u64() {
                lit(u)
            } else if let Some(f) = n.as_f64() {
                lit(f)
            } else {
                lit(n.to_string())
            }
        }
        JsonLiteral::Bool(b) => lit(*b),
    }
}

fn and_all(exprs: Vec<Expr>) -> Expr {
    exprs
        .into_iter()
        .reduce(|a, b| a.and(b))
        .unwrap_or(lit(true))
}

fn or_all(exprs: Vec<Expr>) -> Expr {
    exprs
        .into_iter()
        .reduce(|a, b| a.or(b))
        .unwrap_or(lit(false))
}

fn sort_field_to_sort_expr(sf: &SortField) -> SortExpr {
    let asc = sf.sort_order() != SortOrder::Desc;
    SortExpr {
        expr: col(sf.field_name.as_str()),
        asc,
        nulls_first: !asc,
    }
}

// ── Per-split join plan builder ─────────────────────────────────────

/// Build a per-split join plan: `inv ⋈ f` with the query filter
/// applied on the inverted index side.
///
/// This is a single split's contribution to the overall query.
/// The join is on `(_doc_id, _segment_ord)` — segment-level
/// co-partitioning between the inverted index and fast fields.
fn build_split_plan(
    ctx: &SessionContext,
    opener: Arc<dyn IndexOpener>,
    query_filter: &Expr,
    has_full_text: bool,
) -> Result<DataFrame> {
    let df_f = ctx.read_table(Arc::new(TantivyTableProvider::from_opener(
        opener.clone(),
    )))?;

    if !has_full_text {
        // No full-text query — just fast field scan with filter.
        return df_f.filter(query_filter.clone());
    }

    // Full-text query: build inv ⋈ f join.
    let df_inv = ctx.read_table(Arc::new(TantivyInvertedIndexProvider::from_opener(
        opener,
    )))?;

    // Apply the full-text filter on the inverted index side.
    let df_inv = df_inv.filter(query_filter.clone())?;

    // Join on (_doc_id, _segment_ord).
    df_inv.join(
        df_f,
        JoinType::Inner,
        &["_doc_id", "_segment_ord"],
        &["_doc_id", "_segment_ord"],
        None,
    )
}

/// Check if a QueryAst contains any full-text query nodes.
fn has_full_text_queries(ast: &QueryAst) -> bool {
    match ast {
        QueryAst::FullText(_) | QueryAst::PhrasePrefix(_) => true,
        QueryAst::Bool(b) => {
            b.must.iter().any(has_full_text_queries)
                || b.should.iter().any(has_full_text_queries)
                || b.filter.iter().any(has_full_text_queries)
        }
        QueryAst::Boost { underlying, .. } => has_full_text_queries(underlying),
        QueryAst::Cache(c) => has_full_text_queries(&c.inner),
        _ => false,
    }
}

// ── SearchRequest → DataFrame ───────────────────────────────────────

/// Translate a [`SearchRequest`] into a DataFusion [`DataFrame`] that
/// spans all provided splits.
///
/// Builds per-split join plans (inv ⋈ f), unions them, then applies
/// sort / limit / offset on top. Uses the DataFrame API throughout —
/// no SQL strings.
///
/// For aggregations, use [`translate_aggregation_request`] on the
/// returned DataFrame.
pub fn build_search_plan(
    ctx: &SessionContext,
    splits: &[SplitMetadata],
    opener_factory: &OpenerFactory,
    request: &SearchRequest,
) -> Result<DataFrame> {
    if splits.is_empty() {
        return Err(DataFusionError::Plan("no splits to search".to_string()));
    }

    // Parse the query AST.
    let query_ast: QueryAst = serde_json::from_str(&request.query_ast).map_err(|e| {
        DataFusionError::Plan(format!("failed to parse query_ast: {e}"))
    })?;
    let filter_expr = query_ast_to_expr(&query_ast)?;
    let has_ft = has_full_text_queries(&query_ast);

    // Build per-split plans and union them.
    let mut per_split_dfs: Vec<DataFrame> = Vec::with_capacity(splits.len());
    for split in splits {
        let opener = opener_factory(split);
        let df = build_split_plan(ctx, opener, &filter_expr, has_ft)?;
        per_split_dfs.push(df);
    }

    let mut result = per_split_dfs.remove(0);
    for df in per_split_dfs {
        result = result.union(df)?;
    }

    // Drop internal columns (_doc_id, _segment_ord, _score, virtual text cols)
    // that the caller doesn't need. Keep only user-facing columns.
    // For now, we keep all columns and let the caller select.

    // Apply sort.
    if !request.sort_fields.is_empty() {
        let sort_exprs: Vec<SortExpr> = request
            .sort_fields
            .iter()
            .map(sort_field_to_sort_expr)
            .collect();
        result = result.sort(sort_exprs)?;
    }

    // Apply limit + offset.
    if request.start_offset > 0 || request.max_hits > 0 {
        let offset = request.start_offset as usize;
        let limit = if request.max_hits > 0 {
            Some(request.max_hits as usize)
        } else {
            None
        };
        result = result.limit(offset, limit)?;
    }

    Ok(result)
}

/// Translate the aggregation portion of a [`SearchRequest`].
///
/// Takes the unioned DataFrame (from `build_search_plan`) and applies
/// ES-style aggregations using tantivy-df's translator.
pub fn build_aggregation_plan(
    df: DataFrame,
    aggregation_json: &str,
) -> Result<std::collections::HashMap<String, DataFrame>> {
    let aggs: tantivy::aggregation::agg_req::Aggregations =
        serde_json::from_str(aggregation_json).map_err(|e| {
            DataFusionError::Plan(format!("failed to parse aggregation request: {e}"))
        })?;
    tantivy_datafusion::translate_aggregations(df, &aggs)
}
