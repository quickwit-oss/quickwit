//! Translates a Quickwit [`SearchRequest`] into a DataFusion [`DataFrame`].
//!
//! The translation builds a per-split join plan (inv ⋈ f), unions them
//! across splits, then applies aggregation / sort / limit on top.
//! Everything uses the DataFrame API — no SQL strings.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
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

    // Use semi join — keeps only left columns where join matches.
    // Then we don't get duplicate columns. But we want the fast field
    // columns, not the inv columns. So we semi-join f against inv.
    let df_f_filtered = df_f.join(
        df_inv,
        JoinType::LeftSemi,
        &["_doc_id", "_segment_ord"],
        &["_doc_id", "_segment_ord"],
        None,
    )?;

    Ok(df_f_filtered)
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

// ── Schema alignment ────────────────────────────────────────────────

/// Compute the canonical schema from the doc mapper's Arrow schema.
///
/// This is the union of all fields across all splits. The doc mapper
/// always represents the latest version of the index schema. Older
/// splits may be missing fields — those get NULL-filled during alignment.
///
/// If `canonical_schema` is `None`, we derive it from the union of
/// all split schemas (for cases where no doc mapper is available).
fn compute_canonical_schema(split_dfs: &[DataFrame]) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen: HashMap<String, DataType> = HashMap::new();

    for df in split_dfs {
        for field in df.schema().fields() {
            if let Some(existing_type) = seen.get(field.name()) {
                // Field exists — type must match (we error later if not).
                let _ = existing_type;
            } else {
                seen.insert(field.name().clone(), field.data_type().clone());
                fields.push(field.as_ref().clone());
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// Align a DataFrame to the canonical schema.
///
/// - Missing columns → added as typed NULL literals
/// - Type mismatches → error (no implicit coercion)
/// - Extra columns → kept (will be dropped by final projection)
///
/// After alignment, all DataFrames have identical schemas and can
/// be safely unioned.
fn align_to_schema(
    df: DataFrame,
    canonical: &SchemaRef,
    split_id: &str,
) -> Result<DataFrame> {
    let df_schema = df.schema().clone();
    let df_fields: HashMap<&str, &arrow::datatypes::DataType> = df_schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();

    // Check for type mismatches.
    for canon_field in canonical.fields() {
        if let Some(&split_type) = df_fields.get(canon_field.name().as_str()) {
            if split_type != canon_field.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "type mismatch for field '{}' in split {}: \
                     split has {:?}, canonical schema has {:?}. \
                     Use CAST() in SQL to convert explicitly.",
                    canon_field.name(),
                    split_id,
                    split_type,
                    canon_field.data_type(),
                )));
            }
        }
    }

    // Build select list: canonical columns in order, NULLs for missing.
    let select_exprs: Vec<Expr> = canonical
        .fields()
        .iter()
        .map(|canon_field| {
            if df_fields.contains_key(canon_field.name().as_str()) {
                col(canon_field.name().as_str())
            } else {
                // Missing column → typed NULL.
                let null_value = ScalarValue::try_from(canon_field.data_type())
                    .unwrap_or(ScalarValue::Utf8(None));
                lit(null_value).alias(canon_field.name())
            }
        })
        .collect();

    df.select(select_exprs)
}

// ── SearchRequest → DataFrame ───────────────────────────────────────

/// Translate a [`SearchRequest`] into a DataFusion [`DataFrame`] that
/// spans all provided splits.
///
/// Builds per-split join plans (inv ⋈ f), aligns schemas across splits
/// (NULL-fill for missing columns, error on type mismatch), unions them,
/// then applies sort / limit / offset on top.
///
/// Uses the DataFrame API throughout — no SQL strings.
///
/// `canonical_schema`: if provided, all splits are aligned to this schema.
/// If `None`, the canonical schema is derived from the union of all split schemas.
pub fn build_search_plan(
    ctx: &SessionContext,
    splits: &[SplitMetadata],
    opener_factory: &OpenerFactory,
    request: &SearchRequest,
    canonical_schema: Option<SchemaRef>,
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

    // Build per-split plans.
    let mut per_split_dfs: Vec<(String, DataFrame)> = Vec::with_capacity(splits.len());
    for split in splits {
        let opener = opener_factory(split);
        let df = build_split_plan(ctx, opener, &filter_expr, has_ft)?;
        per_split_dfs.push((split.split_id.clone(), df));
    }

    // Compute or use provided canonical schema.
    let canonical = canonical_schema.unwrap_or_else(|| {
        let dfs: Vec<&DataFrame> = per_split_dfs.iter().map(|(_, df)| df).collect();
        compute_canonical_schema(
            &dfs.into_iter().cloned().collect::<Vec<_>>(),
        )
    });

    // Align each split's DataFrame to the canonical schema.
    let mut aligned: Vec<DataFrame> = Vec::with_capacity(per_split_dfs.len());
    for (split_id, df) in per_split_dfs {
        let aligned_df = align_to_schema(df, &canonical, &split_id)?;
        aligned.push(aligned_df);
    }

    // UNION ALL across splits.
    let mut result = aligned.remove(0);
    for df in aligned {
        result = result.union(df)?;
    }

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
