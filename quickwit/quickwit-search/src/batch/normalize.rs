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

//! Datadog-specific query normalization applied before batching.
//!
//! Two quirks are handled here so that `combine` can stay generic:
//!
//! 1. **Field-presence stripping**: Log Explorer sends facet requests as `field_presence(service)
//!    AND <base>` with a `terms(field: service)` agg. Since tantivy's terms agg skips docs without
//!    the field, the clause is redundant and is stripped so facet and list requests hash to the
//!    same key.
//!
//! 2. **Timestamp quantization**: The webapp issues list, histogram, and facet requests in quick
//!    succession; their `timestamp` bounds drift by ≤1 s. Rounding down to 3-second buckets absorbs
//!    the drift so all three land on the same batch.

use std::ops::Bound;

use quickwit_proto::search::SearchRequest;
use quickwit_query::JsonLiteral;
use quickwit_query::query_ast::QueryAst;
use tantivy::aggregation::agg_req::{AggregationVariants, Aggregations};

/// Bucket size for timestamp quantization, matching the bridge's
/// `timestampQuantizationSeconds`.
const TIMESTAMP_QUANTIZATION_SECS: i64 = 3;

/// Minimum range duration (in seconds) for timestamp quantization to apply.
/// Shorter ranges are left untouched — a 3s shift on a 5s window is too significant,
/// and short-range queries are cheap enough that batching them is not worth it.
const MIN_QUANTIZATION_RANGE_SECS: i64 = 30;

/// Parses an RFC3339 string to unix seconds.
fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    let dt = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()?;
    Some(dt.unix_timestamp())
}

/// Extracts unix seconds from a `Bound<JsonLiteral>` containing an RFC3339 string.
fn bound_secs(bound: &Bound<JsonLiteral>) -> Option<i64> {
    match bound {
        Bound::Included(JsonLiteral::String(s)) | Bound::Excluded(JsonLiteral::String(s)) => {
            parse_rfc3339_secs(s)
        }
        _ => None,
    }
}

/// Quantizes an RFC3339 string bound to the nearest `TIMESTAMP_QUANTIZATION_SECS` bucket.
fn quantize_bound(bound: Bound<JsonLiteral>) -> Bound<JsonLiteral> {
    match bound {
        Bound::Included(JsonLiteral::String(s)) => {
            let q = quantize_rfc3339(&s).unwrap_or(s);
            Bound::Included(JsonLiteral::String(q))
        }
        Bound::Excluded(JsonLiteral::String(s)) => {
            let q = quantize_rfc3339(&s).unwrap_or(s);
            Bound::Excluded(JsonLiteral::String(q))
        }
        other => other,
    }
}

fn quantize_rfc3339(s: &str) -> Option<String> {
    let secs = parse_rfc3339_secs(s)?;
    let quantized_secs = (secs / TIMESTAMP_QUANTIZATION_SECS) * TIMESTAMP_QUANTIZATION_SECS;
    let quantized = time::OffsetDateTime::from_unix_timestamp(quantized_secs).ok()?;
    quantized
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

/// Recursively quantizes `timestamp` range bounds in the query AST.
fn quantize_timestamp_ranges(ast: &mut QueryAst) {
    match ast {
        QueryAst::Range(range) if range.field == "timestamp" => {
            let lo_secs = bound_secs(&range.lower_bound);
            let hi_secs = bound_secs(&range.upper_bound);
            let too_short = match (lo_secs, hi_secs) {
                (Some(lo), Some(hi)) => (hi - lo) < MIN_QUANTIZATION_RANGE_SECS,
                _ => false,
            };
            if !too_short {
                let lower = std::mem::replace(&mut range.lower_bound, Bound::Unbounded);
                let upper = std::mem::replace(&mut range.upper_bound, Bound::Unbounded);
                range.lower_bound = quantize_bound(lower);
                range.upper_bound = quantize_bound(upper);
            }
        }
        QueryAst::Bool(bq) => {
            for clause in bq
                .must
                .iter_mut()
                .chain(bq.should.iter_mut())
                .chain(bq.must_not.iter_mut())
                .chain(bq.filter.iter_mut())
            {
                quantize_timestamp_ranges(clause);
            }
        }
        QueryAst::Cache(c) => quantize_timestamp_ranges(&mut c.inner),
        QueryAst::Boost { underlying, .. } => quantize_timestamp_ranges(underlying),
        _ => {}
    }
}

/// Returns true if adding documents without `field` to the result set would not
/// change this aggregation's output. Only checks the top-level agg type —
/// sub-aggregations are shielded by their parent bucket.
fn agg_unaffected_by_missing_field(agg: &AggregationVariants, field: &str) -> bool {
    // If `missing` is set, docs without a value for the field are substituted with it and
    // counted, so adding more such docs *does* change the result — only safe to strip when
    // `missing` is unset.
    match agg {
        AggregationVariants::Terms(t) => t.field == field && t.missing.is_none(),
        AggregationVariants::Range(r) => r.field == field,
        AggregationVariants::Histogram(h) => h.field == field,
        AggregationVariants::DateHistogram(d) => d.field == field,
        AggregationVariants::Average(a) => a.field_name() == field && a.missing.is_none(),
        AggregationVariants::Count(c) => c.field_name() == field && c.missing.is_none(),
        AggregationVariants::Max(m) => m.field_name() == field && m.missing.is_none(),
        AggregationVariants::Min(m) => m.field_name() == field && m.missing.is_none(),
        AggregationVariants::Sum(s) => s.field_name() == field && s.missing.is_none(),
        AggregationVariants::Stats(s) => s.field_name() == field && s.missing.is_none(),
        AggregationVariants::ExtendedStats(e) => e.field_name() == field && e.missing.is_none(),
        AggregationVariants::Percentiles(p) => p.field_name() == field && p.missing.is_none(),
        AggregationVariants::Cardinality(c) => c.field_name() == field && c.missing.is_none(),
        // filter uses a query, not a field — extra docs could change the result
        AggregationVariants::Filter(_) => false,
        // we should check that each source targets the field, unfortunately
        // sources struct does not expose its field. Conservative: never strip.
        AggregationVariants::Composite(_) => false,
        AggregationVariants::MultiTerms(mt) => mt
            .terms
            .iter()
            .any(|t| t.field == field && t.missing.is_none()),
        // top_hits returns docs — extra docs change the result
        AggregationVariants::TopHits(_) => false,
        // multi_terms groups by multiple fields simultaneously; check if all target the field
        AggregationVariants::MultiTerms(mt) => {
            mt.terms.iter().any(|t| t.field == field)
        }
    }
}

/// Returns true if stripping `field_presence(field)` from the query is safe
/// given the aggregations: every top-level agg must be unaffected by documents
/// missing `field`.
fn can_strip_field_presence(aggs: &Aggregations, field: &str) -> bool {
    !aggs.is_empty()
        && aggs
            .values()
            .all(|agg| agg_unaffected_by_missing_field(&agg.agg, field))
}

/// Strips `field_presence` clauses from the query's top-level `must` when they
/// are redundant given the aggregations.
///
/// Stripping widens the query, so `num_hits` in the response may be inflated
/// (includes docs missing the field). Aggregation values remain correct.
fn strip_field_presence(ast: &mut QueryAst, aggs: Option<&Aggregations>) {
    let QueryAst::Bool(bq) = ast else {
        return;
    };
    let Some(aggs) = aggs else {
        return;
    };
    bq.must.retain(|clause| {
        let QueryAst::FieldPresence(fp) = clause else {
            return true;
        };
        !can_strip_field_presence(aggs, &fp.field)
    });
}

/// Normalizes a `SearchRequest` for batching by applying DD-specific query
/// transformations. The output `query_ast` is deterministic (stable serialization
/// through `QueryAst`) and safe to hash for batch grouping.
pub(super) fn normalize_request(req: SearchRequest) -> SearchRequest {
    let Ok(mut ast) = serde_json::from_str::<QueryAst>(&req.query_ast) else {
        return req;
    };
    let aggs: Option<Aggregations> = req
        .aggregation_request
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());

    strip_field_presence(&mut ast, aggs.as_ref());
    quantize_timestamp_ranges(&mut ast);

    let query_ast = serde_json::to_string(&ast).unwrap_or(req.query_ast.clone());
    SearchRequest { query_ast, ..req }
}
