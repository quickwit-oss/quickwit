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

//! Query rewriter that normalises `trace_id` equality expressions so that a
//! user can search with any of the formats Pomsky uses to store trace IDs:
//!
//! - 32-char lowercase hex (128-bit, when `_dd.p.tid` is present)
//! - unsigned decimal string (64-bit fallback)
//! - 16-char lowercase hex of the lower 64 bits
//!
//! Mirrors logs-backend's `TraceIDRewriter`.

use std::convert::Infallible;

use quickwit_query::query_ast::{
    BoolQuery, FullTextQuery, QueryAst, QueryAstTransformer, TermQuery,
};

/// Applies `TraceIdQueryRewriter` to `query_ast` and returns the rewritten AST.
pub(crate) fn apply_trace_id_rewrite(query_ast: QueryAst) -> QueryAst {
    let Ok(Some(rewritten)) = TraceIdQueryRewriter.transform(query_ast) else {
        unreachable!("TraceIdQueryRewriter never returns None or Err at the top level")
    };
    rewritten
}

pub(crate) struct TraceIdQueryRewriter;

impl QueryAstTransformer for TraceIdQueryRewriter {
    type Err = Infallible;

    fn transform_term(&mut self, term_query: TermQuery) -> Result<Option<QueryAst>, Self::Err> {
        if term_query.field != "trace_id" {
            return Ok(Some(term_query.into()));
        }
        Ok(Some(
            rewrite_trace_id_value(&term_query.value).unwrap_or_else(|| term_query.into()),
        ))
    }

    fn transform_full_text(
        &mut self,
        full_text: FullTextQuery,
    ) -> Result<Option<QueryAst>, Self::Err> {
        if full_text.field != "trace_id" {
            return Ok(Some(full_text.into()));
        }
        Ok(Some(
            rewrite_trace_id_value(&full_text.text).unwrap_or_else(|| full_text.into()),
        ))
    }
}

/// Rewrites a `trace_id` equality value into a `BoolQuery` covering all stored
/// formats, or returns `None` to pass through unchanged.
///
/// All cases produce a 2-way OR: a direct `trace_id` match plus a
/// `trace_id_low` exact match (the lower-64 decimal, always populated at
/// ingest regardless of how `trace_id` itself is stored).
///
/// | Input | `trace_id` clause | `trace_id_low` clause |
/// |---|---|---|
/// | 32-char lowercase hex | hex as-is | decimal of lower 64 |
/// | 128-bit decimal (> u64::MAX, ≤ 39 digits) | converted to 32-char hex | decimal of lower 64 |
/// | 64-bit decimal (fits u64) | decimal as-is | same decimal |
/// | ≤ 16-char hex | hex as-is | decimal equivalent |
/// | > 32 chars non-decimal, invalid hex | pass through (returns `None`) | — |
pub(crate) fn rewrite_trace_id_value(value: &str) -> Option<QueryAst> {
    let make_or = |trace_id_val: &str, trace_id_low_val: &str| -> QueryAst {
        BoolQuery {
            should: vec![
                TermQuery {
                    field: "trace_id".to_string(),
                    value: trace_id_val.to_string(),
                }
                .into(),
                TermQuery {
                    field: "trace_id_low".to_string(),
                    value: trace_id_low_val.to_string(),
                }
                .into(),
            ],
            minimum_should_match: Some(1),
            must: Vec::new(),
            must_not: Vec::new(),
            filter: Vec::new(),
        }
        .into()
    };

    // Dispatch all-digit values by numeric range so that 128-bit decimals with
    // ≤ 32 digits (e.g. 18446744073709551616 = 2^64) are handled correctly
    // regardless of their string length.
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        if value.len() > 39 {
            return None; // Too large for u128.
        }
        if let Ok(n) = value.parse::<u64>() {
            // 64-bit decimal: decimal is both the trace_id value (fallback storage)
            // and the trace_id_low value.
            let decimal = n.to_string();
            return Some(make_or(&decimal, &decimal));
        }
        if let Ok(n) = value.parse::<u128>() {
            // 128-bit decimal: convert to the 32-char hex form stored by ingest.
            let hex32 = format!("{n:032x}");
            let lower_decimal = (n as u64).to_string();
            return Some(make_or(&hex32, &lower_decimal));
        }
        return None; // Overflows u128.
    }

    // 32-char lowercase hex: the canonical 128-bit storage format.
    if value.len() == 32 && value.is_ascii() {
        if let (Ok(_), Ok(lower)) = (
            u64::from_str_radix(&value[..16], 16),
            u64::from_str_radix(&value[16..], 16),
        ) {
            return Some(make_or(value, &lower.to_string()));
        }
        return None; // Invalid hex.
    }

    // ≤ 16-char hex: convert to lower-64 decimal for the trace_id_low match.
    if value.len() <= 16
        && let Ok(n) = u64::from_str_radix(value, 16)
    {
        return Some(make_or(value, &n.to_string()));
    }

    None // Unrecognised format; pass through.
}
