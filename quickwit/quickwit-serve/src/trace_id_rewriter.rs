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

use std::collections::HashSet;
use std::convert::Infallible;

use quickwit_query::query_ast::{BoolQuery, FullTextQuery, QueryAst, QueryAstTransformer, TermQuery};

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

/// Rewrites a `trace_id` equality value into a BoolQuery covering all stored
/// formats, or returns `None` to pass through unchanged. All clauses are on
/// `trace_id` only — `trace_id_low` is never referenced in the query.
///
/// - 32-char valid lowercase hex → 2-way OR: `trace_id = hex` (128-bit stored)
///   and `trace_id = decimal(lower_64)` (64-bit decimal fallback stored).
/// - Decimal or short hex (< 32 chars) → `trace_id = value` (direct) plus
///   `trace_id_low = decimal(value)` which covers both 128-bit hex-stored and
///   64-bit decimal-stored traces. Both decimal and hex interpretations are tried.
/// - 128-bit decimal (33–39 all-digit chars) → converted to 32-char hex, then
///   treated identically to the 32-char hex case (2-way OR).
/// - > 39 chars, > 32 non-decimal chars, or invalid 32-char hex → None (pass through).
pub(crate) fn rewrite_trace_id_value(value: &str) -> Option<QueryAst> {
    let make_term = |val: &str| -> QueryAst {
        TermQuery {
            field: "trace_id".to_string(),
            value: val.to_string(),
        }
        .into()
    };
    let make_trace_id_low = |decimal: &str| -> QueryAst {
        TermQuery {
            field: "trace_id_low".to_string(),
            value: decimal.to_string(),
        }
        .into()
    };
    let should = if value.len() > 32 {
        // 128-bit decimal: all ASCII digits, at most 39 chars (u128::MAX has 39 digits).
        if value.len() <= 39 && value.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(n) = value.parse::<u128>() {
                let hex32 = format!("{n:032x}");
                let lower_decimal = (n as u64).to_string();
                vec![make_term(&hex32), make_term(&lower_decimal)]
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else if value.len() == 32 && value.is_ascii() {
        let upper_valid = u64::from_str_radix(&value[..16], 16).is_ok();
        let lower = u64::from_str_radix(&value[16..], 16);
        if let (true, Ok(lower_bits)) = (upper_valid, lower) {
            vec![make_term(value), make_term(&lower_bits.to_string())]
        } else {
            return None;
        }
    } else {
        // Direct match covers the case where trace_id itself stores this exact value.
        // trace_id_low always holds the lower-64 decimal, covering both 128-bit hex-stored
        // and 64-bit decimal-stored spans. Try both hex and decimal interpretations.
        let mut should = vec![make_term(value)];
        let mut seen = HashSet::new();
        // Try hex interpretation (at most 16 hex chars).
        if value.len() <= 16 && let Ok(n) = u64::from_str_radix(value, 16) {
            let decimal = n.to_string();
            if seen.insert(decimal.clone()) {
                should.push(make_trace_id_low(&decimal));
            }
        }
        // Try decimal interpretation.
        if let Ok(n) = value.parse::<u64>() {
            let decimal = n.to_string();
            if seen.insert(decimal.clone()) {
                should.push(make_trace_id_low(&decimal));
            }
        }
        should
    };
    Some(
        BoolQuery {
            should,
            minimum_should_match: Some(1),
            must: Vec::new(),
            must_not: Vec::new(),
            filter: Vec::new(),
        }
        .into(),
    )
}
