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

use crate::query_ast::{BoolQuery, FullTextQuery, QueryAst, QueryAstTransformer, TermQuery};

/// Applies `TraceIdQueryRewriter` to `query_ast` and returns the rewritten AST.
pub fn apply_trace_id_rewrite(query_ast: QueryAst) -> QueryAst {
    let Ok(Some(rewritten)) = TraceIdQueryRewriter.transform(query_ast) else {
        unreachable!("TraceIdQueryRewriter never returns None or Err at the top level")
    };
    rewritten
}

pub struct TraceIdQueryRewriter;

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
/// Logs now store 128-bit trace IDs as 32-char hex (matching spans), so the
/// primary clause is always `trace_id = hex`. Backward-compat clauses cover
/// logs ingested before that change (stored as raw decimal strings) and spans
/// (via `trace_id_low`).
///
/// | Input | Clauses produced |
/// |---|---|
/// | 32-char hex | `trace_id=hex`, `trace_id=decimal(lower_64)` (old logs), `trace_id_low=decimal` (spans) |
/// | 128-bit decimal | `trace_id=hex32`, `trace_id=original_decimal` (old logs), `trace_id_low=lower_decimal` (spans) |
/// | 64-bit decimal | `trace_id=decimal`, `trace_id_low=decimal` |
/// | ≤ 16-char hex | `trace_id=hex`, `trace_id=decimal(n)` (old logs), `trace_id_low=decimal(n)` (spans) |
/// | > 32 non-decimal, invalid hex | pass through (`None`) |
pub fn rewrite_trace_id_value(value: &str) -> Option<QueryAst> {
    let term = |field: &str, val: &str| -> QueryAst {
        TermQuery {
            field: field.to_string(),
            value: val.to_string(),
        }
        .into()
    };
    let bool_should = |should: Vec<QueryAst>| -> QueryAst {
        BoolQuery {
            should,
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
            // 64-bit decimal: no conversion needed; covers both decimal-stored
            // logs and 64-bit fallback spans.
            let decimal = n.to_string();
            return Some(bool_should(vec![
                term("trace_id", &decimal),
                term("trace_id_low", &decimal),
            ]));
        }
        if let Ok(n) = value.parse::<u128>() {
            // 128-bit decimal: ingest now normalises these to 32-char hex, but
            // old logs stored the raw decimal string — include both forms.
            let hex32 = format!("{n:032x}");
            let lower_decimal = (n as u64).to_string();
            return Some(bool_should(vec![
                term("trace_id", &hex32),             // new logs (hex) + spans
                term("trace_id", value),              // old logs (raw 128-bit decimal)
                term("trace_id_low", &lower_decimal), // spans
            ]));
        }
        return None; // Overflows u128.
    }

    // 32-char lowercase hex: the canonical 128-bit storage format.
    if value.len() == 32 && value.is_ascii() {
        if let (Ok(_), Ok(lower)) = (
            u64::from_str_radix(&value[..16], 16),
            u64::from_str_radix(&value[16..], 16),
        ) {
            let lower_decimal = lower.to_string();
            return Some(bool_should(vec![
                term("trace_id", value),              // new logs (hex) + spans
                term("trace_id", &lower_decimal),     // old logs (64-bit decimal)
                term("trace_id_low", &lower_decimal), // spans
            ]));
        }
        return None; // Invalid hex.
    }

    // ≤ 16-char hex: convert to decimal for trace_id_low (spans) and old logs.
    if value.len() <= 16
        && let Ok(n) = u64::from_str_radix(value, 16)
    {
        let decimal = n.to_string();
        return Some(bool_should(vec![
            term("trace_id", value),        // direct hex match (future-proof)
            term("trace_id", &decimal),     // old logs (64-bit decimal)
            term("trace_id_low", &decimal), // spans
        ]));
    }

    None // Unrecognised format; pass through.
}
