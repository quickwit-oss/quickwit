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

//! Lower the parsed KQL AST to a Quickwit `QueryAst` using only existing
//! variants — `BoolQuery`, `FullTextQuery`, `RangeQuery`,
//! `FieldPresenceQuery`, `WildcardQuery`, `MatchAll`, and (for bare
//! default-field values) the deferred-resolution `UserInputQuery` vessel.
//!
//! Keeping the lowering surface confined to existing variants means this
//! whole feature can ship without touching the core `QueryAst` enum or any
//! of its consumers (`QueryAstVisitor`, `tag_pruning`, root-search).

use std::ops::Bound;

use crate::kql::ast::{KqlAst, KqlValue, RangeOp};
use crate::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextMode, FullTextParams, FullTextQuery, QueryAst,
    RangeQuery, UserInputQuery, WildcardQuery,
};
use crate::{BooleanOperand, JsonLiteral, MatchAllOrNone};

/// Lower a parsed `KqlAst` to a `QueryAst`.
///
/// Bare default-field values (e.g. `error`, `"phrase"`) are wrapped in
/// `UserInputQuery` so the search root resolves them against each index's
/// `default_search_fields` at request time. The other clause shapes lower
/// directly to their concrete `QueryAst` variants.
///
/// `default_fields` carries the user-supplied `search_fields` override (the
/// `?search_field=` REST parameter / `fields` JSON DSL key). It is threaded
/// through to the `UserInputQuery` vessels so that, when the caller
/// explicitly chose a field list, that list takes precedence over the
/// docmapper defaults — same convention the Tantivy-grammar path uses.
pub(crate) fn lower_kql_ast(
    ast: KqlAst,
    default_fields: Option<&[String]>,
    lenient: bool,
) -> anyhow::Result<QueryAst> {
    match ast {
        KqlAst::And(children) => {
            let must = lower_children(children, default_fields, lenient)?;
            Ok(BoolQuery {
                must,
                ..Default::default()
            }
            .into())
        }
        KqlAst::Or(children) => {
            let should = lower_children(children, default_fields, lenient)?;
            Ok(BoolQuery {
                should,
                ..Default::default()
            }
            .into())
        }
        KqlAst::Not(inner) => {
            let inner_ast = lower_kql_ast(*inner, default_fields, lenient)?;
            Ok(BoolQuery {
                must_not: vec![inner_ast],
                ..Default::default()
            }
            .into())
        }
        KqlAst::FieldValue { field, value } => Ok(lower_field_value(field, value, lenient)),
        KqlAst::FieldRange { field, op, value } => Ok(lower_field_range(field, op, value)),
        KqlAst::FieldExists { field } => Ok(FieldPresenceQuery { field }.into()),
        KqlAst::DefaultValue(value) => Ok(lower_default_value(value, default_fields, lenient)),
    }
}

fn lower_children(
    children: Vec<KqlAst>,
    default_fields: Option<&[String]>,
    lenient: bool,
) -> anyhow::Result<Vec<QueryAst>> {
    children
        .into_iter()
        .map(|child| lower_kql_ast(child, default_fields, lenient))
        .collect()
}

fn lower_field_value(field: String, value: KqlValue, lenient: bool) -> QueryAst {
    match value {
        KqlValue::Literal(text) => {
            if contains_unescaped_wildcard(&text) {
                WildcardQuery {
                    field,
                    value: text,
                    lenient,
                    case_insensitive: false,
                }
                .into()
            } else {
                full_text_query(field, text, intersection_mode(), lenient)
            }
        }
        KqlValue::Phrase(text) => full_text_query(field, text, phrase_mode(), lenient),
    }
}

/// Lower a bare value (no `field:` qualifier) by wrapping it in a
/// `UserInputQuery`, which the search root resolves against each index's
/// `default_search_fields`.
///
/// The Tantivy-grammar parser handles bare terms, wildcards, and quoted
/// phrases identically to KQL for the single-token forms produced by the
/// KQL lexer, so reusing the existing vessel is correct here.
fn lower_default_value(
    value: KqlValue,
    default_fields: Option<&[String]>,
    lenient: bool,
) -> QueryAst {
    // Fast path: bare `*` short-circuits to MatchAll — cheaper than running
    // a wildcard automaton, and works even when no default fields are
    // configured.
    if let KqlValue::Literal(text) = &value
        && text == "*"
    {
        return QueryAst::MatchAll;
    }
    let user_text = render_value_for_tantivy_grammar(&value);
    UserInputQuery {
        user_text,
        default_fields: default_fields.map(|fs| fs.to_vec()),
        default_operator: BooleanOperand::And,
        lenient,
    }
    .into()
}

/// Encode a KQL bare value as a Tantivy-grammar string suitable for
/// `UserInputQuery::user_text`. Any character with special meaning in the
/// Tantivy grammar is backslash-escaped so a value the user intended as a
/// literal (e.g. `+error`) is not reinterpreted as a Tantivy operator.
fn render_value_for_tantivy_grammar(value: &KqlValue) -> String {
    match value {
        KqlValue::Literal(text) => escape_tantivy_literal(text),
        KqlValue::Phrase(text) => format!("\"{}\"", escape_phrase_contents(text)),
    }
}

fn escape_tantivy_literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        // `*` and `?` deliberately pass through unescaped — both KQL and the
        // Tantivy grammar interpret them as wildcards with the same
        // semantics, so re-escaping would suppress the wildcard.
        if matches!(
            ch,
            '+' | '-'
                | '!'
                | '('
                | ')'
                | '{'
                | '}'
                | '['
                | ']'
                | '^'
                | '"'
                | '~'
                | ':'
                | '\\'
                | '/'
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn escape_phrase_contents(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(ch, '\\' | '"') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn lower_field_range(field: String, op: RangeOp, value: String) -> QueryAst {
    let literal = parse_range_literal(value);
    let (lower_bound, upper_bound) = match op {
        RangeOp::Gt => (Bound::Excluded(literal), Bound::Unbounded),
        RangeOp::Gte => (Bound::Included(literal), Bound::Unbounded),
        RangeOp::Lt => (Bound::Unbounded, Bound::Excluded(literal)),
        RangeOp::Lte => (Bound::Unbounded, Bound::Included(literal)),
    };
    RangeQuery {
        field,
        lower_bound,
        upper_bound,
    }
    .into()
}

/// Numeric bounds emit `JsonLiteral::Number` so the RangeQuery layer can
/// coerce against numeric columns directly; non-numeric inputs (ISO
/// timestamps, IPs) fall back to `JsonLiteral::String`.
fn parse_range_literal(value: String) -> JsonLiteral {
    if let Ok(number) = value.parse::<serde_json::Number>() {
        return JsonLiteral::Number(number);
    }
    JsonLiteral::String(value)
}

fn full_text_query(field: String, text: String, mode: FullTextMode, lenient: bool) -> QueryAst {
    FullTextQuery {
        field,
        text,
        params: FullTextParams {
            tokenizer: None,
            mode,
            zero_terms_query: MatchAllOrNone::MatchNone,
        },
        lenient,
    }
    .into()
}

fn intersection_mode() -> FullTextMode {
    FullTextMode::PhraseFallbackToIntersection
}

fn phrase_mode() -> FullTextMode {
    FullTextMode::Phrase { slop: 0 }
}

fn contains_unescaped_wildcard(text: &str) -> bool {
    text.chars().any(|c| c == '*' || c == '?')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kql::parse_kql;

    fn lower_ok(input: &str, default_fields: Option<&[&str]>) -> QueryAst {
        let owned: Option<Vec<String>> =
            default_fields.map(|fs| fs.iter().map(|s| s.to_string()).collect());
        let ast = parse_kql(input).expect("parse");
        lower_kql_ast(ast, owned.as_deref(), false).expect("lower")
    }

    #[test]
    fn test_lower_field_value_eager() {
        let ast = lower_ok("level:error", None);
        let QueryAst::FullText(q) = ast else { panic!() };
        assert_eq!(q.field, "level");
        assert_eq!(q.text, "error");
    }

    #[test]
    fn test_lower_phrase_uses_phrase_mode() {
        let ast = lower_ok(r#"msg:"connection refused""#, None);
        let QueryAst::FullText(q) = ast else { panic!() };
        assert_eq!(q.params.mode, FullTextMode::Phrase { slop: 0 });
    }

    #[test]
    fn test_lower_wildcard_value() {
        let ast = lower_ok("level:err*", None);
        let QueryAst::Wildcard(q) = ast else { panic!() };
        assert_eq!(q.value, "err*");
    }

    #[test]
    fn test_lower_exists() {
        let ast = lower_ok("level:*", None);
        let QueryAst::FieldPresence(q) = ast else {
            panic!()
        };
        assert_eq!(q.field, "level");
    }

    #[test]
    fn test_lower_range_numeric() {
        let ast = lower_ok("size:>=10", None);
        let QueryAst::Range(q) = ast else { panic!() };
        let Bound::Included(JsonLiteral::Number(number)) = &q.lower_bound else {
            panic!()
        };
        assert_eq!(number.as_i64(), Some(10));
    }

    #[test]
    fn test_lower_range_lt_strict_excludes_upper_bound() {
        let ast = lower_ok("size:<10", None);
        let QueryAst::Range(q) = ast else { panic!() };
        assert_eq!(q.lower_bound, Bound::Unbounded);
        let Bound::Excluded(JsonLiteral::Number(number)) = &q.upper_bound else {
            panic!("expected Excluded numeric upper bound")
        };
        assert_eq!(number.as_i64(), Some(10));
    }

    #[test]
    fn test_lower_range_gt_strict_excludes_lower_bound() {
        let ast = lower_ok("size:>10", None);
        let QueryAst::Range(q) = ast else { panic!() };
        let Bound::Excluded(JsonLiteral::Number(number)) = &q.lower_bound else {
            panic!("expected Excluded numeric lower bound")
        };
        assert_eq!(number.as_i64(), Some(10));
        assert_eq!(q.upper_bound, Bound::Unbounded);
    }

    #[test]
    fn test_lower_range_lte_includes_upper_bound() {
        let ast = lower_ok("size:<=10", None);
        let QueryAst::Range(q) = ast else { panic!() };
        assert_eq!(q.lower_bound, Bound::Unbounded);
        let Bound::Included(JsonLiteral::Number(number)) = &q.upper_bound else {
            panic!("expected Included numeric upper bound")
        };
        assert_eq!(number.as_i64(), Some(10));
    }

    #[test]
    fn test_lower_range_string_fallback() {
        let ast = lower_ok(r#"ts:>="2025-01-01T00:00:00Z""#, None);
        let QueryAst::Range(q) = ast else { panic!() };
        assert_eq!(
            q.lower_bound,
            Bound::Included(JsonLiteral::String("2025-01-01T00:00:00Z".into()))
        );
    }

    #[test]
    fn test_lower_bare_value_becomes_user_input_query() {
        // Bare default-field values defer resolution via UserInputQuery so
        // the root can fan them out against each index's default_search_fields.
        let ast = lower_ok("error", None);
        let QueryAst::UserInput(uiq) = ast else {
            panic!("expected UserInput vessel, got {ast:?}")
        };
        assert_eq!(uiq.user_text, "error");
        assert_eq!(uiq.default_fields, None);
    }

    #[test]
    fn test_lower_bare_value_with_explicit_fields_threads_them_through() {
        let ast = lower_ok("error", Some(&["body", "summary"]));
        let QueryAst::UserInput(uiq) = ast else {
            panic!()
        };
        assert_eq!(
            uiq.default_fields,
            Some(vec!["body".to_string(), "summary".to_string()])
        );
    }

    #[test]
    fn test_lower_bare_phrase_renders_quoted_user_text() {
        let ast = lower_ok(r#""job started""#, None);
        let QueryAst::UserInput(uiq) = ast else {
            panic!()
        };
        assert_eq!(uiq.user_text, r#""job started""#);
    }

    #[test]
    fn test_lower_bare_star_uses_match_all_fast_path() {
        assert_eq!(lower_ok("*", None), QueryAst::MatchAll);
    }

    #[test]
    fn test_lower_bare_value_escapes_tantivy_specials() {
        // `+error` in KQL is a literal value, but `+` is a Tantivy-grammar
        // operator. The encoder must escape it so the deferred parse at
        // root treats the value literally.
        let ast = parse_kql(r"\+error").unwrap();
        let lowered = lower_kql_ast(ast, None, false).unwrap();
        let QueryAst::UserInput(uiq) = lowered else {
            panic!()
        };
        assert_eq!(uiq.user_text, r"\+error");
    }

    #[test]
    fn test_lower_bare_phrase_escapes_inner_quote() {
        // A KQL phrase containing an inner `"` (escaped as `\"` in input)
        // becomes the unescaped text `she said "hi"` in the AST. Rendering
        // for Tantivy must re-escape the inner quote.
        let ast = parse_kql(r#""she said \"hi\"""#).unwrap();
        let lowered = lower_kql_ast(ast, None, false).unwrap();
        let QueryAst::UserInput(uiq) = lowered else {
            panic!()
        };
        assert_eq!(uiq.user_text, r#""she said \"hi\"""#);
    }

    #[test]
    fn test_lower_and_or_compose() {
        let ast = lower_ok("level:error and service:api", None);
        let QueryAst::Bool(q) = ast else { panic!() };
        assert_eq!(q.must.len(), 2);

        let ast = lower_ok("level:error or level:warn", None);
        let QueryAst::Bool(q) = ast else { panic!() };
        assert_eq!(q.should.len(), 2);
    }

    #[test]
    fn test_lower_not() {
        let ast = lower_ok("not level:error", None);
        let QueryAst::Bool(q) = ast else { panic!() };
        assert_eq!(q.must_not.len(), 1);
    }

    #[test]
    fn test_lower_value_group_distributes() {
        let direct = lower_ok("level:error or level:warn", None);
        let grouped = lower_ok("level:(error or warn)", None);
        assert_eq!(direct, grouped);
    }

    #[test]
    fn test_lower_lenient_propagates_to_full_text() {
        let ast = parse_kql("level:error").unwrap();
        let lowered = lower_kql_ast(ast, None, true).unwrap();
        let QueryAst::FullText(q) = lowered else {
            panic!()
        };
        assert!(q.lenient);
    }

    #[test]
    fn test_lower_lenient_propagates_to_wildcard() {
        let ast = parse_kql("level:err*").unwrap();
        let lowered = lower_kql_ast(ast, None, true).unwrap();
        let QueryAst::Wildcard(q) = lowered else {
            panic!()
        };
        assert!(q.lenient);
    }
}
