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

//! Conformance corpus drawn from the public KQL grammar that Kibana
//! documents at <https://www.elastic.co/docs/explore-analyze/query-filter/languages/kql>.
//!
//! Each entry pins the `KqlAst` we produce for a documented KQL idiom and
//! pairs it with an explicit note when our behavior intentionally diverges
//! from Kibana (e.g. unsupported nested-field syntax, range-value quoting).
//!
//! When KQL features that Kibana supports diverge, the
//! `KibanaCase::deviation` field carries a short note explaining the gap.
//! A test fails if the corpus produces an unexpected outcome — the goal is
//! to make divergence loud and visible rather than silent.

#![cfg(test)]

use crate::kql::ast::{KqlAst, KqlValue, RangeOp};
use crate::kql::parse_kql;

/// One entry in the conformance corpus.
///
/// `outcome = Ok(ast)` pins an exact expected AST. `outcome = Err(reason)`
/// asserts the parser rejects the input — useful for documenting features
/// Kibana supports that we deliberately don't.
struct KibanaCase {
    /// Short test name used in panic messages.
    name: &'static str,
    /// KQL string drawn from the Kibana grammar reference.
    input: &'static str,
    /// Expected outcome — either the AST we produce, or a deliberate rejection.
    outcome: Outcome,
    /// Non-empty when our behavior intentionally diverges from Kibana; the
    /// note is printed in the panic message and helps future readers
    /// understand why the corpus entry looks the way it does.
    deviation: Option<&'static str>,
}

enum Outcome {
    /// Parse must succeed and produce this AST.
    Parses(KqlAst),
    /// Parse must reject with an error containing this substring.
    Rejects { contains: &'static str },
}

fn fv(field: &str, value: &str) -> KqlAst {
    KqlAst::FieldValue {
        field: field.into(),
        value: KqlValue::Literal(value.into()),
    }
}

fn fp(field: &str, phrase: &str) -> KqlAst {
    KqlAst::FieldValue {
        field: field.into(),
        value: KqlValue::Phrase(phrase.into()),
    }
}

/// The corpus itself.
///
/// Inputs map to documented Kibana KQL behavior where supported; otherwise
/// the `deviation` field calls out the gap.
fn corpus() -> Vec<KibanaCase> {
    vec![
        // === Field-value matching ==========================================
        KibanaCase {
            name: "field_value_unquoted",
            input: "response:200",
            outcome: Outcome::Parses(fv("response", "200")),
            deviation: None,
        },
        KibanaCase {
            name: "field_value_quoted_phrase",
            input: r#"message:"Quick brown fox""#,
            outcome: Outcome::Parses(fp("message", "Quick brown fox")),
            deviation: None,
        },
        KibanaCase {
            name: "bare_default_term",
            input: "fox",
            outcome: Outcome::Parses(KqlAst::DefaultValue(KqlValue::Literal("fox".into()))),
            deviation: None,
        },
        KibanaCase {
            name: "bare_default_phrase",
            input: r#""Quick brown fox""#,
            outcome: Outcome::Parses(KqlAst::DefaultValue(KqlValue::Phrase(
                "Quick brown fox".into(),
            ))),
            deviation: None,
        },
        // === Boolean composition ===========================================
        KibanaCase {
            name: "and_lowercase",
            input: "response:200 and extension:php",
            outcome: Outcome::Parses(KqlAst::And(vec![
                fv("response", "200"),
                fv("extension", "php"),
            ])),
            deviation: None,
        },
        KibanaCase {
            name: "and_uppercase_accepted",
            input: "response:200 AND extension:php",
            outcome: Outcome::Parses(KqlAst::And(vec![
                fv("response", "200"),
                fv("extension", "php"),
            ])),
            deviation: Some(
                "Kibana accepts case-insensitive keywords; we match — `AND`/`OR`/`NOT` work too",
            ),
        },
        KibanaCase {
            name: "or_alternative",
            input: "response:200 or response:201",
            outcome: Outcome::Parses(KqlAst::Or(vec![
                fv("response", "200"),
                fv("response", "201"),
            ])),
            deviation: None,
        },
        KibanaCase {
            name: "not_exclusion",
            input: "not response:200",
            outcome: Outcome::Parses(KqlAst::Not(Box::new(fv("response", "200")))),
            deviation: None,
        },
        KibanaCase {
            name: "implicit_and_via_juxtaposition",
            input: "response:200 extension:php",
            outcome: Outcome::Parses(KqlAst::And(vec![
                fv("response", "200"),
                fv("extension", "php"),
            ])),
            deviation: None,
        },
        // === Precedence + grouping =========================================
        KibanaCase {
            name: "or_binds_looser_than_and",
            input: "a:1 or b:2 and c:3",
            outcome: Outcome::Parses(KqlAst::Or(vec![
                fv("a", "1"),
                KqlAst::And(vec![fv("b", "2"), fv("c", "3")]),
            ])),
            deviation: None,
        },
        KibanaCase {
            name: "parens_override_precedence",
            input: "(a:1 or b:2) and c:3",
            outcome: Outcome::Parses(KqlAst::And(vec![
                KqlAst::Or(vec![fv("a", "1"), fv("b", "2")]),
                fv("c", "3"),
            ])),
            deviation: None,
        },
        // === Value groups ==================================================
        KibanaCase {
            name: "value_group_distributes_or",
            input: "response:(200 or 201)",
            outcome: Outcome::Parses(KqlAst::Or(vec![
                fv("response", "200"),
                fv("response", "201"),
            ])),
            deviation: None,
        },
        KibanaCase {
            name: "value_group_distributes_and",
            input: "tags:(prod and critical)",
            outcome: Outcome::Parses(KqlAst::And(vec![
                fv("tags", "prod"),
                fv("tags", "critical"),
            ])),
            deviation: Some(
                "Kibana documents `field:(a and b)` to mean `field=a` AND `field=b` on the same \
                 document — we lower to the same shape",
            ),
        },
        // === Wildcards =====================================================
        KibanaCase {
            name: "wildcard_in_value",
            input: "machine.os:win*",
            outcome: Outcome::Parses(fv("machine.os", "win*")),
            deviation: None,
        },
        KibanaCase {
            name: "field_exists_via_star",
            input: "response:*",
            outcome: Outcome::Parses(KqlAst::FieldExists {
                field: "response".into(),
            }),
            deviation: None,
        },
        // === Ranges ========================================================
        KibanaCase {
            name: "range_gte_int",
            input: "bytes:>=1000",
            outcome: Outcome::Parses(KqlAst::FieldRange {
                field: "bytes".into(),
                op: RangeOp::Gte,
                value: "1000".into(),
            }),
            deviation: None,
        },
        KibanaCase {
            name: "range_lt_int",
            input: "bytes:<1000",
            outcome: Outcome::Parses(KqlAst::FieldRange {
                field: "bytes".into(),
                op: RangeOp::Lt,
                value: "1000".into(),
            }),
            deviation: None,
        },
        KibanaCase {
            name: "range_compound_via_and",
            input: "bytes:>=200 and bytes:<500",
            outcome: Outcome::Parses(KqlAst::And(vec![
                KqlAst::FieldRange {
                    field: "bytes".into(),
                    op: RangeOp::Gte,
                    value: "200".into(),
                },
                KqlAst::FieldRange {
                    field: "bytes".into(),
                    op: RangeOp::Lt,
                    value: "500".into(),
                },
            ])),
            deviation: None,
        },
        KibanaCase {
            name: "range_value_requires_quoting_for_iso_timestamp",
            input: r#"@timestamp:<"2025-01-01T00:00:00Z""#,
            outcome: Outcome::Parses(KqlAst::FieldRange {
                field: "@timestamp".into(),
                op: RangeOp::Lt,
                value: "2025-01-01T00:00:00Z".into(),
            }),
            deviation: Some(
                "Kibana accepts unquoted ISO timestamps in range values; we require quotes \
                 because our lexer tokenizes on `:`. Documented constraint.",
            ),
        },
        // === Escape semantics ==============================================
        KibanaCase {
            name: "escaped_colon_in_field_name",
            input: r"foo\:bar:value",
            outcome: Outcome::Parses(KqlAst::FieldValue {
                field: "foo:bar".into(),
                value: KqlValue::Literal("value".into()),
            }),
            deviation: None,
        },
        KibanaCase {
            name: "escaped_keyword_becomes_literal",
            input: r"\and:value",
            outcome: Outcome::Parses(KqlAst::FieldValue {
                field: "and".into(),
                value: KqlValue::Literal("value".into()),
            }),
            deviation: Some(
                "Kibana treats `\\and` similarly — backslash escape removes keyword semantics so \
                 a field literally named `and` is reachable.",
            ),
        },
        // === Deliberately-divergent rejections =============================
        KibanaCase {
            name: "nested_field_qualifier_rejected",
            input: "level:(severity:high)",
            outcome: Outcome::Rejects {
                contains: "nested field qualifier",
            },
            deviation: Some(
                "Kibana errors on this syntax. We also reject — silently rebinding the inner \
                 qualifier would be a wrong-data footgun.",
            ),
        },
        KibanaCase {
            name: "dangling_colon_rejected",
            input: "level:",
            outcome: Outcome::Rejects { contains: "value" },
            deviation: None,
        },
        KibanaCase {
            name: "unbalanced_paren_rejected",
            input: "(level:error",
            outcome: Outcome::Rejects {
                contains: "expected",
            },
            deviation: None,
        },
        KibanaCase {
            name: "empty_input_rejected",
            input: "",
            outcome: Outcome::Rejects { contains: "empty" },
            deviation: None,
        },
        // === Documented Kibana features we don't support ===================
        KibanaCase {
            name: "nested_field_object_syntax_not_supported",
            input: r#"items:{ name:foo and value:42 }"#,
            outcome: Outcome::Rejects {
                contains: "nested-field syntax",
            },
            deviation: Some(
                "Kibana supports `nested_field:{ ... }` for Elasticsearch nested types. Quickwit \
                 has no nested-field type, so the lexer rejects `{`/`}` with a clear error \
                 pointing users to flat dotted paths.",
            ),
        },
    ]
}

#[test]
fn kibana_corpus_matches_documented_grammar() {
    let mut failures: Vec<String> = Vec::new();
    for case in corpus() {
        let parsed = parse_kql(case.input);
        match (&case.outcome, parsed) {
            (Outcome::Parses(expected), Ok(actual)) if &actual == expected => {
                // pass
            }
            (Outcome::Parses(expected), Ok(actual)) => {
                failures.push(format!(
                        "[{name}] input {input:?} produced unexpected AST\n  expected: \
                         {expected:?}\n  actual:   {actual:?}\n  deviation note: {deviation:?}",
                        name = case.name,
                        input = case.input,
                        deviation = case.deviation,
                    ));
            }
            (Outcome::Parses(_), Err(err)) => {
                failures.push(format!(
                    "[{name}] input {input:?} was expected to parse but errored: {err}\n  \
                     deviation note: {deviation:?}",
                    name = case.name,
                    input = case.input,
                    deviation = case.deviation,
                ));
            }
            (Outcome::Rejects { contains }, Err(err)) => {
                if !err.to_string().contains(contains) {
                    failures.push(format!(
                        "[{name}] input {input:?} was rejected as expected but error did not \
                         mention {contains:?}\n  actual error: {err}",
                        name = case.name,
                        input = case.input,
                    ));
                }
            }
            (Outcome::Rejects { contains }, Ok(actual)) => {
                failures.push(format!(
                    "[{name}] input {input:?} was expected to be rejected (containing \
                     {contains:?}) but parsed to {actual:?}\n  deviation note: {deviation:?}",
                    name = case.name,
                    input = case.input,
                    deviation = case.deviation,
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} Kibana-corpus case(s) drifted:\n\n{}",
        failures.len(),
        failures.join("\n\n"),
    );
}
