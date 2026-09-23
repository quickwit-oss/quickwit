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

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tantivy::jitexpr::ast::{Function, Literal, UntypedExpr};
use tantivy::query::doc_predicate_query::{DocPredicateQuery, JitExprPredicate};
use tantivy::schema::{FieldType, Schema as TantivySchema};

use super::{BuildTantivyAst, BuildTantivyAstContext, QueryAst, RegexQuery, TantivyQueryAst};
use crate::tokenizers::RAW_TOKENIZER_NAME;
use crate::{InvalidQuery, find_field_or_hit_dynamic};

/// A boolean predicate expressed as a calculated-field expression.
///
/// The expression is serialized as a jitexpr string, for example `(GT duration 1i64)`.
/// Variable names refer to fast fields, resolved by Tantivy separately for each segment.
/// Query construction validates boolean result types; column binding and JIT compilation
/// happen when Tantivy creates the segment scorer. Callers must warm the referenced fast
/// fields before running a synchronous search against remote storage.
///
/// A narrow subset of predicates of the form
/// `(EQ (REGEXP_EXTRACT field "^prefix(capture)suffix$" 1u64) "literal")` may be rewritten
/// to a [`RegexQuery`] when `field` is a fast, indexed, raw-tokenized string field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CalcFieldQuery {
    #[serde(with = "jitexpr_serde")]
    pub expression: UntypedExpr,
}

impl From<CalcFieldQuery> for QueryAst {
    fn from(calc_field_query: CalcFieldQuery) -> Self {
        QueryAst::CalcField(calc_field_query)
    }
}

impl CalcFieldQuery {
    /// Attempts to lower `(EQ (REGEXP_EXTRACT field pattern 1u64) "literal")` into a
    /// [`RegexQuery`].
    ///
    /// Returns `None` whenever the expression shape or field is not eligible, so callers
    /// can keep the calculated-predicate path.
    pub fn try_optimize_to_regex_query(&self, schema: &TantivySchema) -> Option<RegexQuery> {
        let (field_name, pattern, literal) = match_eq_regexp_extract(&self.expression)?;
        if !is_fast_indexed_raw_string_field(field_name, schema) {
            return None;
        }
        let regex = substitute_single_capture(pattern, literal)?;
        Some(RegexQuery {
            field: field_name.to_string(),
            regex,
        })
    }
}

impl BuildTantivyAst for CalcFieldQuery {
    fn build_tantivy_ast_impl(
        &self,
        _context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        // Eligible REGEXP_EXTRACT equality predicates are rewritten to RegexQuery earlier
        // in query_builder so warmup visitors see the optimized AST.
        let predicate = JitExprPredicate::new(self.expression.clone())
            .context("invalid calculated predicate expression")
            .map_err(InvalidQuery::Other)?;
        let doc_predicate_query = DocPredicateQuery::from(predicate);
        Ok(TantivyQueryAst::from(doc_predicate_query))
    }
}

/// Matches `(EQ (REGEXP_EXTRACT field pattern 1u64) "literal")`.
fn match_eq_regexp_extract(expression: &UntypedExpr) -> Option<(&str, &str, &str)> {
    let UntypedExpr::FnCall {
        function: Function::Eq,
        args,
    } = expression
    else {
        return None;
    };
    let [extract, UntypedExpr::Literal(Literal::String(literal))] = args.as_slice() else {
        return None;
    };
    let UntypedExpr::FnCall {
        function: Function::RegexpExtract,
        args: extract_args,
    } = extract
    else {
        return None;
    };
    let [
        UntypedExpr::Variable(field_name),
        UntypedExpr::Literal(Literal::String(pattern)),
        UntypedExpr::Literal(Literal::U64(1)),
    ] = extract_args.as_slice()
    else {
        return None;
    };
    Some((field_name.as_ref(), pattern.as_ref(), literal.as_ref()))
}

fn is_fast_indexed_raw_string_field(field_name: &str, schema: &TantivySchema) -> bool {
    let Some((_field, field_entry, json_path)) = find_field_or_hit_dynamic(field_name, schema)
    else {
        return false;
    };
    // Narrow scope: plain string fields only, not JSON subpaths.
    if !json_path.is_empty() {
        return false;
    }
    if !field_entry.is_fast() || !field_entry.is_indexed() {
        return false;
    }
    let FieldType::Str(text_options) = field_entry.field_type() else {
        return false;
    };
    let Some(text_indexing) = text_options.get_indexing_options() else {
        return false;
    };
    text_indexing.tokenizer() == RAW_TOKENIZER_NAME
}

/// Rewrites `^prefix(capture)suffix$` by replacing the single `(...)` group with the
/// escaped literal. Rejects special groups, nested/extra parentheses, and unanchored
/// patterns. Also requires the equality literal to match the capture subpattern, otherwise
/// `REGEXP_EXTRACT` can never equal that literal and rewriting would over-match.
/// Tantivy FST regexes already match whole dictionary terms, so anchors are stripped.
fn substitute_single_capture(pattern: &str, literal: &str) -> Option<String> {
    let pattern = pattern.strip_prefix('^')?.strip_suffix('$')?;
    let open = pattern.find('(')?;
    let close = pattern[open + 1..].find(')')? + open + 1;
    // Reject `(?:...)` / other special groups, nested parentheses, and a second group.
    if pattern.as_bytes().get(open + 1) == Some(&b'?')
        || pattern[open + 1..close].contains(['(', ')'])
        || pattern[close + 1..].contains('(')
    {
        return None;
    }
    let capture = &pattern[open + 1..close];
    // `EQ(REGEXP_EXTRACT(...), L)` also requires L to match the capture pattern.
    let capture_re = regex::Regex::new(&format!("^(?:{capture})$")).ok()?;
    if !capture_re.is_match(literal) {
        return None;
    }
    Some(format!(
        "{}{}{}",
        &pattern[..open],
        regex::escape(literal),
        &pattern[close + 1..]
    ))
}

mod jitexpr_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use tantivy::jitexpr::ast::UntypedExpr;

    pub fn serialize<S>(expression: &UntypedExpr, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&tantivy::jitexpr::ast::serialize(expression))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<UntypedExpr, D::Error>
    where D: Deserializer<'de> {
        let expression = String::deserialize(deserializer)?;
        tantivy::jitexpr::ast::deserialize(&expression).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tantivy::collector::Count;
    use tantivy::jitexpr::ast::deserialize;
    use tantivy::query::doc_predicate_query::DocPredicateQuery;
    use tantivy::schema::{FAST, STRING, Schema, TEXT};
    use tantivy::{Index, TantivyDocument, doc};

    use super::{CalcFieldQuery, substitute_single_capture};
    use crate::query_ast::{BuildTantivyAstContext, QueryAst};

    fn calc_field(expression: &str) -> QueryAst {
        CalcFieldQuery {
            expression: deserialize(expression).unwrap(),
        }
        .into()
    }

    fn calc_field_query(expression: &str) -> CalcFieldQuery {
        CalcFieldQuery {
            expression: deserialize(expression).unwrap(),
        }
    }

    #[test]
    fn test_calc_field_serde_roundtrip() {
        let query = calc_field("(GT custom.duration 1i64)");
        let serialized = json!({
            "type": "calc_field",
            "expression": "(GT custom.duration 1i64)"
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), serialized);
        assert_eq!(
            serde_json::from_value::<QueryAst>(serialized).unwrap(),
            query
        );
        assert!(format!("{query:?}").contains("(GT custom.duration 1i64)"));
    }

    #[test]
    fn test_calc_field_rejects_malformed_serialization() {
        for expression in [")", "(GT duration", "(UNKNOWN duration)"] {
            let serialized = json!({"type": "calc_field", "expression": expression});
            assert!(
                serde_json::from_value::<QueryAst>(serialized).is_err(),
                "{expression}"
            );
        }
        for serialized in [
            json!({"type": "calc_field"}),
            json!({"type": "calc_field", "expression": 42}),
            json!({"type": "calc_field", "expression": {}}),
        ] {
            assert!(serde_json::from_value::<QueryAst>(serialized).is_err());
        }
    }

    fn test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let duration = schema_builder.add_i64_field("duration", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        writer
            .add_document(doc!(duration => 1i64, label => "keep"))
            .unwrap();
        writer
            .add_document(doc!(duration => 3i64, label => "drop"))
            .unwrap();
        writer
            .add_document(doc!(duration => 7i64, label => "keep"))
            .unwrap();
        writer.add_document(doc!(label => "keep")).unwrap();
        writer.commit().unwrap();
        index
    }

    #[test]
    fn test_calc_field_search() {
        let index = test_index();
        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        for (expression, expected_count) in [
            ("true", 4),
            ("false", 0),
            ("(GT duration 2i64)", 2),
            ("(EQ label \"keep\")", 3),
            ("(GT absent 2i64)", 0),
        ] {
            let query = calc_field(expression)
                .build_tantivy_query(&context)
                .unwrap();
            assert!(query.downcast_ref::<DocPredicateQuery>().is_some());
            assert_eq!(
                searcher.search(&*query, &Count).unwrap(),
                expected_count,
                "{expression}"
            );
        }
    }

    #[test]
    fn test_substitute_single_capture() {
        assert_eq!(
            substitute_single_capture("^svc-([a-z]+)-prod$", "api").as_deref(),
            Some("svc-api-prod")
        );
        assert_eq!(
            substitute_single_capture("^svc-(a.b)-prod$", "a+b").as_deref(),
            Some(r"svc-a\+b-prod")
        );
        // Literal does not match the capture class: rewrite must not over-match.
        assert!(substitute_single_capture("^svc-([a-z]+)-prod$", "123").is_none());
        assert!(substitute_single_capture("svc-([a-z]+)-prod", "api").is_none());
        assert!(substitute_single_capture("^svc-([a-z]+)-([a-z]+)$", "api").is_none());
        assert!(substitute_single_capture("^svc-(?:[a-z]+)-prod$", "api").is_none());
    }

    #[test]
    fn test_calc_field_regexp_extract_eq_optimize() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("service", STRING | FAST);
        schema_builder.add_text_field("indexed_only", STRING);
        schema_builder.add_text_field("fast_tokenized", TEXT | FAST);
        let schema = schema_builder.build();

        let optimized =
            calc_field_query(r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#)
                .try_optimize_to_regex_query(&schema)
                .expect("eligible expression should optimize");
        assert_eq!(optimized.field, "service");
        assert_eq!(optimized.regex, "svc-api-prod");

        for expression in [
            r#"(EQ "api" (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64))"#,
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 0u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "123")"#,
            r#"(EQ (REGEXP_EXTRACT fast_tokenized "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT indexed_only "^svc-([a-z]+)-prod$" 1u64) "api")"#,
        ] {
            assert!(
                calc_field_query(expression)
                    .try_optimize_to_regex_query(&schema)
                    .is_none(),
                "{expression}"
            );
        }
    }

    #[test]
    fn test_calc_field_regexp_extract_eq_matches_jit_predicate() {
        let mut schema_builder = Schema::builder();
        let service = schema_builder.add_text_field("service", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        for value in [
            "svc-api-prod",
            "svc-web-prod",
            "svc-123-prod",
            "other",
            "svc-api-prod-extra",
        ] {
            writer.add_document(doc!(service => value)).unwrap();
        }
        writer.commit().unwrap();

        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        for (expression, expected_count) in [
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#,
                1usize,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "web")"#,
                1,
            ),
            // Literal outside the capture class: always false for both paths.
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "123")"#,
                0,
            ),
        ] {
            let jit_count = searcher
                .search(
                    &*calc_field(expression)
                        .build_tantivy_query(&context)
                        .unwrap(),
                    &Count,
                )
                .unwrap();
            assert_eq!(jit_count, expected_count, "jit {expression}");

            match calc_field_query(expression).try_optimize_to_regex_query(&schema) {
                Some(regex_query) => {
                    let optimized_count = searcher
                        .search(
                            &*QueryAst::from(regex_query)
                                .build_tantivy_query(&context)
                                .unwrap(),
                            &Count,
                        )
                        .unwrap();
                    assert_eq!(optimized_count, jit_count, "optimized {expression}");
                }
                None => {
                    // Ineligible rewrite must still agree with the JIT result above.
                    assert_eq!(jit_count, expected_count, "fallback {expression}");
                }
            }
        }
    }
}
