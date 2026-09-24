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
/// `(EQ (REGEXP_EXTRACT field "prefix(capture)suffix" 1u64) "literal")` (or the swapped
/// literal/extract form) may be accelerated with an FST [`RegexQuery`] when `field` is a
/// fast, indexed, raw-tokenized string field.
///
/// Fully anchored patterns (`^…$`) lower to an equivalent [`RegexQuery`] alone. Otherwise
/// the FST regex is only a *prefilter* (it may over-match) and the original calculated
/// predicate remains as the exact filter.
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

/// Result of lowering an `EQ(REGEXP_EXTRACT(…), literal)` predicate.
#[derive(Clone, Debug, PartialEq)]
pub enum RegexpExtractEqOptimize {
    /// Fully anchored pattern: the [`RegexQuery`] alone matches the predicate exactly.
    Exact(RegexQuery),
    /// Unanchored or half-anchored: the [`RegexQuery`] is a superset prefilter; callers
    /// must intersect with the original [`CalcFieldQuery`].
    Prefilter(RegexQuery),
}

impl CalcFieldQuery {
    /// Attempts to lower `(EQ (REGEXP_EXTRACT field pattern 1u64) "literal")` (or the
    /// swapped form) into an FST [`RegexQuery`], either as an exact rewrite or as a
    /// prefilter.
    ///
    /// Returns `None` whenever the expression shape or field is not eligible.
    pub fn try_optimize_regexp_extract_eq(
        &self,
        schema: &TantivySchema,
    ) -> Option<RegexpExtractEqOptimize> {
        let (field_name, pattern, literal) = match_eq_regexp_extract(&self.expression)?;
        if !is_fast_indexed_raw_string_field(field_name, schema) {
            return None;
        }
        let (regex, fully_anchored) = substitute_single_capture(pattern, literal)?;
        let regex_query = RegexQuery {
            field: field_name.to_string(),
            regex,
        };
        if fully_anchored {
            Some(RegexpExtractEqOptimize::Exact(regex_query))
        } else {
            Some(RegexpExtractEqOptimize::Prefilter(regex_query))
        }
    }
}

impl BuildTantivyAst for CalcFieldQuery {
    fn build_tantivy_ast_impl(
        &self,
        _context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        // Eligible REGEXP_EXTRACT equality predicates may be rewritten or prefiltered
        // with an FST RegexQuery earlier in query_builder; this path stays exact.
        let predicate = JitExprPredicate::new(self.expression.clone())
            .context("invalid calculated predicate expression")
            .map_err(InvalidQuery::Other)?;
        let doc_predicate_query = DocPredicateQuery::from(predicate);
        Ok(TantivyQueryAst::from(doc_predicate_query))
    }
}

/// Matches `(EQ (REGEXP_EXTRACT field pattern 1u64) "literal")` and the swapped form
/// `(EQ "literal" (REGEXP_EXTRACT field pattern 1u64))`.
fn match_eq_regexp_extract(expression: &UntypedExpr) -> Option<(&str, &str, &str)> {
    let UntypedExpr::FnCall {
        function: Function::Eq,
        args,
    } = expression
    else {
        return None;
    };
    let [left, right] = args.as_slice() else {
        return None;
    };
    let (extract, literal) = match (left, right) {
        (extract, UntypedExpr::Literal(Literal::String(literal))) => (extract, literal),
        (UntypedExpr::Literal(Literal::String(literal)), extract) => (extract, literal),
        _ => return None,
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

/// Builds an FST whole-term regex for `EQ(REGEXP_EXTRACT(..., pattern, 1), literal)`.
///
/// Returns `(regex, fully_anchored)`. When `fully_anchored` is true (`^…$`), the regex
/// alone is equivalent to the predicate. Otherwise it is only a *superset* prefilter.
///
/// Replaces the single `(...)` group with the escaped literal. Rejects special
/// groups and nested/extra parentheses. Requires the equality literal to match
/// the capture subpattern (otherwise EQ is always false).
///
/// Tantivy FST regexes match whole terms and reject `^`/`$`, so anchors are stripped.
/// Missing sides are wrapped with `.*`.
fn substitute_single_capture(pattern: &str, literal: &str) -> Option<(String, bool)> {
    let anchored_start = pattern.starts_with('^');
    let anchored_end = pattern.ends_with('$');
    let fully_anchored = anchored_start && anchored_end;
    let body = match (anchored_start, anchored_end) {
        (true, true) => &pattern[1..pattern.len() - 1],
        (true, false) => &pattern[1..],
        (false, true) => &pattern[..pattern.len() - 1],
        (false, false) => pattern,
    };

    let open = body.find('(')?;
    let close = body[open + 1..].find(')')? + open + 1;
    // Reject `(?:...)` / other special groups, nested parentheses, and a second group.
    if body.as_bytes().get(open + 1) == Some(&b'?')
        || body[open + 1..close].contains(['(', ')'])
        || body[close + 1..].contains('(')
    {
        return None;
    }
    let capture = &body[open + 1..close];
    // Impossible EQ: literal outside the capture class.
    let capture_re = regex::Regex::new(&format!("^(?:{capture})$")).ok()?;
    if !capture_re.is_match(literal) {
        return None;
    }

    // Whole-term regex:
    //   ^prefix(C)suffix$ + L  →  prefix{escape(L)}suffix          (exact)
    //   ^prefix(C)suffix  + L  →  prefix{escape(L)}suffix.*        (prefilter)
    //    prefix(C)suffix$ + L  →  .*prefix{escape(L)}suffix        (prefilter)
    //    prefix(C)suffix  + L  →  .*prefix{escape(L)}suffix.*      (prefilter)
    let mut rewritten = String::new();
    if !anchored_start {
        rewritten.push_str(".*");
    }
    rewritten.push_str(&body[..open]);
    rewritten.push_str(&regex::escape(literal));
    rewritten.push_str(&body[close + 1..]);
    if !anchored_end {
        rewritten.push_str(".*");
    }
    Some((rewritten, fully_anchored))
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

    use super::{CalcFieldQuery, RegexpExtractEqOptimize, substitute_single_capture};
    use crate::query_ast::{BoolQuery, BuildTantivyAstContext, QueryAst};

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

    fn optimized_ast(calc: CalcFieldQuery, schema: &Schema) -> QueryAst {
        match calc
            .try_optimize_regexp_extract_eq(schema)
            .expect("eligible expression should optimize")
        {
            RegexpExtractEqOptimize::Exact(regex_query) => regex_query.into(),
            RegexpExtractEqOptimize::Prefilter(regex_query) => BoolQuery {
                filter: vec![regex_query.into(), QueryAst::CalcField(calc)],
                ..Default::default()
            }
            .into(),
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
            substitute_single_capture("^svc-([a-z]+)-prod$", "api"),
            Some(("svc-api-prod".to_string(), true))
        );
        assert_eq!(
            substitute_single_capture("svc-([a-z]+)-prod", "api"),
            Some((".*svc-api-prod.*".to_string(), false))
        );
        assert_eq!(
            substitute_single_capture("^svc-([a-z]+)", "api"),
            Some(("svc-api.*".to_string(), false))
        );
        assert_eq!(
            substitute_single_capture("([a-z]+)-prod$", "api"),
            Some((".*api-prod".to_string(), false))
        );
        assert_eq!(
            substitute_single_capture("^svc-(a.b)-prod$", "a+b"),
            Some((r"svc-a\+b-prod".to_string(), true))
        );
        // Literal does not match the capture class: no useful rewrite.
        assert!(substitute_single_capture("^svc-([a-z]+)-prod$", "123").is_none());
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

        let RegexpExtractEqOptimize::Exact(exact) =
            calc_field_query(r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#)
                .try_optimize_regexp_extract_eq(&schema)
                .expect("fully anchored expression should optimize exactly")
        else {
            panic!("expected Exact rewrite");
        };
        assert_eq!(exact.field, "service");
        assert_eq!(exact.regex, "svc-api-prod");

        let RegexpExtractEqOptimize::Exact(swapped) =
            calc_field_query(r#"(EQ "api" (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64))"#)
                .try_optimize_regexp_extract_eq(&schema)
                .expect("swapped EQ args should optimize exactly")
        else {
            panic!("expected Exact rewrite");
        };
        assert_eq!(swapped, exact);

        let RegexpExtractEqOptimize::Prefilter(unanchored) =
            calc_field_query(r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)-prod" 1u64) "api")"#)
                .try_optimize_regexp_extract_eq(&schema)
                .expect("unanchored pattern should produce a prefilter")
        else {
            panic!("expected Prefilter rewrite");
        };
        assert_eq!(unanchored.regex, ".*svc-api-prod.*");

        for expression in [
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 0u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "123")"#,
            r#"(EQ (REGEXP_EXTRACT fast_tokenized "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT indexed_only "^svc-([a-z]+)-prod$" 1u64) "api")"#,
        ] {
            assert!(
                calc_field_query(expression)
                    .try_optimize_regexp_extract_eq(&schema)
                    .is_none(),
                "{expression}"
            );
        }
    }

    #[test]
    fn test_calc_field_regexp_extract_eq_optimize_matches_jit_predicate() {
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
            // Prefilter over-matches (longer capture / earlier different extract); JIT rejects.
            "svc-apixyz",
            "svc-web-prod-svc-api-prod",
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
            // Unanchored: svc-api-prod and svc-api-prod-extra (not the earlier-other-id doc).
            (
                r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)-prod" 1u64) "api")"#,
                2,
            ),
            // Half-anchored: svc-api-prod and svc-api-prod-extra match; svc-apixyz does not
            // (capture apixyz) even though the FST prefilter svc-api.* would over-match it.
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)" 1u64) "api")"#,
                2,
            ),
            // Literal outside the capture class: always false.
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

            match calc_field_query(expression).try_optimize_regexp_extract_eq(&schema) {
                Some(optimize) => {
                    if let RegexpExtractEqOptimize::Prefilter(ref regex_query) = optimize {
                        let regex_only_count = searcher
                            .search(
                                &*QueryAst::from(regex_query.clone())
                                    .build_tantivy_query(&context)
                                    .unwrap(),
                                &Count,
                            )
                            .unwrap();
                        assert!(
                            regex_only_count >= jit_count,
                            "prefilter must be a superset for {expression}: \
                             regex={regex_only_count} jit={jit_count}"
                        );
                    }

                    let optimized_count = searcher
                        .search(
                            &*optimized_ast(calc_field_query(expression), &schema)
                                .build_tantivy_query(&context)
                                .unwrap(),
                            &Count,
                        )
                        .unwrap();
                    assert_eq!(optimized_count, jit_count, "optimized {expression}");
                }
                None => {
                    assert_eq!(jit_count, expected_count, "fallback {expression}");
                }
            }
        }
    }
}
