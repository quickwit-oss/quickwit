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
use regex_syntax::ast::{self, AssertionKind, Ast};
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
/// literal/extract form) may be accelerated with an FST [`RegexQuery`] *prefilter* when
/// `field` is a fast, indexed, raw-tokenized string field. The prefilter may over-match;
/// callers must keep the original calculated predicate as the exact filter.
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
    /// Builds an FST [`RegexQuery`] that is a *superset* of documents matching
    /// `(EQ (REGEXP_EXTRACT field pattern 1u64) "literal")` (or the swapped form).
    ///
    /// The prefilter may over-match; callers must intersect with this [`CalcFieldQuery`].
    ///
    /// Returns `None` whenever the expression shape or field is not eligible.
    pub fn try_prefilter_regex_query(&self, schema: &TantivySchema) -> Option<RegexQuery> {
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
        // Eligible REGEXP_EXTRACT equality predicates may be wrapped with an FST
        // RegexQuery prefilter earlier in query_builder; this path stays exact.
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
    // The JIT reads fast-field values while the prefilter reads indexed terms: both must hold
    // the untransformed value.
    let fast_field_is_raw = matches!(
        text_options.get_fast_field_tokenizer_name(),
        None | Some(RAW_TOKENIZER_NAME)
    );
    text_indexing.tokenizer() == RAW_TOKENIZER_NAME && fast_field_is_raw
}

/// Matches any sequence of characters, including newlines.
const ANY_TEXT: &str = "(?s:.*)";

/// Builds an FST whole-term regex that is a *superset* of terms for which
/// `EQ(REGEXP_EXTRACT(..., pattern, 1), literal)` holds.
///
/// Parses `pattern` with [`regex_syntax`], requires exactly one capturing group that is a
/// direct part of the top-level concatenation (not inside an alternation or repetition),
/// replaces that group with the escaped literal, and requires the literal to match the
/// capture subpattern (otherwise EQ is always false).
///
/// Tantivy FST regexes match whole terms and reject `^`/`$`, so start/end anchors
/// are stripped. Missing sides are wrapped with [`ANY_TEXT`].
fn substitute_single_capture(pattern: &str, literal: &str) -> Option<String> {
    let ast = ast::parse::Parser::new().parse(pattern).ok()?;
    let group = {
        let mut groups = Vec::new();
        collect_capturing_groups(&ast, &mut groups);
        match groups.as_slice() {
            [group] => *group,
            _ => return None,
        }
    };
    let is_top_level_part = match &ast {
        Ast::Concat(concat) => concat
            .asts
            .iter()
            .any(|part| matches!(part, Ast::Group(part_group) if part_group.span == group.span)),
        Ast::Group(root_group) => root_group.span == group.span,
        _ => false,
    };
    if !is_top_level_part {
        return None;
    }

    let (body_start, body_end, anchored_start, anchored_end) = body_bounds(&ast, pattern.len());
    let group_start = group.span.start.offset;
    let group_end = group.span.end.offset;
    if group_start < body_start || group_end > body_end {
        return None;
    }

    let capture = &pattern[group.ast.span().start.offset..group.ast.span().end.offset];
    // Impossible EQ: literal outside the capture class.
    let capture_re = regex::Regex::new(&format!("^(?:{capture})$")).ok()?;
    if !capture_re.is_match(literal) {
        return None;
    }

    // Superset whole-term regex (may over-match leftmost extract+EQ), with `…` = ANY_TEXT:
    //   ^prefix(C)suffix$ + L  →  prefix{escape(L)}suffix
    //   ^prefix(C)suffix  + L  →  prefix{escape(L)}suffix…
    //    prefix(C)suffix$ + L  →  …prefix{escape(L)}suffix
    //    prefix(C)suffix  + L  →  …prefix{escape(L)}suffix…
    let mut rewritten = String::new();
    if !anchored_start {
        rewritten.push_str(ANY_TEXT);
    }
    rewritten.push_str(&pattern[body_start..group_start]);
    rewritten.push_str(&regex_syntax::escape(literal));
    rewritten.push_str(&pattern[group_end..body_end]);
    if !anchored_end {
        rewritten.push_str(ANY_TEXT);
    }
    // FST regexes support no look-around (`\b`, `^` behind flags or inside the pattern, ...):
    // such patterns must stay on the JIT path instead of failing the query.
    tantivy_fst::Regex::new(&rewritten).ok()?;
    Some(rewritten)
}

/// Byte range of `pattern` after stripping a leading `^`/`\A` and trailing `$`/`\z`.
fn body_bounds(ast: &Ast, pattern_len: usize) -> (usize, usize, bool, bool) {
    let parts: Vec<&Ast> = match ast {
        Ast::Concat(concat) => concat.asts.iter().collect(),
        _ => vec![ast],
    };

    let mut body_start = 0;
    let mut body_end = pattern_len;
    let mut anchored_start = false;
    let mut anchored_end = false;

    if let Some(Ast::Assertion(assertion)) = parts.first()
        && matches!(
            assertion.kind,
            AssertionKind::StartLine | AssertionKind::StartText
        )
    {
        anchored_start = true;
        body_start = assertion.span.end.offset;
    }
    if let Some(Ast::Assertion(assertion)) = parts.last()
        && matches!(
            assertion.kind,
            AssertionKind::EndLine | AssertionKind::EndText
        )
    {
        anchored_end = true;
        body_end = assertion.span.start.offset;
    }
    (body_start, body_end, anchored_start, anchored_end)
}

fn collect_capturing_groups<'a>(ast: &'a Ast, groups: &mut Vec<&'a ast::Group>) {
    match ast {
        Ast::Group(group) => {
            if group.is_capturing() {
                groups.push(group);
            }
            collect_capturing_groups(&group.ast, groups);
        }
        Ast::Repetition(repetition) => collect_capturing_groups(&repetition.ast, groups),
        Ast::Alternation(alternation) => {
            for child in &alternation.asts {
                collect_capturing_groups(child, groups);
            }
        }
        Ast::Concat(concat) => {
            for child in &concat.asts {
                collect_capturing_groups(child, groups);
            }
        }
        _ => {}
    }
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

    fn prefilter_conjunction(calc: CalcFieldQuery, schema: &Schema) -> QueryAst {
        let regex_query = calc
            .try_prefilter_regex_query(schema)
            .expect("eligible expression should produce a prefilter");
        BoolQuery {
            filter: vec![regex_query.into(), QueryAst::CalcField(calc)],
            ..Default::default()
        }
        .into()
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
            substitute_single_capture("svc-([a-z]+)-prod", "api").as_deref(),
            Some("(?s:.*)svc-api-prod(?s:.*)")
        );
        assert_eq!(
            substitute_single_capture("^svc-([a-z]+)", "api").as_deref(),
            Some("svc-api(?s:.*)")
        );
        assert_eq!(
            substitute_single_capture("([a-z]+)-prod$", "api").as_deref(),
            Some("(?s:.*)api-prod")
        );
        assert_eq!(
            substitute_single_capture("^svc-(a.b)-prod$", "a+b").as_deref(),
            Some(r"svc-a\+b-prod")
        );
        assert_eq!(
            substitute_single_capture("^.*userid=([A-Z0-9]+).*$", "USER42").as_deref(),
            Some(".*userid=USER42.*")
        );
        // Literal does not match the capture class: no useful rewrite.
        assert!(substitute_single_capture("^svc-([a-z]+)-prod$", "123").is_none());
        assert!(substitute_single_capture("^svc-([a-z]+)-([a-z]+)$", "api").is_none());
        assert!(substitute_single_capture("^svc-(?:[a-z]+)-prod$", "api").is_none());
        // The capture must be a direct part of the top-level concatenation.
        assert!(substitute_single_capture("x|([a-z]+)", "api").is_none());
        assert!(substitute_single_capture("(?:a([a-z]+))+", "api").is_none());
        assert!(substitute_single_capture("(?:id=([a-z]+))?end", "api").is_none());
        // Look-arounds left in the rewritten regex are not supported by FST regexes.
        assert!(substitute_single_capture(r"\bsvc-([a-z]+)", "api").is_none());
        assert!(substitute_single_capture("(?m)^svc-([a-z]+)", "api").is_none());
        assert!(substitute_single_capture("a^svc-([a-z]+)", "api").is_none());
    }

    #[test]
    fn test_calc_field_regexp_extract_eq_prefilter() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("service", STRING | FAST);
        schema_builder.add_text_field("indexed_only", STRING);
        schema_builder.add_text_field("fast_tokenized", TEXT | FAST);
        schema_builder.add_text_field("fast_lowercased", STRING.set_fast("lowercase"));
        let schema = schema_builder.build();

        let prefilter =
            calc_field_query(r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#)
                .try_prefilter_regex_query(&schema)
                .expect("eligible expression should produce a prefilter");
        assert_eq!(prefilter.field, "service");
        assert_eq!(prefilter.regex, "svc-api-prod");

        let swapped =
            calc_field_query(r#"(EQ "api" (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64))"#)
                .try_prefilter_regex_query(&schema)
                .expect("swapped EQ args should produce a prefilter");
        assert_eq!(swapped, prefilter);

        let unanchored =
            calc_field_query(r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)-prod" 1u64) "api")"#)
                .try_prefilter_regex_query(&schema)
                .expect("unanchored pattern should produce a prefilter");
        assert_eq!(unanchored.regex, "(?s:.*)svc-api-prod(?s:.*)");

        for expression in [
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 0u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "123")"#,
            r#"(EQ (REGEXP_EXTRACT fast_tokenized "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT indexed_only "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT fast_lowercased "^svc-([a-z]+)-prod$" 1u64) "api")"#,
        ] {
            assert!(
                calc_field_query(expression)
                    .try_prefilter_regex_query(&schema)
                    .is_none(),
                "{expression}"
            );
        }
    }

    #[test]
    fn test_calc_field_regexp_extract_eq_prefilter_matches_jit_predicate() {
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
            // Multi-line value: the prefilter wrappers must match newlines.
            "line1\nsvc-api-prod\nline3",
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
            (
                r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)-prod" 1u64) "api")"#,
                3,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "other|svc-([a-z]+)-prod" 1u64) "api")"#,
                3,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "\\bsvc-([a-z]+)-prod" 1u64) "api")"#,
                3,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)" 1u64) "api")"#,
                2,
            ),
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

            match calc_field_query(expression).try_prefilter_regex_query(&schema) {
                Some(regex_query) => {
                    let regex_only_count = searcher
                        .search(
                            &*QueryAst::from(regex_query)
                                .build_tantivy_query(&context)
                                .unwrap(),
                            &Count,
                        )
                        .unwrap();
                    assert!(
                        regex_only_count >= jit_count,
                        "prefilter must be a superset for {expression}: regex={regex_only_count} \
                         jit={jit_count}"
                    );

                    let optimized_count = searcher
                        .search(
                            &*prefilter_conjunction(calc_field_query(expression), &schema)
                                .build_tantivy_query(&context)
                                .unwrap(),
                            &Count,
                        )
                        .unwrap();
                    assert_eq!(optimized_count, jit_count, "prefilter∧jit {expression}");
                }
                None => {
                    assert_eq!(jit_count, expected_count, "fallback {expression}");
                }
            }
        }
    }
}
