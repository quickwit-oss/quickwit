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

use super::regex_extract_eq::RegexExtractEqPlan;
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
/// Predicates of the form `(EQ (REGEXP_EXTRACT field "prefix(capture)suffix" 1u64) "literal")`
/// (or the swapped literal/extract form, with any capture index or none for the whole match) on
/// a non-JSON string fast field are evaluated once per distinct value instead of once per
/// document: the dictionary is walked with an FST *prefilter* regex, each accepted value is
/// checked exactly, and documents are selected by their first value. They match the same
/// documents as the JIT path. When the field is also indexed with the raw tokenizer, only the
/// documents in the postings of the matching values are visited; callers must then also warm the
/// field's term dictionary and postings with the regex of
/// [`CalcFieldQuery::try_prefilter_regex_query`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Builds an FST [`RegexQuery`] accepting a *superset* of the values matching
    /// `(EQ (REGEXP_EXTRACT field pattern capture_index) "literal")` (or the swapped form).
    ///
    /// Available only when the indexed terms contain the same raw values as the fast field, in
    /// which case the query reads the postings of the terms this regex accepts, and they must be
    /// warmed.
    ///
    /// Returns `None` whenever the expression shape or field is not eligible.
    pub fn try_prefilter_regex_query(&self, schema: &TantivySchema) -> Option<RegexQuery> {
        let plan = self.regex_extract_eq_plan(schema)?;
        if !plan.reads_postings() {
            return None;
        }
        Some(RegexQuery {
            field: plan.fast_field_name().to_string(),
            regex: plan.prefilter_regex().to_string(),
        })
    }

    fn regex_extract_eq_plan(&self, schema: &TantivySchema) -> Option<RegexExtractEqPlan> {
        let (field_name, pattern, capture_index, literal) =
            match_eq_regexp_extract(&self.expression)?;
        if !is_str_fast_field(field_name, schema) {
            return None;
        }
        let isolated_pattern = isolate_capture(pattern, capture_index)?;
        let prefilter_regex = substitute_single_capture(&isolated_pattern, literal)?;
        let terms_are_fast_field_values = is_raw_term_prefilter_compatible(field_name, schema);
        RegexExtractEqPlan::new(
            field_name,
            prefilter_regex,
            &isolated_pattern,
            literal,
            terms_are_fast_field_values,
        )
    }
}

impl BuildTantivyAst for CalcFieldQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        if let Some(plan) = self.regex_extract_eq_plan(context.schema) {
            return Ok(plan.build_query());
        }
        let predicate = JitExprPredicate::new(self.expression.clone())
            .context("invalid calculated predicate expression")
            .map_err(InvalidQuery::Other)?;
        let doc_predicate_query = DocPredicateQuery::from(predicate);
        Ok(TantivyQueryAst::from(doc_predicate_query))
    }
}

/// Matches `(EQ (REGEXP_EXTRACT field pattern [capture_index]) "literal")` and the swapped form
/// `(EQ "literal" (REGEXP_EXTRACT field pattern [capture_index]))`, and returns the field, the
/// pattern, the capture index (0, the whole match, when omitted) and the literal.
fn match_eq_regexp_extract(expression: &UntypedExpr) -> Option<(&str, &str, u64, &str)> {
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
    let (field_name, pattern, capture_index) = match extract_args.as_slice() {
        [
            UntypedExpr::Variable(field_name),
            UntypedExpr::Literal(Literal::String(pattern)),
        ] => (field_name, pattern, 0),
        [
            UntypedExpr::Variable(field_name),
            UntypedExpr::Literal(Literal::String(pattern)),
            UntypedExpr::Literal(Literal::U64(capture_index)),
        ] => (field_name, pattern, *capture_index),
        _ => return None,
    };
    Some((
        field_name.as_ref(),
        pattern.as_ref(),
        capture_index,
        literal.as_ref(),
    ))
}

fn is_str_fast_field(field_name: &str, schema: &TantivySchema) -> bool {
    let Some((_field, field_entry, json_path)) = find_field_or_hit_dynamic(field_name, schema)
    else {
        return false;
    };
    // Narrow scope: plain string fields only, not JSON subpaths.
    if !json_path.is_empty() {
        return false;
    }
    field_entry.is_fast() && matches!(field_entry.field_type(), FieldType::Str(_))
}

fn is_raw_term_prefilter_compatible(field_name: &str, schema: &TantivySchema) -> bool {
    let Some((_field, field_entry, json_path)) = find_field_or_hit_dynamic(field_name, schema)
    else {
        return false;
    };
    if !json_path.is_empty() {
        return false;
    }
    let FieldType::Str(text_options) = field_entry.field_type() else {
        return false;
    };
    let Some(text_indexing) = text_options.get_indexing_options() else {
        return false;
    };
    let fast_field_is_raw = matches!(
        text_options.get_fast_field_tokenizer_name(),
        None | Some(RAW_TOKENIZER_NAME)
    );
    text_indexing.tokenizer() == RAW_TOKENIZER_NAME && fast_field_is_raw
}

/// Matches any sequence of characters, including newlines.
const ANY_TEXT: &str = "(?s:.*)";

/// Rewrites `pattern` into an equivalent pattern whose only capturing group, group 1, captures
/// what group `capture_index` of `pattern` captures (the whole match for index 0).
///
/// The other capturing groups become non-capturing: Rust regexes have no backreferences, so
/// capturing never changes where they match. Returns `None` when `pattern` does not parse or has
/// no group `capture_index`.
fn isolate_capture(pattern: &str, capture_index: u64) -> Option<String> {
    let ast = ast::parse::Parser::new().parse(pattern).ok()?;
    let mut groups = Vec::new();
    collect_capturing_groups(&ast, &mut groups);
    if capture_index > groups.len() as u64 {
        return None;
    }
    let mut isolated = String::with_capacity(pattern.len() + 2);
    let mut copied_until = 0;
    // `collect_capturing_groups` lists groups by increasing offset.
    for group in groups {
        if group.capture_index().map(u64::from) == Some(capture_index) {
            continue;
        }
        isolated.push_str(&pattern[copied_until..group.span.start.offset]);
        isolated.push_str("(?:");
        copied_until = group.ast.span().start.offset;
    }
    isolated.push_str(&pattern[copied_until..]);
    if capture_index != 0 {
        return Some(isolated);
    }
    // Anchors are zero-width, so the whole match is what the body between them matches.
    let isolated_ast = ast::parse::Parser::new().parse(&isolated).ok()?;
    let (body_start, body_end, _, _) = body_bounds(&isolated_ast, isolated.len());
    Some(format!(
        "{}({}){}",
        &isolated[..body_start],
        &isolated[body_start..body_end],
        &isolated[body_end..]
    ))
}

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
    use regex::Regex;
    use serde_json::json;
    use tantivy::collector::Count;
    use tantivy::jitexpr::ast::deserialize;
    use tantivy::query::doc_predicate_query::DocPredicateQuery;
    use tantivy::schema::{FAST, STRING, Schema};
    use tantivy::{Index, TantivyDocument, doc};

    use super::{CalcFieldQuery, isolate_capture, substitute_single_capture};
    use crate::query_ast::{BuildTantivyAstContext, QueryAst};

    fn calc_field(expression: &str) -> QueryAst {
        calc_field_query(expression).into()
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
    fn test_isolate_capture() {
        for (pattern, capture_index, expected) in [
            ("^([a-z]+)-([a-z]+)$", 1, "^([a-z]+)-(?:[a-z]+)$"),
            ("^([a-z]+)-([a-z]+)$", 2, "^(?:[a-z]+)-([a-z]+)$"),
            (
                "^([a-z]+)-([a-z]+)-([a-z]+)-([0-9]+)-([a-z0-9-]+)$",
                5,
                "^(?:[a-z]+)-(?:[a-z]+)-(?:[a-z]+)-(?:[0-9]+)-([a-z0-9-]+)$",
            ),
            ("((a)b)(c)", 2, "(?:(a)b)(?:c)"),
            (r"(?P<svc>[a-z]+)-(\d+)", 1, r"(?P<svc>[a-z]+)-(?:\d+)"),
            (r"(?P<svc>[a-z]+)-(\d+)", 2, r"(?:[a-z]+)-(\d+)"),
            ("^svc-([a-z]+)-prod$", 0, "^(svc-(?:[a-z]+)-prod)$"),
            ("svc-[a-z]+", 0, "(svc-[a-z]+)"),
            ("a|b", 0, "(a|b)"),
        ] {
            assert_eq!(
                isolate_capture(pattern, capture_index).as_deref(),
                Some(expected),
                "{pattern} {capture_index}"
            );
        }
        assert!(isolate_capture("^svc-([a-z]+)$", 2).is_none());
        assert!(isolate_capture("(", 1).is_none());
    }

    #[test]
    fn test_isolate_capture_preserves_requested_match() {
        for (pattern, capture_index, values) in [
            (
                r"^([a-z]+)-([a-z]+)-prod$",
                2,
                &["svc-api-prod", "svc-web-prod", "invalid"][..],
            ),
            (
                r"^svc-(?P<name>[a-z]+)-(prod)$",
                1,
                &["svc-api-prod", "svc-web-prod", "svc-123-prod"][..],
            ),
            (
                r"^([a-z]+)-([a-z]+)-([a-z]+)-([0-9]+)-([a-z0-9-]+)$",
                3,
                &[
                    "svc-api-prod-42-us-east-1",
                    "svc-web-dev-7-eu-west-2",
                    "invalid",
                ][..],
            ),
            (
                r"^([a-z]+)-([a-z]+)-([a-z]+)-([0-9]+)-([a-z0-9-]+)$",
                5,
                &[
                    "svc-api-prod-42-us-east-1",
                    "svc-web-dev-7-eu-west-2",
                    "svc-api-prod-x-us-east-1",
                ][..],
            ),
            (
                r"^svc-([a-z]+)-prod$",
                0,
                &["svc-api-prod", "svc-web-prod", "svc-api-dev"][..],
            ),
            (
                r"svc-[a-z]+",
                0,
                &["prefix svc-api suffix", "svc-web", "other"][..],
            ),
        ] {
            let isolated_pattern = isolate_capture(pattern, capture_index).unwrap();
            let original_regex = Regex::new(pattern).unwrap();
            let isolated_regex = Regex::new(&isolated_pattern).unwrap();
            for value in values {
                let original_capture = original_regex
                    .captures(value)
                    .and_then(|captures| captures.get(capture_index as usize))
                    .map(|capture| capture.as_str());
                let isolated_capture = isolated_regex
                    .captures(value)
                    .and_then(|captures| captures.get(1))
                    .map(|capture| capture.as_str());
                assert_eq!(
                    isolated_capture, original_capture,
                    "pattern={pattern}, isolated={isolated_pattern}, index={capture_index}, \
                     value={value}"
                );
            }
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
    }
}
