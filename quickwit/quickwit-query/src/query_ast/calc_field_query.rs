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
use tantivy::schema::{FieldType, Schema as TantivySchema, TextFieldIndexing};

use super::regex_extract_eq::{PostingsTarget, RegexExtractEqSpec};
use super::regex_query::json_str_term_prefix;
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
/// Eligible predicates of the form
/// `(EQ (REGEXP_EXTRACT field "prefix(capture)suffix" 1u64) "literal")` (or the swapped
/// literal/extract form, with any capture index or none for the whole match) on a string fast
/// field or a fast JSON subfield are evaluated once per distinct value instead of once per
/// document: the dictionary is walked with an FST *prefilter* regex, each accepted value is checked
/// exactly, and documents are selected by their first value. They match the same documents as the
/// JIT path.
///
/// In particular, `REGEXP_EXTRACT` sees only the first value of a multivalued field. A matching
/// later value does not make the predicate match.
///
/// On raw-indexed fields, including JSON subfields, segments where every matching value is indexed
/// visit only the postings of the matching terms. Other segments fall back to checking every
/// document's first value. Callers must warm the raw field's term dictionary and postings with the
/// regex of [`CalcFieldQuery::try_prefilter_regex_query`].
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
    /// Available only for fields, or JSON subfields, indexed with the raw tokenizer and a raw fast
    /// field. Compilation
    /// is shared with query construction and other splits by a bounded process-wide cache.
    ///
    /// Returns `None` whenever the expression shape or field is not eligible.
    pub fn try_prefilter_regex_query(&self, schema: &TantivySchema) -> Option<RegexQuery> {
        self.regex_extract_eq_spec(schema)?
            .try_build_warmup_prefilter_query()
    }

    /// Rewrites an eligible expression into uncompiled prefilter and isolated-pattern strings.
    ///
    /// Does not compile any regex. Warmup discovery and query building share compiled FST and
    /// exact regexes through bounded process-wide component caches across splits.
    fn regex_extract_eq_spec(&self, schema: &TantivySchema) -> Option<RegexExtractEqSpec> {
        let (field_name, pattern, capture_index, literal) =
            match_eq_regexp_extract(&self.expression)?;
        // Resolve the field once: the same metadata determines both fast-field eligibility and
        // whether the postings-based scorer can be used.
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(field_name, schema)?;
        // Non-fast fields cannot use the value dictionary scorer.
        if !field_entry.is_fast() {
            return None;
        }
        // The fast-field scorer remains valid without indexing. Postings are safe only when raw
        // indexing preserves the values of a raw fast field, so that every term is verbatim one
        // of the fast-field values. The scorer checks per segment that the matching values are
        // all indexed.
        let postings_target = match field_entry.field_type() {
            FieldType::Str(text_options) if json_path.is_empty() => is_raw_fast_and_indexed(
                text_options.get_fast_field_tokenizer_name(),
                text_options.get_indexing_options(),
            )
            .then(|| PostingsTarget::new(field, Vec::new())),
            // A JSON subfield has its own string column, opened by the scorer under the same
            // name as the JIT predicate. Its string terms are those of the JSON field starting
            // with the subfield's path and string type prefix.
            FieldType::JsonObject(json_options) if !json_path.is_empty() => {
                is_raw_fast_and_indexed(
                    json_options.get_fast_field_tokenizer_name(),
                    json_options.get_text_indexing_options(),
                )
                .then(|| {
                    let term_prefix = json_str_term_prefix(field, json_path, json_options);
                    PostingsTarget::new(field, term_prefix)
                })
            }
            _ => return None,
        };
        // Normalize the requested capture to group 1 so the exact matcher can keep the same
        // capture semantics regardless of the original capture index.
        let isolated_pattern = isolate_capture(pattern, capture_index)?;
        // Replace the isolated capture with the literal to build an FST superset prefilter.
        let prefilter_regex = substitute_single_capture(&isolated_pattern, literal)?;
        Some(RegexExtractEqSpec::new(
            field_name,
            prefilter_regex,
            isolated_pattern,
            literal,
            postings_target,
        ))
    }
}

/// Returns whether a field with these options indexes its string values with the raw tokenizer
/// and stores them unnormalized in its fast field.
fn is_raw_fast_and_indexed(
    fast_field_tokenizer_name: Option<&str>,
    indexing_options: Option<&TextFieldIndexing>,
) -> bool {
    let fast_field_is_raw = matches!(fast_field_tokenizer_name, None | Some(RAW_TOKENIZER_NAME));
    fast_field_is_raw
        && matches!(
            indexing_options,
            Some(text_indexing) if text_indexing.tokenizer() == RAW_TOKENIZER_NAME
        )
}

impl BuildTantivyAst for CalcFieldQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        // Compilation is shared with warmup discovery and other splits by the component caches.
        if let Some(plan) = self
            .regex_extract_eq_spec(context.schema)
            .and_then(RegexExtractEqSpec::compile_execution_plan)
        {
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

/// Matches any sequence of characters, including newlines.
const ANY_TEXT: &str = "(?s:.*)";

/// Rewrites `pattern` into an equivalent pattern whose only capturing group, group 1, captures
/// what group `capture_index` of `pattern` captures (the whole match for index 0).
///
/// The other capturing groups become non-capturing: Rust regexes have no backreferences, so
/// capturing never changes where they match. Returns `None` when `pattern` does not parse or has
/// no group `capture_index`.
///
/// Synthesized AST nodes reuse the span of the whole pattern: spans are meaningless in the
/// rewritten AST, which is only printed, and the printer ignores them.
fn isolate_capture(pattern: &str, capture_index: u64) -> Option<String> {
    let mut ast = parse_regex(pattern)?;
    let span = *ast.span();
    let has_capture = anonymize_captures_except(&mut ast, capture_index);
    if capture_index != 0 {
        return has_capture.then(|| ast.to_string());
    }
    // Anchors are zero-width, so the whole match is what the body between them matches.
    let (start_anchor, body, end_anchor) = split_anchors(ast);
    let whole_match = Ast::group(ast::Group {
        span,
        kind: ast::GroupKind::CaptureIndex(1),
        ast: Box::new(ast::Concat { span, asts: body }.into_ast()),
    });
    let asts = start_anchor
        .into_iter()
        .chain(std::iter::once(whole_match))
        .chain(end_anchor)
        .collect();
    Some(ast::Concat { span, asts }.into_ast().to_string())
}

/// Makes every capturing group of `ast` non-capturing except group `capture_index`, and returns
/// whether that group exists.
fn anonymize_captures_except(ast: &mut Ast, capture_index: u64) -> bool {
    match ast {
        Ast::Group(group) => {
            let is_kept = group.capture_index().map(u64::from) == Some(capture_index);
            if group.is_capturing() && !is_kept {
                group.kind = ast::GroupKind::NonCapturing(ast::Flags {
                    span: group.span,
                    items: Vec::new(),
                });
            }
            let has_nested_capture = anonymize_captures_except(&mut group.ast, capture_index);
            is_kept || has_nested_capture
        }
        Ast::Repetition(repetition) => {
            anonymize_captures_except(&mut repetition.ast, capture_index)
        }
        Ast::Alternation(alternation) => {
            let mut has_capture = false;
            for child in &mut alternation.asts {
                has_capture |= anonymize_captures_except(child, capture_index);
            }
            has_capture
        }
        Ast::Concat(concat) => {
            let mut has_capture = false;
            for child in &mut concat.asts {
                has_capture |= anonymize_captures_except(child, capture_index);
            }
            has_capture
        }
        _ => false,
    }
}

/// Builds an FST whole-term regex that is a *superset* of terms for which
/// `EQ(REGEXP_EXTRACT(..., pattern, 1), literal)` holds. Here `pattern` is the
/// output of [`isolate_capture`], so group 1 represents the requested capture
/// from the original pattern.
///
/// Parses `pattern` with [`regex_syntax`], requires exactly one capturing group that is a
/// direct part of the top-level concatenation (not inside an alternation or repetition),
/// and replaces that group with the escaped literal. If the literal cannot match the capture
/// subpattern, the resulting prefilter may still yield candidates, but the exact matcher rejects
/// them.
///
/// Tantivy FST regexes match whole terms and reject `^`/`$`, so start/end anchors
/// are stripped. Missing sides are wrapped with [`ANY_TEXT`]. A trailing `$` under the multi-line
/// flag is kept, so the unsupported assertion makes the prefilter construction fall back safely.
///
/// Synthesized AST nodes reuse the span of the whole pattern: spans are meaningless in the
/// rewritten AST, which is only printed, and the printer ignores them.
fn substitute_single_capture(pattern: &str, literal: &str) -> Option<String> {
    let ast = parse_regex(pattern)?;
    let span = *ast.span();
    let ignores_whitespace = mentions_flag(&ast, ast::Flag::IgnoreWhitespace);
    let mut groups = Vec::new();
    collect_capturing_groups(&ast, &mut groups);
    if groups.len() != 1 {
        return None;
    }
    let (start_anchor, mut parts, end_anchor) = split_anchors(ast);
    if matches!(
        parts.last(),
        Some(Ast::Assertion(assertion)) if assertion.kind == AssertionKind::EndLine
    ) {
        // Under `m`, `$` also matches before a newline and therefore cannot be stripped.
        return None;
    }
    // The capture must be a direct part of the top-level concatenation.
    let capture_position = parts
        .iter()
        .enumerate()
        .find_map(|(position, part)| match part {
            Ast::Group(group) if group.is_capturing() => Some(position),
            _ => None,
        })?;

    // Superset whole-term regex (may over-match leftmost extract+EQ), with `…` = ANY_TEXT:
    //   ^prefix(C)suffix$ + L  →  prefix{escape(L)}suffix
    //   ^prefix(C)suffix  + L  →  prefix{escape(L)}suffix…
    //    prefix(C)suffix$ + L  →  …prefix{escape(L)}suffix
    //    prefix(C)suffix  + L  →  …prefix{escape(L)}suffix…
    let mut literal_ast = parse_regex(&regex_syntax::escape(literal))?;
    if ignores_whitespace {
        // `escape` leaves whitespace as is, which the `x` flag would ignore: disable it.
        let items = vec![
            ast::FlagsItem {
                span,
                kind: ast::FlagsItemKind::Negation,
            },
            ast::FlagsItem {
                span,
                kind: ast::FlagsItemKind::Flag(ast::Flag::IgnoreWhitespace),
            },
        ];
        literal_ast = Ast::group(ast::Group {
            span,
            kind: ast::GroupKind::NonCapturing(ast::Flags { span, items }),
            ast: Box::new(literal_ast),
        });
    }
    parts[capture_position] = literal_ast;
    if start_anchor.is_none() {
        parts.insert(0, parse_regex(ANY_TEXT)?);
    }
    if end_anchor.is_none() {
        parts.push(parse_regex(ANY_TEXT)?);
    }
    Some(ast::Concat { span, asts: parts }.into_ast().to_string())
}

fn parse_regex(pattern: &str) -> Option<Ast> {
    ast::parse::Parser::new().parse(pattern).ok()
}

/// Returns whether any flag group of `ast` sets or clears `flag`.
fn mentions_flag(ast: &Ast, flag: ast::Flag) -> bool {
    struct FlagFinder {
        flag: ast::Flag,
        found: bool,
    }

    impl ast::Visitor for FlagFinder {
        type Output = bool;
        type Err = std::convert::Infallible;

        fn finish(self) -> Result<bool, Self::Err> {
            Ok(self.found)
        }

        fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
            let flags = match node {
                Ast::Flags(set_flags) => Some(&set_flags.flags),
                Ast::Group(group) => group.flags(),
                _ => None,
            };
            if let Some(flags) = flags
                && flags.flag_state(self.flag).is_some()
            {
                self.found = true;
            }
            Ok(())
        }
    }

    let Ok(found) = ast::visit(ast, FlagFinder { flag, found: false });
    found
}

/// Splits the top-level concatenation of `ast` into a leading `^`/`\A`, the body parts and a
/// trailing `$`/`\z`.
///
/// A trailing `$` stays in the body when `ast` uses the multi-line flag, since it may then match
/// before a newline and depends on the flags in scope. A leading `^` precedes any flag group, so
/// it always matches the start of the text.
fn split_anchors(mut ast: Ast) -> (Option<Ast>, Vec<Ast>, Option<Ast>) {
    let mut parts = if let Ast::Concat(concat) = &mut ast {
        // `Ast` implements `Drop`, so take the parts instead of deep-cloning them.
        std::mem::take(&mut concat.asts)
    } else {
        vec![ast]
    };
    // Only top-level flag declarations affect a trailing top-level assertion. Flags in a scoped
    // non-capturing group do not leak out of that group.
    let mut multi_line_at_end = false;
    for part in &parts {
        if let Ast::Flags(set_flags) = part
            && let Some(enabled) = set_flags.flags.flag_state(ast::Flag::MultiLine)
        {
            multi_line_at_end = enabled;
        }
    }
    let start_anchor = if matches!(
        parts.first(),
        Some(Ast::Assertion(assertion))
            if matches!(assertion.kind, AssertionKind::StartLine | AssertionKind::StartText)
    ) {
        Some(parts.remove(0))
    } else {
        None
    };
    let end_anchor = if matches!(
        parts.last(),
        Some(Ast::Assertion(assertion))
            if assertion.kind == AssertionKind::EndText
                || (assertion.kind == AssertionKind::EndLine && !multi_line_at_end)
    ) {
        parts.pop()
    } else {
        None
    };
    (start_anchor, parts, end_anchor)
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
            // A multi-line `$` stays within the scope of the flags preceding it.
            ("^(?m)id=[a-z]+$", 0, "^((?m)id=[a-z]+$)"),
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
            (
                r"^(?m)id=[a-z]+$",
                0,
                &["id=abc\nrest", "id=abc", "id=ABC"][..],
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
        // The exact matcher rejects literals outside the capture class.
        assert_eq!(
            substitute_single_capture("^svc-([a-z]+)-prod$", "123").as_deref(),
            Some("svc-123-prod")
        );
        assert!(substitute_single_capture("^svc-([a-z]+)-([a-z]+)$", "api").is_none());
        assert!(substitute_single_capture("^svc-(?:[a-z]+)-prod$", "api").is_none());
        // The capture must be a direct part of the top-level concatenation.
        assert!(substitute_single_capture("x|([a-z]+)", "api").is_none());
        assert!(substitute_single_capture("(?:a([a-z]+))+", "api").is_none());
        assert!(substitute_single_capture("(?:id=([a-z]+))?end", "api").is_none());
        // A multi-line `$` also matches before a newline, so it cannot be stripped.
        assert!(substitute_single_capture("^(?m)id=([a-z]+)$", "abc").is_none());
        // Without a trailing `$`, or when `m` is disabled before it, the prefilter is safe.
        for pattern in ["(?m)id=([a-z]+)", "^(?m)(?-m)id=([a-z]+)$"] {
            let prefilter = substitute_single_capture(pattern, "abc").unwrap();
            assert!(tantivy_fst::Regex::new(&prefilter).is_ok(), "{prefilter}");
        }
    }

    #[test]
    fn test_substitute_single_capture_keeps_literal_whitespace() {
        let prefilter = substitute_single_capture("^(?x)svc - (.+)$", "a b").unwrap();
        assert_eq!(prefilter, "(?x)svc-(?-x:a b)");
        let prefilter_regex = Regex::new(&format!("^(?:{prefilter})$")).unwrap();
        assert!(prefilter_regex.is_match("svc-a b"));
    }
}
