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

use std::sync::Arc;

use tantivy::query::{
    BitSetDocSet, ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use tantivy::{DocId, DocSet, Score, SegmentReader, TantivyError};
use tantivy_common::BitSet;

use super::TantivyQueryAst;

/// Compiled plan evaluating `EQ(REGEXP_EXTRACT(field, pattern, 1), literal)` once per distinct
/// fast-field value instead of once per document.
///
/// Hidden contracts:
/// - `prefilter_automaton` accepts a superset of the values satisfying the predicate.
/// - `fast_field_name` names a non-JSON string fast field. Values are read from the same column,
///   dictionary and first-value rule as the JIT predicate (`load_str_input`), so both paths match
///   the same documents.
/// - `value_matches` must keep the semantics of the jitexpr `REGEXP_EXTRACT` function
///   (`Regex::new(pattern)`, leftmost-first `captures`, group 1).
/// - Unlike the JIT predicate, which treats a column it fails to open as absent, the scorer returns
///   errors from opening the column or reading its dictionary, so a corrupt split fails the search
///   instead of silently matching nothing.
pub(crate) struct RegexExtractEqPlan {
    fast_field_name: String,
    prefilter_regex: String,
    prefilter_automaton: tantivy_fst::Regex,
    extract_regex: regex::Regex,
    literal: String,
}

impl RegexExtractEqPlan {
    /// Compiles the plan, or returns `None` when the predicate must stay on the JIT path.
    ///
    /// FST regexes reject look-arounds, lazy repetitions and byte classes, and cap the automaton
    /// size, so a valid `REGEXP_EXTRACT` pattern may still have no usable prefilter.
    pub(crate) fn new(
        fast_field_name: &str,
        prefilter_regex: String,
        pattern: &str,
        literal: &str,
    ) -> Option<Self> {
        let prefilter_automaton = tantivy_fst::Regex::new(&prefilter_regex).ok()?;
        let extract_regex = regex::Regex::new(pattern).ok()?;
        Some(RegexExtractEqPlan {
            fast_field_name: fast_field_name.to_string(),
            prefilter_regex,
            prefilter_automaton,
            extract_regex,
            literal: literal.to_string(),
        })
    }

    pub(crate) fn fast_field_name(&self) -> &str {
        &self.fast_field_name
    }

    /// Whole-value FST regex accepting a superset of the matching values.
    pub(crate) fn prefilter_regex(&self) -> &str {
        &self.prefilter_regex
    }

    fn value_matches(&self, value_bytes: &[u8]) -> bool {
        let Ok(value) = std::str::from_utf8(value_bytes) else {
            return false;
        };
        let Some(capture) = self
            .extract_regex
            .captures(value)
            .and_then(|captures| captures.get(1))
        else {
            return false;
        };
        capture.as_str() == self.literal
    }

    pub(crate) fn build_query(self) -> TantivyQueryAst {
        RegexExtractEqQuery {
            plan: Arc::new(self),
        }
        .into()
    }
}

/// Matches documents whose first fast-field value satisfies
/// `REGEXP_EXTRACT(value, pattern, 1) == literal`, checking each distinct value once.
#[derive(Clone)]
struct RegexExtractEqQuery {
    plan: Arc<RegexExtractEqPlan>,
}

impl std::fmt::Debug for RegexExtractEqQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("RegexExtractEqQuery")
            .field("fast_field_name", &self.plan.fast_field_name)
            .field("extract_regex", &self.plan.extract_regex.as_str())
            .field("literal", &self.plan.literal)
            .finish()
    }
}

impl Query for RegexExtractEqQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(RegexExtractEqWeight {
            plan: self.plan.clone(),
        }))
    }
}

struct RegexExtractEqWeight {
    plan: Arc<RegexExtractEqPlan>,
}

impl Weight for RegexExtractEqWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let Some(str_column) = reader.fast_fields().str(&self.plan.fast_field_name)? else {
            // Without the column every value is `None`, which `EQ` never matches.
            return Ok(Box::new(EmptyScorer));
        };
        let dictionary = str_column.dictionary();
        let num_values = u32::try_from(dictionary.num_terms()).map_err(|_| {
            TantivyError::InternalError(format!(
                "fast field `{}` has more than u32::MAX distinct values",
                self.plan.fast_field_name
            ))
        })?;
        let mut matching_ords = BitSet::with_max_value(num_values);
        let mut value_stream = dictionary
            .search(&self.plan.prefilter_automaton)
            .into_stream()?;
        while value_stream.advance() {
            if self.plan.value_matches(value_stream.key()) {
                // `term_ord < num_values`, which fits in u32.
                matching_ords.insert(value_stream.term_ord() as u32);
            }
        }
        if matching_ords.len() == 0 {
            return Ok(Box::new(EmptyScorer));
        }
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        let ords = str_column.ords();
        // The JIT predicate evaluates the first value of each document only.
        for doc in 0..max_doc {
            let Some(ord) = ords.first(doc) else {
                continue;
            };
            if matching_ords.contains(ord as u32) {
                doc_bitset.insert(doc);
            }
        }
        let doc_set = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_set, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "document #({doc}) does not match"
            )));
        }
        Ok(Explanation::new("RegexExtractEqQuery", 1.0))
    }
}

#[cfg(test)]
mod tests {
    use tantivy::collector::Count;
    use tantivy::jitexpr::ast::deserialize;
    use tantivy::query::doc_predicate_query::{DocPredicateQuery, JitExprPredicate};
    use tantivy::schema::{FAST, STRING, Schema, TEXT, TextOptions};
    use tantivy::tokenizer::MAX_TOKEN_LEN;
    use tantivy::{Index, TantivyDocument, doc};

    use crate::query_ast::{BuildTantivyAstContext, CalcFieldQuery, QueryAst};

    fn calc_field(expression: &str) -> QueryAst {
        calc_field_query(expression).into()
    }

    fn calc_field_query(expression: &str) -> CalcFieldQuery {
        CalcFieldQuery {
            expression: deserialize(expression).unwrap(),
        }
    }

    #[test]
    fn test_regex_extract_eq_prefilter() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("service", STRING | FAST);
        schema_builder.add_text_field("indexed_only", STRING);
        schema_builder.add_text_field("fast_only", FAST);
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
            r#"(EQ (REGEXP_EXTRACT indexed_only "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT fast_only "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT fast_tokenized "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT fast_lowercased "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            // FST regexes reject look-arounds and lazy repetitions.
            r#"(EQ (REGEXP_EXTRACT service "\\bsvc-([a-z]+)" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "(?m)^svc-([a-z]+)" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "a^svc-([a-z]+)" 1u64) "api")"#,
            r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)-.*?x" 1u64) "api")"#,
        ] {
            assert!(
                calc_field_query(expression)
                    .try_prefilter_regex_query(&schema)
                    .is_none(),
                "{expression}"
            );
        }
    }

    fn jit_count(searcher: &tantivy::Searcher, expression: &str) -> usize {
        let predicate = JitExprPredicate::new(deserialize(expression).unwrap()).unwrap();
        searcher
            .search(&DocPredicateQuery::from(predicate), &Count)
            .unwrap()
    }

    #[test]
    fn test_regex_extract_eq_term_query_matches_jit_predicate() {
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
            // Capture stopped by a character outside its class; leftmost-first extracts.
            "svc-apiX",
            "id=12 id=1",
            "id=1",
            "id=123",
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
                3,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "svc-([a-z]+)" 1u64) "api")"#,
                4,
            ),
            (r#"(EQ (REGEXP_EXTRACT service "id=([0-9]+)" 1u64) "1")"#, 1),
            (
                r#"(EQ (REGEXP_EXTRACT service "id=([0-9]+)" 1u64) "12")"#,
                1,
            ),
            (
                r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "123")"#,
                0,
            ),
        ] {
            assert_eq!(
                jit_count(&searcher, expression),
                expected_count,
                "jit {expression}"
            );
            let calc_field_query_built = calc_field(expression)
                .build_tantivy_query(&context)
                .unwrap();
            let calc_field_count = searcher.search(&*calc_field_query_built, &Count).unwrap();
            assert_eq!(calc_field_count, expected_count, "calc field {expression}");

            let prefilter = calc_field_query(expression).try_prefilter_regex_query(&schema);
            assert_eq!(
                calc_field_query_built
                    .downcast_ref::<DocPredicateQuery>()
                    .is_none(),
                prefilter.is_some(),
                "eligible expressions must not build the JIT query: {expression}"
            );
            let Some(regex_query) = prefilter else {
                continue;
            };
            let regex_only_count = searcher
                .search(
                    &*QueryAst::from(regex_query)
                        .build_tantivy_query(&context)
                        .unwrap(),
                    &Count,
                )
                .unwrap();
            assert!(
                regex_only_count >= expected_count,
                "prefilter must be a superset for {expression}: regex={regex_only_count} \
                 expected={expected_count}"
            );
        }
    }

    /// Indexes one document per entry of `documents` in `field_options` and asserts that the
    /// eligible `expression` matches exactly `expected_count` documents, like the JIT.
    fn assert_regex_extract_eq_matches_jit(
        field_options: TextOptions,
        documents: &[&[&str]],
        expression: &str,
        expected_count: usize,
    ) {
        let mut schema_builder = Schema::builder();
        let service = schema_builder.add_text_field("service", field_options);
        let mut index = Index::create_in_ram(schema_builder.build());
        index.set_fast_field_tokenizers(
            crate::get_quickwit_fastfield_normalizer_manager()
                .tantivy_manager()
                .clone(),
        );
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        for values in documents {
            let mut document = TantivyDocument::default();
            for value in *values {
                document.add_text(service, value);
            }
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();

        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let searcher = index.reader().unwrap().searcher();
        let query = calc_field(expression)
            .build_tantivy_query(&context)
            .unwrap();
        assert!(query.downcast_ref::<DocPredicateQuery>().is_none());
        assert_eq!(jit_count(&searcher, expression), expected_count, "jit");
        assert_eq!(searcher.search(&*query, &Count).unwrap(), expected_count);
    }

    #[test]
    fn test_regex_extract_eq_matches_first_value_only() {
        assert_regex_extract_eq_matches_jit(
            STRING | FAST,
            &[
                &["aaa", "svc-api-prod"],
                &["svc-api-prod", "zzz"],
                &["aaa", "zzz"],
            ],
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            1,
        );
    }

    #[test]
    fn test_regex_extract_eq_matches_documents_without_value() {
        assert_regex_extract_eq_matches_jit(
            STRING | FAST,
            &[
                &[],
                &["svc-api-prod"],
                &[],
                &["svc-web-prod"],
                &["svc-api-prod"],
            ],
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            2,
        );
    }

    #[test]
    fn test_regex_extract_eq_matches_values_longer_than_max_token_len() {
        let long_value = format!("svc-api-prod{}", "x".repeat(MAX_TOKEN_LEN));
        assert_regex_extract_eq_matches_jit(
            STRING | FAST,
            &[&[long_value.as_str()], &["svc-web-prod"]],
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod" 1u64) "api")"#,
            1,
        );
    }

    #[test]
    fn test_regex_extract_eq_matches_normalized_fast_field() {
        assert_regex_extract_eq_matches_jit(
            STRING.set_fast("lowercase"),
            &[&["SVC-API-PROD"], &["svc-api-prod"], &["SVC-WEB-PROD"]],
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#,
            2,
        );
    }
}
