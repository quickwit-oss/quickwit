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

use tantivy::columnar::{Cardinality, Column, StrColumn};
use tantivy::index::InvertedIndexReader;
use tantivy::postings::TermInfo;
use tantivy::query::{
    BitSetDocSet, ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use tantivy::schema::IndexRecordOption;
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED, TantivyError};
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
/// - When `terms_are_fast_field_values` is set, every term of the field is one of its fast-field
///   values: the raw tokenizer may drop a value (such as a long one) but never alters it, and
///   merges drop the values without alive documents from both dictionaries. Callers then warm the
///   term dictionary and postings with `prefilter_regex` before searching.
/// - Unlike the JIT predicate, which treats a column it fails to open as absent, the scorer returns
///   errors from opening the column or reading its dictionary, so a corrupt split fails the search
///   instead of silently matching nothing.
pub(crate) struct RegexExtractEqPlan {
    fast_field_name: String,
    prefilter_regex: String,
    prefilter_automaton: tantivy_fst::Regex,
    extract_regex: regex::Regex,
    literal: String,
    terms_are_fast_field_values: bool,
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
        terms_are_fast_field_values: bool,
    ) -> Option<Self> {
        let prefilter_automaton = tantivy_fst::Regex::new(&prefilter_regex).ok()?;
        let extract_regex = regex::Regex::new(pattern).ok()?;
        Some(RegexExtractEqPlan {
            fast_field_name: fast_field_name.to_string(),
            prefilter_regex,
            prefilter_automaton,
            extract_regex,
            literal: literal.to_string(),
            terms_are_fast_field_values,
        })
    }

    pub(crate) fn fast_field_name(&self) -> &str {
        &self.fast_field_name
    }

    /// Whether the scorer may read the field's term dictionary and postings.
    pub(crate) fn reads_postings(&self) -> bool {
        self.terms_are_fast_field_values
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
        let num_values = u32::try_from(str_column.dictionary().num_terms()).map_err(|_| {
            TantivyError::InternalError(format!(
                "fast field `{}` has more than u32::MAX distinct values",
                self.plan.fast_field_name
            ))
        })?;
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        if let Some(inverted_index) = self.inverted_index_with_all_values(reader, &str_column)? {
            self.collect_from_postings(&inverted_index, &str_column, num_values, &mut doc_bitset)?;
        } else {
            self.collect_from_first_values(&str_column, num_values, max_doc, &mut doc_bitset)?;
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

impl RegexExtractEqWeight {
    /// Returns the field's inverted index when its terms are exactly the fast-field values. Both
    /// dictionaries are then sorted the same way, so term ordinals are fast-field value ordinals.
    fn inverted_index_with_all_values(
        &self,
        reader: &SegmentReader,
        str_column: &StrColumn,
    ) -> tantivy::Result<Option<Arc<InvertedIndexReader>>> {
        if !self.plan.terms_are_fast_field_values {
            return Ok(None);
        }
        let field = reader.schema().get_field(&self.plan.fast_field_name)?;
        let inverted_index = reader.inverted_index(field)?;
        // The terms are a subset of the fast-field values, so equal counts mean the tokenizer
        // dropped no value.
        if inverted_index.terms().num_terms() != str_column.dictionary().num_terms() {
            return Ok(None);
        }
        Ok(Some(inverted_index))
    }

    /// Walks the term dictionary and visits only the documents in the postings of the matching
    /// terms. Requires the terms to be exactly the fast-field values.
    fn collect_from_postings(
        &self,
        inverted_index: &InvertedIndexReader,
        str_column: &StrColumn,
        num_values: u32,
        doc_bitset: &mut BitSet,
    ) -> tantivy::Result<()> {
        let mut matching_ords = BitSet::with_max_value(num_values);
        let mut matching_term_infos: Vec<TermInfo> = Vec::new();
        let mut term_stream = inverted_index
            .terms()
            .search(&self.plan.prefilter_automaton)
            .into_stream()?;
        while term_stream.advance() {
            if self.plan.value_matches(term_stream.key()) {
                // Term ordinals are fast-field value ordinals, lower than `num_values`.
                matching_ords.insert(term_stream.term_ord() as u32);
                matching_term_infos.push(term_stream.value().clone());
            }
        }
        let ords = str_column.ords();
        // A document of a single-valued column holds only the value of the term, which matches.
        let check_first_value = ords.get_cardinality() == Cardinality::Multivalued;
        for term_info in &matching_term_infos {
            let mut postings =
                inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            let mut doc = postings.doc();
            while doc != TERMINATED {
                // A multivalued document may hold the matching value after its first value.
                if !check_first_value || first_value_matches(ords, &matching_ords, doc) {
                    doc_bitset.insert(doc);
                }
                doc = postings.advance();
            }
        }
        Ok(())
    }

    /// Walks the fast-field dictionary and checks the first value of every document.
    fn collect_from_first_values(
        &self,
        str_column: &StrColumn,
        num_values: u32,
        max_doc: DocId,
        doc_bitset: &mut BitSet,
    ) -> tantivy::Result<()> {
        let mut matching_ords = BitSet::with_max_value(num_values);
        let mut value_stream = str_column
            .dictionary()
            .search(&self.plan.prefilter_automaton)
            .into_stream()?;
        while value_stream.advance() {
            if self.plan.value_matches(value_stream.key()) {
                // `term_ord < num_values`, which fits in u32.
                matching_ords.insert(value_stream.term_ord() as u32);
            }
        }
        if matching_ords.len() == 0 {
            return Ok(());
        }
        let ords = str_column.ords();
        for doc in 0..max_doc {
            if first_value_matches(ords, &matching_ords, doc) {
                doc_bitset.insert(doc);
            }
        }
        Ok(())
    }
}

/// The JIT predicate evaluates the first value of each document only.
fn first_value_matches(ords: &Column<u64>, matching_ords: &BitSet, doc: DocId) -> bool {
    let Some(ord) = ords.first(doc) else {
        return false;
    };
    matching_ords.contains(ord as u32)
}

#[cfg(test)]
mod tests {
    use tantivy::collector::Count;
    use tantivy::jitexpr::ast::deserialize;
    use tantivy::query::doc_predicate_query::{DocPredicateQuery, JitExprPredicate};
    use tantivy::schema::{FAST, STRING, Schema, TEXT, TextOptions};
    use tantivy::tokenizer::MAX_TOKEN_LEN;
    use tantivy::{Index, TantivyDocument, doc};

    use crate::DEFAULT_REMOVE_TOKEN_LENGTH;
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
        index.set_tokenizers(
            crate::create_default_quickwit_tokenizer_manager()
                .tantivy_manager()
                .clone(),
        );
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
    fn test_regex_extract_eq_matches_values_around_indexed_len_limit() {
        let expression = r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod" 1u64) "api")"#;
        let value_of_len = |len: usize| format!("svc-api-prod{}", "x".repeat(len - 12));
        let indexed_value = value_of_len(DEFAULT_REMOVE_TOKEN_LENGTH - 1);
        let unindexed_value = value_of_len(DEFAULT_REMOVE_TOKEN_LENGTH);
        // Only indexed matching values: documents are read from the postings.
        assert_regex_extract_eq_matches_jit(
            STRING | FAST,
            &[
                &[indexed_value.as_str()],
                &["svc-api-prod"],
                &["svc-web-prod"],
            ],
            expression,
            2,
        );
        // A matching value missing from the postings: every document is checked.
        assert_regex_extract_eq_matches_jit(
            STRING | FAST,
            &[
                &[indexed_value.as_str()],
                &[unindexed_value.as_str()],
                &["svc-web-prod"],
            ],
            expression,
            2,
        );
    }

    #[test]
    fn test_regex_extract_eq_matches_after_deletes_and_merge() {
        let expression = r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod" 1u64) "api")"#;
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", tantivy::schema::INDEXED);
        let service = schema_builder.add_text_field("service", STRING | FAST);
        let mut index = Index::create_in_ram(schema_builder.build());
        index.set_tokenizers(
            crate::create_default_quickwit_tokenizer_manager()
                .tantivy_manager()
                .clone(),
        );
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
        // The long value is not a term, so the postings are complete only once it is merged away.
        let unindexed_value = format!("svc-api-prod{}", "x".repeat(DEFAULT_REMOVE_TOKEN_LENGTH));
        for (doc_id, value) in [
            (0u64, unindexed_value.as_str()),
            (1, "svc-api-prod"),
            (2, "svc-web-prod"),
        ] {
            writer
                .add_document(doc!(id => doc_id, service => value))
                .unwrap();
        }
        writer.commit().unwrap();
        writer
            .add_document(doc!(id => 3u64, service => "svc-api-prod"))
            .unwrap();
        writer.delete_term(tantivy::Term::from_field_u64(id, 0));
        writer.commit().unwrap();

        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let query = calc_field(expression)
            .build_tantivy_query(&context)
            .unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        assert_eq!(jit_count(&searcher, expression), 2);
        assert_eq!(searcher.search(&*query, &Count).unwrap(), 2);

        let segment_ids = index.searchable_segment_ids().unwrap();
        writer.merge(&segment_ids).wait().unwrap();
        writer.wait_merging_threads().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        assert_eq!(jit_count(&searcher, expression), 2);
        assert_eq!(searcher.search(&*query, &Count).unwrap(), 2);
    }

    #[test]
    fn test_regex_extract_eq_matches_fast_only_field() {
        assert_regex_extract_eq_matches_jit(
            TextOptions::from(FAST),
            &[
                &["aaa", "svc-api-prod"],
                &["svc-api-prod"],
                &["svc-web-prod"],
            ],
            r#"(EQ (REGEXP_EXTRACT service "^svc-([a-z]+)-prod$" 1u64) "api")"#,
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
