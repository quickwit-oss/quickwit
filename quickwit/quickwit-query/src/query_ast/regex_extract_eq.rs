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

use std::ops::RangeInclusive;
use std::sync::Arc;

use tantivy::columnar::{Cardinality, Column, ColumnValues};
use tantivy::query::{
    BitSetDocSet, ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use tantivy::{DocId, DocSet, Score, SegmentReader, TantivyError};
use tantivy_common::BitSet;

use super::TantivyQueryAst;

/// Above this many runs of consecutive matching ordinals, one block-decoding pass over the column
/// is cheaper than one range scan per run.
const MAX_ORD_RANGE_SCANS: usize = 4;
const DECODE_BLOCK_LEN: usize = 1024;

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
        let mut matching_ords = MatchingOrds::new(num_values);
        let mut value_stream = dictionary
            .search(&self.plan.prefilter_automaton)
            .into_stream()?;
        while value_stream.advance() {
            if self.plan.value_matches(value_stream.key()) {
                matching_ords.insert(value_stream.term_ord());
            }
        }
        if matching_ords.is_empty() {
            return Ok(Box::new(EmptyScorer));
        }
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        collect_first_value_matches(str_column.ords(), &matching_ords, max_doc, &mut doc_bitset);
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

/// Ordinals of the fast-field values satisfying the predicate.
struct MatchingOrds {
    bitset: BitSet,
    /// Runs of consecutive matching ordinals, or `None` once there are more than
    /// `MAX_ORD_RANGE_SCANS` of them.
    runs: Option<Vec<RangeInclusive<u64>>>,
}

impl MatchingOrds {
    fn new(num_values: u32) -> Self {
        MatchingOrds {
            bitset: BitSet::with_max_value(num_values),
            runs: Some(Vec::new()),
        }
    }

    /// `ord` must be lower than `num_values` and greater than every previously inserted ordinal,
    /// which holds when inserting in dictionary stream order.
    fn insert(&mut self, ord: u64) {
        self.bitset.insert(ord as u32);
        let Some(runs) = &mut self.runs else {
            return;
        };
        if let Some(last_run) = runs.last_mut()
            && *last_run.end() + 1 == ord
        {
            *last_run = *last_run.start()..=ord;
            return;
        }
        if runs.len() == MAX_ORD_RANGE_SCANS {
            self.runs = None;
            return;
        }
        runs.push(ord..=ord);
    }

    fn contains(&self, ord: u64) -> bool {
        self.bitset.contains(ord as u32)
    }

    fn is_empty(&self) -> bool {
        self.bitset.len() == 0
    }
}

/// How documents are selected from the ordinal column.
#[derive(Debug)]
enum OrdScanStrategy<'a> {
    /// One range scan per run of matching ordinals.
    RangeScans(&'a [RangeInclusive<u64>]),
    /// One pass decoding ordinals in blocks.
    BlockDecoding,
    /// One first-value lookup per document.
    FirstValueLoop,
}

fn ord_scan_strategy(
    cardinality: Cardinality,
    matching_ords: &MatchingOrds,
) -> OrdScanStrategy<'_> {
    match (cardinality, &matching_ords.runs) {
        // A range scan returns documents having *any* value in the run, which is their first
        // value only when documents have at most one value.
        (Cardinality::Full | Cardinality::Optional, Some(runs)) => {
            OrdScanStrategy::RangeScans(runs)
        }
        // Row ids are doc ids only when every document has exactly one value.
        (Cardinality::Full, None) => OrdScanStrategy::BlockDecoding,
        (Cardinality::Optional, None) | (Cardinality::Multivalued, _) => {
            OrdScanStrategy::FirstValueLoop
        }
    }
}

/// Inserts into `doc_bitset` every document whose first value ordinal is in `matching_ords`,
/// which is the value the JIT predicate evaluates.
fn collect_first_value_matches(
    ords: &Column<u64>,
    matching_ords: &MatchingOrds,
    max_doc: DocId,
    doc_bitset: &mut BitSet,
) {
    match ord_scan_strategy(ords.get_cardinality(), matching_ords) {
        OrdScanStrategy::RangeScans(runs) => {
            collect_by_range_scans(ords, runs, max_doc, doc_bitset)
        }
        OrdScanStrategy::BlockDecoding => {
            collect_by_block_decoding(ords, matching_ords, doc_bitset)
        }
        OrdScanStrategy::FirstValueLoop => {
            collect_by_first_value_loop(ords, matching_ords, max_doc, doc_bitset)
        }
    }
}

/// Requires a column with at most one value per document.
fn collect_by_range_scans(
    ords: &Column<u64>,
    runs: &[RangeInclusive<u64>],
    max_doc: DocId,
    doc_bitset: &mut BitSet,
) {
    let mut docs: Vec<DocId> = Vec::new();
    for run in runs {
        docs.clear();
        ords.get_docids_for_value_range(run.clone(), 0..max_doc, &mut docs);
        for &doc in &docs {
            doc_bitset.insert(doc);
        }
    }
}

/// Requires a column with exactly one value per document.
fn collect_by_block_decoding(
    ords: &Column<u64>,
    matching_ords: &MatchingOrds,
    doc_bitset: &mut BitSet,
) {
    let num_rows = ords.values.num_vals();
    let mut block = [0u64; DECODE_BLOCK_LEN];
    for block_start in (0..num_rows).step_by(DECODE_BLOCK_LEN) {
        let block_len = (num_rows - block_start).min(DECODE_BLOCK_LEN as u32) as usize;
        let block = &mut block[..block_len];
        ords.values.get_range(block_start as u64, block);
        for (offset, &ord) in block.iter().enumerate() {
            if matching_ords.contains(ord) {
                doc_bitset.insert(block_start + offset as u32);
            }
        }
    }
}

fn collect_by_first_value_loop(
    ords: &Column<u64>,
    matching_ords: &MatchingOrds,
    max_doc: DocId,
    doc_bitset: &mut BitSet,
) {
    for doc in 0..max_doc {
        let Some(ord) = ords.first(doc) else {
            continue;
        };
        if matching_ords.contains(ord) {
            doc_bitset.insert(doc);
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::columnar::{Cardinality, StrColumn};
    use tantivy::schema::{FAST, STRING, Schema};
    use tantivy::{DocId, Index, TantivyDocument};
    use tantivy_common::BitSet;

    use super::{
        MAX_ORD_RANGE_SCANS, MatchingOrds, OrdScanStrategy, collect_by_block_decoding,
        collect_by_first_value_loop, collect_by_range_scans, collect_first_value_matches,
        ord_scan_strategy,
    };

    const NUM_VALUES: u64 = 10;

    /// Builds a one-segment string column. Values `v0`..`v9` get ordinals 0..9 when all present.
    fn build_str_column(documents: &[Vec<String>]) -> (StrColumn, DocId) {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("field", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 15_000_000)
            .unwrap();
        for values in documents {
            let mut document = TantivyDocument::default();
            for value in values {
                document.add_text(field, value);
            }
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let str_column = segment_reader.fast_fields().str("field").unwrap().unwrap();
        assert_eq!(str_column.dictionary().num_terms() as u64, NUM_VALUES);
        (str_column, segment_reader.max_doc())
    }

    /// 40 documents cycling through `v0`..`v9`; every third one has no value if `with_missing`.
    fn single_valued_documents(with_missing: bool) -> Vec<Vec<String>> {
        (0..40u64)
            .map(|doc| {
                if with_missing && doc % 3 == 2 {
                    return Vec::new();
                }
                vec![format!("v{}", (doc * 7) % NUM_VALUES)]
            })
            .collect()
    }

    fn matching_ords(ords: &[u64]) -> MatchingOrds {
        let mut matching_ords = MatchingOrds::new(NUM_VALUES as u32);
        for &ord in ords {
            matching_ords.insert(ord);
        }
        matching_ords
    }

    fn bitset_docs(bitset: &BitSet, max_doc: DocId) -> Vec<DocId> {
        (0..max_doc).filter(|&doc| bitset.contains(doc)).collect()
    }

    #[test]
    fn test_matching_ords_tracks_runs_up_to_limit() {
        let ords = matching_ords(&[0, 1, 2, 5, 7, 8]);
        assert_eq!(ords.runs, Some(vec![0..=2, 5..=5, 7..=8]));
        assert!(ords.contains(5) && !ords.contains(6));

        let separate_ords: Vec<u64> = (0..MAX_ORD_RANGE_SCANS as u64).map(|i| i * 2).collect();
        assert_eq!(
            matching_ords(&separate_ords).runs.map(|runs| runs.len()),
            Some(MAX_ORD_RANGE_SCANS)
        );
        let too_many_ords: Vec<u64> = (0..=MAX_ORD_RANGE_SCANS as u64).map(|i| i * 2).collect();
        assert_eq!(matching_ords(&too_many_ords).runs, None);
        assert!(matching_ords(&[]).is_empty());
    }

    #[test]
    fn test_ord_scan_strategy() {
        let few_runs = matching_ords(&[1, 2, 5]);
        let many_runs = matching_ords(&[0, 2, 4, 6, 8]);
        for (cardinality, ords, expected) in [
            (Cardinality::Full, &few_runs, "range_scans"),
            (Cardinality::Optional, &few_runs, "range_scans"),
            (Cardinality::Multivalued, &few_runs, "first_value_loop"),
            (Cardinality::Full, &many_runs, "block_decoding"),
            (Cardinality::Optional, &many_runs, "first_value_loop"),
            (Cardinality::Multivalued, &many_runs, "first_value_loop"),
        ] {
            let strategy = match ord_scan_strategy(cardinality, ords) {
                OrdScanStrategy::RangeScans(_) => "range_scans",
                OrdScanStrategy::BlockDecoding => "block_decoding",
                OrdScanStrategy::FirstValueLoop => "first_value_loop",
            };
            assert_eq!(strategy, expected, "{cardinality:?} {:?}", ords.runs);
        }
    }

    #[test]
    fn test_strategies_match_first_value_loop() {
        for with_missing in [false, true] {
            let documents = single_valued_documents(with_missing);
            let (str_column, max_doc) = build_str_column(&documents);
            let ords = str_column.ords();
            let expected_cardinality = if with_missing {
                Cardinality::Optional
            } else {
                Cardinality::Full
            };
            assert_eq!(ords.get_cardinality(), expected_cardinality);

            for matching in [&[3][..], &[2, 3, 4], &[1, 4, 7], &[0, 2, 4, 6, 8], &[9]] {
                let matching_ords = matching_ords(matching);
                // Document `doc` holds `v{(doc * 7) % 10}`, whose ordinal is that same number.
                let expected: Vec<DocId> = (0..max_doc)
                    .filter(|&doc| {
                        !documents[doc as usize].is_empty()
                            && matching.contains(&((doc as u64 * 7) % NUM_VALUES))
                    })
                    .collect();

                let mut loop_bitset = BitSet::with_max_value(max_doc);
                collect_by_first_value_loop(ords, &matching_ords, max_doc, &mut loop_bitset);
                assert_eq!(bitset_docs(&loop_bitset, max_doc), expected, "{matching:?}");

                let mut dispatched_bitset = BitSet::with_max_value(max_doc);
                collect_first_value_matches(ords, &matching_ords, max_doc, &mut dispatched_bitset);
                assert_eq!(bitset_docs(&dispatched_bitset, max_doc), expected);

                if let Some(runs) = &matching_ords.runs {
                    let mut range_bitset = BitSet::with_max_value(max_doc);
                    collect_by_range_scans(ords, runs, max_doc, &mut range_bitset);
                    assert_eq!(
                        bitset_docs(&range_bitset, max_doc),
                        expected,
                        "{matching:?}"
                    );
                }
                if !with_missing {
                    let mut block_bitset = BitSet::with_max_value(max_doc);
                    collect_by_block_decoding(ords, &matching_ords, &mut block_bitset);
                    assert_eq!(
                        bitset_docs(&block_bitset, max_doc),
                        expected,
                        "{matching:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_multivalued_column_uses_first_value_loop() {
        let documents: Vec<Vec<String>> = (0..NUM_VALUES)
            .map(|i| vec![format!("v{i}"), format!("v{}", (i + 1) % NUM_VALUES)])
            .collect();
        let (str_column, max_doc) = build_str_column(&documents);
        let ords = str_column.ords();
        assert_eq!(ords.get_cardinality(), Cardinality::Multivalued);

        let matching_ords = matching_ords(&[3]);
        assert!(matches!(
            ord_scan_strategy(ords.get_cardinality(), &matching_ords),
            OrdScanStrategy::FirstValueLoop
        ));
        let mut dispatched_bitset = BitSet::with_max_value(max_doc);
        collect_first_value_matches(ords, &matching_ords, max_doc, &mut dispatched_bitset);
        let expected: Vec<DocId> = (0..max_doc)
            .filter(|&doc| ords.first(doc) == Some(3))
            .collect();
        assert_eq!(expected.len(), 1);
        assert_eq!(bitset_docs(&dispatched_bitset, max_doc), expected);

        // A range scan would also select the document holding `v3` as its second value.
        let mut range_bitset = BitSet::with_max_value(max_doc);
        collect_by_range_scans(ords, &[3..=3], max_doc, &mut range_bitset);
        assert_eq!(bitset_docs(&range_bitset, max_doc).len(), 2);
    }
}
