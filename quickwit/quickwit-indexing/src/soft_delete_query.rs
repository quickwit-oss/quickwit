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

use std::collections::HashMap;

use tantivy::index::SegmentId;
use tantivy::query::{EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED, TantivyError, Term};

/// A tantivy [`Query`] that matches specific doc IDs within their respective segments.
///
/// Built from the `soft_deleted_doc_ids` fields of the input [`SplitMetadata`] structs, this
/// query is passed to [`IndexWriter::delete_query`] so that the matched documents are marked for
/// deletion and then physically removed during the subsequent tantivy merge. The query itself only
/// identifies which documents to remove; the actual deletion is performed by the caller.
#[derive(Clone, Debug)]
pub(crate) struct SoftDeletedDocIdsQuery {
    /// Maps each segment ID to the **sorted** list of doc IDs to delete within that segment.
    docs_per_segment: HashMap<SegmentId, Vec<DocId>>,
}

impl SoftDeletedDocIdsQuery {
    pub(crate) fn new(docs_per_segment: HashMap<SegmentId, Vec<DocId>>) -> Self {
        Self { docs_per_segment }
    }
}

impl Query for SoftDeletedDocIdsQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(SoftDeletedDocIdsWeight {
            docs_per_segment: self.docs_per_segment.clone(),
        }))
    }

    fn query_terms<'a>(&'a self, _visitor: &mut dyn FnMut(&'a Term, bool)) {
        // Doc-ID–based query — no index terms to visit.
    }
}

/// Minimal `DocSet + Scorer` over a pre-sorted, deduplicated list of doc IDs.
///
/// Starts positioned at the first document (no initial `advance()` call required).
struct SortedDocIdScorer {
    doc_ids: Vec<DocId>,
    pos: usize,
}

impl DocSet for SortedDocIdScorer {
    fn advance(&mut self) -> DocId {
        self.pos += 1;
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        // Binary-search to the first id >= target.
        self.pos = self.doc_ids.partition_point(|&id| id < target);
        self.doc()
    }

    fn doc(&self) -> DocId {
        self.doc_ids.get(self.pos).copied().unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.doc_ids.len() as u32
    }
}

impl Scorer for SortedDocIdScorer {
    fn score(&mut self) -> Score {
        1.0
    }
}

struct SoftDeletedDocIdsWeight {
    docs_per_segment: HashMap<SegmentId, Vec<DocId>>,
}

impl Weight for SoftDeletedDocIdsWeight {
    fn scorer(&self, reader: &SegmentReader, _boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let Some(doc_ids) = self.docs_per_segment.get(&reader.segment_id()) else {
            return Ok(Box::new(EmptyScorer));
        };
        // Filter defensively: doc IDs must be < max_doc.  The BTreeSet source guarantees
        // strict ascending order, which SortedDocIdScorer requires.
        let doc_ids: Vec<DocId> = doc_ids
            .iter()
            .copied()
            .filter(|&id| id < reader.max_doc())
            .collect();
        if doc_ids.is_empty() {
            return Ok(Box::new(EmptyScorer));
        }
        Ok(Box::new(SortedDocIdScorer { doc_ids, pos: 0 }))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let is_deleted = self
            .docs_per_segment
            .get(&reader.segment_id())
            .map(|ids| ids.binary_search(&doc).is_ok())
            .unwrap_or(false);
        if is_deleted {
            Ok(Explanation::new("SoftDeletedDocIdsQuery", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(format!(
                "Document #{doc} is not soft-deleted in this segment"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tantivy::collector::TopDocs;
    use tantivy::index::SegmentId;
    use tantivy::query::AllQuery;
    use tantivy::schema::{STORED, Schema, TEXT, Value};
    use tantivy::{Index, IndexWriter, ReloadPolicy, TERMINATED, TantivyDocument, doc};

    use super::*;

    /// Build an in-RAM single-segment index where each entry in `texts` becomes
    /// one stored document.  All documents are committed in a single pass so
    /// tantivy assigns them contiguous doc IDs starting at 0.
    fn make_index(texts: &[&str]) -> tantivy::Result<(Index, tantivy::schema::Field)> {
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer(15_000_000)?;
        for text in texts {
            writer.add_document(doc!(body => *text))?;
        }
        writer.commit()?;
        Ok((index, body))
    }

    /// Apply `query` via `IndexWriter::delete_query`, commit, and return a
    /// freshly-opened reader that reflects the resulting deletion state.
    fn apply_delete_query(
        index: &Index,
        query: SoftDeletedDocIdsQuery,
    ) -> tantivy::Result<tantivy::IndexReader> {
        let mut writer: IndexWriter = index.writer(15_000_000)?;
        writer.delete_query(Box::new(query))?;
        writer.commit()?;
        index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
    }

    /// Collect and sort the stored body values of all live documents so that
    /// tests can assert on the exact surviving content, independent of score
    /// ordering.
    fn live_bodies(
        reader: &tantivy::IndexReader,
        body: tantivy::schema::Field,
    ) -> tantivy::Result<Vec<String>> {
        let searcher = reader.searcher();
        let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(1_000).order_by_score())?;
        let mut texts: Vec<String> = top_docs
            .iter()
            .map(|(_, addr)| {
                let doc: TantivyDocument = searcher.doc(*addr).unwrap();
                doc.get_first(body)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        texts.sort();
        Ok(texts)
    }

    #[test]
    fn test_scorer_starts_at_first_doc() {
        let scorer = SortedDocIdScorer {
            doc_ids: vec![3, 7, 12],
            pos: 0,
        };
        assert_eq!(
            scorer.doc(),
            3,
            "scorer must be positioned at the first doc on creation"
        );
    }

    #[test]
    fn test_scorer_advance_iterates_all_docs() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![2u32, 5, 8, 11],
            pos: 0,
        };

        assert_eq!(scorer.doc(), 2);
        assert_eq!(scorer.advance(), 5);
        assert_eq!(scorer.advance(), 8);
        assert_eq!(scorer.advance(), 11);
        assert_eq!(scorer.advance(), TERMINATED);
        // Subsequent advances keep returning TERMINATED.
        assert_eq!(scorer.doc(), TERMINATED);
        assert_eq!(scorer.advance(), TERMINATED);
    }

    #[test]
    fn test_scorer_advance_on_empty_list() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![],
            pos: 0,
        };
        assert_eq!(scorer.doc(), TERMINATED);
        assert_eq!(scorer.advance(), TERMINATED);
    }

    #[test]
    fn test_scorer_seek_exact_hit() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![1u32, 3, 7, 10, 15],
            pos: 0,
        };

        assert_eq!(scorer.seek(7), 7);
        assert_eq!(scorer.doc(), 7);
        // Seeking to the same target again is idempotent.
        assert_eq!(scorer.seek(7), 7);
        assert_eq!(scorer.seek(15), 15);
        assert_eq!(scorer.doc(), 15);
    }

    #[test]
    fn test_scorer_seek_between_entries() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![1u32, 3, 7, 10, 15],
            pos: 0,
        };
        // Target falls between 3 and 7 → should land on 7.
        assert_eq!(scorer.seek(4), 7);
        assert_eq!(scorer.doc(), 7);
        // Target falls between 10 and 15 → should land on 15.
        assert_eq!(scorer.seek(11), 15);
        assert_eq!(scorer.doc(), 15);
    }

    #[test]
    fn test_scorer_seek_past_end_returns_terminated() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![1u32, 3, 7],
            pos: 0,
        };
        assert_eq!(scorer.seek(100), TERMINATED);
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn test_scorer_seek_terminated_sentinel() {
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![1u32, 3, 7],
            pos: 0,
        };
        assert_eq!(scorer.seek(TERMINATED), TERMINATED);
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn test_scorer_size_hint_returns_total_doc_count() {
        // size_hint reports the total number of doc IDs, not the remaining ones.
        let mut scorer = SortedDocIdScorer {
            doc_ids: vec![1u32, 3, 7, 10],
            pos: 0,
        };
        assert_eq!(scorer.size_hint(), 4);
        scorer.advance();
        assert_eq!(
            scorer.size_hint(),
            4,
            "size_hint must remain constant regardless of position"
        );
    }

    #[test]
    fn test_explain_matched_doc_returns_ok() -> tantivy::Result<()> {
        let (index, _) = make_index(&["alpha", "beta", "gamma"])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let seg_reader = &searcher.segment_readers()[0];
        let segment_id = seg_reader.segment_id();

        let weight = SoftDeletedDocIdsWeight {
            docs_per_segment: HashMap::from([(segment_id, vec![0u32, 2u32])]),
        };

        // Docs 0 and 2 are targeted → explain must succeed.
        assert!(weight.explain(seg_reader, 0).is_ok());
        assert!(weight.explain(seg_reader, 2).is_ok());
        Ok(())
    }

    #[test]
    fn test_explain_unmatched_doc_returns_err() -> tantivy::Result<()> {
        let (index, _) = make_index(&["alpha", "beta", "gamma"])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let seg_reader = &searcher.segment_readers()[0];
        let segment_id = seg_reader.segment_id();

        let weight = SoftDeletedDocIdsWeight {
            docs_per_segment: HashMap::from([(segment_id, vec![0u32, 2u32])]),
        };

        // Doc 1 is not targeted → explain must return an error.
        assert!(weight.explain(seg_reader, 1).is_err());
        Ok(())
    }

    #[test]
    fn test_explain_unknown_segment_returns_err() -> tantivy::Result<()> {
        let (index, _) = make_index(&["alpha"])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let seg_reader = &searcher.segment_readers()[0];

        // Weight has no entry for any segment → every doc must produce an error.
        let weight = SoftDeletedDocIdsWeight {
            docs_per_segment: HashMap::new(),
        };
        assert!(weight.explain(seg_reader, 0).is_err());
        Ok(())
    }

    #[test]
    fn test_delete_query_removes_targeted_docs() -> tantivy::Result<()> {
        let (index, _) = make_index(&["a", "b", "c", "d", "e"])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let seg_readers = searcher.segment_readers();
        assert_eq!(
            seg_readers.len(),
            1,
            "expected a single segment after one commit"
        );
        let segment_id = seg_readers[0].segment_id();
        drop(searcher);

        // Target doc IDs 1 ("b") and 3 ("d").
        let query = SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![1u32, 3u32])]));
        let reader_after = apply_delete_query(&index, query)?;
        let searcher_after = reader_after.searcher();
        let seg = &searcher_after.segment_readers()[0];

        assert_eq!(seg.num_docs(), 3, "exactly 3 docs must survive");
        Ok(())
    }

    #[test]
    fn test_delete_query_leaves_correct_docs_alive() -> tantivy::Result<()> {
        let (index, body) = make_index(&["a", "b", "c", "d", "e"])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let segment_id = {
            let searcher = reader.searcher();
            searcher.segment_readers()[0].segment_id()
        };

        // Delete docs 1 ("b") and 3 ("d"); "a", "c", "e" must survive.
        let query = SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![1u32, 3u32])]));
        let reader_after = apply_delete_query(&index, query)?;

        let surviving = live_bodies(&reader_after, body)?;
        assert_eq!(surviving, vec!["a", "c", "e"]);
        Ok(())
    }

    #[test]
    fn test_delete_query_removes_all_docs() -> tantivy::Result<()> {
        let (index, _) = make_index(&["x", "y", "z"])?;
        let segment_id = {
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();
            searcher.segment_readers()[0].segment_id()
        };

        let query =
            SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![0u32, 1u32, 2u32])]));
        let reader_after = apply_delete_query(&index, query)?;
        let searcher_after = reader_after.searcher();

        let total_live_docs: u32 = searcher_after
            .segment_readers()
            .iter()
            .map(|r| r.num_docs())
            .sum();
        assert_eq!(total_live_docs, 0, "all docs must be deleted");
        Ok(())
    }

    #[test]
    fn test_delete_query_boundary_doc_ids() -> tantivy::Result<()> {
        // Deleting the very first (0) and very last (3) doc IDs exercises the boundary
        // positions of SortedDocIdScorer.
        let (index, body) = make_index(&["a", "b", "c", "d"])?;
        let segment_id = {
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();
            searcher.segment_readers()[0].segment_id()
        };

        let query = SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![0u32, 3u32])]));
        let reader_after = apply_delete_query(&index, query)?;

        let surviving = live_bodies(&reader_after, body)?;
        assert_eq!(surviving, vec!["b", "c"]);
        Ok(())
    }

    #[test]
    fn test_delete_query_single_doc() -> tantivy::Result<()> {
        let (index, body) = make_index(&["keep", "remove", "keep-too"])?;
        let segment_id = {
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();
            searcher.segment_readers()[0].segment_id()
        };

        let query = SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![1u32])]));
        let reader_after = apply_delete_query(&index, query)?;

        let surviving = live_bodies(&reader_after, body)?;
        assert_eq!(surviving, vec!["keep", "keep-too"]);
        Ok(())
    }

    #[test]
    fn test_delete_query_unknown_segment_id_has_no_effect() -> tantivy::Result<()> {
        let (index, _) = make_index(&["a", "b", "c"])?;

        // Obtain a segment ID that definitely does not belong to `index` by
        // creating an independent second index.
        let (other_index, _) = make_index(&["z"])?;
        let foreign_id: SegmentId = {
            let other_reader: tantivy::IndexReader = other_index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let other_searcher = other_reader.searcher();
            other_searcher.segment_readers()[0].segment_id()
        };

        // Targeting all three doc IDs under the foreign segment must not delete anything.
        let query =
            SoftDeletedDocIdsQuery::new(HashMap::from([(foreign_id, vec![0u32, 1u32, 2u32])]));
        let reader_after = apply_delete_query(&index, query)?;
        let searcher_after = reader_after.searcher();

        assert_eq!(
            searcher_after.segment_readers()[0].num_docs(),
            3,
            "unknown segment ID must leave all docs intact"
        );
        Ok(())
    }

    #[test]
    fn test_delete_query_out_of_range_doc_ids_are_ignored() -> tantivy::Result<()> {
        // The index has 2 docs (max_doc = 2, valid IDs are 0 and 1).
        // Providing only out-of-range IDs must not delete anything.
        let (index, _) = make_index(&["a", "b"])?;
        let segment_id = {
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();
            searcher.segment_readers()[0].segment_id()
        };

        let query =
            SoftDeletedDocIdsQuery::new(HashMap::from([(segment_id, vec![10u32, 20u32, 100u32])]));
        let reader_after = apply_delete_query(&index, query)?;
        let searcher_after = reader_after.searcher();

        assert_eq!(
            searcher_after.segment_readers()[0].num_docs(),
            2,
            "out-of-range doc IDs must be silently ignored"
        );
        Ok(())
    }

    #[test]
    fn test_delete_query_empty_map_has_no_effect() -> tantivy::Result<()> {
        let (index, _) = make_index(&["a", "b", "c"])?;
        let query = SoftDeletedDocIdsQuery::new(HashMap::new());
        let reader_after = apply_delete_query(&index, query)?;
        let searcher_after = reader_after.searcher();

        assert_eq!(
            searcher_after.segment_readers()[0].num_docs(),
            3,
            "empty docs-per-segment map must delete nothing"
        );
        Ok(())
    }
}
