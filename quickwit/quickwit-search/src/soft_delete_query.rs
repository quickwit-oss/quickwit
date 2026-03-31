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

use std::fmt;
use std::sync::Arc;

use tantivy::query::{EnableScoring, Exclude, Explanation, Query, QueryClone, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED};

/// A [`DocSet`] backed by a sorted, deduplicated vector of doc IDs.
///
/// Used as the excluding [`DocSet`] argument passed to [`Exclude`] when
/// constructing a scorer inside [`SoftDeleteWeight`].
///
/// # Invariant
///
/// The underlying slice must be sorted in strictly ascending order and free of
/// duplicates. This is guaranteed by [`SoftDeleteQuery::new`], which sorts and
/// deduplicates the input before storing it.
struct SortedDocIdSet {
    doc_ids: Arc<Vec<DocId>>,
    /// Index of the current document inside `doc_ids`.
    cursor: usize,
}

impl SortedDocIdSet {
    fn new(doc_ids: Arc<Vec<DocId>>) -> Self {
        SortedDocIdSet { doc_ids, cursor: 0 }
    }
}

impl DocSet for SortedDocIdSet {
    #[inline]
    fn advance(&mut self) -> DocId {
        self.cursor += 1;
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        // The DocSet contract guarantees seek() is always called with a
        // non-decreasing target, so we only need to scan forward from cursor.
        let remaining = self.doc_ids.get(self.cursor..).unwrap_or(&[]);
        let offset = remaining.partition_point(|&id| id < target);
        self.cursor += offset;
        self.doc()
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.doc_ids.get(self.cursor).copied().unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.doc_ids.len().saturating_sub(self.cursor) as u32
    }
}

/// [`Weight`] produced by [`SoftDeleteQuery`].
///
/// Wraps the inner weight's scorer with [`Exclude`] to filter out
/// soft-deleted doc IDs transparently across all collection paths.
struct SoftDeleteWeight {
    inner: Box<dyn Weight>,
    deleted_doc_ids: Arc<Vec<DocId>>,
}

impl Weight for SoftDeleteWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let inner_scorer = self.inner.scorer(reader, boost)?;
        let excluded = SortedDocIdSet::new(Arc::clone(&self.deleted_doc_ids));
        Ok(Box::new(Exclude::new(inner_scorer, excluded)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        self.inner.explain(reader, doc)
    }
}

/// A tantivy [`Query`] that wraps another query and excludes a fixed set of
/// soft-deleted doc IDs from every result set it produces.
pub(crate) struct SoftDeleteQuery {
    inner: Box<dyn Query>,
    /// Sorted, deduplicated tantivy doc IDs to exclude.
    deleted_doc_ids: Arc<Vec<DocId>>,
}

impl SoftDeleteQuery {
    /// Creates a new [`SoftDeleteQuery`].
    ///
    /// `deleted_doc_ids` may be supplied in any order and may contain
    /// duplicates; this constructor sorts and deduplicates the input.
    pub(crate) fn new(inner: Box<dyn Query>, mut deleted_doc_ids: Vec<DocId>) -> Self {
        deleted_doc_ids.sort_unstable();
        deleted_doc_ids.dedup();
        SoftDeleteQuery {
            inner,
            deleted_doc_ids: Arc::new(deleted_doc_ids),
        }
    }
}

impl Clone for SoftDeleteQuery {
    fn clone(&self) -> Self {
        SoftDeleteQuery {
            inner: self.inner.box_clone(),
            deleted_doc_ids: Arc::clone(&self.deleted_doc_ids),
        }
    }
}

impl fmt::Debug for SoftDeleteQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SoftDeleteQuery")
            .field("inner", &self.inner)
            .field("num_deleted", &self.deleted_doc_ids.len())
            .finish()
    }
}

impl Query for SoftDeleteQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        let inner_weight = self.inner.weight(enable_scoring)?;
        Ok(Box::new(SoftDeleteWeight {
            inner: inner_weight,
            deleted_doc_ids: Arc::clone(&self.deleted_doc_ids),
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.inner.query_terms(visitor);
    }
}

#[cfg(test)]
mod tests {
    use tantivy::collector::Count;
    use tantivy::query::AllQuery;
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{Index, IndexWriter};

    use super::*;

    /// Creates a single-segment, in-RAM index containing `num_docs` documents.
    ///
    /// Returns `(index, reader)`. The tantivy doc IDs are 0-based and
    /// contiguous inside the single segment, so doc ID `k` corresponds to the
    /// (k+1)-th inserted document.
    fn make_index(num_docs: usize) -> tantivy::Result<(Index, tantivy::IndexReader)> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer(15_000_000)?;
        for i in 0..num_docs {
            writer.add_document(tantivy::doc!(text_field => format!("doc {i}")))?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        Ok((index, reader))
    }

    // ── SortedDocIdSet unit tests ─────────────────────────────────────────────

    #[test]
    fn test_sorted_doc_id_set_advance_through_all() {
        let ids = Arc::new(vec![2u32, 5, 8, 11]);
        let mut ds = SortedDocIdSet::new(ids);

        assert_eq!(ds.doc(), 2);
        assert_eq!(ds.advance(), 5);
        assert_eq!(ds.advance(), 8);
        assert_eq!(ds.advance(), 11);
        // Advancing past the last element returns TERMINATED via unwrap_or.
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
        // Subsequent advances keep returning TERMINATED: cursor increments past
        // doc_ids.len(), get() returns None, unwrap_or yields TERMINATED.
        assert_eq!(ds.advance(), TERMINATED);
    }

    #[test]
    fn test_sorted_doc_id_set_empty() {
        let mut ds = SortedDocIdSet::new(Arc::new(vec![]));
        assert_eq!(ds.doc(), TERMINATED);
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.seek(0), TERMINATED);
    }

    #[test]
    fn test_sorted_doc_id_set_seek_exact_hit() {
        let ids = Arc::new(vec![1u32, 3, 7, 10, 15]);
        let mut ds = SortedDocIdSet::new(ids);

        assert_eq!(ds.seek(7), 7);
        assert_eq!(ds.doc(), 7);

        // Seeking to the same target is idempotent.
        assert_eq!(ds.seek(7), 7);

        assert_eq!(ds.seek(10), 10);
        assert_eq!(ds.doc(), 10);
    }

    #[test]
    fn test_sorted_doc_id_set_seek_between_entries() {
        let ids = Arc::new(vec![1u32, 3, 7, 10, 15]);
        let mut ds = SortedDocIdSet::new(ids);

        // Target falls between 3 and 7 → should return 7.
        assert_eq!(ds.seek(4), 7);
        assert_eq!(ds.doc(), 7);

        // Target falls between 10 and 15 → should return 15.
        assert_eq!(ds.seek(11), 15);
        assert_eq!(ds.doc(), 15);
    }

    #[test]
    fn test_sorted_doc_id_set_seek_past_last_entry() {
        let ids = Arc::new(vec![1u32, 3, 7]);
        let mut ds = SortedDocIdSet::new(ids);

        assert_eq!(ds.seek(100), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
    }

    #[test]
    fn test_sorted_doc_id_set_seek_terminated_sentinel() {
        let ids = Arc::new(vec![1u32, 3, 7]);
        let mut ds = SortedDocIdSet::new(ids);

        assert_eq!(ds.seek(TERMINATED), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
    }

    #[test]
    fn test_sorted_doc_id_set_seek_before_current_position() {
        // After advancing past the start, seeking to the current doc must not
        // go backwards.
        let ids = Arc::new(vec![1u32, 5, 9]);
        let mut ds = SortedDocIdSet::new(ids);

        ds.advance(); // cursor → 5
        // Seeking to 5 (= current) must keep returning 5.
        assert_eq!(ds.seek(5), 5);
        assert_eq!(ds.doc(), 5);
    }

    #[test]
    fn test_sorted_doc_id_set_size_hint_decrements() {
        let ids = Arc::new(vec![1u32, 3, 7, 10]);
        let mut ds = SortedDocIdSet::new(ids);

        assert_eq!(ds.size_hint(), 4);
        ds.advance();
        assert_eq!(ds.size_hint(), 3);
        ds.advance();
        ds.advance();
        ds.advance(); // now TERMINATED
        assert_eq!(ds.size_hint(), 0);
    }

    #[test]
    fn test_soft_delete_query_no_deleted_docs() -> tantivy::Result<()> {
        let (_index, reader) = make_index(5)?;
        let searcher = reader.searcher();

        let query = SoftDeleteQuery::new(Box::new(AllQuery), vec![]);
        assert_eq!(searcher.search(&query, &Count)?, 5);
        Ok(())
    }

    #[test]
    fn test_soft_delete_query_excludes_subset() -> tantivy::Result<()> {
        let (_index, reader) = make_index(5)?;
        let searcher = reader.searcher();

        // Delete doc IDs 1 and 3; 0, 2, 4 should remain.
        let query = SoftDeleteQuery::new(Box::new(AllQuery), vec![1, 3]);
        assert_eq!(searcher.search(&query, &Count)?, 3);
        Ok(())
    }

    #[test]
    fn test_soft_delete_query_excludes_all_docs() -> tantivy::Result<()> {
        let (_index, reader) = make_index(3)?;
        let searcher = reader.searcher();

        let query = SoftDeleteQuery::new(Box::new(AllQuery), vec![0, 1, 2]);
        assert_eq!(searcher.search(&query, &Count)?, 0);
        Ok(())
    }

    #[test]
    fn test_soft_delete_query_count_method_matches_search() -> tantivy::Result<()> {
        let (_index, reader) = make_index(10)?;
        let searcher = reader.searcher();

        // Delete every even doc ID.
        let deleted: Vec<u32> = (0..10).filter(|x| x % 2 == 0).collect();
        let query = SoftDeleteQuery::new(Box::new(AllQuery), deleted);

        let count_via_search = searcher.search(&query, &Count)?;
        let count_via_method = query.count(&searcher)?;

        assert_eq!(count_via_search, 5);
        assert_eq!(count_via_method, 5);
        Ok(())
    }
}
