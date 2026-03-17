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
