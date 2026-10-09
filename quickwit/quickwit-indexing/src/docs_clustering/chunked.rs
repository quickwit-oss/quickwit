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

//! Chunked document clustering: order documents *before* they reach the index writer.
//!
//! The default clustering mode records every document's fingerprint and, when the split is
//! finalized, rewrites the whole segment in clustered order (`finalize_with_doc_id_mapping`).
//! That rewrite re-sorts every postings list and fast-field column, and goes through an
//! uncompressed temporary doc store of roughly one byte per source byte.
//!
//! In chunked mode, the indexer buffers up to `chunk_num_docs` documents per split, orders the
//! buffer with the same [`DocIdClusterer`] logic, then adds the documents to the index writer in
//! that order. The segment is built clustered: it is finalized without any doc id mapping and
//! without a temporary doc store.
//!
//! Locality is per chunk instead of per split. On OTel logs, 1M-document chunks keep about 99%
//! of the doc-store size reduction of split-wide clustering.

use std::mem;

use super::{DocIdClusterer, Fingerprint};

/// Buffers documents and releases them in clustered order, one chunk at a time.
pub struct ChunkedDocsClusterer<T> {
    chunk_num_docs: usize,
    pending: Vec<(Option<Fingerprint>, T)>,
}

impl<T> ChunkedDocsClusterer<T> {
    pub fn new(chunk_num_docs: usize) -> Self {
        assert!(chunk_num_docs > 0, "chunk size must be positive");
        Self {
            chunk_num_docs,
            pending: Vec::new(),
        }
    }

    /// Buffers a document. Returns true when the chunk is full and should be drained.
    pub fn push(&mut self, fingerprint_opt: Option<Fingerprint>, item: T) -> bool {
        self.pending.push((fingerprint_opt, item));
        self.pending.len() >= self.chunk_num_docs
    }

    pub fn num_pending(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Takes the buffered documents in clustered order.
    ///
    /// The order is exactly the one [`DocIdClusterer`] would produce for these documents:
    /// largest fingerprint groups first at every level, documents without a fingerprint last,
    /// insertion order within a group.
    pub fn drain_in_cluster_order(&mut self) -> Vec<T> {
        let pending = mem::take(&mut self.pending);
        let mut clusterer = DocIdClusterer::default();
        let mut items: Vec<Option<T>> = Vec::with_capacity(pending.len());
        for (local_id, (fingerprint_opt, item)) in pending.into_iter().enumerate() {
            clusterer.push(fingerprint_opt, local_id as u32);
            items.push(Some(item));
        }
        clusterer
            .into_sorted_doc_ids()
            .into_iter()
            .map(|local_id| {
                items[local_id as usize]
                    .take()
                    .expect("each local id is emitted exactly once")
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkedDocsClusterer;
    use crate::docs_clustering::{DocIdClusterer, Fingerprint};

    #[test]
    fn test_chunked_clusterer_signals_full_chunk() {
        let mut clusterer = ChunkedDocsClusterer::new(3);
        assert!(!clusterer.push(None, 0));
        assert!(!clusterer.push(None, 1));
        assert!(clusterer.push(None, 2));
        assert_eq!(clusterer.num_pending(), 3);
        assert_eq!(clusterer.drain_in_cluster_order(), vec![0, 1, 2]);
        assert!(clusterer.is_empty());
    }

    #[test]
    fn test_chunked_clusterer_matches_doc_id_clusterer_order() {
        let fingerprints = [
            Some(Fingerprint::new([1, 1])),
            Some(Fingerprint::new([2, 1])),
            None,
            Some(Fingerprint::new([2, 2])),
            Some(Fingerprint::new([1, 1])),
            Some(Fingerprint::new([2, 1])),
            Some(Fingerprint::new([2, 1])),
            None,
        ];
        let mut chunked = ChunkedDocsClusterer::new(100);
        let mut reference = DocIdClusterer::default();
        for (doc_id, fingerprint) in fingerprints.iter().enumerate() {
            chunked.push(fingerprint.clone(), doc_id as u32);
            reference.push(fingerprint.clone(), doc_id as u32);
        }
        let expected = reference.into_sorted_doc_ids();
        assert_eq!(chunked.drain_in_cluster_order(), expected);
        // Group [2, 1] (3 docs) comes before [1, 1] (2 docs), unfingerprinted docs last.
        assert_eq!(expected, vec![1, 5, 6, 3, 0, 4, 2, 7]);
    }
}
