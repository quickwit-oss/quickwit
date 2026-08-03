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

//! Document sorting from per-document fingerprints.
//!
//! `DocIdClusterer` is the bridge between per-document fingerprints and Tantivy doc ID remapping.
//! The `Indexer` pushes each document's original doc ID with its optional fingerprint. During
//! split finalization, the clusterer turns those groups into the `DocIdMapping` used to write the
//! final segment layout.
//!
//! ```text
//! incoming docs in indexing order:
//!     doc 0 -> hashes [A, X]
//!     doc 1 -> hashes [A, Y]
//!     doc 2 -> hashes [B, X]
//!     doc 3 -> no fingerprint
//!     doc 4 -> hashes [A, X]
//!
//! sort groups:
//!     A:
//!       X: [0, 4]
//!       Y: [1]
//!     B:
//!       X: [2]
//!     unsorted: [3]
//!
//! final mapping:
//!     [0, 4, 1, 2, 3]
//! ```
//!
//! Groups are emitted largest first at every fingerprint level. Documents without a fingerprint are
//! kept in insertion order after the fingerprinted sort groups.

use std::cmp::Reverse;
use std::mem;

use fnv::FnvHashMap;
use quickwit_metrics::{Labels, histogram};
use smallvec::SmallVec;
use tantivy::DocId;
use tantivy::indexer::DocIdMapping;

use super::Fingerprint;
use crate::metrics::DOCS_SORT_GROUP_SIZE;

// We inline as many DocIds as possible to avoid heap allocations.
// This is done by calculating the inline capacity based on the size of the Vec<DocId> and the
// SmallVec<[DocId; 0]> overhead.
const DOC_IDS_INLINE_CAPACITY: usize = (mem::size_of::<Vec<DocId>>()
    - mem::size_of::<SmallVec<[DocId; 0]>>())
    / mem::size_of::<DocId>();
type ClusterDocIds = SmallVec<[DocId; DOC_IDS_INLINE_CAPACITY]>;
const _: () = assert!(mem::size_of::<ClusterDocIds>() == mem::size_of::<Vec<DocId>>());

#[derive(Default)]
pub struct DocIdClusterer {
    root: ClusterGroup,
    unclustered_docs: ClusterDocIds,
}

/// A node in the fingerprint prefix tree.
///
/// Each level of the tree partitions documents by one fingerprint hash. Internal nodes hold their
/// children keyed by the hash at their level; leaf nodes (documents that share the exact same
/// fingerprint) hold the corresponding doc IDs in insertion order.
#[derive(Default)]
struct ClusterGroup {
    num_docs: usize,
    doc_ids: ClusterDocIds,
    children: FnvHashMap<u64, ClusterGroup>,
}

impl DocIdClusterer {
    pub fn push(&mut self, fingerprint_opt: Option<Fingerprint>, doc_id: DocId) {
        match fingerprint_opt {
            Some(fingerprint) => self.root.push(&fingerprint, doc_id),
            None => self.unclustered_docs.push(doc_id),
        }
    }

    pub fn into_doc_id_mapping(self) -> anyhow::Result<DocIdMapping> {
        let doc_ids = self.into_sorted_doc_ids();
        let doc_id_mapping = DocIdMapping::new_permutation(doc_ids)?;
        Ok(doc_id_mapping)
    }

    /// Internal iteration avoids heap allocations and the complex traversal state required by an
    /// external iterator over the recursive cluster tree.
    pub(crate) fn observe_cluster_group_sizes<const N: usize>(&self, labels: Labels<N>) {
        let h = histogram!(parent: DOCS_SORT_GROUP_SIZE, labels: [labels]);
        self.root.for_each_leaf(&mut |cluster_group_doc_ids| {
            h.observe(cluster_group_doc_ids.len() as f64);
        });
    }

    fn into_sorted_doc_ids(self) -> Vec<DocId> {
        let mut doc_ids = Vec::with_capacity(self.root.num_docs + self.unclustered_docs.len());
        self.root.append_sorted_doc_ids(&mut doc_ids);
        doc_ids.extend_from_slice(&self.unclustered_docs);
        doc_ids
    }
}

impl ClusterGroup {
    fn push(&mut self, fingerprint: &[u64], doc_id: DocId) {
        self.num_docs += 1;
        match fingerprint.split_first() {
            Some((fingerprint_hash, rest)) => {
                self.children
                    .entry(*fingerprint_hash)
                    .or_default()
                    .push(rest, doc_id);
            }
            None => self.doc_ids.push(doc_id),
        }
    }

    fn append_sorted_doc_ids(self, doc_ids: &mut Vec<DocId>) {
        doc_ids.extend_from_slice(&self.doc_ids);
        let mut children: Vec<ClusterGroup> = self.children.into_values().collect();
        // Larger groups are emitted first.
        children.sort_unstable_by_key(|child| Reverse(child.num_docs));
        for child in children {
            child.append_sorted_doc_ids(doc_ids);
        }
    }

    fn for_each_leaf(&self, f: &mut impl FnMut(&[DocId])) {
        if !self.doc_ids.is_empty() {
            f(&self.doc_ids);
        }
        for child in self.children.values() {
            child.for_each_leaf(f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DocIdClusterer, Fingerprint};

    fn fingerprint<const N: usize>(hashes: [u64; N]) -> Fingerprint {
        Fingerprint::new(hashes)
    }

    #[test]
    fn emits_largest_schema_groups_first() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(Some(fingerprint([1, 1])), 0);
        clusterer.push(Some(fingerprint([2, 1])), 1);
        clusterer.push(Some(fingerprint([1, 2])), 2);
        clusterer.push(Some(fingerprint([2, 1])), 3);
        clusterer.push(Some(fingerprint([1, 1])), 4);

        assert_eq!(clusterer.into_sorted_doc_ids(), [0, 4, 2, 1, 3]);
    }

    #[test]
    fn emits_largest_sort_groups_first_within_schema() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(Some(fingerprint([1, 1])), 0);
        clusterer.push(Some(fingerprint([1, 2])), 1);
        clusterer.push(Some(fingerprint([1, 2])), 2);
        clusterer.push(Some(fingerprint([1, 1])), 3);
        clusterer.push(Some(fingerprint([1, 2])), 4);

        assert_eq!(clusterer.into_sorted_doc_ids(), [1, 2, 4, 0, 3]);
    }

    #[test]
    fn emits_largest_groups_first_with_one_fingerprint_level() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(Some(fingerprint([1])), 0);
        clusterer.push(Some(fingerprint([2])), 1);
        clusterer.push(Some(fingerprint([1])), 2);
        clusterer.push(Some(fingerprint([3])), 3);
        clusterer.push(Some(fingerprint([3])), 4);
        clusterer.push(Some(fingerprint([3])), 5);

        assert_eq!(clusterer.into_sorted_doc_ids(), [3, 4, 5, 0, 2, 1]);
    }

    #[test]
    fn emits_largest_groups_first_at_each_fingerprint_level() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(Some(fingerprint([1, 1, 1])), 0);
        clusterer.push(Some(fingerprint([1, 1, 2])), 1);
        clusterer.push(Some(fingerprint([1, 2, 1])), 2);
        clusterer.push(Some(fingerprint([1, 2, 1])), 3);
        clusterer.push(Some(fingerprint([2, 1, 1])), 4);
        clusterer.push(Some(fingerprint([1, 2, 2])), 5);
        clusterer.push(Some(fingerprint([1, 2, 1])), 6);
        clusterer.push(Some(fingerprint([1, 1, 1])), 7);

        assert_eq!(clusterer.into_sorted_doc_ids(), [2, 3, 6, 5, 0, 7, 1, 4]);
    }

    #[test]
    fn appends_unsorted_docs_in_insertion_order() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(None, 0);
        clusterer.push(Some(fingerprint([1, 1])), 1);
        clusterer.push(None, 2);
        clusterer.push(Some(fingerprint([1, 1])), 3);
        clusterer.push(None, 4);

        assert_eq!(clusterer.into_sorted_doc_ids(), [1, 3, 0, 2, 4]);
    }

    #[test]
    fn preserves_order_when_all_docs_are_unsorted() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(None, 0);
        clusterer.push(None, 1);
        clusterer.push(None, 2);

        assert_eq!(clusterer.into_sorted_doc_ids(), [0, 1, 2]);
    }

    #[test]
    fn preserves_split_doc_ids_across_batches() {
        let mut clusterer = DocIdClusterer::default();
        clusterer.push(Some(fingerprint([1, 1])), 0);
        clusterer.push(Some(fingerprint([2, 1])), 1);
        // Simulate a second batch appended to the same split: doc IDs must keep increasing.
        clusterer.push(Some(fingerprint([1, 1])), 2);
        clusterer.push(None, 3);

        assert_eq!(clusterer.into_sorted_doc_ids(), [0, 2, 1, 3]);
    }
}
