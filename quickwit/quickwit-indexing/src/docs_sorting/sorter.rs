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
//! `DocIdSorter` is the bridge between per-document fingerprints and Tantivy doc ID remapping.
//! The `Indexer` pushes each document's original doc ID with its optional fingerprint. During
//! split finalization, the sorter turns those groups into the `DocIdMapping` used to write the
//! final segment layout.
//!
//! ```text
//! incoming docs in indexing order:
//!     doc 0 -> schema A, grouping X
//!     doc 1 -> schema A, grouping Y
//!     doc 2 -> schema B, grouping X
//!     doc 3 -> no fingerprint
//!     doc 4 -> schema A, grouping X
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
//! Schema groups are emitted largest first, and grouping sort groups within each schema are also
//! emitted largest first. Documents without a fingerprint are kept in insertion order after the
//! fingerprinted sort groups.

use std::cmp::Reverse;
use std::mem;

use fnv::FnvHashMap;
use smallvec::SmallVec;
use tantivy::DocId;
use tantivy::indexer::DocIdMapping;

use super::Fingerprint;

// We inline as many DocIds as possible to avoid heap allocations.
// This is done by calculating the inline capacity based on the size of the Vec<DocId> and the
// SmallVec<[DocId; 0]> overhead.
const DOC_IDS_INLINE_CAPACITY: usize = (mem::size_of::<Vec<DocId>>()
    - mem::size_of::<SmallVec<[DocId; 0]>>())
    / mem::size_of::<DocId>();
type SortGroupDocIds = SmallVec<[DocId; DOC_IDS_INLINE_CAPACITY]>;
const _: () = assert!(mem::size_of::<SortGroupDocIds>() == mem::size_of::<Vec<DocId>>());

#[derive(Default)]
pub struct DocIdSorter {
    docs_by_schema_fingerprint: FnvHashMap<u64, SchemaSortGroup>,
    unsorted_docs: SortGroupDocIds,
}

#[derive(Default)]
struct SchemaSortGroup {
    num_docs: usize,
    docs_by_grouping_fingerprint: FnvHashMap<u64, SortGroupDocIds>,
}

impl DocIdSorter {
    pub fn push(&mut self, fingerprint_opt: Option<Fingerprint>, doc_id: DocId) {
        match fingerprint_opt {
            Some(fingerprint) => {
                let schema_group = self
                    .docs_by_schema_fingerprint
                    .entry(fingerprint.schema)
                    .or_default();
                schema_group
                    .docs_by_grouping_fingerprint
                    .entry(fingerprint.grouping)
                    .or_default()
                    .push(doc_id);
                schema_group.num_docs += 1;
            }
            None => self.unsorted_docs.push(doc_id),
        }
    }

    pub fn sort_group_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.docs_by_schema_fingerprint
            .values()
            .flat_map(|schema_group| schema_group.docs_by_grouping_fingerprint.values())
            .map(SortGroupDocIds::len)
    }

    pub fn into_doc_id_mapping(self, num_docs: u64) -> anyhow::Result<DocIdMapping> {
        let doc_ids = self.into_sorted_doc_ids();
        debug_assert_eq!(doc_ids.len(), num_docs as usize);
        let doc_id_mapping = DocIdMapping::new_permutation(doc_ids)?;
        Ok(doc_id_mapping)
    }

    fn into_sorted_doc_ids(self) -> Vec<DocId> {
        let mut schema_groups: Vec<SchemaSortGroup> =
            self.docs_by_schema_fingerprint.into_values().collect();
        schema_groups.sort_unstable_by_key(|schema_group| Reverse(schema_group.num_docs));

        schema_groups
            .into_iter()
            .flat_map(|schema_group| {
                let mut sort_groups: Vec<SortGroupDocIds> = schema_group
                    .docs_by_grouping_fingerprint
                    .into_values()
                    .collect();
                sort_groups.sort_unstable_by_key(|sort_group| Reverse(sort_group.len()));
                sort_groups.into_iter().flatten()
            })
            .chain(self.unsorted_docs)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{DocIdSorter, Fingerprint};

    fn fingerprint(schema_fingerprint: u64, grouping_fingerprint: u64) -> Fingerprint {
        Fingerprint {
            schema: schema_fingerprint,
            grouping: grouping_fingerprint,
        }
    }

    #[test]
    fn emits_largest_schema_groups_first() {
        let mut sorter = DocIdSorter::default();
        sorter.push(Some(fingerprint(1, 1)), 0);
        sorter.push(Some(fingerprint(2, 1)), 1);
        sorter.push(Some(fingerprint(1, 2)), 2);
        sorter.push(Some(fingerprint(2, 1)), 3);
        sorter.push(Some(fingerprint(1, 1)), 4);

        assert_eq!(sorter.into_sorted_doc_ids(), [0, 4, 2, 1, 3]);
    }

    #[test]
    fn emits_largest_sort_groups_first_within_schema() {
        let mut sorter = DocIdSorter::default();
        sorter.push(Some(fingerprint(1, 1)), 0);
        sorter.push(Some(fingerprint(1, 2)), 1);
        sorter.push(Some(fingerprint(1, 2)), 2);
        sorter.push(Some(fingerprint(1, 1)), 3);
        sorter.push(Some(fingerprint(1, 2)), 4);

        assert_eq!(sorter.into_sorted_doc_ids(), [1, 2, 4, 0, 3]);
    }

    #[test]
    fn appends_unsorted_docs_in_insertion_order() {
        let mut sorter = DocIdSorter::default();
        sorter.push(None, 0);
        sorter.push(Some(fingerprint(1, 1)), 1);
        sorter.push(None, 2);
        sorter.push(Some(fingerprint(1, 1)), 3);
        sorter.push(None, 4);

        assert_eq!(sorter.into_sorted_doc_ids(), [1, 3, 0, 2, 4]);
    }

    #[test]
    fn preserves_order_when_all_docs_are_unsorted() {
        let mut sorter = DocIdSorter::default();
        sorter.push(None, 0);
        sorter.push(None, 1);
        sorter.push(None, 2);

        assert_eq!(sorter.into_sorted_doc_ids(), [0, 1, 2]);
    }

    #[test]
    fn preserves_split_doc_ids_across_batches() {
        let mut sorter = DocIdSorter::default();
        sorter.push(Some(fingerprint(1, 1)), 0);
        sorter.push(Some(fingerprint(2, 1)), 1);
        // Simulate a second batch appended to the same split: doc IDs must keep increasing.
        sorter.push(Some(fingerprint(1, 1)), 2);
        sorter.push(None, 3);

        assert_eq!(sorter.into_sorted_doc_ids(), [0, 2, 1, 3]);
    }

    #[test]
    fn reports_leaf_grouping_sort_group_sizes() {
        let mut sorter = DocIdSorter::default();
        sorter.push(Some(fingerprint(1, 1)), 0);
        sorter.push(Some(fingerprint(1, 2)), 1);
        sorter.push(Some(fingerprint(1, 2)), 2);
        sorter.push(Some(fingerprint(2, 1)), 3);

        let mut sort_group_sizes = sorter.sort_group_sizes().collect::<Vec<_>>();
        sort_group_sizes.sort_unstable();
        assert_eq!(sort_group_sizes, [1, 1, 2]);
    }
}
