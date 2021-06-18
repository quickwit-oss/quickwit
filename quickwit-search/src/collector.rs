// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use quickwit_doc_mapping::DocMapper;
use quickwit_doc_mapping::SortBy;
use quickwit_doc_mapping::SortOrder;
use quickwit_proto::LeafSearchResult;
use quickwit_proto::PartialHit;
use quickwit_proto::SearchRequest;
use tantivy::collector::Collector;
use tantivy::collector::SegmentCollector;
use tantivy::fastfield::DynamicFastFieldReader;
use tantivy::fastfield::FastFieldReader;
use tantivy::schema::Field;
use tantivy::DocId;
use tantivy::Score;
use tantivy::SegmentOrdinal;
use tantivy::SegmentReader;

use crate::filters::TimestampFilter;
use crate::partial_hit_sorting_key;

/// The `SortingFieldComputer` can be seen as the specialization of `SortBy` applied to a specific `SegmentReader`.
/// Its role is to compute the sorting field given a `DocId`.
enum SortingFieldComputer {
    SortByFastField {
        fast_field_reader: DynamicFastFieldReader<u64>,
        order: SortOrder,
    },
    /// If undefined, we simply sort by DocIds.
    SortByDocId,
}

impl SortingFieldComputer {
    /// Returns the ranking key for the given element
    fn compute_sorting_field(&self, doc_id: DocId) -> u64 {
        match self {
            SortingFieldComputer::SortByFastField {
                fast_field_reader,
                order,
            } => {
                let field_val = fast_field_reader.get(doc_id);
                match order {
                    // Descending is our most common case.
                    SortOrder::Desc => field_val,
                    // We get Ascending order by using a decreasing mapping over u64 as the sorting_field.
                    SortOrder::Asc => u64::MAX - field_val,
                }
            }
            SortingFieldComputer::SortByDocId => 0u64,
        }
    }
}

/// Takes a user-defined sorting criteria and resolves it to a
/// segment specific `SortFieldComputer`.
fn resolve_sort_by(
    sort_by: &SortBy,
    segment_reader: &SegmentReader,
) -> tantivy::Result<SortingFieldComputer> {
    match sort_by {
        SortBy::SortByFastField { field, order } => {
            let fast_field_reader = segment_reader.fast_fields().u64_lenient(*field)?;
            Ok(SortingFieldComputer::SortByFastField {
                fast_field_reader,
                order: *order,
            })
        }
        SortBy::DocId => Ok(SortingFieldComputer::SortByDocId),
    }
}

/// PartialHitHeapItem order is the inverse of the natural order
/// so that we actually have a min-heap.
#[derive(Clone, Copy)]
struct PartialHitHeapItem {
    sorting_field_value: u64,
    doc_id: DocId,
}

impl PartialOrd for PartialHitHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.cmp(self))
    }
}

impl Ord for PartialHitHeapItem {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let by_sorting_field = other
            .sorting_field_value
            .partial_cmp(&self.sorting_field_value)
            .unwrap_or(Ordering::Equal);

        let lazy_order_by_doc_id = || {
            self.doc_id
                .partial_cmp(&other.doc_id)
                .unwrap_or(Ordering::Equal)
        };

        // In case of a tie on the feature, we sort by ascending `DocId`.
        by_sorting_field.then_with(lazy_order_by_doc_id)
    }
}

impl PartialEq for PartialHitHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PartialHitHeapItem {}

/// Quickwit collector working at the scale of the segment.
pub struct QuickwitSegmentCollector {
    num_hits: u64,
    split_id: String,
    sort_by: SortingFieldComputer,
    hits: BinaryHeap<PartialHitHeapItem>,
    max_hits: usize,
    segment_ord: u32,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl QuickwitSegmentCollector {
    fn at_capacity(&self) -> bool {
        self.hits.len() >= self.max_hits
    }

    fn collect_top_k(&mut self, doc_id: DocId) {
        let sorting_field: u64 = self.sort_by.compute_sorting_field(doc_id);
        if self.at_capacity() {
            if let Some(limit_sorting_field) = self.hits.peek().map(|head| head.sorting_field_value)
            {
                // In case of a tie, we keep the document with a lower `DocId`.
                if limit_sorting_field < sorting_field {
                    if let Some(mut head) = self.hits.peek_mut() {
                        head.sorting_field_value = sorting_field;
                        head.doc_id = doc_id;
                    }
                }
            }
        } else {
            // we have not reached capacity yet, so we can just push the
            // element.
            self.hits.push(PartialHitHeapItem {
                sorting_field_value: sorting_field,
                doc_id,
            });
        }
    }

    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(ref timestamp_filter) = self.timestamp_filter_opt {
            return timestamp_filter.is_within_range(doc_id);
        }
        true
    }
}

impl SegmentCollector for QuickwitSegmentCollector {
    type Fruit = LeafSearchResult;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        if !self.accept_document(doc_id) {
            return;
        }

        self.num_hits += 1;
        self.collect_top_k(doc_id);
    }

    fn harvest(self) -> LeafSearchResult {
        let segment_ord = self.segment_ord;
        // TODO use into_iter_sorted() once it gets stable.
        let split_id = self.split_id;
        let partial_hits: Vec<PartialHit> = self
            .hits
            .into_sorted_vec()
            .into_iter()
            .map(|hit| PartialHit {
                sorting_field_value: hit.sorting_field_value,
                segment_ord,
                doc_id: hit.doc_id,
                split: split_id.clone(),
            })
            .collect();
        LeafSearchResult {
            num_hits: self.num_hits,
            partial_hits,
        }
    }
}

/// The quickwit collector is the tantivy Collector used in Quickwit.
///
/// It defines the data that should be accumulated about the documents matching
/// the query.
#[derive(Clone)]
pub struct QuickwitCollector {
    pub split_id: String,
    pub start_offset: usize,
    pub max_hits: usize,
    pub sort_by: SortBy,
    pub timestamp_field_opt: Option<Field>,
    pub start_timestamp_opt: Option<i64>,
    pub end_timestamp_opt: Option<i64>,
}

impl QuickwitCollector {
    pub fn for_split(&self, split_id: String) -> Self {
        let mut self_clone = self.clone();
        self_clone.split_id = split_id;
        self_clone
    }

    pub fn fast_fields(&self) -> Vec<Field> {
        let mut fast_fields = vec![];
        if let Some(ref timestamp_field) = self.timestamp_field_opt {
            fast_fields.push(*timestamp_field);
        }

        if let SortBy::SortByFastField { field, .. } = self.sort_by {
            fast_fields.push(field);
        }

        //TODO: find a way to add remaining fastfields from user search request.

        fast_fields
    }
}

impl Collector for QuickwitCollector {
    type Child = QuickwitSegmentCollector;
    type Fruit = LeafSearchResult;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let sort_by = resolve_sort_by(&self.sort_by, segment_reader)?;
        // Regardless of the start_offset, we need to collect top-K
        // starting from 0 for every leaves.
        let leaf_max_hits = self.max_hits + self.start_offset;

        let timestamp_filter_opt = if let Some(timestamp_field) = self.timestamp_field_opt {
            let timestamp_filter = TimestampFilter::new(
                timestamp_field,
                self.start_timestamp_opt,
                self.end_timestamp_opt,
                segment_reader,
            )?;
            Some(timestamp_filter)
        } else {
            None
        };

        Ok(QuickwitSegmentCollector {
            num_hits: 0u64,
            split_id: self.split_id.clone(),
            sort_by,
            hits: BinaryHeap::with_capacity(leaf_max_hits),
            segment_ord,
            max_hits: leaf_max_hits,
            timestamp_filter_opt,
        })
    }

    fn requires_scoring(&self) -> bool {
        // We do not need BM25 scoring in Quickwit.
        // By returning false, we inform tantivy that it does not need to decompress
        // term frequencies.
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<LeafSearchResult>) -> tantivy::Result<Self::Fruit> {
        // We want the hits in [start_offset..start_offset + max_hits).
        // All leaves will return their top [0..max_hits) documents.
        // We compute the overall [0..start_offset + max_hits) documents ...
        let num_hits = self.start_offset + self.max_hits;
        let mut merged_leaf_result = merge_leaf_results(segment_fruits, num_hits);
        // ... and drop the first [..start_offets) hits.
        merged_leaf_result
            .partial_hits
            .drain(0..self.start_offset)
            .count(); //< we just use count as a way to consume the entire iterator.
        Ok(merged_leaf_result)
    }
}

/// Merges a set of Leaf Results.
fn merge_leaf_results(leaf_results: Vec<LeafSearchResult>, max_hits: usize) -> LeafSearchResult {
    // Optimization: No merging needed if there is only one result.
    if leaf_results.len() == 1 {
        return leaf_results.into_iter().next().unwrap_or_default(); //< default is actually never called
    }
    let num_hits: u64 = leaf_results
        .iter()
        .map(|leaf_result| leaf_result.num_hits)
        .sum();
    let all_partial_hits: Vec<PartialHit> = leaf_results
        .into_iter()
        .flat_map(|leaf_result| leaf_result.partial_hits)
        .collect();
    // TODO optimize
    let top_k_partial_hits = top_k_partial_hits(all_partial_hits, max_hits);
    LeafSearchResult {
        num_hits,
        partial_hits: top_k_partial_hits,
    }
}

/// Mutates partial_hits so that it contains the top-num_hitso hits,
/// and so that these elements are sorted.
///
/// TODO we could possibly optimize the sort away (but I doubt it matters).
fn top_k_partial_hits(mut partial_hits: Vec<PartialHit>, num_hits: usize) -> Vec<PartialHit> {
    partial_hits.sort_by(|left, right| {
        let left_key = partial_hit_sorting_key(left);
        let right_key = partial_hit_sorting_key(right);
        left_key.cmp(&right_key)
    });
    partial_hits.truncate(num_hits);
    partial_hits
}

/// Builds the QuickwitCollector, in function of the information that was requested by the user.
pub fn make_collector(
    doc_mapper: &dyn DocMapper,
    search_request: &SearchRequest,
) -> QuickwitCollector {
    QuickwitCollector {
        split_id: String::new(),
        start_offset: search_request.start_offset as usize,
        max_hits: search_request.max_hits as usize,
        sort_by: doc_mapper.default_sort_by(),
        timestamp_field_opt: doc_mapper.timestamp_field(),
        start_timestamp_opt: search_request.start_timestamp,
        end_timestamp_opt: search_request.end_timestamp,
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::top_k_partial_hits;
    use quickwit_proto::PartialHit;

    use super::PartialHitHeapItem;
    use std::cmp::Ordering;

    #[test]
    fn test_partial_hit_ordered_by_sorting_field() {
        let lesser_score = PartialHitHeapItem {
            sorting_field_value: 1u64,
            doc_id: 1u32,
        };
        let higher_score = PartialHitHeapItem {
            sorting_field_value: 2u64,
            doc_id: 1u32,
        };
        assert_eq!(lesser_score.cmp(&higher_score), Ordering::Greater);
    }

    #[test]
    fn test_merge_partial_hits_no_tie() {
        let make_doc = |sorting_field_value: u64| PartialHit {
            sorting_field_value,
            split: "split1".to_string(),
            segment_ord: 0u32,
            doc_id: 0u32,
        };
        assert_eq!(
            top_k_partial_hits(vec![make_doc(1u64), make_doc(3u64), make_doc(2u64),], 2),
            vec![make_doc(3), make_doc(2)]
        );
    }

    #[test]
    fn test_merge_partial_hits_with_tie() {
        let make_hit_given_split_id = |split_id: u64| PartialHit {
            sorting_field_value: 0u64,
            split: format!("split_{}", split_id),
            segment_ord: 0u32,
            doc_id: 0u32,
        };
        assert_eq!(
            top_k_partial_hits(
                vec![
                    make_hit_given_split_id(1u64),
                    make_hit_given_split_id(3u64),
                    make_hit_given_split_id(2u64),
                ],
                2
            ),
            vec![make_hit_given_split_id(1), make_hit_given_split_id(2)]
        );
    }
}
