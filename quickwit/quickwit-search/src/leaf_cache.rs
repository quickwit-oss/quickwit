// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::ops::Bound;

use prost::Message;
use quickwit_proto::{LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets};
use quickwit_storage::{MemorySizedCache, OwnedBytes};

/// A cache to memoize `leaf_search_single_split` results.
pub struct LeafSearchCache {
    content: MemorySizedCache<CacheKey>,
}

impl LeafSearchCache {
    pub fn new(capacity: usize) -> LeafSearchCache {
        LeafSearchCache {
            content: MemorySizedCache::with_capacity_in_bytes(
                capacity,
                &quickwit_storage::STORAGE_METRICS.partial_request_cache,
            ),
        }
    }
    pub fn get(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
    ) -> Option<LeafSearchResponse> {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);
        let encoded_result = self.content.get(&key)?;
        // this should never fail
        LeafSearchResponse::decode(&*encoded_result).ok()
    }

    pub fn put(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        result: LeafSearchResponse,
    ) {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);

        let encoded_result = result.encode_to_vec();
        self.content.put(key, OwnedBytes::new(encoded_result));
    }
}

/// A key inside a [`LeafSearchCache`].
#[derive(Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    /// The split this entry refers to
    split_id: String,
    /// The request this matches. The timerange of the request was removed.
    request: SearchRequest,
    /// The effective time range of the request, that is, the intersection of the timerange
    /// requested, and the timerange covered by the split.
    merged_time_range: Range,
}

impl CacheKey {
    fn from_split_meta_and_request(
        split_info: SplitIdAndFooterOffsets,
        mut search_request: SearchRequest,
    ) -> Self {
        let split_time_range = Range::from_bounds(split_info.time_range());
        let request_time_range = Range::from_bounds(search_request.time_range());
        let merged_time_range = request_time_range.intersect(&split_time_range);

        search_request.start_timestamp = None;
        search_request.end_timestamp = None;

        CacheKey {
            split_id: split_info.split_id,
            request: search_request,
            merged_time_range,
        }
    }
}

/// A (half-open) range bounded inclusively below and exclusively above [start..end).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Range {
    start: i64,
    end: Option<i64>,
}

impl Range {
    /// Create a Range from bounds.
    fn from_bounds(range: impl std::ops::RangeBounds<i64>) -> Self {
        let empty_range = Range {
            start: 0,
            end: Some(0),
        };

        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => {
                // if we exclude i64::MAX from the start bound, the range is necessarily empty
                if let Some(start) = start.checked_add(1) {
                    start
                } else {
                    return empty_range;
                }
            }
            Bound::Unbounded => i64::MIN,
        };
        let end = match range.end_bound() {
            // if we include i64::MAX at the end bound, this is essentially boundless
            Bound::Included(end) => end.checked_add(1),
            Bound::Excluded(end) => Some(*end),
            Bound::Unbounded => None,
        };

        Range { start, end }
    }

    /// Normalize empty ranges to be 0..0
    fn normalize(self) -> Range {
        let empty_range = Range {
            start: 0,
            end: Some(0),
        };
        match self {
            Range {
                start,
                end: Some(end),
            } if start >= end => empty_range,
            any => any,
        }
    }

    /// Return the intersection of self and other.
    fn intersect(&self, other: &Range) -> Range {
        let start = self.start.max(other.start);

        let end = match (self.end, other.end) {
            (Some(this), Some(other)) => Some(this.min(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };
        Range { start, end }.normalize()
    }
}

impl std::ops::RangeBounds<i64> for Range {
    fn start_bound(&self) -> Bound<&i64> {
        Bound::Included(&self.start)
    }

    fn end_bound(&self) -> Bound<&i64> {
        self.end.as_ref().map_or(Bound::Unbounded, Bound::Excluded)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::{
        LeafSearchResponse, PartialHit, SearchRequest, SortValue, SplitIdAndFooterOffsets,
    };

    use super::LeafSearchCache;

    #[test]
    fn test_leaf_search_cache_no_timestamp() {
        let cache = LeafSearchCache::new(64_000_000);

        let split_1 = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: None,
            timestamp_end: None,
        };

        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: None,
            timestamp_end: None,
        };

        let query_1 = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test2".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let result = LeafSearchResponse {
            failed_splits: Vec::new(),
            intermediate_aggregation_result: None,
            num_attempted_splits: 0,
            num_hits: 1234,
            partial_hits: vec![PartialHit {
                doc_id: 1,
                segment_ord: 0,
                sort_value: Some(SortValue::U64(0u64)),
                split_id: "split_1".to_string(),
            }],
        };

        assert!(cache.get(split_1.clone(), query_1.clone()).is_none());

        cache.put(split_1.clone(), query_1.clone(), result.clone());
        assert_eq!(cache.get(split_1.clone(), query_1.clone()).unwrap(), result);
        assert!(cache.get(split_2, query_1).is_none());
        assert!(cache.get(split_1, query_2).is_none());
    }

    #[test]
    fn test_leaf_search_cache_timestamp() {
        let cache = LeafSearchCache::new(64_000_000);

        let split_1 = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(100),
            timestamp_end: Some(199),
        };
        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(249),
        };
        let split_3 = SplitIdAndFooterOffsets {
            split_id: "split_3".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(249),
        };

        let query_1 = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test".to_string(),
            start_timestamp: Some(100),
            end_timestamp: Some(250),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_1bis = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test".to_string(),
            start_timestamp: Some(150),
            end_timestamp: Some(300),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test2".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_2bis = SearchRequest {
            index_id: "test-idx".to_string(),
            query_ast: "test2".to_string(),
            start_timestamp: Some(50),
            end_timestamp: Some(200),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let result = LeafSearchResponse {
            failed_splits: Vec::new(),
            intermediate_aggregation_result: None,
            num_attempted_splits: 0,
            num_hits: 1234,
            partial_hits: vec![PartialHit {
                doc_id: 1,
                segment_ord: 0,
                sort_value: Some(SortValue::U64(0)),
                split_id: "split_1".to_string(),
            }],
        };

        // for split_1, 1 and 1bis cover different timestamp ranges
        cache.put(split_1.clone(), query_1.clone(), result.clone());
        assert!(cache.get(split_1.clone(), query_1.clone()).is_some());
        assert!(cache.get(split_1.clone(), query_1bis.clone()).is_none());

        // for split_2, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_2.clone(), query_1.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_1).is_some());
        assert!(cache.get(split_2.clone(), query_1bis).is_some());

        // for split_1, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_1.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_1.clone(), query_2.clone()).is_some());
        assert!(cache.get(split_1, query_2bis.clone()).is_some());

        // for split_2, 2 covers everything, but 2bis cover only a subrange
        cache.put(split_2.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_2.clone()).is_some());
        assert!(cache.get(split_2, query_2bis.clone()).is_none());

        // same for split_3, but we try caching the bounded request and query for the unbounded one
        cache.put(split_3.clone(), query_2bis.clone(), result);
        assert!(cache.get(split_3.clone(), query_2).is_none());
        assert!(cache.get(split_3, query_2bis).is_some());
    }
}
