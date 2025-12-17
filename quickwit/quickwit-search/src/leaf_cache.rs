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

use std::ops::{Bound, RangeBounds};

use prost::Message;
use quickwit_proto::search::{
    CountHits, LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets,
};
use quickwit_proto::types::SplitId;
use quickwit_storage::{MemorySizedCache, OwnedBytes};
use tantivy::index::SegmentId;

/// A cache to memoize `leaf_search_single_split` results.
pub struct LeafSearchCache {
    content: MemorySizedCache<CacheKey>,
}

// TODO we could be smarter about search_after. If we have a cached request with a search_after
// (possibly equal to None) A, and a corresponding response with the 1st element having the value
// B, and we receive a 2nd request with a search_after such that A <= C < B, we can serve from
// cache directly. Only the case A = C < B is currently handled.
// TODO if we don't request counting all results, have no aggregation, and we get a request we can
// match, the merged_time_range is strictly smaller, and every hit we had fits in the new
// timebound, we can reply from cache, saying we hit only result.partial_hits.len() res. It always
// undercount, and necessarily returns the right hits.
// TODO if we stored a result for X hits, but a subsequent request asks for Y < X hits, we can
// modify the answer and serve from cache.
// TODO mix of 1 and 3.
// TODO this means given a request for X documents, we could search for k*X docs in each split,
// truncate to X while merging, and get free results from cache for at least the next k subsequent
// queries which vary only by search_after.

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
    split_id: SplitId,
    /// The request this matches. The timerange of the request was removed.
    request: SearchRequest,
    /// The effective time range of the request, that is, the intersection of the timerange
    /// requested, and the timerange covered by the split.
    merged_time_range: HalfOpenRange,
}

impl CacheKey {
    fn from_split_meta_and_request(
        split_info: SplitIdAndFooterOffsets,
        mut search_request: SearchRequest,
    ) -> Self {
        let split_time_range = HalfOpenRange::from_bounds(split_info.time_range());
        let request_time_range = HalfOpenRange::from_bounds(search_request.time_range());
        let merged_time_range = request_time_range.intersect(&split_time_range);

        search_request.start_timestamp = None;
        search_request.end_timestamp = None;
        // it doesn't matter whether or not we count all hits at the scale of a
        // single split: either we did process it and got everything, or we didn't.
        search_request.count_hits = CountHits::CountAll.into();

        CacheKey {
            split_id: split_info.split_id,
            request: search_request,
            merged_time_range,
        }
    }
}

/// A (half-open) range bounded inclusively below and exclusively above [start..end).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct HalfOpenRange {
    start: i64,
    end: Option<i64>,
}

impl HalfOpenRange {
    fn empty_range() -> HalfOpenRange {
        HalfOpenRange {
            start: 0,
            end: Some(0),
        }
    }

    /// Create a Range from bounds.
    fn from_bounds(range: impl RangeBounds<i64>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => {
                // if we exclude i64::MAX from the start bound, the range is necessarily empty
                if let Some(start) = start.checked_add(1) {
                    start
                } else {
                    return Self::empty_range();
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

        HalfOpenRange { start, end }.normalize()
    }

    fn is_empty(self) -> bool {
        !self.contains(&self.start)
    }

    /// Normalize empty ranges to be 0..0
    fn normalize(self) -> HalfOpenRange {
        if self.is_empty() {
            Self::empty_range()
        } else {
            self
        }
    }

    /// Return the intersection of self and other.
    fn intersect(&self, other: &HalfOpenRange) -> HalfOpenRange {
        let start = self.start.max(other.start);
        let end = match (self.end, other.end) {
            (Some(this), Some(other)) => Some(this.min(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };
        HalfOpenRange { start, end }.normalize()
    }
}

impl RangeBounds<i64> for HalfOpenRange {
    fn start_bound(&self) -> Bound<&i64> {
        Bound::Included(&self.start)
    }

    fn end_bound(&self) -> Bound<&i64> {
        if let Some(end_bound) = &self.end {
            Bound::Excluded(end_bound)
        } else {
            Bound::Unbounded
        }
    }
}

pub struct PredicateCacheImpl {
    content: MemorySizedCache<(SplitId, String)>,
}

impl PredicateCacheImpl {
    pub fn new(capacity: usize) -> Self {
        PredicateCacheImpl {
            content: MemorySizedCache::with_capacity_in_bytes(
                capacity,
                &quickwit_storage::STORAGE_METRICS.predicate_cache,
            ),
        }
    }
}

impl quickwit_query::query_ast::PredicateCache for PredicateCacheImpl {
    fn get(
        &self,
        split_id: SplitId,
        query_ast_json: String,
    ) -> Option<(SegmentId, quickwit_query::query_ast::HitSet)> {
        let encoded_result = self.content.get(&(split_id, query_ast_json))?;
        let (segment_id_bytes, hits_buffer) = encoded_result.split(32);
        let segment_id =
            SegmentId::from_uuid_string(str::from_utf8(&segment_id_bytes).ok()?).ok()?;
        let hits = quickwit_query::query_ast::HitSet::from_buffer(hits_buffer);
        Some((segment_id, hits))
    }

    fn put(
        &self,
        split_id: SplitId,
        query_ast_json: String,
        segment: SegmentId,
        hits: quickwit_query::query_ast::HitSet,
    ) {
        let hits_buffer = hits.into_buffer();
        let mut buffer = Vec::with_capacity(32 + hits_buffer.len());
        buffer.extend_from_slice(segment.uuid_string().as_bytes());
        buffer.extend_from_slice(&hits_buffer);
        self.content
            .put((split_id, query_ast_json), OwnedBytes::new(buffer));
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{
        LeafSearchResponse, PartialHit, ResourceStats, SearchRequest, SortValue,
        SplitIdAndFooterOffsets,
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
            num_docs: 0,
        };

        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0,
        };

        let query_1 = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "test".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
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
            num_attempted_splits: 1,
            num_successful_splits: 1,
            num_hits: 1234,
            partial_hits: vec![PartialHit {
                doc_id: 1,
                segment_ord: 0,
                sort_value: Some(SortValue::U64(0u64).into()),
                sort_value2: None,
                split_id: "split_1".to_string(),
            }],
            resource_stats: None,
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
            num_docs: 0,
        };
        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(249),
            num_docs: 0,
        };
        let split_3 = SplitIdAndFooterOffsets {
            split_id: "split_3".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(249),
            num_docs: 0,
        };

        let query_1 = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "test".to_string(),
            start_timestamp: Some(100),
            end_timestamp: Some(250),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_1bis = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "test".to_string(),
            start_timestamp: Some(150),
            end_timestamp: Some(300),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "test2".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_2bis = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
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
            num_attempted_splits: 1,
            num_successful_splits: 1,
            num_hits: 1234,
            partial_hits: vec![PartialHit {
                doc_id: 1,
                segment_ord: 0,
                sort_value: Some(SortValue::U64(0).into()),
                sort_value2: None,
                split_id: "split_1".to_string(),
            }],
            resource_stats: Some(ResourceStats::default()),
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
