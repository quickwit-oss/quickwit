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
use std::sync::Arc;
use std::time::Duration;

use feuer::{ObjectAccessHistories, ObjectKeyHash, RetentionScorer};
use prost::Message;
use quick_cache::sync::Cache;
use quickwit_config::{CacheConfig, StorageCacheConfig};
use quickwit_proto::search::{
    CountHits, LeafResourceStats, LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets,
};
use quickwit_query::query_ast::{HitSet, PredicateCache};
use quickwit_storage::metrics::{CacheMetrics, PARTIAL_REQUEST_CACHE, PREDICATE_CACHE};
use quickwit_storage::{FeuerCache, FeuerIoQueues, OwnedBytes};
use tantivy::index::SegmentId;

/// A cache to memoize `leaf_search_single_split` results.
pub struct LeafSearchCache {
    cache: FeuerCache,
    lookup_metrics: Option<&'static CacheMetrics>,
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
    pub fn new(config: &CacheConfig) -> LeafSearchCache {
        let memory_capacity = config.memory_capacity().as_u64();
        LeafSearchCache {
            cache: FeuerCache::memory(memory_capacity, "partial_request")
                .expect("invalid Feuer memory cache configuration"),
            lookup_metrics: (memory_capacity > 0).then_some(&PARTIAL_REQUEST_CACHE),
        }
    }

    /// Use a supplied Feuer cache.
    pub fn with_feuer_cache(cache: FeuerCache) -> Self {
        Self {
            cache,
            lookup_metrics: Some(&PARTIAL_REQUEST_CACHE),
        }
    }

    pub async fn get_async(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
    ) -> Option<LeafSearchResponse> {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);
        let bytes = self.cache.get(&key).await;
        if let Some(metrics) = self.lookup_metrics {
            metrics.record_get(bytes.as_ref().map(|bytes| bytes.len()));
        }
        let encoded_result = bytes?;
        match LeafSearchResponse::decode(&*encoded_result) {
            Ok(response) => Some(response),
            Err(error) => {
                quickwit_common::rate_limited_warn!(
                    limit_per_min = 10,
                    error = ?error,
                    "error when deserializing leaf response from cache"
                );
                None
            }
        }
    }

    pub fn put(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        mut result: LeafSearchResponse,
    ) {
        // We overwrite the original stats so that
        // we get the right counter on cache hits.
        result.resource_stats = Some(LeafResourceStats {
            partial_result_cache_num_splits: 1,
            partial_result_cache_num_docs: split_info.num_docs,
            ..Default::default()
        });
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);
        let encoded_result = result.encode_to_vec();
        self.cache.put(&key, OwnedBytes::new(encoded_result));
    }
}

/// A key inside a [`LeafSearchCache`].
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
struct CacheKey {
    /// The split this entry refers to
    split_id: String,
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
        // Priority only affects scheduling, not search results.
        search_request.priority = 0;

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

/// CPU nanoseconds saved per cached byte, weighted by request frequency.
struct PredicateCacheScorer {
    // Feuer has no eviction callback. Keep cost metadata in a separate size-limited cache.
    // Forgotten costs score zero until a read restores them from the cached payload.
    cpu_times: Cache<ObjectKeyHash, u64>,
}

impl RetentionScorer for PredicateCacheScorer {
    fn score(
        &self,
        key: &ObjectKeyHash,
        payload_len: u64,
        histories: &ObjectAccessHistories,
    ) -> f64 {
        let Some(cpu_nanos) = self.cpu_times.peek(key) else {
            return 0.0;
        };
        let demand = histories.decayed_request_count(key);
        demand * cpu_nanos as f64 / payload_len.max(1) as f64
    }
}

pub struct PredicateCacheImpl {
    cache: FeuerCache,
    scorer: Arc<PredicateCacheScorer>,
    lookup_metrics: Option<&'static CacheMetrics>,
    enabled: bool,
}

impl PredicateCacheImpl {
    pub fn new(config: &CacheConfig) -> Self {
        let memory_capacity = config.memory_capacity().as_u64();
        Self::open(
            &StorageCacheConfig {
                directory: None,
                memory_capacity: bytesize::ByteSize::b(memory_capacity),
                disk_capacity: bytesize::ByteSize::b(0),
            },
            &FeuerIoQueues::default(),
        )
        .expect("invalid Feuer memory cache configuration")
    }

    /// Open a cache whose retention score accounts for each predicate's fill CPU time.
    pub fn open(config: &StorageCacheConfig, queues: &FeuerIoQueues) -> anyhow::Result<Self> {
        let scorer = Arc::new(PredicateCacheScorer {
            cpu_times: Cache::new(100_000),
        });
        let enabled = config.memory_capacity.as_u64() > 0 || config.disk_capacity.as_u64() > 0;
        let cache = FeuerCache::open(
            config,
            queues,
            "predicate_cache",
            "predicate",
            Some(scorer.clone()),
            2, // Version 2 adds fill CPU nanoseconds after the segment ID.
        )?;
        Ok(Self {
            cache,
            scorer,
            lookup_metrics: enabled.then_some(&PREDICATE_CACHE),
            enabled,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

fn decode_predicate_entry(encoded_result: OwnedBytes) -> Option<(SegmentId, HitSet, u64)> {
    let segment_id_str = str::from_utf8(encoded_result.get(..32)?).ok()?;
    let segment_id = SegmentId::from_uuid_string(segment_id_str).ok()?;
    let cpu_nanos = u64::from_le_bytes(encoded_result.get(32..40)?.try_into().ok()?);
    let hits = HitSet::try_from_buffer(encoded_result.slice(40..encoded_result.len()))?;
    Some((segment_id, hits, cpu_nanos))
}

#[async_trait::async_trait]
impl PredicateCache for PredicateCacheImpl {
    async fn get(&self, split_id: String, query_ast_json: String) -> Option<(SegmentId, HitSet)> {
        if !self.enabled {
            return None;
        }
        let key = (split_id.as_str(), query_ast_json.as_str());
        let bytes = self.cache.get(&key).await;
        if let Some(metrics) = self.lookup_metrics {
            metrics.record_get(bytes.as_ref().map(|bytes| bytes.len()));
        }
        let encoded_result = bytes?;
        let entry = decode_predicate_entry(encoded_result);
        if entry.is_none() {
            quickwit_common::rate_limited_warn!(
                limit_per_min = 10,
                split_id,
                "error when deserializing predicate entry from cache"
            );
        }
        let (segment, hits, cpu_nanos) = entry?;
        self.scorer
            .cpu_times
            .insert(ObjectKeyHash::from_hash(&key), cpu_nanos);
        Some((segment, hits))
    }

    fn put(
        &self,
        split_id: String,
        query_ast_json: String,
        segment: SegmentId,
        hits: HitSet,
        cpu_time: Duration,
    ) {
        if !self.enabled {
            return;
        }
        let hits_buffer = hits.into_buffer();
        let cpu_nanos =
            u64::try_from(cpu_time.as_nanos()).expect("predicate CPU time exceeds u64 nanoseconds");
        let mut buffer = Vec::with_capacity(40 + hits_buffer.len());
        buffer.extend_from_slice(segment.uuid_string().as_bytes());
        buffer.extend_from_slice(&cpu_nanos.to_le_bytes());
        buffer.extend_from_slice(&hits_buffer);
        let key = (split_id.as_str(), query_ast_json.as_str());
        self.scorer
            .cpu_times
            .insert(ObjectKeyHash::from_hash(&key), cpu_nanos);
        self.cache.put(&key, OwnedBytes::new(buffer));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytesize::ByteSize;
    use quickwit_common::thread_pool::run_cpu_intensive;
    use quickwit_proto::search::{
        CountHits, LeafResourceStats, LeafSearchResponse, PartialHit, SearchRequest, SortValue,
        SplitIdAndFooterOffsets,
    };
    use quickwit_query::query_ast::{HitSet, PredicateCache};
    use tantivy::index::SegmentId;

    use super::{
        Cache, CacheKey, FeuerCache, LeafSearchCache, ObjectAccessHistories, ObjectKeyHash,
        PredicateCacheImpl, PredicateCacheScorer, RetentionScorer,
    };

    // This synchronous test helper is used with memory-only caches.
    fn get(
        cache: &LeafSearchCache,
        split_info: SplitIdAndFooterOffsets,
        request: SearchRequest,
    ) -> Option<LeafSearchResponse> {
        futures::executor::block_on(cache.get_async(split_info, request))
    }

    #[test]
    fn test_leaf_search_cache_no_timestamp() {
        let cache = LeafSearchCache::new(&ByteSize::mb(64).into());

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

        assert!(get(&cache, split_1.clone(), query_1.clone()).is_none());

        // `LeafSearchCache::put` overwrites `resource_stats` with the
        // cache-hit counters; reads always see those, not the original stats.
        let expected = LeafSearchResponse {
            resource_stats: Some(LeafResourceStats {
                partial_result_cache_num_splits: 1,
                partial_result_cache_num_docs: split_1.num_docs,
                ..Default::default()
            }),
            ..result.clone()
        };
        cache.put(split_1.clone(), query_1.clone(), result);
        assert_eq!(
            get(&cache, split_1.clone(), query_1.clone()).unwrap(),
            expected
        );
        assert!(get(&cache, split_2, query_1).is_none());
        assert!(get(&cache, split_1, query_2).is_none());
    }

    #[test]
    fn test_leaf_search_cache_timestamp() {
        let cache = LeafSearchCache::new(&ByteSize::mb(64).into());

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
            resource_stats: Some(LeafResourceStats::default()),
        };

        // for split_1, 1 and 1bis cover different timestamp ranges
        cache.put(split_1.clone(), query_1.clone(), result.clone());
        assert!(get(&cache, split_1.clone(), query_1.clone()).is_some());
        assert!(get(&cache, split_1.clone(), query_1bis.clone()).is_none());

        // for split_2, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_2.clone(), query_1.clone(), result.clone());
        assert!(get(&cache, split_2.clone(), query_1).is_some());
        assert!(get(&cache, split_2.clone(), query_1bis).is_some());

        // for split_1, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_1.clone(), query_2.clone(), result.clone());
        assert!(get(&cache, split_1.clone(), query_2.clone()).is_some());
        assert!(get(&cache, split_1, query_2bis.clone()).is_some());

        // for split_2, 2 covers everything, but 2bis cover only a subrange
        cache.put(split_2.clone(), query_2.clone(), result.clone());
        assert!(get(&cache, split_2.clone(), query_2.clone()).is_some());
        assert!(get(&cache, split_2, query_2bis.clone()).is_none());

        // same for split_3, but we try caching the bounded request and query for the unbounded one
        cache.put(split_3.clone(), query_2bis.clone(), result);
        assert!(get(&cache, split_3.clone(), query_2).is_none());
        assert!(get(&cache, split_3, query_2bis).is_some());
    }

    #[test]
    fn test_cache_key_covers_request_fields() {
        let split = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            ..Default::default()
        };
        let request = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "test".to_string(),
            max_hits: 10,
            ..Default::default()
        };
        let cache = LeafSearchCache::new(&ByteSize::mb(1).into());
        cache.put(
            split.clone(),
            request.clone(),
            LeafSearchResponse::default(),
        );
        assert!(get(&cache, split.clone(), request.clone()).is_some());
        for equivalent_request in [
            SearchRequest {
                count_hits: CountHits::Underestimate.into(),
                ..request.clone()
            },
            SearchRequest {
                priority: -10,
                ..request.clone()
            },
            SearchRequest {
                priority: 10,
                ..request.clone()
            },
        ] {
            assert!(get(&cache, split.clone(), equivalent_request).is_some());
        }

        // Query, hit-limit, snippet-field, and index-pattern changes must change the key.
        for altered_request in [
            SearchRequest {
                query_ast: "test2".to_string(),
                ..request.clone()
            },
            SearchRequest {
                max_hits: 11,
                ..request.clone()
            },
            SearchRequest {
                snippet_fields: vec!["body".to_string()],
                ..request.clone()
            },
            SearchRequest {
                index_id_patterns: vec!["other-idx".to_string()],
                ..request.clone()
            },
        ] {
            assert!(get(&cache, split.clone(), altered_request).is_none());
        }

        // And so must a change of split.
        let other_split = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            ..Default::default()
        };
        assert!(get(&cache, other_split, request).is_none());
    }

    #[tokio::test]
    async fn test_invalid_leaf_cache_entries_are_misses() {
        let split = SplitIdAndFooterOffsets::default();
        let request = SearchRequest::default();
        let key = CacheKey::from_split_meta_and_request(split.clone(), request.clone());
        for bytes in [vec![0xff], vec![0x08, 0x80]] {
            let cache = LeafSearchCache::new(&ByteSize::mb(1).into());
            cache
                .cache
                .put(&key, quickwit_storage::OwnedBytes::new(bytes));
            assert!(cache.cache.get(&key).await.is_some());
            // The malformed entry can remain in the cache across lookups.
            for _ in 0..2 {
                assert!(
                    cache
                        .get_async(split.clone(), request.clone())
                        .await
                        .is_none()
                );
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_predicate_cache_entries_are_misses() {
        let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
        let header = [segment.uuid_string().as_bytes(), &0u64.to_le_bytes()].concat();
        for bytes in [
            vec![],
            vec![b'0'; 31],
            vec![0xff; 36],
            vec![b'z'; 36],
            header.clone(),
            [header.as_slice(), &[0; 3]].concat(),
            // The first block decodes, but the second has an invalid bit width.
            [header.as_slice(), &64u32.to_ne_bytes(), &[0, 33]].concat(),
        ] {
            let cache = PredicateCacheImpl::new(&ByteSize::mb(1).into());
            let key = ("split", "query");
            cache
                .cache
                .put(&key, quickwit_storage::OwnedBytes::new(bytes));
            assert!(cache.cache.get(&key).await.is_some());
            for _ in 0..2 {
                assert!(cache.get("split".into(), "query".into()).await.is_none());
            }
        }
    }

    #[test]
    fn test_memory_caches_without_tokio_runtime() {
        // Zero disables caching. Feuer admits values larger than a positive capacity target.
        for (capacity, supplied) in [(0, false), (1, false), (1_000_000, true)] {
            let config = ByteSize::b(capacity).into();
            let split = SplitIdAndFooterOffsets::default();
            let request = SearchRequest::default();
            let cache = if supplied {
                LeafSearchCache::with_feuer_cache(
                    FeuerCache::memory(capacity, "partial_request").unwrap(),
                )
            } else {
                LeafSearchCache::new(&config)
            };
            if capacity > 0 {
                assert!(std::ptr::eq(
                    cache.lookup_metrics.unwrap(),
                    &*super::PARTIAL_REQUEST_CACHE
                ));
            }
            assert!(get(&cache, split.clone(), request.clone()).is_none());
            cache.put(
                split.clone(),
                request.clone(),
                LeafSearchResponse {
                    num_hits: 7,
                    ..Default::default()
                },
            );
            let key = CacheKey::from_split_meta_and_request(split.clone(), request.clone());
            assert_eq!(
                futures::executor::block_on(cache.cache.get(&key)).is_some(),
                capacity > 0
            );
            assert_eq!(
                get(&cache, split, request).map(|response| response.num_hits),
                (capacity > 0).then_some(7),
            );

            let cache = PredicateCacheImpl::new(&config);
            if capacity > 0 {
                assert!(std::ptr::eq(
                    cache.lookup_metrics.unwrap(),
                    &*super::PREDICATE_CACHE
                ));
            }
            assert_eq!(cache.is_enabled(), capacity > 0);
            let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
            cache.put(
                "split".into(),
                "query".into(),
                segment,
                HitSet::empty(),
                Duration::from_millis(1),
            );
            assert_eq!(
                futures::executor::block_on(cache.get("split".into(), "query".into())).map(
                    |(segment, hits)| {
                        assert!(hits.is_empty());
                        segment
                    }
                ),
                (capacity > 0).then_some(segment),
            );
        }
    }

    #[tokio::test]
    async fn test_supplied_leaf_cache_put_in_async_runtime() {
        let cache = LeafSearchCache::with_feuer_cache(
            quickwit_storage::FeuerCache::memory(1_000_000, "partial_request").unwrap(),
        );
        let split = SplitIdAndFooterOffsets::default();
        let request = SearchRequest::default();
        for _ in 0..2 {
            cache.put(
                split.clone(),
                request.clone(),
                LeafSearchResponse {
                    num_hits: 7,
                    ..Default::default()
                },
            );
            assert_eq!(
                cache
                    .get_async(split.clone(), request.clone())
                    .await
                    .unwrap()
                    .num_hits,
                7,
            );
        }
    }

    #[tokio::test]
    async fn test_predicate_cache_separates_keys() {
        let cache = PredicateCacheImpl::new(&ByteSize::mb(64).into());
        let segment_id = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();

        cache.put(
            "split-1".to_string(),
            "first-query".to_string(),
            segment_id,
            HitSet::empty(),
            Duration::from_millis(1),
        );

        let (cached_segment_id, cached_hits) = cache
            .get("split-1".to_string(), "first-query".to_string())
            .await
            .unwrap();
        assert_eq!(cached_segment_id, segment_id);
        assert!(cached_hits.is_empty());
        assert!(cache.cache.get(&("split-1", "first-query")).await.is_some());
        for (split, query) in [
            ("split-1", "second-query"),
            ("split-2", "first-query"),
            // Same concatenated bytes as the cached pair, with a different field boundary.
            ("split-1first-", "query"),
        ] {
            assert!(
                cache
                    .get(split.to_string(), query.to_string())
                    .await
                    .is_none()
            );
        }
    }

    #[test]
    fn test_cost_metadata_eviction_and_zero_cost_entries() {
        let scorer = PredicateCacheScorer {
            cpu_times: Cache::new(1),
        };
        let histories = ObjectAccessHistories::new();
        let first = ObjectKeyHash::from_hash(&"first");
        let second = ObjectKeyHash::from_hash(&"second");
        histories.record_request(&first);
        histories.record_request(&second);
        scorer.cpu_times.insert(first, 100);
        scorer.cpu_times.insert(second, 0);
        assert_eq!(scorer.score(&first, 100, &histories), 0.0);
        assert_eq!(scorer.score(&second, 100, &histories), 0.0);
        scorer.cpu_times.insert(first, 100);
        assert_eq!(
            scorer.score(&first, 100, &histories),
            histories.decayed_request_count(&first)
        );
    }

    #[tokio::test]
    async fn test_predicate_fill_costs_are_stored_and_restored() {
        let cache = PredicateCacheImpl::new(&ByteSize::mb(1).into());
        let restored = PredicateCacheImpl::new(&ByteSize::mb(1).into());
        let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
        let histories = ObjectAccessHistories::new();
        for (query, cpu_time) in [
            ("cheap", Duration::from_micros(10)),
            ("expensive", Duration::from_millis(1)),
            ("known_absent", Duration::from_millis(10)),
        ] {
            let key = ("split", query);
            let hash = ObjectKeyHash::from_hash(&key);
            histories.record_request(&hash);
            cache.put(
                "split".into(),
                query.into(),
                segment,
                HitSet::empty(),
                cpu_time,
            );
            let bytes = cache.cache.get(&key).await.unwrap();
            let size = bytes.len() as u64;
            let expected =
                histories.decayed_request_count(&hash) * cpu_time.as_nanos() as f64 / size as f64;
            assert_eq!(cache.scorer.score(&hash, size, &histories), expected);

            // Simulate a recovered disk payload with no in-memory cost metadata.
            restored.cache.put(&key, bytes);
            assert_eq!(restored.scorer.score(&hash, size, &histories), 0.0);
            let (cached_segment, hits) = restored.get("split".into(), query.into()).await.unwrap();
            assert_eq!(cached_segment, segment);
            assert!(hits.is_empty());
            assert_eq!(restored.scorer.score(&hash, size, &histories), expected);
            assert_eq!(
                restored.scorer.score(&hash, size * 2, &histories),
                expected / 2.0
            );
            assert!(restored.scorer.score(&hash, 0, &histories).is_finite());
            histories.record_request(&hash);
            assert!(restored.scorer.score(&hash, size, &histories) > expected);
        }
    }

    #[tokio::test]
    async fn test_predicate_cache_put_from_async_runtime_and_cpu_pool() {
        // Calling Handle::block_on for a put would panic inside the async task.
        let cache = Arc::new(PredicateCacheImpl::new(&ByteSize::mb(1).into()));
        assert!(cache.get("split".into(), "query".into()).await.is_none());
        let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
        cache.put(
            "split".into(),
            "query".into(),
            segment,
            HitSet::empty(),
            Duration::from_millis(1),
        );
        let (cached_segment, hits) = cache.get("split".into(), "query".into()).await.unwrap();
        assert_eq!(cached_segment, segment);
        assert!(hits.is_empty());
        let cache_for_write = cache.clone();
        run_cpu_intensive(move || {
            cache_for_write.put(
                "split".into(),
                "other-query".into(),
                segment,
                HitSet::empty(),
                Duration::from_millis(1),
            );
        })
        .await
        .unwrap();
        let (cached_segment, hits) = cache
            .get("split".into(), "other-query".into())
            .await
            .unwrap();
        assert_eq!(cached_segment, segment);
        assert!(hits.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_result_caches_use_shared_disk_directory() {
        let directory = tempfile::tempdir().unwrap();
        let mut config: quickwit_config::SearcherConfig =
            serde_json::from_value(serde_json::json!({
                "storage_cache": {"disk_capacity": "8MiB", "memory_capacity": "1MiB"},
                "partial_request_cache": {"disk_capacity": "16MiB", "memory_capacity": "1MiB"},
                "predicate_cache": {"disk_capacity": "32MiB", "memory_capacity": "2MiB"},
            }))
            .unwrap();
        config.storage_cache_directory = Some(directory.path().to_path_buf());
        let mut context = crate::SearcherContext::new_without_invoker(config, None);
        context
            .initialize_storage_caches(&quickwit_storage::StorageResolver::unconfigured())
            .await
            .unwrap();
        assert!(context.storage_cache_opt.is_some());
        assert!(context.predicate_cache.is_enabled());
        for file_name in ["storage_cache", "predicate_cache", "partial_response_cache"] {
            assert!(directory.path().join(file_name).is_file());
        }

        let split = SplitIdAndFooterOffsets {
            split_id: "split".into(),
            num_docs: 42,
            ..Default::default()
        };
        let request = SearchRequest::default();
        assert!(
            context
                .leaf_search_cache
                .get_async(split.clone(), request.clone())
                .await
                .is_none()
        );
        context.leaf_search_cache.put(
            split.clone(),
            request.clone(),
            LeafSearchResponse {
                num_hits: 7,
                ..Default::default()
            },
        );
        let cached = context
            .leaf_search_cache
            .get_async(split, request)
            .await
            .unwrap();
        assert_eq!(cached.num_hits, 7);
        let stats = cached.resource_stats.unwrap();
        assert_eq!(stats.partial_result_cache_num_splits, 1);
        assert_eq!(stats.partial_result_cache_num_docs, 42);

        let predicate = context.predicate_cache;
        let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
        predicate.put(
            "split".into(),
            "term".into(),
            segment,
            HitSet::empty(),
            Duration::from_millis(1),
        );
        // Disk-backed reads can run directly on the async task.
        let (cached_segment, hits) = predicate.get("split".into(), "term".into()).await.unwrap();
        assert_eq!(cached_segment, segment);
        assert!(hits.is_empty());
        let predicate_for_write = predicate.clone();
        run_cpu_intensive(move || {
            predicate_for_write.put(
                "split".into(),
                "other-term".into(),
                segment,
                HitSet::empty(),
                Duration::from_millis(1),
            );
        })
        .await
        .unwrap();
        assert!(
            predicate
                .get("split".into(), "other-term".into())
                .await
                .unwrap()
                .1
                .is_empty()
        );
    }
}
