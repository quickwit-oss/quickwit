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
use std::path::Path;

use bytesize::ByteSize;
use foyer::DeviceBuilder as _;
use prost::Message;
use quickwit_config::CacheConfig;
use quickwit_proto::search::{
    CountHits, LeafResourceStats, LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets,
};
use quickwit_proto::types::SplitId;
use quickwit_storage::{MemorySizedCache, OwnedBytes};
use serde::{Deserialize, Serialize};
use tantivy::index::SegmentId;
use tracing::{info, warn};

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

/// A cache to memoize `leaf_search_single_split` results.
///
/// Supports two modes:
/// - **MemoryOnly**: In-memory LRU cache (existing behavior).
/// - **Hybrid**: foyer-based memory + disk cache. Evicted entries spill to local NVMe instead of
///   being lost, so the next access reads from disk (~0.1ms) instead of S3 (~50-200ms).
// The two variants differ significantly in size (`foyer::HybridCache` is large), but boxing the
// `Hybrid` variant would add an indirection on every cache hit, which we want to avoid on the
// search hot path.
#[allow(clippy::large_enum_variant)]
pub enum LeafSearchCache {
    MemoryOnly {
        content: MemorySizedCache<CacheKey>,
    },
    Hybrid {
        cache: foyer::HybridCache<CacheKey, CachedValue>,
        metrics: quickwit_storage::metrics::SingleCacheMetrics,
    },
}

/// Serializable wrapper around encoded protobuf bytes for the foyer disk cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CachedValue(Vec<u8>);

impl LeafSearchCache {
    /// Create a memory-only cache (backward-compatible, no disk tier).
    pub fn new(config: &CacheConfig) -> LeafSearchCache {
        LeafSearchCache::MemoryOnly {
            content: MemorySizedCache::from_config(
                config,
                &quickwit_storage::STORAGE_METRICS.partial_request_cache,
            ),
        }
    }

    /// Create a hybrid cache with memory + disk tiers.
    ///
    /// The memory tier uses the capacity from `config`. The disk tier uses the
    /// provided path and capacity. When entries are evicted from memory, they
    /// spill to the disk tier.
    pub async fn new_hybrid(
        config: &CacheConfig,
        disk_cache_path: &Path,
        disk_cache_capacity: ByteSize,
    ) -> anyhow::Result<LeafSearchCache> {
        // ensure the directory exists
        tokio::fs::create_dir_all(disk_cache_path).await?;

        let memory_capacity = config.capacity().as_u64() as usize;
        let disk_capacity = disk_cache_capacity.as_u64() as usize;

        info!(
            memory_capacity_mb = memory_capacity / 1024 / 1024,
            disk_capacity_mb = disk_capacity / 1024 / 1024,
            disk_path = %disk_cache_path.display(),
            "building hybrid leaf search cache"
        );

        let device = foyer::FsDeviceBuilder::new(disk_cache_path)
            .with_capacity(disk_capacity)
            .build()?;
        let engine_config = foyer::BlockEngineConfig::new(device);

        let hybrid_cache: foyer::HybridCache<CacheKey, CachedValue> =
            foyer::HybridCacheBuilder::new()
                .memory(memory_capacity)
                .storage()
                .with_engine_config(engine_config)
                .build()
                .await?;

        let metrics = quickwit_storage::STORAGE_METRICS
            .partial_request_cache
            .cache_metrics
            .clone();

        Ok(LeafSearchCache::Hybrid {
            cache: hybrid_cache,
            metrics,
        })
    }

    pub async fn get(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
    ) -> Option<LeafSearchResponse> {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);
        match self {
            LeafSearchCache::MemoryOnly { content } => {
                let encoded_result = content.get(&key)?;
                LeafSearchResponse::decode(&*encoded_result).ok()
            }
            LeafSearchCache::Hybrid { cache, metrics } => {
                let memory_size = cache.memory().usage();
                let entry = cache.get(&key).await;
                match entry {
                    Ok(Some(entry)) => {
                        metrics.hits_num_items.inc();
                        let bytes = &entry.value().0;
                        metrics.hits_num_bytes.inc_by(bytes.len() as u64);
                        tracing::debug!(
                            split_id = %key.split_id,
                            memory_usage = memory_size,
                            "hybrid cache HIT"
                        );
                        LeafSearchResponse::decode(bytes.as_slice()).ok()
                    }
                    Ok(None) => {
                        metrics.misses_num_items.inc();
                        tracing::debug!(
                            split_id = %key.split_id,
                            memory_usage = memory_size,
                            "hybrid cache MISS"
                        );
                        None
                    }
                    Err(err) => {
                        metrics.misses_num_items.inc();
                        warn!(error = %err, "hybrid cache get failed, treating as miss");
                        None
                    }
                }
            }
        }
    }

    /// Insert a result into the cache. This is synchronous — foyer's `insert()` writes
    /// to the memory tier immediately and spills to disk asynchronously in the background.
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
        match self {
            LeafSearchCache::MemoryOnly { content } => {
                content.put(key, OwnedBytes::new(encoded_result));
            }
            LeafSearchCache::Hybrid { cache, metrics } => {
                metrics.in_cache_num_bytes.add(encoded_result.len() as i64);
                metrics.in_cache_count.inc();
                cache.insert(key, CachedValue(encoded_result));
            }
        }
    }
}

/// A key inside a [`LeafSearchCache`].
///
/// `Serialize`/`Deserialize` are needed for the foyer disk cache (via the `Code` trait).
#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// A cache for split footers, optionally backed by a foyer disk tier.
///
/// `get` is async because the disk tier requires it; the memory-only
/// variant returns immediately.
// See `LeafSearchCache` comment: boxing the `Hybrid` variant would add a per-hit indirection.
#[allow(clippy::large_enum_variant)]
pub enum SplitFooterCache {
    Memory {
        content: MemorySizedCache<String>,
    },
    Hybrid {
        cache: foyer::HybridCache<String, FooterValue>,
        metrics: quickwit_storage::metrics::SingleCacheMetrics,
    },
}

/// Serializable wrapper around footer bytes for the foyer disk cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FooterValue(Vec<u8>);

impl SplitFooterCache {
    pub fn new(config: &CacheConfig) -> Self {
        SplitFooterCache::Memory {
            content: MemorySizedCache::from_config(
                config,
                &quickwit_storage::STORAGE_METRICS.split_footer_cache,
            ),
        }
    }

    pub async fn new_hybrid(
        config: &CacheConfig,
        disk_cache_path: &Path,
        disk_cache_capacity: ByteSize,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(disk_cache_path).await?;

        let memory_capacity = config.capacity().as_u64() as usize;
        let disk_capacity = disk_cache_capacity.as_u64() as usize;

        info!(
            memory_capacity_mb = memory_capacity / 1024 / 1024,
            disk_capacity_mb = disk_capacity / 1024 / 1024,
            disk_path = %disk_cache_path.display(),
            "building hybrid split footer cache"
        );

        let device = foyer::FsDeviceBuilder::new(disk_cache_path)
            .with_capacity(disk_capacity)
            .build()?;
        let engine_config = foyer::BlockEngineConfig::new(device);

        let hybrid_cache: foyer::HybridCache<String, FooterValue> =
            foyer::HybridCacheBuilder::new()
                .memory(memory_capacity)
                .storage()
                .with_engine_config(engine_config)
                .build()
                .await?;

        let metrics = quickwit_storage::STORAGE_METRICS
            .split_footer_cache
            .cache_metrics
            .clone();

        Ok(SplitFooterCache::Hybrid {
            cache: hybrid_cache,
            metrics,
        })
    }

    pub async fn get(&self, split_id: &str) -> Option<OwnedBytes> {
        match self {
            // `MemorySizedCache<String>::get` requires `&K` (not `&str`) because
            // `Arc<String>: Borrow<str>` is not implemented. One small alloc per hit.
            SplitFooterCache::Memory { content } => content.get(&split_id.to_owned()),
            SplitFooterCache::Hybrid { cache, metrics } => match cache.get(split_id).await {
                Ok(Some(entry)) => {
                    metrics.hits_num_items.inc();
                    let bytes = &entry.value().0;
                    metrics.hits_num_bytes.inc_by(bytes.len() as u64);
                    Some(OwnedBytes::new(bytes.clone()))
                }
                Ok(None) => {
                    metrics.misses_num_items.inc();
                    None
                }
                Err(err) => {
                    metrics.misses_num_items.inc();
                    warn!(error = %err, "hybrid split footer cache get failed, treating as miss");
                    None
                }
            },
        }
    }

    pub fn put(&self, split_id: String, bytes: OwnedBytes) {
        match self {
            SplitFooterCache::Memory { content } => content.put(split_id, bytes),
            SplitFooterCache::Hybrid { cache, metrics } => {
                metrics.in_cache_num_bytes.add(bytes.len() as i64);
                metrics.in_cache_count.inc();
                cache.insert(split_id, FooterValue(bytes.to_vec()));
            }
        }
    }
}

pub struct PredicateCacheImpl {
    content: MemorySizedCache<(SplitId, String)>,
}

impl PredicateCacheImpl {
    pub fn new(config: &CacheConfig) -> Self {
        PredicateCacheImpl {
            content: MemorySizedCache::from_config(
                config,
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
    use bytesize::ByteSize;
    use quickwit_proto::search::{
        LeafResourceStats, LeafSearchResponse, PartialHit, SearchRequest, SortValue,
        SplitIdAndFooterOffsets,
    };

    use super::LeafSearchCache;

    #[tokio::test]
    async fn test_leaf_search_cache_no_timestamp() {
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

        assert!(cache.get(split_1.clone(), query_1.clone()).await.is_none());

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
            cache.get(split_1.clone(), query_1.clone()).await.unwrap(),
            expected
        );
        assert!(cache.get(split_2, query_1).await.is_none());
        assert!(cache.get(split_1, query_2).await.is_none());
    }

    #[tokio::test]
    async fn test_leaf_search_cache_timestamp() {
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
        assert!(cache.get(split_1.clone(), query_1.clone()).await.is_some());
        assert!(
            cache
                .get(split_1.clone(), query_1bis.clone())
                .await
                .is_none()
        );

        // for split_2, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_2.clone(), query_1.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_1).await.is_some());
        assert!(cache.get(split_2.clone(), query_1bis).await.is_some());

        // for split_1, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_1.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_1.clone(), query_2.clone()).await.is_some());
        assert!(cache.get(split_1, query_2bis.clone()).await.is_some());

        // for split_2, 2 covers everything, but 2bis cover only a subrange
        cache.put(split_2.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_2.clone()).await.is_some());
        assert!(cache.get(split_2, query_2bis.clone()).await.is_none());

        // same for split_3, but we try caching the bounded request and query for the unbounded one
        cache.put(split_3.clone(), query_2bis.clone(), result);
        assert!(cache.get(split_3.clone(), query_2).await.is_none());
        assert!(cache.get(split_3, query_2bis).await.is_some());
    }

    #[tokio::test]
    async fn test_hybrid_cache_basic() {
        let tmp_dir =
            std::env::temp_dir().join(format!("quickwit-test-hybrid-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        let cache =
            LeafSearchCache::new_hybrid(&ByteSize::mb(64).into(), &tmp_dir, ByteSize::mb(100))
                .await
                .expect("failed to create hybrid cache");

        let split = SplitIdAndFooterOffsets {
            split_id: "split_hybrid".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0,
        };

        let query = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: "hybrid_test".to_string(),
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
            num_hits: 42,
            partial_hits: vec![PartialHit {
                doc_id: 1,
                segment_ord: 0,
                sort_value: Some(SortValue::U64(0u64).into()),
                sort_value2: None,
                split_id: "split_hybrid".to_string(),
            }],
            resource_stats: None,
        };

        // miss before put
        assert!(
            cache.get(split.clone(), query.clone()).await.is_none(),
            "expected miss before put"
        );

        // put
        cache.put(split.clone(), query.clone(), result.clone());

        // hit after put
        let cached = cache.get(split.clone(), query.clone()).await;
        assert!(cached.is_some(), "expected hit after put, got None");
        assert_eq!(cached.unwrap().num_hits, 42);
    }
}
