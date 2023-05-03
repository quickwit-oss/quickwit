use quickwit_proto::{LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets};
use quickwit_storage::MemorySizedCache;

pub struct LeafSearchCache {
    content: MemorySizedCache<CacheKey, LeafSearchResponse>,
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
        self.content.get(&key)
    }

    pub fn put(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        result: LeafSearchResponse,
    ) {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);

        self.content.put(key, result);
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    split_id: String,
    request: SearchRequest,
    request_time_range: Range,
}

impl CacheKey {
    fn from_split_meta_and_request(
        split_info: SplitIdAndFooterOffsets,
        mut search_request: SearchRequest,
    ) -> Self {
        let split_time_range = Range {
            start: split_info.timestamp_start,
            end: split_info.timestamp_end,
        };
        let request_time_range = Range {
            start: search_request.start_timestamp,
            end: search_request.end_timestamp,
        }
        .crop(&split_time_range);

        search_request.start_timestamp = None;
        search_request.end_timestamp = None;

        CacheKey {
            split_id: split_info.split_id,
            request: search_request,
            request_time_range,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Range {
    start: Option<i64>,
    end: Option<i64>,
}

impl Range {
    fn crop(&self, other: &Range) -> Range {
        let start = match (self.start, other.start) {
            (Some(this), Some(other)) => Some(this.max(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };

        let end = match (self.end, other.end) {
            (Some(this), Some(other)) => Some(this.min(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };
        Range { start, end }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::{LeafSearchResponse, PartialHit, SearchRequest, SplitIdAndFooterOffsets};

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
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test2".to_string(),
            search_fields: vec!["body".to_string()],
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
                sorting_field_value: 0,
                split_id: "split_1".to_string(),
            }],
        };

        assert!(cache.get(split_1.clone(), query_1.clone()).is_none());

        cache.put(split_1.clone(), query_1.clone(), result.clone());
        assert_eq!(cache.get(split_1.clone(), query_1.clone()).unwrap(), result);
        assert!(cache.get(split_2.clone(), query_1.clone()).is_none());
        assert!(cache.get(split_1.clone(), query_2.clone()).is_none());
    }

    #[test]
    fn test_leaf_search_cache_timestamp() {
        let cache = LeafSearchCache::new(64_000_000);

        let split_1 = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(100),
            timestamp_end: Some(200),
        };
        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(250),
        };
        let split_3 = SplitIdAndFooterOffsets {
            split_id: "split_3".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: Some(150),
            timestamp_end: Some(250),
        };

        let query_1 = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: Some(100),
            end_timestamp: Some(250),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_1bis = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: Some(150),
            end_timestamp: Some(300),
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };

        let query_2 = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test2".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            ..Default::default()
        };
        let query_2bis = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test2".to_string(),
            search_fields: vec!["body".to_string()],
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
                sorting_field_value: 0,
                split_id: "split_1".to_string(),
            }],
        };

        // for split_1, 1 and 1bis cover different timestamp ranges
        cache.put(split_1.clone(), query_1.clone(), result.clone());
        assert!(cache.get(split_1.clone(), query_1.clone()).is_some());
        assert!(cache.get(split_1.clone(), query_1bis.clone()).is_none());

        // for split_2, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_2.clone(), query_1.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_1.clone()).is_some());
        assert!(cache.get(split_2.clone(), query_1bis.clone()).is_some());

        // for split_1, both 1 and 1bis cover everything, so it should cache-hit
        cache.put(split_1.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_1.clone(), query_2.clone()).is_some());
        assert!(cache.get(split_1.clone(), query_2bis.clone()).is_some());

        // for split_2, 2 covers everything, but 2bis cover only a subrange
        cache.put(split_2.clone(), query_2.clone(), result.clone());
        assert!(cache.get(split_2.clone(), query_2.clone()).is_some());
        assert!(cache.get(split_2.clone(), query_2bis.clone()).is_none());

        // same for split_3, but we try caching the bounded request and query for the unbounded one
        cache.put(split_3.clone(), query_2bis.clone(), result.clone());
        assert!(cache.get(split_3.clone(), query_2.clone()).is_none());
        assert!(cache.get(split_3.clone(), query_2bis.clone()).is_some());
    }
}
