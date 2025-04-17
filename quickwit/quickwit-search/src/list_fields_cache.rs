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

use quickwit_proto::search::{
    ListFields, SplitIdAndFooterOffsets, deserialize_split_fields, serialize_split_fields,
};
use quickwit_proto::types::SplitId;
use quickwit_storage::{MemorySizedCache, OwnedBytes};

/// A cache to memoize `leaf_search_single_split` results.
pub struct ListFieldsCache {
    content: MemorySizedCache<CacheKey>,
}

// TODO For now this simply caches the whole ListFieldsEntryResponse. We could
// be more clever and cache aggregates instead.
impl ListFieldsCache {
    pub fn new(capacity: usize) -> ListFieldsCache {
        ListFieldsCache {
            content: MemorySizedCache::with_capacity_in_bytes(
                capacity,
                &quickwit_storage::STORAGE_METRICS.partial_request_cache,
            ),
        }
    }
    pub fn get(&self, split_info: SplitIdAndFooterOffsets) -> Option<ListFields> {
        let key = CacheKey::from_split_meta(split_info);
        let encoded_result = self.content.get(&key)?;
        // this should never fail
        deserialize_split_fields(encoded_result).ok()
    }

    pub fn put(&self, split_info: SplitIdAndFooterOffsets, list_fields: ListFields) {
        let key = CacheKey::from_split_meta(split_info);

        let encoded_result = serialize_split_fields(list_fields);
        self.content.put(key, OwnedBytes::new(encoded_result));
    }
}

/// A key inside a [`ListFieldsCache`].
#[derive(Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    /// The split this entry refers to
    split_id: SplitId,
}

impl CacheKey {
    fn from_split_meta(split_info: SplitIdAndFooterOffsets) -> Self {
        CacheKey {
            split_id: split_info.split_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{
        ListFieldType, ListFields, ListFieldsEntryResponse, SplitIdAndFooterOffsets,
    };

    use super::ListFieldsCache;

    #[test]
    fn test_list_fields_cache() {
        let cache = ListFieldsCache::new(64_000_000);

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

        let result = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index4".to_string()],
        };

        assert!(cache.get(split_1.clone()).is_none());

        let list_fields = ListFields {
            fields: vec![result.clone()],
        };

        cache.put(split_1.clone(), list_fields.clone());
        assert_eq!(cache.get(split_1.clone()).unwrap(), list_fields);
        assert!(cache.get(split_2).is_none());
    }
}
