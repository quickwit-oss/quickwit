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

use quickwit_proto::search::{
    deserialize_split_fields, serialize_split_fields, ListFields, SplitIdAndFooterOffsets,
};
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

/// A key inside a [`LeafSearchCache`].
#[derive(Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    /// The split this entry refers to
    split_id: String,
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
        };

        let split_2 = SplitIdAndFooterOffsets {
            split_id: "split_2".to_string(),
            split_footer_start: 0,
            split_footer_end: 100,
            timestamp_start: None,
            timestamp_end: None,
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
