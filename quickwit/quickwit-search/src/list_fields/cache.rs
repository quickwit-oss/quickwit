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

use std::hash::Hash;

use quickwit_config::CacheConfig;
use quickwit_storage::{FeuerMemoryCache, OwnedBytes};
use siphasher::sip128::{Hasher128, SipHasher13};

/// Feuer memory cache for immutable per-split field metadata.
pub struct ListFieldsCache {
    cache: FeuerMemoryCache,
}

impl ListFieldsCache {
    pub fn new(config: &CacheConfig) -> ListFieldsCache {
        Self {
            cache: FeuerMemoryCache::new(
                config.capacity().as_u64(),
                "list_fields",
                &quickwit_storage::metrics::PARTIAL_REQUEST_CACHE,
            )
            .expect("invalid Feuer memory cache configuration"),
        }
    }

    pub fn get(&self, split_id: &str) -> Option<OwnedBytes> {
        self.cache.get(split_key(split_id))
    }

    pub fn put(&self, split_id: String, serialized_split_fields: OwnedBytes) {
        self.cache
            .put(split_key(&split_id), serialized_split_fields);
    }
}

fn split_key(split_id: &str) -> u128 {
    let mut hasher = SipHasher13::new();
    split_id.hash(&mut hasher);
    hasher.finish128().as_u128()
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;

    #[test]
    fn list_fields_cache_get_miss_returns_none() {
        let cache = ListFieldsCache::new(&CacheConfig::default_with_capacity(ByteSize::mb(1)));
        assert!(cache.get("test-split-id").is_none());
    }

    #[test]
    fn list_fields_cache_put_then_get_roundtrips() {
        let cache = ListFieldsCache::new(&CacheConfig::default_with_capacity(ByteSize::mb(1)));
        let payload = OwnedBytes::new(b"hello".to_vec());
        cache.put("test-split-id".to_string(), payload.clone());
        assert_eq!(cache.get("test-split-id"), Some(payload));
        assert!(cache.get("other-split-id").is_none());
    }

    #[test]
    fn list_fields_cache_uses_memory_capacity_with_disk_configured() {
        // List-fields shares the partial-request memory capacity, while remaining memory-only.
        let config: CacheConfig = serde_json::from_value(serde_json::json!({
            "memory_capacity": "1MiB",
            "disk_capacity": "8MiB",
        }))
        .unwrap();
        let cache = ListFieldsCache::new(&config);
        cache.put("split".to_string(), OwnedBytes::new(&b"fields"[..]));
        assert_eq!(cache.get("split").unwrap().as_slice(), b"fields");
    }

    #[test]
    fn list_fields_cache_zero_capacity_disables_caching() {
        let cache = ListFieldsCache::new(&CacheConfig::no_cache());
        cache.put("split".to_string(), OwnedBytes::new(&b"fields"[..]));
        assert!(cache.get("split").is_none());
    }

    #[test]
    fn list_fields_cache_uses_feuer_soft_capacity_and_immutable_entries() {
        let cache = ListFieldsCache::new(&ByteSize::b(1).into());
        cache.put("split".to_string(), OwnedBytes::new(&b"fields"[..]));
        cache.put("split".to_string(), OwnedBytes::new(&b"replacement"[..]));
        assert_eq!(cache.get("split").unwrap().as_slice(), b"fields");
    }
}
