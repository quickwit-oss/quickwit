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

use quickwit_config::CacheConfig;
use quickwit_storage::{MemorySizedCache, OwnedBytes};

pub struct ListFieldsCache {
    cache: MemorySizedCache<String>,
}

impl ListFieldsCache {
    pub fn new(config: &CacheConfig) -> ListFieldsCache {
        Self {
            cache: MemorySizedCache::from_config(
                config,
                &quickwit_storage::metrics::PARTIAL_REQUEST_CACHE,
            ),
        }
    }

    pub fn get(&self, split_id: &String) -> Option<OwnedBytes> {
        self.cache.get(split_id)
    }

    pub fn put(&self, split_id: String, serialized_split_fields: OwnedBytes) {
        self.cache.put(split_id, serialized_split_fields);
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;

    #[test]
    fn list_fields_cache_get_miss_returns_none() {
        let cache = ListFieldsCache::new(&CacheConfig::default_with_capacity(ByteSize::mb(1)));
        assert!(cache.get(&"test-split-id".to_string()).is_none());
    }

    #[test]
    fn list_fields_cache_put_then_get_roundtrips() {
        let cache = ListFieldsCache::new(&CacheConfig::default_with_capacity(ByteSize::mb(1)));
        let payload = OwnedBytes::new(b"hello".to_vec());
        cache.put("test-split-id".to_string(), payload.clone());
        assert_eq!(cache.get(&"test-split-id".to_string()), Some(payload));
    }
}
