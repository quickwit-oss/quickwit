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
use quickwit_storage::metrics::{CacheMetrics, PARTIAL_REQUEST_CACHE};
use quickwit_storage::{FeuerCache, OwnedBytes};

/// Feuer memory cache for immutable per-split field metadata.
pub struct ListFieldsCache {
    cache: FeuerCache,
    metrics: Option<&'static CacheMetrics>,
}

impl ListFieldsCache {
    pub fn new(config: &CacheConfig) -> ListFieldsCache {
        Self {
            cache: FeuerCache::memory(config.memory_capacity().as_u64(), "list_fields")
                .expect("invalid Feuer memory cache configuration"),
            metrics: (config.memory_capacity().as_u64() > 0).then_some(&PARTIAL_REQUEST_CACHE),
        }
    }

    pub async fn get(&self, split_id: &str) -> Option<OwnedBytes> {
        let bytes = self.cache.get(split_id.to_owned()).await;
        if let Some(metrics) = self.metrics {
            metrics.record_get(bytes.as_ref().map(|bytes| bytes.len()));
        }
        bytes
    }

    pub async fn put(&self, split_id: String, serialized_split_fields: OwnedBytes) {
        self.cache.put(split_id, serialized_split_fields).await;
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;

    #[tokio::test]
    async fn list_fields_cache_get_miss_returns_none() {
        let cache =
            ListFieldsCache::new(&CacheConfig::default_with_memory_capacity(ByteSize::mb(1)));
        assert!(cache.get("test-split-id").await.is_none());
    }

    #[tokio::test]
    async fn list_fields_cache_put_then_get_roundtrips() {
        let cache =
            ListFieldsCache::new(&CacheConfig::default_with_memory_capacity(ByteSize::mb(1)));
        let payload = OwnedBytes::new(b"hello".to_vec());
        cache
            .put("test-split-id".to_string(), payload.clone())
            .await;
        assert_eq!(cache.get("test-split-id").await, Some(payload));
        assert!(cache.get("other-split-id").await.is_none());
    }

    #[tokio::test]
    async fn list_fields_cache_uses_memory_capacity_with_disk_configured() {
        // List-fields shares the partial-request memory capacity, while remaining memory-only.
        let config: CacheConfig = serde_json::from_value(serde_json::json!({
            "memory_capacity": "1MiB",
            "disk_capacity": "8MiB",
        }))
        .unwrap();
        let cache = ListFieldsCache::new(&config);
        cache
            .put("split".to_string(), OwnedBytes::new(&b"fields"[..]))
            .await;
        assert_eq!(cache.get("split").await.unwrap().as_slice(), b"fields");
    }

    #[tokio::test]
    async fn list_fields_cache_zero_capacity_disables_caching() {
        let cache = ListFieldsCache::new(&CacheConfig::no_cache());
        cache
            .put("split".to_string(), OwnedBytes::new(&b"fields"[..]))
            .await;
        assert!(cache.get("split").await.is_none());
    }

    #[tokio::test]
    async fn list_fields_cache_retains_first_value_above_capacity() {
        let cache = ListFieldsCache::new(&ByteSize::b(1).into());
        cache
            .put("split".to_string(), OwnedBytes::new(&b"fields"[..]))
            .await;
        cache
            .put("split".to_string(), OwnedBytes::new(&b"replacement"[..]))
            .await;
        assert_eq!(cache.get("split").await.unwrap().as_slice(), b"fields");
    }
}
