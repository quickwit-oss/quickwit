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

use std::future::Future;
use std::hash::Hash;

use quickwit_config::CacheConfig;
use quickwit_storage::metrics::CacheMetrics;
use quickwit_storage::{FeuerCache, FeuerMemoryCache, OwnedBytes};
use serde::Serialize;

use super::CacheKeyHasher;

pub(super) enum ResultCache {
    Memory {
        cache: FeuerMemoryCache,
        hasher: CacheKeyHasher,
    },
    Disk {
        cache: FeuerCache,
        runtime: tokio::runtime::Handle,
    },
}

impl ResultCache {
    pub fn memory(
        config: &CacheConfig,
        cache_name: &'static str,
        metrics: &'static CacheMetrics,
    ) -> Self {
        Self::Memory {
            cache: FeuerMemoryCache::new(config.capacity().as_u64(), cache_name, metrics)
                .expect("invalid Feuer memory cache configuration"),
            hasher: CacheKeyHasher::random(),
        }
    }

    pub fn disk(cache: FeuerCache) -> Self {
        Self::Disk {
            cache,
            runtime: tokio::runtime::Handle::current(),
        }
    }

    pub async fn get(&self, key: &(impl Hash + Serialize)) -> Option<OwnedBytes> {
        match self {
            Self::Memory { cache, hasher } => cache.get(hasher.hash(key).0),
            Self::Disk { cache, .. } => cache.get(object_key(key)).await,
        }
    }

    pub async fn put(&self, key: &(impl Hash + Serialize), bytes: OwnedBytes) -> bool {
        match self {
            Self::Memory { cache, hasher } => cache.put(hasher.hash(key).0, bytes),
            Self::Disk { cache, .. } => {
                cache.put(object_key(key), bytes).await;
                true
            }
        }
    }

    /// Bridge Tantivy's synchronous predicate API. Disk-backed calls MUST run on a CPU or
    /// blocking thread, never a Tokio worker. The saved runtime drives Feuer's I/O tasks.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Self::Memory { .. } => futures::executor::block_on(future),
            Self::Disk { runtime, .. } => runtime.block_on(future),
        }
    }
}

fn object_key(key: &impl Serialize) -> String {
    // Unlike the randomized in-memory hash, this identity survives restarts. Bump the
    // version when changing key normalization or the encoded result format.
    format!(
        "v1:{}",
        serde_json::to_string(key).expect("cache keys are serializable")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_result_cache_uses_feuer_without_a_runtime() {
        let cache = ResultCache::memory(
            &bytesize::ByteSize::b(1).into(),
            "predicate",
            &quickwit_storage::metrics::PREDICATE_CACHE,
        );
        // Feuer admits oversized immutable entries against its soft capacity target.
        // The former MemorySizedCache rejected entries larger than its capacity.
        assert!(cache.block_on(cache.put(&"key", OwnedBytes::new(&b"result"[..]))));
        assert_eq!(
            cache.block_on(cache.get(&"key")).unwrap().as_slice(),
            b"result"
        );
        assert!(cache.block_on(cache.get(&"other-key")).is_none());
    }

    #[test]
    fn test_zero_memory_capacity_disables_result_caching() {
        let cache = ResultCache::memory(
            &CacheConfig::no_cache(),
            "predicate",
            &quickwit_storage::metrics::PREDICATE_CACHE,
        );
        assert!(!cache.block_on(cache.put(&"key", OwnedBytes::new(&b"result"[..]))));
        assert!(cache.block_on(cache.get(&"key")).is_none());
    }

    #[test]
    fn test_object_keys_are_stable_and_separate_entries() {
        let borrowed_key = ("split", "predicate");
        let owned_key = ("split".to_string(), "predicate".to_string());
        assert_eq!(object_key(&borrowed_key), object_key(&owned_key));
        assert!(object_key(&borrowed_key).starts_with("v1:"));
        assert_ne!(
            object_key(&borrowed_key),
            object_key(&("other-split", "predicate"))
        );
        assert_ne!(
            object_key(&borrowed_key),
            object_key(&("split", "other-predicate"))
        );
    }
}
