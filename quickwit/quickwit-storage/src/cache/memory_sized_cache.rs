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

use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use quickwit_config::CacheConfig;

use crate::OwnedBytes;
use crate::cache::base_cache::{AnyCache, FakeCacheEntry};
use crate::cache::slice_address::{SliceAddress, SliceAddressKey, SliceAddressRef};
use crate::metrics::CacheMetrics;

struct CacheState<K: Hash + Eq> {
    cache: AnyCache<K>,
    virtual_caches: Vec<AnyCache<K, FakeCacheEntry>>,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> CacheState<K> {
    fn from_config(cache_config: &CacheConfig, cache_counters: &'static CacheMetrics) -> Self {
        let cache = AnyCache::from_policy_and_capacity(
            cache_config.policy(),
            cache_config.capacity(),
            cache_counters.cache_metrics.clone(),
        );
        let virtual_caches = cache_config
            .virtual_caches
            .iter()
            .cloned()
            .map(|mut virtual_cache_config| {
                AnyCache::from_policy_and_capacity(
                    virtual_cache_config.policy_for_virtual_cache(cache_config.policy()),
                    virtual_cache_config.capacity_for_virtual_cache(cache_config.capacity()),
                    cache_counters.virtual_cache(&virtual_cache_config),
                )
            })
            .collect();
        CacheState {
            cache,
            virtual_caches,
        }
    }

    fn infinite(cache_counters: &'static CacheMetrics) -> Self {
        CacheState {
            cache: AnyCache::unbounded(cache_counters.cache_metrics.clone()),
            // there is no point in having virtual caches for an unbounded cache
            virtual_caches: Vec::new(),
        }
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<OwnedBytes>
    where
        K: Borrow<Q>,
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        for virtual_cache in &mut self.virtual_caches {
            // we simulate an access on all virtual caches
            virtual_cache.get(cache_key);
        }
        self.cache.get(cache_key)
    }

    fn put(&mut self, key: K, bytes: OwnedBytes) {
        for virtual_cache in &mut self.virtual_caches {
            // we simulate an access on all virtual caches
            virtual_cache.put(key.clone(), FakeCacheEntry(bytes.len()));
        }

        self.cache.put(key, bytes)
    }
}

/// A simple in-resident memory slice cache.
pub struct MemorySizedCache<K: Hash + Eq = SliceAddress> {
    inner: Mutex<CacheState<K>>,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> MemorySizedCache<K> {
    /// Creates an slice cache with the given capacity.
    pub fn from_config(cache_config: &CacheConfig, cache_counters: &'static CacheMetrics) -> Self {
        MemorySizedCache {
            inner: Mutex::new(CacheState::from_config(cache_config, cache_counters)),
        }
    }

    /// Creates a slice cache that never removes any entry.
    pub fn with_infinite_capacity(cache_counters: &'static CacheMetrics) -> Self {
        MemorySizedCache {
            inner: Mutex::new(CacheState::infinite(cache_counters)),
        }
    }

    /// If available, returns the cached view of the slice.
    pub fn get<Q>(&self, cache_key: &Q) -> Option<OwnedBytes>
    where
        K: Borrow<Q>,
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.lock().unwrap().get(cache_key)
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    pub fn put(&self, val: K, bytes: OwnedBytes) {
        self.inner.lock().unwrap().put(val, bytes);
    }
}

impl MemorySizedCache<SliceAddress> {
    /// If available, returns the cached view of the slice.
    pub fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let slice_address_ref = SliceAddressRef { path, byte_range };
        self.get(&slice_address_ref as &dyn SliceAddressKey)
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    pub fn put_slice(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        let slice_address = SliceAddress { path, byte_range };
        self.put(slice_address, bytes);
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;
    use crate::cache::base_cache::LRU_MIN_TIME_SINCE_LAST_ACCESS;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    #[tokio::test]
    async fn test_cache_edge_condition() {
        tokio::time::pause();
        let cache = MemorySizedCache::<String>::from_config(
            &ByteSize::b(5).into(),
            &CACHE_METRICS_FOR_TESTS,
        );
        {
            let data = OwnedBytes::new(&b"abc"[..]);
            cache.put("3".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
        }
        {
            let data = OwnedBytes::new(&b"de"[..]);
            cache.put("2".to_string(), data);
            // our first entry should still be here.
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(&"2".to_string()).unwrap(), &b"de"[..]);
        }
        {
            let data = OwnedBytes::new(&b"fghij"[..]);
            cache.put("5".to_string(), data);
            // Eviction should not happen, because all items in cache are too young.
            assert!(cache.get(&"5".to_string()).is_none());
        }
        tokio::time::advance(LRU_MIN_TIME_SINCE_LAST_ACCESS.mul_f32(1.1f32)).await;
        {
            let data = OwnedBytes::new(&b"fghij"[..]);
            cache.put("5".to_string(), data);
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
            // our two first entries should have be removed from the cache
            assert!(cache.get(&"2".to_string()).is_none());
            assert!(cache.get(&"3".to_string()).is_none());
        }
        tokio::time::advance(LRU_MIN_TIME_SINCE_LAST_ACCESS.mul_f32(1.1f32)).await;
        {
            let data = OwnedBytes::new(&b"klmnop"[..]);
            cache.put("6".to_string(), data);
            // The entry put should have been dismissed as it is too large for the cache
            assert!(cache.get(&"6".to_string()).is_none());
            // The previous entry should however be remaining.
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
        }
    }

    #[test]
    fn test_cache_edge_unlimited_capacity() {
        let cache = MemorySizedCache::with_infinite_capacity(&CACHE_METRICS_FOR_TESTS);
        {
            let data = OwnedBytes::new(&b"abc"[..]);
            cache.put("3".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
        }
        {
            let data = OwnedBytes::new(&b"de"[..]);
            cache.put("2".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(&"2".to_string()).unwrap(), &b"de"[..]);
        }
    }

    #[test]
    fn test_cache() {
        let cache =
            MemorySizedCache::from_config(&ByteSize::kb(10).into(), &CACHE_METRICS_FOR_TESTS);
        assert!(cache.get(&"hello.seg").is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        cache.put("hello.seg", data);
        assert_eq!(cache.get(&"hello.seg").unwrap(), &b"werwer"[..]);
    }

    #[test]
    fn test_cache_no_cache() {
        let cache =
            MemorySizedCache::from_config(&CacheConfig::no_cache(), &CACHE_METRICS_FOR_TESTS);
        assert!(cache.get(&"hello.seg").is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        cache.put("hello.seg", data);
        assert!(cache.get(&"hello.seg").is_none());
    }
}
