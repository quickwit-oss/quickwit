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
use crate::cache::mem_usage::MemUsage;
use crate::cache::slice_address::{SliceAddress, SliceAddressKey, SliceAddressRef};
use crate::metrics::CacheMetrics;

struct CacheState<K: Hash + Eq> {
    cache: AnyCache<K>,
    virtual_caches: Vec<AnyCache<K, FakeCacheEntry>>,
}

impl<K: Hash + Eq + Clone + MemUsage + Send + Sync + 'static> CacheState<K> {
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

impl<K: Hash + Eq + Clone + MemUsage + Send + Sync + 'static> MemorySizedCache<K> {
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
    ///
    /// An entry is charged for its key as well as its value, so this may fail silently if the key
    /// and the owned_bytes slice together are larger than the cache capacity.
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
    ///
    /// An entry is charged for its key as well as its value, so this may fail silently if the key
    /// and the owned_bytes slice together are larger than the cache capacity.
    pub fn put_slice(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        let slice_address = SliceAddress { path, byte_range };
        self.put(slice_address, bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use bytesize::ByteSize;
    use quickwit_config::CachePolicy;

    use super::*;
    use crate::cache::base_cache::LRU_MIN_TIME_SINCE_LAST_ACCESS;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    /// Memory charged for one of the single-character `String` keys used below: the inline size of
    /// a `String` plus its one byte of heap. Entries are charged their key on top of their value,
    /// so capacities in these tests are expressed relative to it.
    const KEY_COST: usize = size_of::<String>() + 1;

    #[tokio::test]
    async fn test_cache_edge_condition() {
        tokio::time::pause();
        // Room for the two first entries ("abc" and "de") and their keys, exactly.
        let capacity = ByteSize::b((2 * KEY_COST + 5) as u64);
        let cache =
            MemorySizedCache::<String>::from_config(&capacity.into(), &CACHE_METRICS_FOR_TESTS);
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
            // Large enough that even alone with its key it overflows the whole cache.
            let data = OwnedBytes::new(vec![0u8; capacity.as_u64() as usize - KEY_COST + 1]);
            cache.put("6".to_string(), data);
            // The entry put should have been dismissed as it is too large for the cache
            assert!(cache.get(&"6".to_string()).is_none());
            // The previous entry should however be remaining.
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
        }
    }

    #[test]
    fn test_cache_charges_the_key() {
        let data = OwnedBytes::new(&b"abc"[..]);
        // A capacity covering the value alone is not enough: the key is charged too.
        let value_only_cache = MemorySizedCache::<String>::from_config(
            &ByteSize::b(data.len() as u64).into(),
            &CACHE_METRICS_FOR_TESTS,
        );
        value_only_cache.put("3".to_string(), data.clone());
        assert!(value_only_cache.get(&"3".to_string()).is_none());

        // Room for the key on top of the value, and the very same entry fits.
        let key_and_value_cache = MemorySizedCache::<String>::from_config(
            &ByteSize::b((KEY_COST + data.len()) as u64).into(),
            &CACHE_METRICS_FOR_TESTS,
        );
        key_and_value_cache.put("3".to_string(), data);
        assert_eq!(
            key_and_value_cache.get(&"3".to_string()).unwrap(),
            &b"abc"[..]
        );
    }

    #[test]
    fn test_every_policy_charges_the_key() {
        const NUM_ENTRIES: usize = 500;
        const KEY_PADDING_LEN: usize = 8_000;
        let capacity = ByteSize::mb(1);
        let keys: Vec<String> = (0..NUM_ENTRIES)
            .map(|i| format!("{i:08}{}", "k".repeat(KEY_PADDING_LEN)))
            .collect();
        // How many entries the budget can hold once keys are charged.
        let expected_num_entries =
            capacity.as_u64() as usize / (keys[0].mem_usage() + "0123456789abcdef".len());

        for policy in [CachePolicy::Lru, CachePolicy::S3Fifo, CachePolicy::TinyLfu] {
            let cache = MemorySizedCache::<String>::from_config(
                &CacheConfig::with_capacity_and_policy(capacity, policy),
                &CACHE_METRICS_FOR_TESTS,
            );
            for key in &keys {
                cache.put(key.clone(), OwnedBytes::new(&b"0123456789abcdef"[..]));
            }

            let num_entries = keys.iter().filter(|key| cache.get(*key).is_some()).count();
            assert_eq!(
                num_entries, expected_num_entries,
                "{policy:?} should have held only the entries whose keys and values fit in \
                 {capacity}"
            );
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
