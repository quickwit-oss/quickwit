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
use std::sync::Mutex;
use std::time::Duration;

use lru::LruCache;
use quickwit_config::CacheConfig;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::OwnedBytes;
use crate::cache::slice_address::{SliceAddress, SliceAddressKey, SliceAddressRef};
use crate::cache::stored_item::{StoredItem, ValueLen};
use crate::metrics::{CacheMetrics, SingleCacheMetrics};

/// We do not evict anything that has been accessed in the last 60s.
///
/// The goal is to behave better on scan access patterns, without being as aggressive as
/// using a MRU strategy.
///
/// TLDR is:
///
/// If two items have been access in the last 60s it is not really worth considering the
/// latter too be more recent than the previous and do an eviction.
/// The difference is not significant enough to raise the probability of its future access.
///
/// On the other hand, for very large queries involving enough data to saturate the cache,
/// we are facing a scanning pattern. If variations of this  query is repeated over and over
/// a regular LRU eviction policy would yield a hit rate of 0.
const MIN_TIME_SINCE_LAST_ACCESS: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Capacity {
    Unlimited,
    InBytes(usize),
}

impl Capacity {
    fn exceeds_capacity(&self, num_bytes: usize) -> bool {
        match *self {
            Capacity::Unlimited => false,
            Capacity::InBytes(capacity_in_bytes) => num_bytes > capacity_in_bytes,
        }
    }
}

struct NeedMutMemorySizedCache<K: Hash + Eq, V = OwnedBytes> {
    lru_cache: LruCache<K, StoredItem<V>>,
    num_items: usize,
    num_bytes: u64,
    capacity: Capacity,
    cache_counters: SingleCacheMetrics,
}

impl<K: Hash + Eq, V> Drop for NeedMutMemorySizedCache<K, V> {
    fn drop(&mut self) {
        // we don't count this toward evicted entries, as we are clearing the whole cache
        self.cache_counters
            .in_cache_count
            .sub(self.num_items as i64);
        self.cache_counters
            .in_cache_num_bytes
            .sub(self.num_bytes as i64);
    }
}

impl<K: Hash + Eq, V: ValueLen + Clone> NeedMutMemorySizedCache<K, V> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: Capacity, cache_counters: SingleCacheMetrics) -> Self {
        NeedMutMemorySizedCache {
            // The limit will be decided by the amount of memory in the cache,
            // not the number of items in the cache.
            // Enforcing this limit is done in the `NeedMutCache` impl.
            lru_cache: LruCache::unbounded(),
            num_items: 0,
            num_bytes: 0,
            capacity,
            cache_counters,
        }
    }

    pub fn record_item(&mut self, num_bytes: u64) {
        self.num_items += 1;
        self.num_bytes += num_bytes;
        self.cache_counters.in_cache_count.inc();
        self.cache_counters.in_cache_num_bytes.add(num_bytes as i64);
    }

    pub fn drop_item(&mut self, num_bytes: u64) {
        self.num_items -= 1;
        self.num_bytes -= num_bytes;
        self.cache_counters.in_cache_count.dec();
        self.cache_counters.in_cache_num_bytes.sub(num_bytes as i64);
        self.cache_counters.evict_num_items.inc();
        self.cache_counters.evict_num_bytes.inc_by(num_bytes);
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.lru_cache.get_mut(cache_key);
        if let Some(item) = item_opt {
            self.cache_counters.hits_num_items.inc();
            self.cache_counters.hits_num_bytes.inc_by(item.len() as u64);
            Some(item.payload())
        } else {
            self.cache_counters.misses_num_items.inc();
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, bytes: V) {
        if self.capacity.exceeds_capacity(bytes.len()) {
            // The value does not fit in the cache. We simply don't store it.
            if self.capacity != Capacity::InBytes(0) {
                warn!(
                    capacity_in_bytes = ?self.capacity,
                    len = bytes.len(),
                    "Downloaded a byte slice larger than the cache capacity."
                );
            }
            return;
        }
        if let Some(previous_data) = self.lru_cache.pop(&key) {
            self.drop_item(previous_data.len() as u64);
        }

        let now = Instant::now();
        while self
            .capacity
            .exceeds_capacity(self.num_bytes as usize + bytes.len())
        {
            if let Some((_, candidate_for_eviction)) = self.lru_cache.peek_lru() {
                let time_since_last_access =
                    now.duration_since(candidate_for_eviction.last_access_time());
                if time_since_last_access < MIN_TIME_SINCE_LAST_ACCESS {
                    // It is not worth doing an eviction.
                    // TODO: It is sub-optimal that we might have needlessly evicted items in this
                    // loop before just returning.
                    return;
                }
            }
            if let Some((_, bytes)) = self.lru_cache.pop_lru() {
                self.drop_item(bytes.len() as u64);
            } else {
                error!(
                    "Logical error. Even after removing all of the items in the cache the \
                     capacity is insufficient. This case is guarded against and should never \
                     happen."
                );
                return;
            }
        }
        self.record_item(bytes.len() as u64);
        self.lru_cache.put(key, StoredItem::new(bytes, now));
    }
}

// A fake entry inside a cache, which the cache believe to be of the given size
#[derive(Clone)]
struct FakeCacheEntry(usize);

impl ValueLen for FakeCacheEntry {
    fn len(&self) -> usize {
        self.0
    }
}

struct CacheState<K: Hash + Eq> {
    cache: NeedMutMemorySizedCache<K>,
    virtual_caches: Vec<NeedMutMemorySizedCache<K, FakeCacheEntry>>,
}

impl<K: Hash + Eq + Clone> CacheState<K> {
    fn from_config(cache_config: &CacheConfig, cache_counters: &'static CacheMetrics) -> Self {
        let cache = NeedMutMemorySizedCache::with_capacity(
            Capacity::InBytes(
                cache_config
                    .capacity
                    .as_u64()
                    .try_into()
                    .unwrap_or(usize::MAX),
            ),
            cache_counters.cache_metrics.clone(),
        );
        let virtual_caches = cache_config
            .virtual_caches
            .iter()
            .cloned()
            .map(|mut virtual_cache_config| {
                let mut capacity = virtual_cache_config.capacity.as_u64();
                if capacity == 0 {
                    // As a special case, if virtual cache capacity is zero, it's assumed to mean
                    // "same capacity as the real cache", but possibly with a
                    // different policy
                    capacity = cache_config.capacity.as_u64();
                    virtual_cache_config.capacity = cache_config.capacity;
                }

                let capacity = capacity.try_into().unwrap_or(usize::MAX);
                NeedMutMemorySizedCache::with_capacity(
                    Capacity::InBytes(capacity),
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
            cache: NeedMutMemorySizedCache::with_capacity(
                Capacity::Unlimited,
                cache_counters.cache_metrics.clone(),
            ),
            // there is no point in having virtual caches for an unbounded cache
            virtual_caches: Vec::new(),
        }
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<OwnedBytes>
    where
        K: Borrow<Q>,
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

impl<K: Hash + Eq + Clone> MemorySizedCache<K> {
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
        tokio::time::advance(super::MIN_TIME_SINCE_LAST_ACCESS.mul_f32(1.1f32)).await;
        {
            let data = OwnedBytes::new(&b"fghij"[..]);
            cache.put("5".to_string(), data);
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
            // our two first entries should have be removed from the cache
            assert!(cache.get(&"2".to_string()).is_none());
            assert!(cache.get(&"3".to_string()).is_none());
        }
        tokio::time::advance(super::MIN_TIME_SINCE_LAST_ACCESS.mul_f32(1.1f32)).await;
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
}
