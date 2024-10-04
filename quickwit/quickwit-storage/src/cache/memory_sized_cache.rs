// Copyright (C) 2024 Quickwit, Inc.
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

use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use moka::sync::Cache;
use quickwit_config::CacheKind;
use tracing::warn;

use crate::cache::slice_address::{SliceAddress, SliceAddressKey, SliceAddressRef};
use crate::metrics::CacheMetrics;
use crate::OwnedBytes;

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

// TODO this actually doesn't need mut access, we should remove the mutext and related complexity
// eventually
struct NeedMutMemorySizedCache<K: Hash + Eq + Send + Sync + 'static> {
    cache: Cache<K, OwnedBytes>,
    capacity: Capacity,
    cache_counters: &'static CacheMetrics,
}

impl<K: Hash + Eq + Send + Sync + 'static> Drop for NeedMutMemorySizedCache<K> {
    fn drop(&mut self) {
        self.cache.run_pending_tasks();
        self.cache_counters
            .in_cache_count
            .sub(self.cache.entry_count() as i64);
        self.cache_counters
            .in_cache_num_bytes
            .sub(self.cache.weighted_size() as i64);
    }
}

impl<K: Hash + Eq + Send + Sync + 'static> NeedMutMemorySizedCache<K> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(
        capacity: Capacity,
        cache_counters: &'static CacheMetrics,
        cache_kind: CacheKind,
    ) -> Self {
        let mut cache_builder = Cache::<K, OwnedBytes>::builder()
            .eviction_policy(match cache_kind {
                CacheKind::Lfu => moka::policy::EvictionPolicy::tiny_lfu(),
                CacheKind::Lru => moka::policy::EvictionPolicy::lru(),
            })
            .weigher(|_k, v| v.len().try_into().unwrap_or(u32::MAX))
            .eviction_listener(|_k, v, _cause| {
                cache_counters.in_cache_count.dec();
                cache_counters.in_cache_num_bytes.sub(v.len() as i64);
            });
        cache_builder = match capacity {
            Capacity::InBytes(capacity) if capacity > 0 => {
                cache_builder.max_capacity(capacity as u64)
            }
            _ => cache_builder,
        };
        NeedMutMemorySizedCache {
            cache: cache_builder.build(),
            capacity,
            cache_counters,
        }
    }

    pub fn record_item(&self, num_bytes: u64) {
        self.cache_counters.in_cache_count.inc();
        self.cache_counters.in_cache_num_bytes.add(num_bytes as i64);
    }

    pub fn get<Q>(&self, cache_key: &Q) -> Option<OwnedBytes>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.cache.get(cache_key);
        if let Some(item) = item_opt {
            self.cache_counters.hits_num_items.inc();
            self.cache_counters.hits_num_bytes.inc_by(item.len() as u64);
            Some(item)
        } else {
            self.cache_counters.misses_num_items.inc();
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&self, key: K, bytes: OwnedBytes) {
        if self.capacity.exceeds_capacity(bytes.len()) {
            // The value does not fit in the cache. We simply don't store it.
            // We do this so we can log, as the underlying cache should already reject this value
            // anyway
            if self.capacity != Capacity::InBytes(0) {
                warn!(
                    capacity_in_bytes = ?self.capacity,
                    len = bytes.len(),
                    "Downloaded a byte slice larger than the cache capacity."
                );
            }
            return;
        }

        self.record_item(bytes.len() as u64);
        self.cache.insert(key, bytes);
    }
}

/// A simple in-resident memory slice cache.
pub struct MemorySizedCache<K: Hash + Eq + Send + Sync + 'static = SliceAddress> {
    inner: Mutex<NeedMutMemorySizedCache<K>>,
}

impl<K: Hash + Eq + Send + Sync + 'static> MemorySizedCache<K> {
    /// Creates an slice cache with the given capacity.
    pub fn with_capacity_in_bytes(
        capacity_in_bytes: usize,
        cache_counters: &'static CacheMetrics,
        cache_kind: CacheKind,
    ) -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(
                Capacity::InBytes(capacity_in_bytes),
                cache_counters,
                cache_kind,
            )),
        }
    }

    /// Creates a slice cache that never removes any entry.
    pub fn with_infinite_capacity(cache_counters: &'static CacheMetrics) -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(
                Capacity::Unlimited,
                cache_counters,
                CacheKind::Lru, // this doesn't matter on unbounded cache
            )),
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

    #[cfg(test)]
    fn force_sync(&self) {
        self.inner.lock().unwrap().cache.run_pending_tasks();
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

    use super::*;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    #[tokio::test]
    async fn test_cache_edge_condition() {
        tokio::time::pause();
        let cache = MemorySizedCache::<String>::with_capacity_in_bytes(
            5,
            &CACHE_METRICS_FOR_TESTS,
            CacheKind::Lfu,
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
            cache.force_sync();
            // based on previous acces patterns, this key isn't as good as the others, so it's not
            // even inserted
            assert!(cache.get(&"5".to_string()).is_none());
            // our two first entries should have been removed from the cache
            assert!(cache.get(&"2".to_string()).is_some());
            assert!(cache.get(&"3".to_string()).is_some());
        }
        {
            for _ in 0..5 {
                assert!(cache.get(&"5".to_string()).is_none());
            }
            let data = OwnedBytes::new(&b"fghij"[..]);
            cache.put("5".to_string(), data);
            cache.force_sync();
            // now that it has some popularity, it should be preferred over the other keys
            assert!(cache.get(&"5".to_string()).is_some());
            // our two first entries should have been removed from the cache
            assert!(cache.get(&"2".to_string()).is_none());
            assert!(cache.get(&"3".to_string()).is_none());
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
        let cache = MemorySizedCache::with_capacity_in_bytes(
            10_000,
            &CACHE_METRICS_FOR_TESTS,
            CacheKind::default(),
        );
        assert!(cache.get(&"hello.seg").is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        cache.put("hello.seg", data);
        assert_eq!(cache.get(&"hello.seg").unwrap(), &b"werwer"[..]);
    }
}
