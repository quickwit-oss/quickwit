// Copyright (C) 2022 Quickwit, Inc.
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

#[cfg(not(feature = "tokio-time"))]
use instant::{Duration, Instant};
use lru::{KeyRef, LruCache};
#[cfg(feature = "tokio-time")]
use tokio::time::{Duration, Instant};
use tracing::{error, warn};

use crate::metrics::CacheMetrics;
use crate::slice_address::{SliceAddress, SliceAddressKey, SliceAddressRef};
use crate::stored_item::StoredItem;
use crate::OwnedBytes;

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

#[derive(Clone, Copy, Debug)]
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

struct NeedMutMemorySizedCache<K: Hash + Eq> {
    lru_cache: LruCache<K, StoredItem>,
    num_items: usize,
    num_bytes: u64,
    capacity: Capacity,
    cache_counters: Option<&'static CacheMetrics>,
}

impl<K: Hash + Eq> Drop for NeedMutMemorySizedCache<K> {
    fn drop(&mut self) {
        if let Some(cache_counters) = self.cache_counters {
            cache_counters.in_cache_count.sub(self.num_items as i64);
            cache_counters.in_cache_num_bytes.sub(self.num_bytes as i64);
        }
    }
}

impl<K: Hash + Eq> NeedMutMemorySizedCache<K> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: Capacity, cache_counters: Option<&'static CacheMetrics>) -> Self {
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
        if let Some(cache_counters) = self.cache_counters {
            cache_counters.in_cache_count.inc();
            cache_counters.in_cache_num_bytes.add(num_bytes as i64);
        }
    }

    pub fn drop_item(&mut self, num_bytes: u64) {
        self.num_items -= 1;
        self.num_bytes -= num_bytes;
        if let Some(cache_counters) = self.cache_counters {
            cache_counters.in_cache_count.dec();
            cache_counters.in_cache_num_bytes.sub(num_bytes as i64);
        }
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<OwnedBytes>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.lru_cache.get_mut(cache_key);
        if let Some(item) = item_opt {
            if let Some(cache_counters) = self.cache_counters {
                cache_counters.hits_num_items.inc();
                cache_counters.hits_num_bytes.inc_by(item.len() as u64);
            }
            Some(item.payload())
        } else {
            if let Some(cache_counters) = self.cache_counters {
                cache_counters.misses_num_items.inc();
            }
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, bytes: OwnedBytes) {
        if self.capacity.exceeds_capacity(bytes.len()) {
            // The value does not fit in the cache. We simply don't store it.
            warn!(
                capacity_in_bytes = ?self.capacity,
                len = bytes.len(),
                "Downloaded a byte slice larger than the cache capacity."
            );
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

/// A simple in-resident memory slice cache.
pub struct MemorySizedCache<K: Hash + Eq = SliceAddress> {
    inner: Mutex<NeedMutMemorySizedCache<K>>,
}

impl<K: Hash + Eq> MemorySizedCache<K> {
    /// Creates an slice cache with the given capacity.
    pub fn with_capacity_in_bytes(
        capacity_in_bytes: usize,
        cache_counters: Option<&'static CacheMetrics>,
    ) -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(
                Capacity::InBytes(capacity_in_bytes),
                cache_counters,
            )),
        }
    }

    /// Creates a slice cache that nevers removes any entry.
    pub fn with_infinite_capacity(cache_counters: Option<&'static CacheMetrics>) -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(
                Capacity::Unlimited,
                cache_counters,
            )),
        }
    }

    /// If available, returns the cached view of the slice.
    pub fn get<Q>(&self, cache_key: &Q) -> Option<OwnedBytes>
    where
        KeyRef<K>: Borrow<Q>,
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

    use super::*;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;

    #[tokio::test]
    async fn test_cache_edge_condition() {
        tokio::time::pause();
        let cache =
            MemorySizedCache::<String>::with_capacity_in_bytes(5, Some(&CACHE_METRICS_FOR_TESTS));
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
        let cache = MemorySizedCache::with_infinite_capacity(Some(&CACHE_METRICS_FOR_TESTS));
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
            MemorySizedCache::with_capacity_in_bytes(10_000, Some(&CACHE_METRICS_FOR_TESTS));
        assert!(cache.get(&"hello.seg").is_none());
        let data = OwnedBytes::new(&b"werwer"[..]);
        cache.put("hello.seg", data);
        assert_eq!(cache.get(&"hello.seg").unwrap(), &b"werwer"[..]);
    }
}
