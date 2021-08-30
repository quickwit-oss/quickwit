// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Mutex;

use bytes::Bytes;
use lru::{KeyRef, LruCache};
use tracing::{error, warn};
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
    lru_cache: LruCache<K, Bytes>,
    num_bytes: usize,
    capacity: Capacity,
}

impl<K: Hash + Eq> NeedMutMemorySizedCache<K> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: Capacity) -> Self {
        NeedMutMemorySizedCache {
            // The limit will be decided by the amount of memory in the cache,
            // not the number of items in the cache.
            // Enforcing this limit is done in the `NeedMutCache` impl.
            lru_cache: LruCache::unbounded(),
            num_bytes: 0,
            capacity,
        }
    }

    pub fn get<'a, Q>(&'a mut self, cache_key: &Q) -> Option<Bytes>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        //fn get(&mut self, cache_key: &K) -> Option<Bytes> {
        self.lru_cache.get(&cache_key).cloned()
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, bytes: Bytes) {
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
            self.num_bytes -= previous_data.len();
        }
        while self.capacity.exceeds_capacity(self.num_bytes + bytes.len()) {
            if let Some((_, bytes)) = self.lru_cache.pop_lru() {
                self.num_bytes -= bytes.len();
            } else {
                error!("Logical error. Even after removing all of the items in the cache the capacity is insufficient. This case is guarded against and should never happen.");
                return;
            }
        }
        self.num_bytes += bytes.len();
        self.lru_cache.put(key, bytes);
    }
}

/// A simple in-resident memory slice cache.
pub struct MemorySizedCache<K: Hash + Eq> {
    inner: Mutex<NeedMutMemorySizedCache<K>>,
}

impl<K: Hash + Eq> MemorySizedCache<K> {
    /// Creates an slice cache with the given capacity.
    pub fn with_capacity_in_bytes(capacity_in_bytes: usize) -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(Capacity::InBytes(
                capacity_in_bytes,
            ))),
        }
    }

    /// Creates a slice cache that nevers removes any entry.
    pub fn with_infinite_capacity() -> Self {
        MemorySizedCache {
            inner: Mutex::new(NeedMutMemorySizedCache::with_capacity(Capacity::Unlimited)),
        }
    }

    /// If available, returns the cached view of the slice.
    pub fn get<'a, Q>(&'a self, cache_key: &Q) -> Option<Bytes>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.lock().unwrap().get(cache_key)
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    pub fn put(&self, val: K, bytes: Bytes) {
        self.inner.lock().unwrap().put(val, bytes);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_cache_edge_condition() {
        let cache = MemorySizedCache::<String>::with_capacity_in_bytes(5);
        {
            let data = Bytes::from_static(&b"abc"[..]);
            cache.put("3".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
        }
        {
            let data = Bytes::from_static(&b"de"[..]);
            cache.put("2".to_string(), data);
            // our first entry should still be here.
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(&"2".to_string()).unwrap(), &b"de"[..]);
        }
        {
            let data = Bytes::from_static(&b"fghij"[..]);
            cache.put("5".to_string(), data);
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
            // our two first entries should have be removed from the cache
            assert!(cache.get(&"2".to_string()).is_none());
            assert!(cache.get(&"3".to_string()).is_none());
        }
        {
            let data = Bytes::from_static(&b"klmnop"[..]);
            cache.put("6".to_string(), data);
            // The entry put should have been dismissed as it is too large for the cache
            assert!(cache.get(&"6".to_string()).is_none());
            // The previous entry should however be remaining.
            assert_eq!(cache.get(&"5".to_string()).unwrap(), &b"fghij"[..]);
        }
    }

    #[test]
    fn test_cache_edge_unlimited_capacity() {
        let cache = MemorySizedCache::with_infinite_capacity();
        {
            let data = Bytes::from_static(&b"abc"[..]);
            cache.put("3".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
        }
        {
            let data = Bytes::from_static(&b"de"[..]);
            cache.put("2".to_string(), data);
            assert_eq!(cache.get(&"3".to_string()).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(&"2".to_string()).unwrap(), &b"de"[..]);
        }
    }

    #[test]
    fn test_cache() {
        let cache = MemorySizedCache::with_capacity_in_bytes(10_000);
        assert!(cache.get(&"hello.seg").is_none());
        let data = Bytes::from_static(&b"werwer"[..]);
        cache.put("hello.seg", data);
        assert_eq!(cache.get(&"hello.seg").unwrap(), &b"werwer"[..]);
    }
}
