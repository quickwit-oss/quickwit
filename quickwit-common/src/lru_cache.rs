/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A struct implementing a cache with the LRU eviction policy.
pub struct LRUCache<K, V> {
    /// Maximum capacity of the cache.
    capacity: usize,
    /// Current size of the cache.
    size: usize,
    /// A hashmap storing the items in this cache.
    items: Arc<Mutex<HashMap<K, CacheItem<V>>>>,
    /// A user provided function for computing an item size.
    compute_item_size: fn(&V) -> usize,
    /// A user provided function for cleaning up and item after eviction.
    dispose_item: fn(&V),
    /// A timer to calculate item last accessed time.
    timer: Instant,
    //TODO make this persistable on disk
    // metadata_file: Pathbuf
}

impl<K: Debug + Eq + Hash + Clone, V: Debug + Clone> LRUCache<K, V> {
    pub fn new(capacity: usize, compute_item_size: fn(&V) -> usize, dispose_item: fn(&V)) -> Self {
        LRUCache {
            capacity,
            size: 0,
            items: Arc::new(Mutex::new(HashMap::new())),
            compute_item_size,
            dispose_item,
            timer: Instant::now(),
        }
    }

    pub fn load(file: PathBuf) -> Self {
        todo!()
    }

    fn store() -> bool {
        todo!()
    }

    /// Puts an item in the cache.
    ///
    /// Some items could be evicted
    pub fn put(&mut self, key: K, value: V) -> bool {
        let item_size = (self.compute_item_size)(&value);

        if item_size > self.capacity {
            return false;
        }

        if !self.try_to_make_room(item_size) {
            return false;
        }

        let mut items = self.items.lock().unwrap();
        let cache_item = CacheItem {
            pin_count: 0,
            last_accessed: self.timer.elapsed().as_nanos(),
            size: item_size,
            value,
        };
        items.insert(key, cache_item);
        self.size += item_size;
        true
    }

    /// Get an item from the cache.
    ///
    /// If an item is found, it's last accessed time and pin count are updated.
    pub fn get(&mut self, key: K) -> Option<CacheItemGuard<K, V>> {
        let mut items = self.items.lock().unwrap();
        let cache_item_opt = items.get_mut(&key);
        match cache_item_opt {
            Some(cache_item) => {
                cache_item.pin_count += 1;
                cache_item.last_accessed = self.timer.elapsed().as_nanos();
                Some(CacheItemGuard {
                    key,
                    value: cache_item.value.clone(),
                    cache_items: self.items.clone(),
                })
            }
            None => None,
        }
    }

    /// Check if item is present in the cache.
    pub fn contains_key(&self, key: &K) -> bool {
        let items = self.items.lock().unwrap();
        items.contains_key(key)
    }

    /// Returns the size of the cache.
    pub fn get_size(&self) -> usize {
        let _items = self.items.lock().unwrap();
        self.size
    }

    fn try_to_make_room(&mut self, required_size: usize) -> bool {
        let mut items = self.items.lock().unwrap();

        //is there already a room to accomodate?
        if self.size + required_size <= self.capacity {
            return true;
        }

        // reorder items by [pin_count, last_accessed]
        let mut key_values = items.iter().collect::<Vec<_>>();
        key_values.sort_by(|(_, left), (_, right)| {
            right
                .pin_count
                .cmp(&left.pin_count)
                .then(right.last_accessed.cmp(&left.last_accessed))
                .reverse()
        });

        // select just enougth removable items to fit the new item.
        let mut needed_size: isize = ((self.size + required_size) - self.capacity) as isize;
        let mut items_to_remove = vec![];
        for (key, cache_item) in key_values {
            if cache_item.pin_count > 0 {
                return false;
            }

            items_to_remove.push(((*key).clone(), (*cache_item).clone()));
            needed_size -= cache_item.size as isize;
            if needed_size <= 0 {
                break;
            }
        }

        // Evict selected items.
        for (key, cache_item) in items_to_remove {
            items.remove(&key);
            self.size -= cache_item.size;
            (self.dispose_item)(&cache_item.value)
        }

        true
    }
}

/// A struct wrapping a cache item to make sure the [`CachItem`] is updated.
pub struct CacheItemGuard<K: Debug + Eq + Hash, V: Debug + Clone> {
    pub key: K,
    pub value: V,
    cache_items: Arc<Mutex<HashMap<K, CacheItem<V>>>>,
}

impl<K: Debug + Eq + Hash, V: Debug + Clone> Drop for CacheItemGuard<K, V> {
    fn drop(&mut self) {
        let mut items = self.cache_items.lock().unwrap();
        items.get_mut(&self.key).unwrap().pin_count -= 1;
    }
}

/// CacheItem is the struct that wrapps the value actually stored in the cache,
/// Along with additional metadata for maintainance.
#[derive(Debug, Clone)]
struct CacheItem<V> {
    /// Denotes how many times this CacheItem is currently used.
    /// We should only evict when pin_count equal 0. The CacheItem could be holding a file path
    /// used by many clients.
    /// So we don't want to evict and dispose(delete) the file while some clients still need it.
    pin_count: u64,

    /// Denotes the last period this CacheItem was accessed.
    /// The value is the elapsed microseconds since this Cache was instanciated.
    last_accessed: u128,

    /// The size of this CacheItem.
    size: usize,

    /// The underlying value of this CacheItem.
    value: V,
}

#[cfg(test)]
mod tests {
    use super::LRUCache;

    fn compute_size(v: &u32) -> usize {
        *v as usize
    }

    fn dispose_item(_: &u32) {
        // println!("disposing {}", v);
    }

    #[test]
    fn test_cannot_put_more_than_capacity() {
        let mut lru_cache: LRUCache<u32, u32> = LRUCache::new(10, compute_size, dispose_item);
        assert!(!lru_cache.put(12, 12));
    }

    #[test]
    fn test_oldest_cache_item_is_evicted() {
        let mut lru_cache: LRUCache<u32, u32> = LRUCache::new(10, compute_size, dispose_item);
        lru_cache.put(5, 5);
        lru_cache.put(1, 1);
        lru_cache.put(2, 2);

        assert!(lru_cache.put(3, 3));
        assert_eq!(lru_cache.get_size(), 6);
        assert!(!lru_cache.contains_key(&5))
    }

    #[test]
    fn test_pinned_oldest_cache_item_is_not_evicted() {
        let mut lru_cache: LRUCache<u32, u32> = LRUCache::new(10, compute_size, dispose_item);
        lru_cache.put(5, 5);
        lru_cache.put(1, 1);
        lru_cache.put(2, 2);

        let _item_gard = lru_cache.get(5).unwrap();
        assert!(lru_cache.put(3, 3));

        assert_eq!(lru_cache.get_size(), 10);
        assert!(lru_cache.contains_key(&5));
        assert!(lru_cache.contains_key(&2));
        assert!(!lru_cache.contains_key(&1));
    }

    #[test]
    fn test_pinned_cache_item_is_unpinned_after_dropping_item_guard() {
        let mut lru_cache: LRUCache<u32, u32> = LRUCache::new(10, compute_size, dispose_item);
        lru_cache.put(5, 5);
        lru_cache.put(1, 1);
        lru_cache.put(2, 2);

        {
            let _item_gard_5 = lru_cache.get(5).unwrap();
            let _item_gard_1 = lru_cache.get(1).unwrap();
            let _item_gard_2 = lru_cache.get(2).unwrap();
            assert!(!lru_cache.put(15, 15));
        }

        assert!(lru_cache.put(3, 3));
        assert_eq!(lru_cache.get_size(), 6);
        assert!(!lru_cache.contains_key(&5))
    }
}
