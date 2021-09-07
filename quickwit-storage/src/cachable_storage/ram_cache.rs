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

use std::ops::Range;
use std::path::Path;

use crate::cache::{NeedMutMemorySizedCache, SliceAddress};
use bytes::Bytes;

// #[derive(Hash, Debug, Clone, PartialEq, Eq)]
// struct SliceAddress {
//     pub path: PathBuf,
//     pub byte_range: Range<usize>,
// }

// struct NeedMutSliceCache {
//     lru_cache: LruCache<SliceAddress, Bytes>,
//     num_bytes: usize,
//     capacity: usize,
// }

// impl NeedMutSliceCache {
//     /// Creates a new NeedMutSliceCache with the given capacity.
//     fn with_capacity(capacity: usize) -> Self {
//         NeedMutSliceCache {
//             // The limit will be decided by the amount of memory in the cache,
//             // not the number of items in the cache.
//             // Enforcing this limit is done in the `NeedMutCache` impl.
//             lru_cache: LruCache::unbounded(),
//             num_bytes: 0,
//             capacity,
//         }
//     }

//     fn get(&mut self, cache_key: &SliceAddress) -> Option<Bytes> {
//         self.lru_cache.get(cache_key).cloned()
//     }

//     /// Attempt to put the given amount of data in the cache.
//     /// This will fail silently if the data is larger than the cache
//     /// capacity.
//     fn put(&mut self, slice_addr: SliceAddress, bytes: Bytes) {
//         if self.exceeds_capacity(bytes.len()) {
//             // The value does not fit in the cache. We simply don't store it.
//             warn!(
//                 capacity_in_bytes = ?self.capacity,
//                 len = bytes.len(),
//                 "Downloaded a byte slice larger than the cache capacity."
//             );
//             return;
//         }
//         if let Some(previous_data) = self.lru_cache.pop(&slice_addr) {
//             self.num_bytes -= previous_data.len();
//         }
//         while self.exceeds_capacity(self.num_bytes + bytes.len()) {
//             if let Some((_, bytes)) = self.lru_cache.pop_lru() {
//                 self.num_bytes -= bytes.len();
//             } else {
//                 error!("Logical error. Even after removing all of the items in the cache the capacity is insufficient. This case is guarded against and should never happen.");
//                 return;
//             }
//         }
//         self.num_bytes += bytes.len();
//         self.lru_cache.put(slice_addr, bytes);
//     }

//     // Remome all cache entries related to the path.
//     fn delete(&mut self, path: &Path) {
//         let addrs_to_remove = self
//             .lru_cache
//             .iter()
//             .filter(|(cache_key, _)| cache_key.path == path)
//             .map(|(cache_key, _)| cache_key.clone())
//             .collect::<Vec<_>>();

//         for addr in addrs_to_remove {
//             self.lru_cache.pop(&addr);
//         }
//     }

//     /// Check if an incomming item will exceed the capacity once inserted.
//     fn exceeds_capacity(&self, num_bytes: usize) -> bool {
//         self.capacity < num_bytes
//     }
// }

/// A simple in-memory resident cache.
pub struct RamCache {
    inner: NeedMutMemorySizedCache<SliceAddress>,
}

impl RamCache {
    /// Creates a ram cache with the given capacity in bytes.
    pub fn new(capacity_in_bytes: usize) -> Self {
        RamCache {
            inner: NeedMutMemorySizedCache::with_capacity_in_bytes(capacity_in_bytes),
        }
    }

    /// If available, returns the cached view of the slice.
    pub fn get(&mut self, path: &Path, bytes_range: Range<usize>) -> Option<Bytes> {
        let slice_addr = SliceAddress {
            path: path.to_path_buf(),
            byte_range: bytes_range,
        };
        self.inner.get(&slice_addr)
    }

    /// Attempt to put the given amount of data in the cache.
    /// This will fail silently if the data is larger than the cache
    /// capacity.
    pub fn put(&mut self, path: &Path, bytes_range: Range<usize>, bytes: Bytes) {
        let slice_addr = SliceAddress {
            path: path.to_path_buf(),
            byte_range: bytes_range,
        };
        self.inner.put(slice_addr, bytes);
    }

    // Remome all cache entries related to the path.
    pub fn delete(&mut self, path: &Path) {
        self.inner.delete(|key| key.path == path);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_cache_edge_condition() {
        let mut cache = RamCache::new(5);
        {
            let data = Bytes::from_static(&b"abc"[..]);
            cache.put(Path::new("3"), 0..3, data);
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
        }
        {
            let data = Bytes::from_static(&b"de"[..]);
            cache.put(Path::new("2"), 0..2, data);
            // our first entry should still be here.
            assert_eq!(cache.get(Path::new("3"), 0..3).unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(Path::new("2"), 0..2).unwrap(), &b"de"[..]);
        }
        {
            let data = Bytes::from_static(&b"fghij"[..]);
            cache.put(Path::new("5"), 0..5, data);
            assert_eq!(cache.get(Path::new("5"), 0..5).unwrap(), &b"fghij"[..]);
            // our two first entries should have be removed from the cache
            assert!(cache.get(Path::new("2"), 0..2).is_none());
            assert!(cache.get(Path::new("3"), 0..3).is_none());
        }
        {
            let data = Bytes::from_static(&b"klmnop"[..]);
            cache.put(Path::new("6"), 0..6, data);
            // The entry put should have been dismissed as it is too large for the cache
            assert!(cache.get(Path::new("6"), 0..6).is_none());
            // The previous entry should however be remaining.
            assert_eq!(cache.get(Path::new("5"), 0..5).unwrap(), &b"fghij"[..]);
        }
    }

    #[test]
    fn test_cache() {
        let mut cache = RamCache::new(10_000);
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_none());
        let data = Bytes::from_static(&b"werwer"[..]);
        cache.put(Path::new("hello.seg"), 1..3, data);
        assert_eq!(
            cache.get(Path::new("hello.seg"), 1..3).unwrap(),
            &b"werwer"[..]
        );
    }

    #[test]
    fn test_cache_different_slice() {
        let mut cache = RamCache::new(10_000);
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_none());
        let data = Bytes::from_static(&b"werwer"[..]);
        // We could actually have a cache hit here, but this is not useful for Quickwit.
        cache.put(Path::new("hello.seg"), 1..3, data);
        assert!(cache.get(Path::new("hello.seg"), 1..3).is_some());
        assert!(cache.get(Path::new("hello.seg"), 2..3).is_none());
    }
}
