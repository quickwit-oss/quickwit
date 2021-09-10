// Copyright (C) 2021 Quickwit, Inc.
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

use std::ops::Range;
use std::path::Path;

use bytes::Bytes;

use crate::cache::{NeedMutMemorySizedCache, SliceAddress};

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
            // our two first entries should have been removed from the cache.
            assert!(cache.get(Path::new("2"), 0..2).is_none());
            assert!(cache.get(Path::new("3"), 0..3).is_none());
        }
        {
            let data = Bytes::from_static(&b"klmnop"[..]);
            cache.put(Path::new("6"), 0..6, data);
            // The entry put should have been dismissed as it is too large for the cache.
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
