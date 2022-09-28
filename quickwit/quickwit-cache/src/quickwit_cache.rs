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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::metrics::CacheMetrics;
use crate::{Cache, MemorySizedCache, OwnedBytes};

const FULL_SLICE: Range<usize> = 0..usize::MAX;

/// Quickwit storage cache with a size limit.
/// It is used currently by to cache only fast fields data.
pub struct QuickwitCache {
    router: Vec<(&'static str, Arc<dyn Cache>)>,
}

impl From<Vec<(&'static str, Arc<dyn Cache>)>> for QuickwitCache {
    fn from(router: Vec<(&'static str, Arc<dyn Cache>)>) -> Self {
        QuickwitCache { router }
    }
}

impl QuickwitCache {
    /// Creates a [`QuickwitCache`] with a cache on fast fields
    /// with a capacity of `fast_field_cache_capacity`.
    pub fn new(
        fast_field_cache_capacity: usize,
        fast_field_cache_counters: Option<&'static CacheMetrics>,
    ) -> Self {
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route(
            ".fast",
            Arc::new(SimpleCache::with_capacity_in_bytes(
                fast_field_cache_capacity as usize,
                fast_field_cache_counters,
            )),
        );
        quickwit_cache
    }

    /// Empties cache.
    pub fn empty() -> QuickwitCache {
        QuickwitCache::from(Vec::new())
    }

    /// Adds a caching route defined by a path suffix. All elements with a path matching
    /// this suffix will be cached.
    pub fn add_route(&mut self, path_suffix: &'static str, route_cache: Arc<dyn Cache>) {
        self.router.push((path_suffix, route_cache));
    }

    fn get_relevant_cache(&self, path: &Path) -> Option<&dyn Cache> {
        for (suffix, cache) in &self.router {
            if path.to_string_lossy().ends_with(suffix) {
                return Some(cache.as_ref());
            }
        }
        None
    }
}

#[async_trait]
impl Cache for QuickwitCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        // We don't check for the presence of the entire file in the
        // cache.
        // That's voluntary to avoid messing with the cache miss counts.
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get(path, byte_range).await;
        }
        None
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        if let Some(cache) = self.get_relevant_cache(path) {
            return cache.get_all(path).await;
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, byte_range, bytes).await;
        }
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        if let Some(cache) = self.get_relevant_cache(&path) {
            cache.put(path, FULL_SLICE, bytes).await;
        }
    }
}

/// The Quickwit cache logic is very simple for the moment.
///
/// It stores hotcache files using an LRU cache.
///
/// HACK! We use `0..usize::MAX` to signify the "entire file".
/// TODO fixme
struct SimpleCache {
    slice_cache: MemorySizedCache,
}

impl SimpleCache {
    fn with_capacity_in_bytes(
        capacity_in_bytes: usize,
        cache_counters: Option<&'static CacheMetrics>,
    ) -> Self {
        SimpleCache {
            slice_cache: MemorySizedCache::with_capacity_in_bytes(
                capacity_in_bytes,
                cache_counters,
            ),
        }
    }
}

#[async_trait]
impl Cache for SimpleCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if let Some(bytes) = self.slice_cache.get_slice(path, byte_range) {
            return Some(bytes);
        }
        None
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.slice_cache.put_slice(path, byte_range, bytes);
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        self.slice_cache.get_slice(path, FULL_SLICE)
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        self.slice_cache.put_slice(path, FULL_SLICE.clone(), bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::QuickwitCache;
    use crate::{Cache, MockCache, OwnedBytes};

    #[tokio::test]
    async fn test_quickwit_cache_get_all() {
        let mock_cache_hotcache = MockCache::default();
        let mut mock_cache_fast = MockCache::default();
        mock_cache_fast
            .expect_get_all()
            .times(1)
            .withf(|path| path == Path::new("bubu/toto.fast"))
            .returning(|_| Some(OwnedBytes::new(&b"aaaa"[..])));
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("hotcache", Arc::new(mock_cache_hotcache));
        quickwit_cache.add_route("fast", Arc::new(mock_cache_fast));
        quickwit_cache.get_all(Path::new("bubu/toto.fast")).await;
    }

    #[tokio::test]
    async fn test_quickwit_cache_get() {
        let mock_cache_hotcache = MockCache::default();
        let mut mock_cache = MockCache::default();
        mock_cache
            .expect_get()
            .times(1)
            .withf(|path, _| path == Path::new("bubu/toto.fast"))
            .returning(|_, _| Some(OwnedBytes::new(&b"aaaaa"[..])));
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("hotcache", Arc::new(mock_cache_hotcache));
        quickwit_cache.add_route("fast", Arc::new(mock_cache));
        quickwit_cache.get(Path::new("bubu/toto.fast"), 5..10).await;
    }

    #[tokio::test]
    async fn test_quickwit_cache_priority() {
        let mut mock_cache_ast = MockCache::default();
        mock_cache_ast
            .expect_get()
            .times(1)
            .withf(|path, _| path == Path::new("bubu/toto.fast"))
            .returning(|_, _| Some(OwnedBytes::new(&b"aaaaa"[..])));
        let mock_cache_fast = MockCache::default();
        let mut quickwit_cache = QuickwitCache::empty();
        quickwit_cache.add_route("ast", Arc::new(mock_cache_ast));
        quickwit_cache.add_route("fast", Arc::new(mock_cache_fast));
        assert_eq!(
            quickwit_cache
                .get(Path::new("bubu/toto.fast"), 5..10)
                .await
                .unwrap(),
            &b"aaaaa"[..]
        );
    }
}
