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

mod byte_range_cache;
mod memory_sized_cache;
mod quickwit_cache;
mod slice_address;
mod storage_with_cache;
mod stored_item;

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
pub use quickwit_cache::QuickwitCache;
pub use storage_with_cache::StorageWithCache;

pub use self::byte_range_cache::ByteRangeCache;
pub use self::memory_sized_cache::MemorySizedCache;
use crate::{OwnedBytes, Storage};

/// Wraps the given directory with a slice cache that is actually global
/// to quickwit.
///
/// FIXME The current approach is quite horrible in that:
/// - it uses a global
/// - it relies on the idea that all of the files we attempt to cache have universally unique names.
///   It happens to be true today, but this might be very error prone in the future.
pub fn wrap_storage_with_cache(
    long_term_cache: Arc<dyn StorageCache>,
    storage: Arc<dyn Storage>,
) -> Arc<dyn Storage> {
    Arc::new(StorageWithCache {
        storage,
        cache: long_term_cache,
    })
}

/// The `StorageCache` trait is the abstraction used to describe the caching logic
/// used in front of a storage. See `StorageWithCache`.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait StorageCache: Send + Sync + 'static {
    /// Try to get a slice from the cache.
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes>;
    /// Try to get the entire file.
    async fn get_all(&self, path: &Path) -> Option<OwnedBytes>;
    /// Put a slice of data into the cache.
    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes);
    /// Put an entire file into the cache.
    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes);
}
