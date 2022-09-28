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

mod memory_sized_cache;
mod metrics;
mod quickwit_cache;
mod slice_address;
mod stored_item;

use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tantivy::directory::OwnedBytes;

pub use self::memory_sized_cache::MemorySizedCache;
pub use self::metrics::CacheMetrics;
pub use crate::quickwit_cache::QuickwitCache;

/// The `Cache` trait is the abstraction used to describe the caching logic
/// used in front of a storage. See `StorageWithCache`.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Cache: Send + Sync + 'static {
    /// Try to get a slice from the cache.
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes>;
    /// Try to get the entire file.
    async fn get_all(&self, path: &Path) -> Option<OwnedBytes>;
    /// Put a slice of data into the cache.
    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes);
    /// Put an entire file into the cache.
    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes);
}
