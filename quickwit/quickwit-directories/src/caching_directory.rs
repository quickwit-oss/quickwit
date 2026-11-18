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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use quickwit_storage::{ByteRangeCache, FileByteRangeCache};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, OwnedBytes};
use tantivy::{Directory, HasLen};
use tracing::error;

/// Returned when reserving memory would make a split search go over its own warmup memory limit.
///
/// Going over the warmup memory limit shared by all split searches is not an error: the
/// reservation waits instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfMemoryError;

impl fmt::Display for OutOfMemoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "out of warmup memory: split budget exceeded")
    }
}

impl std::error::Error for OutOfMemoryError {}

impl From<OutOfMemoryError> for io::Error {
    fn from(out_of_memory_error: OutOfMemoryError) -> io::Error {
        io::Error::new(io::ErrorKind::OutOfMemory, out_of_memory_error)
    }
}

/// Memory reserved for a read in progress.
///
/// Released on drop, except the bytes passed to `keep` once the read bytes are in the cache.
/// So the memory is also released if the read fails, panics, or if its future is dropped.
#[must_use = "dropping a memory reservation releases its memory right away"]
pub struct MemoryReservation<'a> {
    memory_reserver: &'a dyn MemoryReserver,
    /// Bytes released on drop.
    num_bytes: u64,
}

impl<'a> MemoryReservation<'a> {
    /// Creates a memory reservation that releases `num_bytes` on drop.
    ///
    /// Contract: only called by `memory_reserver`, from [`MemoryReserver::reserve_memory`], for
    /// `num_bytes` it has just reserved. A reservation created for bytes that were not reserved
    /// would release them anyway.
    pub fn new(memory_reserver: &'a dyn MemoryReserver, num_bytes: u64) -> MemoryReservation<'a> {
        MemoryReservation {
            memory_reserver,
            num_bytes,
        }
    }

    /// Keeps `num_added_bytes` reserved, the bytes the read added to the cache, and releases the
    /// rest.
    fn keep(mut self, num_added_bytes: u64) {
        debug_assert!(
            num_added_bytes <= self.num_bytes,
            "cannot keep more bytes than reserved"
        );
        self.num_bytes = self.num_bytes.saturating_sub(num_added_bytes);
    }
}

impl Drop for MemoryReservation<'_> {
    fn drop(&mut self) {
        if self.num_bytes > 0 {
            self.memory_reserver.release_memory(self.num_bytes);
        }
    }
}

/// Reserves memory for the data a [`CachingDirectory`] downloads and keeps in its cache.
///
/// Only async reads can reserve memory: a [`CachingDirectory`] with a memory reserver fails sync
/// reads that miss its cache.
#[async_trait]
pub trait MemoryReserver: Send + Sync + 'static {
    /// Called on each cache miss of an async read, before reading `num_bytes` from the underlying
    /// directory. May wait until the memory is available, but must fail right away if it can
    /// never be.
    ///
    /// On success, returns a [`MemoryReservation`], which gives the memory back through
    /// `release_memory` when dropped, except for the bytes the read added to the cache.
    ///
    /// On error, the read fails with an `io::Error` of kind `OutOfMemory`: the underlying directory
    /// is not called and nothing is cached.
    ///
    /// Contract: the memory is reserved before the read starts, so the caller must not hold
    /// anything another read waits on.
    async fn reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError>;

    /// Gives back `num_bytes` reserved by `reserve_memory` that a read did not add to the cache:
    /// the read failed, panicked, its future was dropped, or the bytes were already cached.
    ///
    /// Contract: `num_bytes` were reserved by an earlier `reserve_memory` call, and are not
    /// released twice. Called from `Drop`, possibly while a panic unwinds, so it must not panic.
    fn release_memory(&self, num_bytes: u64);
}

/// The caching directory is a simple cache that wraps another directory.
#[derive(Clone)]
pub struct CachingDirectory {
    underlying: Arc<dyn Directory>,
    // TODO fixme: that's a pretty ugly cache we have here.
    cache: ByteRangeCache,
    /// If `None`, the memory of the cache is not accounted for.
    memory_reserver_opt: Option<Arc<dyn MemoryReserver>>,
}

impl CachingDirectory {
    /// Creates a new CachingDirectory.
    ///
    /// Warning: The resulting CacheDirectory will cache all information without ever
    /// removing any item from the cache.
    pub fn new_unbounded(underlying: Arc<dyn Directory>) -> CachingDirectory {
        let byte_range_cache = ByteRangeCache::with_infinite_capacity();
        CachingDirectory::new(underlying, byte_range_cache)
    }

    /// Creates a new CachingDirectory.
    ///
    /// Warning: The resulting CacheDirectory will cache all information without ever
    /// removing any item from the cache.
    pub fn new(underlying: Arc<dyn Directory>, cache: ByteRangeCache) -> CachingDirectory {
        CachingDirectory {
            underlying,
            cache,
            memory_reserver_opt: None,
        }
    }

    /// Reserves memory in `memory_reserver` before reading each cache miss.
    ///
    /// Sync reads then fail on cache misses: they cannot wait for memory. In the search path, the
    /// underlying `StorageDirectory` does not support sync reads anyway.
    ///
    /// The directory holds `memory_reserver` until it is dropped.
    pub fn with_memory_reserver(
        mut self,
        memory_reserver: Arc<dyn MemoryReserver>,
    ) -> CachingDirectory {
        self.memory_reserver_opt = Some(memory_reserver);
        self
    }
}

impl fmt::Debug for CachingDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CachingDirectory({:?})", self.underlying)
    }
}

struct CachingFileHandle {
    path: PathBuf,
    cache: FileByteRangeCache,
    underlying_filehandle: Arc<dyn FileHandle>,
    memory_reserver_opt: Option<Arc<dyn MemoryReserver>>,
}

impl fmt::Debug for CachingFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CachingFileHandle(path={:?}, underlying={:?})",
            self.path,
            self.underlying_filehandle.as_ref()
        )
    }
}

impl CachingFileHandle {
    /// Returned by a sync read that misses the cache, when the memory is accounted for.
    fn sync_cache_miss_error(&self, byte_range: &Range<usize>) -> io::Error {
        let error = "unsupported operation: sync reads cannot reserve warmup memory, so they must \
                     hit the cache";
        error!(error, path = ?self.path, ?byte_range);
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{error}: {}", self.path.display()),
        )
    }
}

#[async_trait]
impl FileHandle for CachingFileHandle {
    /// Contract: with a memory reserver, only reads data that is already in the cache.
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(byte_range.clone()) {
            return Ok(bytes);
        }
        if self.memory_reserver_opt.is_some() {
            return Err(self.sync_cache_miss_error(&byte_range));
        }
        let owned_bytes = self.underlying_filehandle.read_bytes(byte_range.clone())?;
        self.cache.put_slice(byte_range, owned_bytes.clone());
        Ok(owned_bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(owned_bytes) = self.cache.get_slice(byte_range.clone()) {
            return Ok(owned_bytes);
        }
        let read_reservation_opt = match &self.memory_reserver_opt {
            Some(memory_reserver) => Some(
                memory_reserver
                    .reserve_memory(byte_range.len() as u64)
                    .await?,
            ),
            None => None,
        };
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        let num_added_bytes = self.cache.put_slice(byte_range, read_bytes.clone());
        if let Some(read_reservation) = read_reservation_opt {
            read_reservation.keep(num_added_bytes);
        }
        Ok(read_bytes)
    }
}

impl HasLen for CachingFileHandle {
    fn len(&self) -> usize {
        self.underlying_filehandle.len()
    }
}

impl Directory for CachingDirectory {
    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let underlying_filehandle = self.underlying.get_file_handle(path)?;
        let caching_file_handle = CachingFileHandle {
            path: path.to_path_buf(),
            cache: self.cache.get_file_cache(path),
            underlying_filehandle,
            memory_reserver_opt: self.memory_reserver_opt.clone(),
        };
        Ok(Arc::new(caching_file_handle))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        let len = file_handle.len();
        let owned_bytes = file_handle
            .read_bytes(0..len)
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(owned_bytes.as_slice().to_vec())
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests;
