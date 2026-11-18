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
        let Some(memory_reserver) = &self.memory_reserver_opt else {
            let read_bytes = self
                .underlying_filehandle
                .read_bytes_async(byte_range.clone())
                .await?;
            self.cache.put_slice(byte_range, read_bytes.clone());
            return Ok(read_bytes);
        };
        let read_reservation = memory_reserver
            .reserve_memory(byte_range.len() as u64)
            .await?;
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        let num_added_bytes = self.cache.put_slice(byte_range, read_bytes.clone());
        read_reservation.keep(num_added_bytes);
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
mod tests {

    use std::io;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use quickwit_storage::ByteRangeCache;
    use tantivy::directory::{FileHandle, OwnedBytes, RamDirectory};
    use tantivy::{Directory, HasLen};

    use super::{CachingDirectory, CachingFileHandle, MemoryReserver, OutOfMemoryError};
    use crate::{DebugProxyDirectory, MemoryReservation};

    /// Reserves memory up to `limit` bytes. Never waits.
    struct LimitedMemoryReserver {
        limit: u64,
        reserved: AtomicU64,
    }

    impl LimitedMemoryReserver {
        fn new(limit: u64) -> Arc<Self> {
            Arc::new(LimitedMemoryReserver {
                limit,
                reserved: AtomicU64::new(0),
            })
        }

        fn reserved(&self) -> u64 {
            self.reserved.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl MemoryReserver for LimitedMemoryReserver {
        async fn reserve_memory(
            &self,
            num_bytes: u64,
        ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
            self.reserved
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                    let new_reserved = reserved + num_bytes;
                    (new_reserved <= self.limit).then_some(new_reserved)
                })
                .map_err(|_| OutOfMemoryError)?;
            Ok(MemoryReservation::new(self, num_bytes))
        }

        fn release_memory(&self, num_bytes: u64) {
            self.reserved.fetch_sub(num_bytes, Ordering::Relaxed);
        }
    }

    /// How the reads of a [`FailingFileHandle`] fail.
    #[derive(Debug, Clone, Copy)]
    enum ReadFailure {
        Error,
        Panic,
        /// The async read never completes. The sync read fails with an error.
        Pending,
    }

    /// A file handle whose reads never succeed.
    #[derive(Debug)]
    struct FailingFileHandle(ReadFailure);

    impl HasLen for FailingFileHandle {
        fn len(&self) -> usize {
            100
        }
    }

    #[async_trait]
    impl FileHandle for FailingFileHandle {
        fn read_bytes(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
            match self.0 {
                ReadFailure::Error | ReadFailure::Pending => Err(io::Error::other("read failed")),
                ReadFailure::Panic => panic!("read panicked"),
            }
        }

        async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
            if let ReadFailure::Pending = self.0 {
                std::future::pending::<()>().await;
            }
            self.read_bytes(byte_range)
        }
    }

    fn failing_caching_file_handle(
        read_failure: ReadFailure,
        memory_reserver: Arc<LimitedMemoryReserver>,
    ) -> CachingFileHandle {
        let path = Path::new("test");
        CachingFileHandle {
            path: path.to_path_buf(),
            cache: ByteRangeCache::with_infinite_capacity().get_file_cache(path),
            underlying_filehandle: Arc::new(FailingFileHandle(read_failure)),
            memory_reserver_opt: Some(memory_reserver),
        }
    }

    #[tokio::test]
    async fn test_caching_file_handle_releases_memory_on_async_read_panic() {
        let memory_reserver = LimitedMemoryReserver::new(100);
        let file_handle = Arc::new(failing_caching_file_handle(
            ReadFailure::Panic,
            memory_reserver.clone(),
        ));
        let join_res = tokio::spawn(async move { file_handle.read_bytes_async(0..5).await }).await;
        assert!(join_res.unwrap_err().is_panic());
        assert_eq!(memory_reserver.reserved(), 0);
    }

    #[tokio::test]
    async fn test_caching_file_handle_releases_memory_when_read_future_is_dropped() {
        let memory_reserver = LimitedMemoryReserver::new(100);
        let file_handle =
            failing_caching_file_handle(ReadFailure::Pending, memory_reserver.clone());
        let read_future = file_handle.read_bytes_async(0..5);
        let timeout_res = tokio::time::timeout(Duration::from_millis(10), read_future).await;
        assert!(timeout_res.is_err());
        // The timeout dropped the read future.
        assert_eq!(memory_reserver.reserved(), 0);
    }

    #[tokio::test]
    async fn test_caching_file_handle_releases_memory_on_async_read_error() {
        let memory_reserver = LimitedMemoryReserver::new(100);
        let file_handle = failing_caching_file_handle(ReadFailure::Error, memory_reserver.clone());
        assert!(file_handle.read_bytes_async(0..5).await.is_err());
        assert_eq!(memory_reserver.reserved(), 0);
    }

    fn debug_proxy_directory_with_file(
        path: &Path,
        content: &[u8],
    ) -> Arc<DebugProxyDirectory<RamDirectory>> {
        let ram_directory = RamDirectory::default();
        ram_directory.atomic_write(path, content).unwrap();
        Arc::new(DebugProxyDirectory::wrap(ram_directory))
    }

    #[test]
    fn test_caching_directory() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, &b"test"[..])?;
        let debug_proxy_directory = Arc::new(DebugProxyDirectory::wrap(ram_directory));
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone());
        caching_directory.atomic_read(test_path)?;
        caching_directory.atomic_read(test_path)?;
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_caching_directory_sync_read_with_memory_reserver_must_hit_cache() {
        let test_path = Path::new("test");
        let debug_proxy_directory = debug_proxy_directory_with_file(test_path, b"hello world");
        let memory_reserver = LimitedMemoryReserver::new(100);
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone())
            .with_memory_reserver(memory_reserver.clone());
        let file_handle = caching_directory.get_file_handle(test_path).unwrap();

        // A sync cache miss fails, without calling the underlying directory.
        let io_error = file_handle.read_bytes(0..5).unwrap_err();
        assert_eq!(io_error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 0);
        assert_eq!(memory_reserver.reserved(), 0);
        // Once the data is in the cache, sync reads work.
        file_handle.read_bytes_async(0..5).await.unwrap();
        assert_eq!(file_handle.read_bytes(0..5).unwrap().as_slice(), b"hello");
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
    }

    #[tokio::test]
    async fn test_caching_directory_reserves_overlapping_bytes_once() {
        let test_path = Path::new("test");
        let debug_proxy_directory = debug_proxy_directory_with_file(test_path, b"hello world");
        let memory_reserver = LimitedMemoryReserver::new(100);
        let byte_range_cache = ByteRangeCache::with_infinite_capacity();
        let caching_directory =
            CachingDirectory::new(debug_proxy_directory.clone(), byte_range_cache.clone())
                .with_memory_reserver(memory_reserver.clone());
        let file_handle = caching_directory.get_file_handle(test_path).unwrap();

        file_handle.read_bytes_async(0..5).await.unwrap();
        // A cache miss: 3..5 is cached, but not 5..8. Only 5..8 stays reserved.
        file_handle.read_bytes_async(3..8).await.unwrap();
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 2);
        assert_eq!(byte_range_cache.get_num_bytes(), 8);
        assert_eq!(memory_reserver.reserved(), 8);
    }

    #[tokio::test]
    async fn test_caching_directory_reserves_memory_on_async_cache_miss() {
        let test_path = Path::new("test");
        let debug_proxy_directory = debug_proxy_directory_with_file(test_path, b"hello world");
        let memory_reserver = LimitedMemoryReserver::new(100);
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone())
            .with_memory_reserver(memory_reserver.clone());
        let file_handle = caching_directory.get_file_handle(test_path).unwrap();

        file_handle.read_bytes_async(0..5).await.unwrap();
        assert_eq!(memory_reserver.reserved(), 5);
        // A cache hit does not reserve memory.
        file_handle.read_bytes_async(0..5).await.unwrap();
        assert_eq!(memory_reserver.reserved(), 5);
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
    }

    #[tokio::test]
    async fn test_caching_directory_memory_reservation_failure() {
        let test_path = Path::new("test");
        let debug_proxy_directory = debug_proxy_directory_with_file(test_path, b"hello world");
        let memory_reserver = LimitedMemoryReserver::new(4);
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone())
            .with_memory_reserver(memory_reserver.clone());
        let file_handle = caching_directory.get_file_handle(test_path).unwrap();

        let io_error = file_handle.read_bytes_async(0..5).await.unwrap_err();
        assert_eq!(io_error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(
            io_error.to_string(),
            "out of warmup memory: split budget exceeded"
        );
        // The underlying directory is never called and nothing is cached.
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 0);
        assert_eq!(memory_reserver.reserved(), 0);
    }
}
