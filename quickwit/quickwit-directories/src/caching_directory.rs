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

/// Returned when reserving memory would go over a warmup memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutOfMemoryError {
    /// The split search would go over its own warmup memory limit.
    SplitBudgetExceeded,
    /// The node would go over the warmup memory limit shared by all split searches.
    GlobalBudgetExceeded,
}

impl fmt::Display for OutOfMemoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutOfMemoryError::SplitBudgetExceeded => {
                write!(formatter, "out of warmup memory: split budget exceeded")
            }
            OutOfMemoryError::GlobalBudgetExceeded => {
                write!(formatter, "out of warmup memory: global budget exceeded")
            }
        }
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
    /// Contract: only called by `memory_reserver`, from
    /// [`MemoryReserver::try_reserve_memory`], for `num_bytes` it has just reserved. A reservation
    /// created for bytes that were not reserved would release them anyway.
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
pub trait MemoryReserver: Send + Sync + 'static {
    /// Called on each cache miss, before reading `num_bytes` from the underlying directory.
    ///
    /// On success, returns a [`MemoryReservation`], which gives the memory back through
    /// `release_memory` when dropped, except for the bytes the read added to the cache.
    ///
    /// On error, the read fails with an `io::Error` of kind `OutOfMemory`: the underlying directory
    /// is not called and nothing is cached.
    fn try_reserve_memory(&self, num_bytes: u64)
    -> Result<MemoryReservation<'_>, OutOfMemoryError>;

    /// Gives back `num_bytes` reserved by `try_reserve_memory` that a read did not add to the
    /// cache: the read failed, panicked, its future was dropped, or the bytes were already cached.
    ///
    /// Contract: `num_bytes` were reserved by an earlier `try_reserve_memory` call, and are not
    /// released twice. Called from `Drop`, possibly while a panic unwinds, so it must not panic.
    fn release_memory(&self, num_bytes: u64);
}

/// A [`MemoryReserver`] that always succeeds. Used when the cache memory is not accounted for.
struct NoopMemoryReserver;

impl MemoryReserver for NoopMemoryReserver {
    fn try_reserve_memory(
        &self,
        num_bytes: u64,
    ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
        Ok(MemoryReservation::new(self, num_bytes))
    }

    fn release_memory(&self, _num_bytes: u64) {}
}

/// The caching directory is a simple cache that wraps another directory.
#[derive(Clone)]
pub struct CachingDirectory {
    underlying: Arc<dyn Directory>,
    // TODO fixme: that's a pretty ugly cache we have here.
    cache: ByteRangeCache,
    memory_reserver: Arc<dyn MemoryReserver>,
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
            memory_reserver: Arc::new(NoopMemoryReserver),
        }
    }

    /// Reserves memory in `memory_reserver` before reading each cache miss.
    ///
    /// The directory holds `memory_reserver` until it is dropped.
    pub fn with_memory_reserver(
        mut self,
        memory_reserver: Arc<dyn MemoryReserver>,
    ) -> CachingDirectory {
        self.memory_reserver = memory_reserver;
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
    memory_reserver: Arc<dyn MemoryReserver>,
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

#[async_trait]
impl FileHandle for CachingFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(byte_range.clone()) {
            return Ok(bytes);
        }
        let read_reservation = self
            .memory_reserver
            .try_reserve_memory(byte_range.len() as u64)?;
        let owned_bytes = self.underlying_filehandle.read_bytes(byte_range.clone())?;
        let num_added_bytes = self.cache.put_slice(byte_range, owned_bytes.clone());
        read_reservation.keep(num_added_bytes);
        Ok(owned_bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(owned_bytes) = self.cache.get_slice(byte_range.clone()) {
            return Ok(owned_bytes);
        }
        let read_reservation = self
            .memory_reserver
            .try_reserve_memory(byte_range.len() as u64)?;
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        let num_added_bytes = self.cache.put_slice(byte_range, read_bytes.clone());
        read_reservation.keep(num_added_bytes);
        Ok(read_bytes)
    }

    async fn read_bytes_batch_async(
        &self,
        requested_ranges: Vec<Range<usize>>,
    ) -> io::Result<Vec<OwnedBytes>> {
        quickwit_storage::read_batch_with_cache(
            requested_ranges,
            |requested_range| self.cache.get_slice(requested_range),
            |ranges_to_fetch| async move {
                // Reserve every miss before starting I/O. Dropping these guards releases all
                // reservations if any reservation or the download fails, or the future is dropped.
                let mut read_reservations = Vec::with_capacity(ranges_to_fetch.len());
                for missing_range in &ranges_to_fetch {
                    read_reservations.push(
                        self.memory_reserver
                            .try_reserve_memory(missing_range.len() as u64)?,
                    );
                }
                let downloaded = self
                    .underlying_filehandle
                    .read_bytes_batch_async(ranges_to_fetch.clone())
                    .await?;
                assert_eq!(downloaded.len(), ranges_to_fetch.len());
                for ((missing_range, bytes), read_reservation) in ranges_to_fetch
                    .into_iter()
                    .zip(&downloaded)
                    .zip(read_reservations)
                {
                    let num_added_bytes = self.cache.put_slice(missing_range, bytes.clone());
                    read_reservation.keep(num_added_bytes);
                }
                Ok(downloaded)
            },
        )
        .await
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
            memory_reserver: self.memory_reserver.clone(),
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

    use async_trait::async_trait;
    use quickwit_storage::ByteRangeCache;
    use tantivy::directory::{FileHandle, OwnedBytes, RamDirectory};
    use tantivy::{Directory, HasLen};

    use super::{CachingDirectory, CachingFileHandle, MemoryReserver, OutOfMemoryError};
    use crate::{DebugProxyDirectory, MemoryReservation};

    /// Reserves memory up to `limit` bytes.
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

    impl MemoryReserver for LimitedMemoryReserver {
        fn try_reserve_memory(
            &self,
            num_bytes: u64,
        ) -> Result<MemoryReservation<'_>, OutOfMemoryError> {
            self.reserved
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                    let new_reserved = reserved + num_bytes;
                    (new_reserved <= self.limit).then_some(new_reserved)
                })
                .map_err(|_| OutOfMemoryError::GlobalBudgetExceeded)?;
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
            memory_reserver,
        }
    }

    /// Exercises the three read APIs with the same cache and reservation assertions.
    #[derive(Clone, Copy, Debug)]
    enum ReadMode {
        Sync,
        Async,
        Batch,
    }

    impl ReadMode {
        async fn read(
            self,
            file_handle: &dyn FileHandle,
            ranges: Vec<Range<usize>>,
        ) -> io::Result<Vec<OwnedBytes>> {
            match self {
                Self::Sync => ranges
                    .into_iter()
                    .map(|range| file_handle.read_bytes(range))
                    .collect(),
                Self::Async => {
                    let mut buffers = Vec::new();
                    for range in ranges {
                        buffers.push(file_handle.read_bytes_async(range).await?);
                    }
                    Ok(buffers)
                }
                Self::Batch => file_handle.read_bytes_batch_async(ranges).await,
            }
        }
    }

    #[tokio::test]
    async fn test_caching_file_handle_releases_memory_on_read_error() {
        for mode in [ReadMode::Sync, ReadMode::Async, ReadMode::Batch] {
            let memory_reserver = LimitedMemoryReserver::new(100);
            let file_handle =
                failing_caching_file_handle(ReadFailure::Error, memory_reserver.clone());
            assert!(mode.read(&file_handle, vec![0..5, 5..10]).await.is_err());
            assert_eq!(memory_reserver.reserved(), 0, "{mode:?}");
            assert!(file_handle.cache.get_slice(0..5).is_none());
            assert!(file_handle.cache.get_slice(5..10).is_none());
        }
    }

    #[test]
    fn test_caching_file_handle_releases_memory_on_read_panic() {
        let memory_reserver = LimitedMemoryReserver::new(100);
        let file_handle = failing_caching_file_handle(ReadFailure::Panic, memory_reserver.clone());
        let read_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            file_handle.read_bytes(0..5)
        }));
        assert!(read_res.is_err());
        assert_eq!(memory_reserver.reserved(), 0);
    }

    #[tokio::test]
    async fn test_caching_file_handle_releases_memory_when_read_future_is_dropped() {
        for (mode, reserved) in [(ReadMode::Async, 5), (ReadMode::Batch, 10)] {
            let memory_reserver = LimitedMemoryReserver::new(100);
            let file_handle =
                failing_caching_file_handle(ReadFailure::Pending, memory_reserver.clone());
            let mut read_future = Box::pin(mode.read(&file_handle, vec![0..5, 5..10]));
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(read_future.as_mut().poll(&mut context).is_pending());
            assert_eq!(memory_reserver.reserved(), reserved, "{mode:?}");
            drop(read_future);
            assert_eq!(memory_reserver.reserved(), 0, "{mode:?}");
            assert!(file_handle.cache.get_slice(0..5).is_none());
            assert!(file_handle.cache.get_slice(5..10).is_none());
        }
    }

    const CONTENT: &[u8] = b"hello world";

    /// A cached file with observable reads, cache size, and reserved memory.
    struct CachedFile {
        handle: Arc<dyn FileHandle>,
        directory: Arc<DebugProxyDirectory<RamDirectory>>,
        cache: ByteRangeCache,
        memory: Arc<LimitedMemoryReserver>,
    }

    impl CachedFile {
        fn new(budget: u64) -> Self {
            let path = Path::new("test");
            let ram = RamDirectory::default();
            ram.atomic_write(path, CONTENT).unwrap();
            let directory = Arc::new(DebugProxyDirectory::wrap(ram));
            let cache = ByteRangeCache::with_infinite_capacity();
            let memory = LimitedMemoryReserver::new(budget);
            let caching = CachingDirectory::new(directory.clone(), cache.clone())
                .with_memory_reserver(memory.clone());
            Self {
                handle: caching.get_file_handle(path).unwrap(),
                directory,
                cache,
                memory,
            }
        }

        fn assert_state(&self, reserved: u64, reads: usize) {
            assert_eq!(self.memory.reserved(), reserved);
            assert_eq!(self.cache.get_num_bytes(), reserved);
            assert_eq!(self.directory.drain_read_operations().count(), reads);
        }

        async fn read(&self, mode: ReadMode, ranges: Vec<Range<usize>>) {
            let bytes = mode
                .read(self.handle.as_ref(), ranges.clone())
                .await
                .unwrap();
            assert_eq!(bytes.len(), ranges.len(), "{mode:?}");
            for (bytes, range) in bytes.iter().zip(ranges) {
                assert_eq!(bytes.as_slice(), &CONTENT[range], "{mode:?}");
            }
        }
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
    async fn test_caching_directory_reserves_cached_bytes_once() {
        for mode in [ReadMode::Sync, ReadMode::Async, ReadMode::Batch] {
            let file = CachedFile::new(100);
            // Miss, hit, then an overlapping miss that retains only three more bytes.
            for (range, reserved, reads) in [(0..5, 5, 1), (0..5, 5, 0), (3..8, 8, 1)] {
                file.read(mode, vec![range]).await;
                file.assert_state(reserved, reads);
            }
        }
    }

    #[tokio::test]
    async fn test_caching_directory_batch_memory_accounting() {
        for (budget, ranges, reserved, reads) in [
            // Overlaps and duplicates count once. Hits and empty ranges need no reservation,
            // even with only seven bytes left on the second read.
            (17, vec![5..10, 0..2, 2..7, 5..10, 4..4], 10, [3, 0]),
            // Large batches bypass lookups, including when every range is already cached.
            (510, vec![0..5; 101], 5, [101, 101]),
        ] {
            let file = CachedFile::new(budget);
            file.read(ReadMode::Sync, vec![0..2]).await;
            file.assert_state(2, 1);
            for reads in reads {
                file.read(ReadMode::Batch, ranges.clone()).await;
                file.assert_state(reserved, reads);
            }
            file.read(ReadMode::Sync, vec![0..5]).await;
            file.assert_state(reserved, 0);
        }
    }

    #[tokio::test]
    async fn test_caching_directory_memory_reservation_failure() {
        for (mode, budget, ranges) in [
            (ReadMode::Sync, 4, vec![0..5]),
            (ReadMode::Async, 4, vec![0..5]),
            (ReadMode::Batch, 8, vec![0..2, 2..6, 6..10]),
        ] {
            let file = CachedFile::new(budget);
            let cached_bytes = if matches!(mode, ReadMode::Batch) {
                file.read(ReadMode::Sync, vec![0..2]).await;
                file.assert_state(2, 1);
                2
            } else {
                0
            };

            // In the batch, the first miss fits but the second exceeds the budget.
            // All APIs reject the read before I/O and retain only previously cached bytes.
            let error = mode.read(file.handle.as_ref(), ranges).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::OutOfMemory, "{mode:?}");
            assert_eq!(
                error.to_string(),
                "out of warmup memory: global budget exceeded"
            );
            file.assert_state(cached_bytes, 0);
        }
    }
}
