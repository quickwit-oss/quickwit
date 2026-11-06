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

/// Returned when reserving memory would go over the warmup memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfMemoryError;

impl From<OutOfMemoryError> for io::Error {
    fn from(_out_of_memory_error: OutOfMemoryError) -> io::Error {
        io::Error::new(io::ErrorKind::OutOfMemory, "out of warmup memory")
    }
}

/// Reserves memory for the data a [`CachingDirectory`] downloads and keeps in its cache.
pub trait MemoryReserver: Send + Sync + 'static {
    /// Called on each cache miss, before reading `num_bytes` from the underlying directory.
    ///
    /// On error, the read fails with an `io::Error` of kind `OutOfMemory`: the underlying directory
    /// is not called and nothing is cached.
    fn try_reserve_memory(&self, num_bytes: u64) -> Result<(), OutOfMemoryError>;
}

/// A [`MemoryReserver`] that always succeeds. Used when the cache memory is not accounted for.
struct NoopMemoryReserver;

impl MemoryReserver for NoopMemoryReserver {
    fn try_reserve_memory(&self, _num_bytes: u64) -> Result<(), OutOfMemoryError> {
        Ok(())
    }
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
            &self.path,
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
        self.memory_reserver
            .try_reserve_memory(byte_range.len() as u64)?;
        let owned_bytes = self.underlying_filehandle.read_bytes(byte_range.clone())?;
        self.cache.put_slice(byte_range, owned_bytes.clone());
        Ok(owned_bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(owned_bytes) = self.cache.get_slice(byte_range.clone()) {
            return Ok(owned_bytes);
        }
        self.memory_reserver
            .try_reserve_memory(byte_range.len() as u64)?;
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        self.cache.put_slice(byte_range, read_bytes.clone());
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
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use tantivy::Directory;
    use tantivy::directory::RamDirectory;

    use super::{CachingDirectory, MemoryReserver, OutOfMemoryError};
    use crate::DebugProxyDirectory;

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
        fn try_reserve_memory(&self, num_bytes: u64) -> Result<(), OutOfMemoryError> {
            self.reserved
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                    let new_reserved = reserved + num_bytes;
                    (new_reserved <= self.limit).then_some(new_reserved)
                })
                .map_err(|_| OutOfMemoryError)?;
            Ok(())
        }
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

    #[test]
    fn test_caching_directory_reserves_memory_on_cache_miss() {
        let test_path = Path::new("test");
        let debug_proxy_directory = debug_proxy_directory_with_file(test_path, b"hello world");
        let memory_reserver = LimitedMemoryReserver::new(100);
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone())
            .with_memory_reserver(memory_reserver.clone());
        let file_handle = caching_directory.get_file_handle(test_path).unwrap();

        file_handle.read_bytes(0..5).unwrap();
        assert_eq!(memory_reserver.reserved(), 5);
        // A cache hit does not reserve memory.
        file_handle.read_bytes(0..5).unwrap();
        assert_eq!(memory_reserver.reserved(), 5);
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
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
        assert_eq!(io_error.to_string(), "out of warmup memory");
        assert!(file_handle.read_bytes(0..5).is_err());
        // The underlying directory is never called and nothing is cached.
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 0);
        assert_eq!(memory_reserver.reserved(), 0);
    }
}
