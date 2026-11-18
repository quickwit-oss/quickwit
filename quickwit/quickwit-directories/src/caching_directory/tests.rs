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
    let file_handle = failing_caching_file_handle(ReadFailure::Pending, memory_reserver.clone());
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
