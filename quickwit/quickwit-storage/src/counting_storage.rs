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
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, PutPayload, Storage, StorageResult};

/// Per-request download counters tracked by [`CountingStorage`].
///
/// `bytes` accumulates the size of every successfully fulfilled read; `requests`
/// counts each call to a read method. Counters are atomic so a single instance
/// can be shared across the wrapper clones used internally by lower storage
/// layers (`HotDirectory`, `BundleStorage`, etc.).
#[derive(Debug, Default)]
pub struct DownloadCounters {
    bytes: AtomicU64,
    requests: AtomicU64,
}

impl DownloadCounters {
    /// Snapshots the current counters as `(bytes, requests)`.
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.requests.load(Ordering::Relaxed),
        )
    }

    fn record_read(&self, num_bytes: u64) {
        self.bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
}

/// Storage proxy that counts the bytes and number of read requests it serves.
///
/// Wrap a base `Storage` with this proxy at the entry point of a request to
/// observe the per-request download volume. Cached layers (split cache, footer
/// cache, hotcache, byte-range cache) live BELOW this wrapper, so reads served
/// from cache do NOT contribute to the counters — that is the desired behavior
/// for a "downloaded from object storage" measurement.
///
/// Write methods (`put`, `delete`, `bulk_delete`) and metadata methods
/// (`exists`, `file_num_bytes`, `check_connectivity`) are passed through
/// without counting; we only record reads that materialise data.
#[derive(Debug)]
pub struct CountingStorage {
    inner: Arc<dyn Storage>,
    counters: Arc<DownloadCounters>,
}

impl CountingStorage {
    /// Wrap a storage object to count for download request and downloaded bytes.
    pub fn instrument_storage(
        inner: Arc<dyn Storage>,
    ) -> (Arc<dyn Storage>, Arc<DownloadCounters>) {
        let counters = Arc::new(DownloadCounters::default());
        let instrumented_storage = Self {
            inner,
            counters: counters.clone(),
        };
        (Arc::new(instrumented_storage), counters)
    }
}

#[async_trait]
impl Storage for CountingStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.inner.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        // We do not know the final byte count without intercepting the writer,
        // so we conservatively count the request only. `copy_to` is not on the
        // hot path of leaf search (only `get_slice` is), so this approximation
        // is acceptable.
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_to(path, output).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<u64> {
        let num_bytes = self.inner.copy_to_file(path, output_path).await?;
        self.counters.record_read(num_bytes);
        Ok(num_bytes)
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let bytes = self.inner.get_slice(path, range).await?;
        self.counters.record_read(bytes.len() as u64);
        Ok(bytes)
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // We approximate the bytes by the requested range length on success.
        // The stream may yield fewer bytes if the caller drops it early, but
        // that is rare and the over-count is bounded by the requested range.
        let range_len = range.len() as u64;
        let stream = self.inner.get_slice_stream(path, range).await?;
        self.counters.record_read(range_len);
        Ok(stream)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let bytes = self.inner.get_all(path).await?;
        self.counters.record_read(bytes.len() as u64);
        Ok(bytes)
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.inner.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.inner.bulk_delete(paths).await
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.inner.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RamStorageBuilder;

    #[tokio::test]
    async fn test_counting_storage_counts_get_slice() {
        let inner = RamStorageBuilder::default()
            .put("foo", b"hello world")
            .build();
        let (storage, counters) = CountingStorage::instrument_storage(Arc::new(inner));

        let bytes = storage.get_slice(Path::new("foo"), 0..5).await.unwrap();
        assert_eq!(bytes.as_slice(), b"hello");

        let bytes = storage.get_slice(Path::new("foo"), 6..11).await.unwrap();
        assert_eq!(bytes.as_slice(), b"world");

        let (download_num_bytes, download_num_requests) = counters.snapshot();
        assert_eq!(download_num_bytes, 10);
        assert_eq!(download_num_requests, 2);
    }

    #[tokio::test]
    async fn test_counting_storage_counts_get_all() {
        let inner = RamStorageBuilder::default().put("foo", b"hello").build();
        let (storage, counters) = CountingStorage::instrument_storage(Arc::new(inner));

        let bytes = storage.get_all(Path::new("foo")).await.unwrap();
        assert_eq!(bytes.as_slice(), b"hello");

        let (download_num_bytes, download_num_requests) = counters.snapshot();
        assert_eq!(download_num_bytes, 5);
        assert_eq!(download_num_requests, 1);
    }

    #[tokio::test]
    async fn test_counting_storage_does_not_count_metadata() {
        let inner = RamStorageBuilder::default().put("foo", b"hello").build();
        let (storage, counters) = CountingStorage::instrument_storage(Arc::new(inner));

        assert!(storage.exists(Path::new("foo")).await.unwrap());
        assert_eq!(storage.file_num_bytes(Path::new("foo")).await.unwrap(), 5);

        let (download_num_bytes, download_num_requests) = counters.snapshot();
        assert_eq!(download_num_bytes, 0);
        assert_eq!(download_num_requests, 0);
    }
}
