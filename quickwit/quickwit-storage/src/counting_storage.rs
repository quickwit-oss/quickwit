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

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, PutPayload, Storage, StorageResult};

/// Field and tantivy segment component a read is counted for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldComponent {
    /// Name of the field, or JSON path of the fast field.
    pub field_name: String,
    /// Extension of the file read by the outermost [`CountingStorage`]: `term`
    /// (term dictionary), `idx` (postings), `pos` (positions), `fast` (fast
    /// fields), `fieldnorm`, ...
    pub component: String,
}

tokio::task_local! {
    /// Field set by [`count_reads_for_field`], and the component resolved by the
    /// outermost [`CountingStorage`], if any.
    static READ_CONTEXT: (String, Option<String>);
}

/// Runs `fut` so that the reads it issues through a [`CountingStorage`] are also
/// counted per [`FieldComponent`] of `field_name`.
///
/// The field is set for the duration of each `poll` of `fut`, so concurrently
/// joined futures count their reads for their own field. Reads issued from a
/// task spawned by `fut` are not counted per field.
pub fn count_reads_for_field<F: Future>(
    field_name: String,
    fut: F,
) -> impl Future<Output = F::Output> {
    READ_CONTEXT.scope((field_name, None), fut)
}

/// Returns the field component a read of `path` is counted for, if it happens
/// within [`count_reads_for_field`].
///
/// The component is taken from the extension of the file read by the outermost
/// `CountingStorage`. Inner storages may read a different file (a
/// `BundleStorage` maps segment files to ranges of the `.split` file), so the
/// outermost component is propagated to them by [`CountingStorage::count_read`].
fn current_field_component(path: &Path) -> Option<FieldComponent> {
    READ_CONTEXT
        .try_with(|(field_name, component)| {
            let component = component.clone().unwrap_or_else(|| {
                let extension = path.extension().unwrap_or_default();
                extension.to_string_lossy().into_owned()
            });
            FieldComponent {
                field_name: field_name.clone(),
                component,
            }
        })
        .ok()
}

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
    /// `(bytes, requests)` of the reads issued within [`count_reads_for_field`].
    per_field_component: Mutex<HashMap<FieldComponent, (u64, u64)>>,
}

impl DownloadCounters {
    /// Snapshots the current counters as `(bytes, requests)`.
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.requests.load(Ordering::Relaxed),
        )
    }

    /// Snapshots the counters of the reads issued within [`count_reads_for_field`]
    /// as `field component -> (bytes, requests)`.
    pub fn per_field_component_snapshot(&self) -> HashMap<FieldComponent, (u64, u64)> {
        self.per_field_component.lock().unwrap().clone()
    }

    fn record_read(&self, num_bytes: u64, field_component_opt: Option<FieldComponent>) {
        self.bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
        let Some(field_component) = field_component_opt else {
            return;
        };
        let mut per_field_component = self.per_field_component.lock().unwrap();
        let (bytes, requests) = per_field_component.entry(field_component).or_default();
        *bytes += num_bytes;
        *requests += 1;
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

    /// Awaits `read` of `path` and counts it, propagating the field component to
    /// the reads of the inner storage.
    async fn count_read<T>(
        &self,
        path: &Path,
        read: impl Future<Output = StorageResult<T>>,
        num_bytes: impl FnOnce(&T) -> u64,
    ) -> StorageResult<T> {
        let Some(field_component) = current_field_component(path) else {
            let output = read.await?;
            self.counters.record_read(num_bytes(&output), None);
            return Ok(output);
        };
        let read_context = (
            field_component.field_name.clone(),
            Some(field_component.component.clone()),
        );
        let output = READ_CONTEXT.scope(read_context, read).await?;
        self.counters
            .record_read(num_bytes(&output), Some(field_component));
        Ok(output)
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
        let read = self.inner.copy_to_file(path, output_path);
        self.count_read(path, read, |num_bytes| *num_bytes).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let read = self.inner.get_slice(path, range);
        self.count_read(path, read, |bytes| bytes.len() as u64)
            .await
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
        let read = self.inner.get_slice_stream(path, range);
        self.count_read(path, read, |_| range_len).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let read = self.inner.get_all(path);
        self.count_read(path, read, |bytes| bytes.len() as u64)
            .await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.inner.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.inner.bulk_delete(paths).await
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.inner.list(prefix)
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

    fn field_component(field_name: &str, component: &str) -> FieldComponent {
        FieldComponent {
            field_name: field_name.to_string(),
            component: component.to_string(),
        }
    }

    #[tokio::test]
    async fn test_counting_storage_counts_reads_per_field_component() {
        let inner = RamStorageBuilder::default()
            .put("seg.fast", b"hello world")
            .put("seg.term", b"hello world")
            .build();
        let (inner_storage, inner_counters) = CountingStorage::instrument_storage(Arc::new(inner));
        let (storage, counters) = CountingStorage::instrument_storage(inner_storage);
        let status_read = count_reads_for_field("status".to_string(), async {
            tokio::task::yield_now().await;
            storage
                .get_slice(Path::new("seg.fast"), 0..5)
                .await
                .unwrap();
        });
        let body_read = count_reads_for_field("body".to_string(), async {
            tokio::task::yield_now().await;
            storage
                .get_slice(Path::new("seg.term"), 5..11)
                .await
                .unwrap();
        });
        tokio::join!(status_read, body_read);
        storage
            .get_slice(Path::new("seg.fast"), 0..1)
            .await
            .unwrap();

        let expected_per_field_component = HashMap::from([
            (field_component("status", "fast"), (5, 1)),
            (field_component("body", "term"), (6, 1)),
        ]);
        for counters in [counters, inner_counters] {
            assert_eq!(counters.snapshot(), (12, 3));
            assert_eq!(
                counters.per_field_component_snapshot(),
                expected_per_field_component
            );
        }
    }

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
