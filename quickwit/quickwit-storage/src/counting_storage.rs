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
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_proto::search::DownloadBytesByComponent;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;

use crate::split_component_ranges::{SplitComponent, SplitComponentRanges};
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, BundleFileRanges, ListObjectsStream, PutPayload, Storage, StorageResult,
};

/// A point-in-time copy of the [`DownloadCounters`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DownloadSnapshot {
    /// Bytes of every successfully fulfilled read.
    pub num_bytes: u64,
    /// Number of calls to a read method.
    pub num_requests: u64,
    /// `num_bytes` broken down by the split data type each read belongs to.
    pub bytes_by_component: DownloadBytesByComponent,
}

/// Per-request download counters tracked by [`CountingStorage`].
///
/// `bytes` accumulates the size of every successfully fulfilled read; `requests`
/// counts each call to a read method. Every counted read additionally adds its bytes to the
/// split data type it belongs to in `component_bytes`. Counters are atomic so a single instance can
/// be shared across the wrapper clones used internally by lower storage layers (`HotDirectory`,
/// `BundleStorage`, etc.).
#[derive(Debug)]
pub struct DownloadCounters {
    bytes: AtomicU64,
    requests: AtomicU64,
    component_bytes: [AtomicU64; SplitComponent::COUNT],
    split_layout: SplitComponentRanges,
}

impl DownloadCounters {
    /// Snapshots all the counters.
    pub fn snapshot(&self) -> DownloadSnapshot {
        let load = |component: SplitComponent| {
            self.component_bytes[component as usize].load(Ordering::Relaxed)
        };
        DownloadSnapshot {
            num_bytes: self.bytes.load(Ordering::Relaxed),
            num_requests: self.requests.load(Ordering::Relaxed),
            bytes_by_component: DownloadBytesByComponent {
                term_dict_num_bytes: load(SplitComponent::TermDict),
                postings_num_bytes: load(SplitComponent::Postings),
                positions_num_bytes: load(SplitComponent::Positions),
                fast_fields_num_bytes: load(SplitComponent::FastFields),
                fieldnorms_num_bytes: load(SplitComponent::FieldNorms),
                footer_num_bytes: load(SplitComponent::Footer),
                other_num_bytes: load(SplitComponent::Other),
            },
        }
    }

    /// Registers the bundle file ranges of the instrumented split, so that body reads can be
    /// attributed to their data type. Call it once the split footer is parsed.
    pub fn set_split_bundle_ranges(&self, bundle_file_ranges: &BundleFileRanges) {
        self.split_layout.set_bundle_file_ranges(bundle_file_ranges);
    }

    fn record_read(&self, path: &Path, start: u64, num_bytes: u64) {
        self.bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
        let component = self.split_layout.classify(path, start, num_bytes);
        self.component_bytes[component as usize].fetch_add(num_bytes, Ordering::Relaxed);
    }
}

/// Storage proxy that counts the bytes and number of read requests it serves.
///
/// Wrap a base `Storage` with this proxy at the entry point of a request to
/// observe the per-request download volume. Cache hits (`storage_cache`, `split_cache`,
/// hotcache, short-lived cache) return before reaching this wrapper, so the counters
/// measure downloads from object storage.
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
    /// Wraps the storage holding `split_file` to count download requests and downloaded bytes,
    /// broken down by split data type.
    pub fn instrument_storage(
        inner: Arc<dyn Storage>,
        split_file: PathBuf,
        footer_range: Range<u64>,
    ) -> (Arc<dyn Storage>, Arc<DownloadCounters>) {
        let counters = Arc::new(DownloadCounters {
            bytes: AtomicU64::default(),
            requests: AtomicU64::default(),
            component_bytes: Default::default(),
            split_layout: SplitComponentRanges::new(split_file, footer_range),
        });
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
        self.counters.record_read(path, 0, num_bytes);
        Ok(num_bytes)
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let start = range.start as u64;
        let bytes = self.inner.get_slice(path, range).await?;
        self.counters.record_read(path, start, bytes.len() as u64);
        Ok(bytes)
    }

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        let start = range.start as u64;
        self.inner.get_slice_into(path, range, buffer).await?;
        self.counters.record_read(path, start, buffer.len() as u64);
        Ok(())
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // We approximate the bytes by the requested range length on success.
        // The stream may yield fewer bytes if the caller drops it early, but
        // that is rare and the over-count is bounded by the requested range.
        let start = range.start as u64;
        let range_len = range.len() as u64;
        let stream = self.inner.get_slice_stream(path, range).await?;
        self.counters.record_read(path, start, range_len);
        Ok(stream)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let bytes = self.inner.get_all(path).await?;
        self.counters.record_read(path, 0, bytes.len() as u64);
        Ok(bytes)
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
    use crate::{RamStorage, RamStorageBuilder};

    /// Instruments storage for a split that the tests never read.
    fn instrument_non_split_storage(
        inner: RamStorage,
    ) -> (Arc<dyn Storage>, Arc<DownloadCounters>) {
        CountingStorage::instrument_storage(Arc::new(inner), PathBuf::from("unread.split"), 0..0)
    }

    #[tokio::test]
    async fn test_counting_storage_counts_get_slice() {
        let inner = RamStorageBuilder::default()
            .put("foo", b"hello world")
            .build();
        let (storage, counters) = instrument_non_split_storage(inner);

        let bytes = storage.get_slice(Path::new("foo"), 0..5).await.unwrap();
        assert_eq!(bytes.as_slice(), b"hello");

        let bytes = storage.get_slice(Path::new("foo"), 6..11).await.unwrap();
        assert_eq!(bytes.as_slice(), b"world");

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.num_bytes, 10);
        assert_eq!(snapshot.num_requests, 2);
    }

    #[tokio::test]
    async fn test_counting_storage_counts_get_slice_into() {
        let inner = RamStorageBuilder::default()
            .put("foo", b"hello world")
            .build();
        let (storage, counters) = instrument_non_split_storage(inner);
        let mut buffer = [0; 5];
        storage
            .get_slice_into(Path::new("foo"), 6..11, &mut buffer)
            .await
            .unwrap();
        assert_eq!(&buffer, b"world");
        let snapshot = counters.snapshot();
        assert_eq!((snapshot.num_bytes, snapshot.num_requests), (5, 1));
        assert!(
            storage
                .get_slice_into(Path::new("missing"), 0..5, &mut buffer)
                .await
                .is_err()
        );
        assert_eq!(
            counters.snapshot(),
            snapshot,
            "failed reads must not count as downloads"
        );
    }

    #[tokio::test]
    async fn test_counting_storage_counts_get_all() {
        let inner = RamStorageBuilder::default().put("foo", b"hello").build();
        let (storage, counters) = instrument_non_split_storage(inner);

        let bytes = storage.get_all(Path::new("foo")).await.unwrap();
        assert_eq!(bytes.as_slice(), b"hello");

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.num_bytes, 5);
        assert_eq!(snapshot.num_requests, 1);
    }

    #[tokio::test]
    async fn test_counting_storage_breaks_down_split_reads_by_component() {
        use crate::{BundleStorage, SplitPayloadBuilder};

        let mut split_builder = SplitPayloadBuilder::default();
        split_builder.add_payload("seg.term".to_string(), Box::new(b"TERMS".to_vec()));
        split_builder.add_payload("seg.fast".to_string(), Box::new(b"FASTDATA".to_vec()));
        let split_payload = split_builder.finalize(b"HOT").unwrap();
        let footer_range = split_payload.footer_range.clone();
        let split_bytes = split_payload.read_all().await.unwrap();
        let inner = RamStorageBuilder::default()
            .put("split.split", split_bytes.as_slice())
            .put("other", b"hello")
            .build();
        let (storage, counters) = CountingStorage::instrument_storage(
            Arc::new(inner),
            PathBuf::from("split.split"),
            footer_range.clone(),
        );

        let footer_bytes = storage
            .get_slice(
                Path::new("split.split"),
                footer_range.start as usize..footer_range.end as usize,
            )
            .await
            .unwrap();
        let (bundle_storage, _hotcache) = BundleStorage::open_from_split_bytes(
            storage.clone(),
            PathBuf::from("split.split"),
            footer_bytes,
        )
        .unwrap();
        counters.set_split_bundle_ranges(bundle_storage.file_ranges());
        bundle_storage.get_all(Path::new("seg.term")).await.unwrap();
        let mut buffer = [0u8; 4];
        bundle_storage
            .get_slice_into(Path::new("seg.fast"), 2..6, &mut buffer)
            .await
            .unwrap();
        storage.get_all(Path::new("other")).await.unwrap();

        let snapshot = counters.snapshot();
        let breakdown = snapshot.bytes_by_component;
        let footer_len = footer_range.end - footer_range.start;
        assert_eq!(breakdown.footer_num_bytes, footer_len);
        assert_eq!(breakdown.term_dict_num_bytes, 5);
        assert_eq!(breakdown.fast_fields_num_bytes, 4);
        assert_eq!(breakdown.other_num_bytes, 5);
        assert_eq!(snapshot.num_bytes, footer_len + 5 + 4 + 5);
        assert_eq!(snapshot.num_requests, 4);
        let breakdown_total = breakdown.term_dict_num_bytes
            + breakdown.postings_num_bytes
            + breakdown.positions_num_bytes
            + breakdown.fast_fields_num_bytes
            + breakdown.fieldnorms_num_bytes
            + breakdown.footer_num_bytes
            + breakdown.other_num_bytes;
        assert_eq!(breakdown_total, snapshot.num_bytes);
    }

    #[tokio::test]
    async fn test_counting_storage_does_not_count_metadata() {
        let inner = RamStorageBuilder::default().put("foo", b"hello").build();
        let (storage, counters) = instrument_non_split_storage(inner);

        assert!(storage.exists(Path::new("foo")).await.unwrap());
        assert_eq!(storage.file_num_bytes(Path::new("foo")).await.unwrap(), 5);

        assert_eq!(counters.snapshot(), DownloadSnapshot::default());
    }
}
