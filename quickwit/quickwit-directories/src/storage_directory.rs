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

use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_storage::{OwnedBytes, Storage};
use tantivy::directory::FileHandle;
use tantivy::directory::error::OpenReadError;
use tantivy::{Directory, HasLen};
use tracing::{error, instrument};

struct StorageDirectoryFileHandle {
    storage_directory: StorageDirectory,
    path: PathBuf,
}

impl HasLen for StorageDirectoryFileHandle {
    fn len(&self) -> usize {
        unimplemented!()
    }
}

impl fmt::Debug for StorageDirectoryFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StorageDirectoryFileHandle({:?}, dir={:?})",
            &self.path, self.storage_directory
        )
    }
}

#[async_trait]
impl FileHandle for StorageDirectoryFileHandle {
    fn read_bytes(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(unsupported_operation(&self.path))
    }

    #[instrument(level = "debug", fields(path = %self.path.to_string_lossy(), byte_range_size = byte_range.end.saturating_sub(byte_range.start)), skip(self))]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.start > byte_range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "byte range starts after its end",
            ));
        }
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let object_bytes = self
            .storage_directory
            .get_slice(&self.path, byte_range)
            .await?;
        Ok(object_bytes)
    }

    async fn read_bytes_batch_async(
        &self,
        ranges: Vec<Range<usize>>,
    ) -> io::Result<Vec<OwnedBytes>> {
        self.storage_directory
            .storage
            .get_slices(&self.path, ranges)
            .await
            .map_err(io::Error::from)
    }
}

/// Directory backed by Quickwit's `Storage` abstraction.
///
/// Batch reads forward the full range list through storage caches and bundle translation.
/// Results preserve input order, duplicates, and empty ranges. Storage caches see the original
/// ranges before a lower `CoalescingStorage` combines all misses and limits backend downloads.
///
/// This directory supports asynchronous reads. Synchronous reads return errors. Writes panic.
#[derive(Clone)]
pub struct StorageDirectory {
    storage: Arc<dyn Storage>,
}

impl Debug for StorageDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StorageDirectory({:?})", self.uri())
    }
}

impl StorageDirectory {
    /// Creates a new StorageDirectory, backed by the given `storage`.
    pub fn new(storage: Arc<dyn Storage>) -> StorageDirectory {
        StorageDirectory { storage }
    }

    /// Fetches a slice of byte from a file asynchronously.
    pub async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
        let payload: OwnedBytes = self.storage.get_slice(path, range).await?;
        Ok(payload)
    }

    /// Fetches an entire file asynchronously.
    pub async fn get_all(&self, path: &Path) -> io::Result<OwnedBytes> {
        let payload: OwnedBytes = self.storage.get_all(path).await?;
        Ok(payload)
    }

    /// Returns the uri associated to the underlying storage.
    pub fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}

fn unsupported_operation(path: &Path) -> io::Error {
    let error = "unsupported operation: `StorageDirectory` only supports async reads";
    error!(error, ?path);
    io::Error::other(format!("{error}: {}", path.display()))
}

impl Directory for StorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(StorageDirectoryFileHandle {
            storage_directory: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            unsupported_operation(path),
            path.to_path_buf(),
        ))
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            unsupported_operation(path),
            path.to_path_buf(),
        ))
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use quickwit_storage::{
        CoalescingStorage, MockStorage, MockStorageCache, StorageErrorKind, wrap_storage_with_cache,
    };
    use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

    use super::*;
    use crate::CachingDirectory;

    #[tokio::test(start_paused = true)]
    async fn test_batch_preserves_order_and_cache_lookups() {
        const DISTANT_RANGE_START: usize = 10 * 1024 * 1024;
        let ranges = vec![
            DISTANT_RANGE_START..DISTANT_RANGE_START + 2,
            0..2,
            1..3,
            DISTANT_RANGE_START..DISTANT_RANGE_START + 2,
            4..4,
            6..8,
            8..10,
            DISTANT_RANGE_START..DISTANT_RANGE_START + 2,
            20..20,
        ];
        let cached_ranges = Arc::new(Mutex::new(HashMap::from([(
            4..6,
            OwnedBytes::new(vec![4, 5]),
        )])));
        let cache_for_get = cached_ranges.clone();
        let lookups = Arc::new(Mutex::new(Vec::new()));
        let lookups_for_get = lookups.clone();
        let mut cache = MockStorageCache::default();
        cache.expect_get().times(15).returning(move |path, range| {
            assert_eq!(path, Path::new("file"));
            lookups_for_get.lock().unwrap().push(range.clone());
            cache_for_get.lock().unwrap().get(&range).cloned()
        });
        cache
            .expect_put()
            .times(7)
            .returning(move |_, range, bytes| {
                cached_ranges.lock().unwrap().insert(range, bytes);
            });
        let downloads = Arc::new(Mutex::new(Vec::new()));
        let downloads_for_storage = downloads.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(move |_, range| {
                downloads_for_storage.lock().unwrap().push(range.clone());
                Ok(Box::new(std::io::Cursor::new(
                    range
                        .map(|position| (position % 251) as u8)
                        .collect::<Vec<_>>(),
                )))
            });
        let collector = Arc::new(CoalescingStorage::new(Arc::new(storage)));
        let storage = wrap_storage_with_cache(Arc::new(cache), collector);
        let directory = StorageDirectory::new(storage);
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        let read_start = tokio::time::Instant::now();
        assert!(
            handle
                .read_bytes_batch_async(Vec::new())
                .await
                .unwrap()
                .is_empty()
        );
        let reversed_range = Range { start: 3, end: 2 };
        assert_eq!(
            handle
                .read_bytes_async(reversed_range)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            handle.read_bytes_async(4..6).await.unwrap().as_slice(),
            &[4, 5]
        );
        assert_eq!(read_start.elapsed(), Duration::ZERO);

        let buffers = handle.read_bytes_batch_async(ranges.clone()).await.unwrap();
        assert_eq!(buffers.len(), ranges.len());
        for (range, bytes) in ranges.iter().zip(&buffers) {
            assert_eq!(
                bytes.as_slice(),
                range
                    .clone()
                    .map(|position| (position % 251) as u8)
                    .collect::<Vec<_>>()
            );
        }
        let repeat_start = tokio::time::Instant::now();
        let repeated = handle.read_bytes_batch_async(ranges.clone()).await.unwrap();
        assert_eq!(
            repeated
                .iter()
                .map(OwnedBytes::as_slice)
                .collect::<Vec<_>>(),
            buffers.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>()
        );
        assert_eq!(repeat_start.elapsed(), Duration::ZERO);
        let mut expected_lookups: Vec<_> = ranges
            .into_iter()
            .filter(|range| !range.is_empty())
            .collect();
        expected_lookups.extend_from_within(..);
        expected_lookups.push(4..6);
        expected_lookups.sort_unstable_by_key(|range| range.start);
        let mut lookups = lookups.lock().unwrap().clone();
        lookups.sort_unstable_by_key(|range| range.start);
        assert_eq!(lookups, expected_lookups);
        let mut downloads = downloads.lock().unwrap().clone();
        downloads.sort_unstable_by_key(|range| range.start);
        assert_eq!(
            downloads,
            vec![0..10, DISTANT_RANGE_START..DISTANT_RANGE_START + 2]
        );
    }

    struct GeneratedReader {
        position: usize,
        end: usize,
        fail_at: Option<usize>,
    }

    impl AsyncRead for GeneratedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.fail_at == Some(self.position) {
                return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
            }
            let end = self.fail_at.unwrap_or(self.end).min(self.end);
            let count = buffer.remaining().min(end - self.position).min(4093);
            for _ in 0..count {
                buffer.put_slice(&[self.position as u8]);
                self.position += 1;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)] // Each caller requests one byte range.
    async fn test_failed_batch_is_not_cached() {
        // Missing file, short gap/data, oversized body, and I/O failures in gaps/data.
        for (end, fail_at, kind) in [
            (None, None, io::ErrorKind::NotFound),
            (Some(10), None, io::ErrorKind::InvalidData),
            (Some(18), None, io::ErrorKind::InvalidData),
            (Some(21), None, io::ErrorKind::InvalidData),
            (Some(20), Some(7), io::ErrorKind::ConnectionReset),
            (Some(20), Some(18), io::ErrorKind::ConnectionReset),
        ] {
            let mut storage = MockStorage::default();
            let attempts = AtomicUsize::new(0);
            storage
                .expect_get_slice_stream()
                .times(2)
                .returning(move |_, range| {
                    assert_eq!(range, 0..20);
                    let (end, fail_at) = if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                        let Some(end) = end else {
                            return Err(StorageErrorKind::NotFound
                                .with_error(anyhow::anyhow!("missing file")));
                        };
                        (end, fail_at)
                    } else {
                        (20, None)
                    };
                    Ok(Box::new(GeneratedReader {
                        position: 0,
                        end,
                        fail_at,
                    }))
                });
            let storage = Arc::new(CoalescingStorage::new(Arc::new(storage)));
            let directory =
                CachingDirectory::new_unbounded(Arc::new(StorageDirectory::new(storage)));
            let handle = directory.get_file_handle(Path::new("file")).unwrap();
            let (first_result, second_result) = tokio::join!(
                handle.read_bytes_batch_async(vec![0..5]),
                handle.read_bytes_batch_async(vec![16..20]),
            );
            assert_eq!(first_result.unwrap_err().kind(), kind);
            assert_eq!(second_result.unwrap_err().kind(), kind);
            for _ in 0..2 {
                let bytes = handle
                    .read_bytes_batch_async(vec![0..5, 16..20])
                    .await
                    .unwrap();
                assert_eq!(bytes[0].as_slice(), &[0, 1, 2, 3, 4]);
                assert_eq!(bytes[1].as_slice(), &[16, 17, 18, 19]);
            }
        }
    }

    #[tokio::test]
    async fn test_batch_exposes_all_ranges_and_cancels_download() {
        let download_writer = Arc::new(Mutex::new(None));
        let writer_for_storage = download_writer.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(1)
            .return_once(move |_, range| {
                // All six descriptors reach the coalescer before the download starts.
                assert_eq!(range, 0..16);
                let (reader, writer) = tokio::io::duplex(1);
                *writer_for_storage.lock().unwrap() = Some(writer);
                Ok(Box::new(reader))
            });
        let storage = Arc::new(CoalescingStorage::new(Arc::new(storage)));
        let directory = StorageDirectory::new(storage);
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        let batch = tokio::spawn(async move {
            let ranges = (0..6).map(|index| index * 3..index * 3 + 1).collect();
            handle.read_bytes_batch_async(ranges).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while download_writer.lock().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        let mut writer = download_writer.lock().unwrap().take().unwrap();
        assert_eq!(
            writer.write_all(&[0]).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}
