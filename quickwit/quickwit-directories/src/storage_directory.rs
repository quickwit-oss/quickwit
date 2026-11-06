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
use futures::{StreamExt, TryStreamExt, stream};
use quickwit_common::coalesce_ranges;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::uri::Uri;
use quickwit_storage::{OwnedBytes, Storage};
use tantivy::directory::FileHandle;
use tantivy::directory::error::OpenReadError;
use tantivy::{Directory, HasLen};
use tokio::io::AsyncReadExt;
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

    #[instrument(level = "debug", fields(path = %self.path.to_string_lossy(), byte_range_size = byte_range.end - byte_range.start), skip(self))]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
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
        byte_ranges: Vec<Range<usize>>,
    ) -> io::Result<Vec<OwnedBytes>> {
        if byte_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let mut buffers = vec![OwnedBytes::empty(); byte_ranges.len()];
        // Plan the complete batch as descriptors, then create at most five download futures.
        let (requests, download_ranges) = run_cpu_intensive(move || {
            let mut requests: Vec<_> = byte_ranges
                .into_iter()
                .enumerate()
                .filter(|(_, range)| range.start != range.end)
                .collect();
            requests.sort_unstable_by_key(|(_, range)| range.start);
            let download_ranges =
                coalesce_ranges(requests.iter().map(|(_, range)| range.clone()).collect());
            (requests, download_ranges)
        })
        .await
        .map_err(io::Error::other)?;
        let mut downloads = stream::iter(download_ranges)
            .map(|range| {
                let first =
                    requests.partition_point(|(_, requested)| requested.start < range.start);
                let end = requests.partition_point(|(_, requested)| requested.start < range.end);
                self.read_batch_range(range, &requests[first..end])
            })
            .buffer_unordered(5);
        while let Some(downloaded) = downloads.try_next().await? {
            for (index, bytes) in downloaded {
                buffers[index] = bytes;
            }
        }
        Ok(buffers)
    }
}

impl StorageDirectoryFileHandle {
    // Requests are sorted, nonempty, and contained in the coalesced download range.
    async fn read_batch_range(
        &self,
        range: Range<usize>,
        mut requests: &[(usize, Range<usize>)],
    ) -> io::Result<Vec<(usize, OwnedBytes)>> {
        let invalid_length = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "download length differs from requested range",
            )
        };
        if let [(index, _)] = requests {
            let bytes = self.read_bytes_async(range.clone()).await?;
            if bytes.len() != range.len() {
                return Err(invalid_length());
            }
            return Ok(vec![(*index, bytes)]);
        }
        let reader = self
            .storage_directory
            .storage
            .get_slice_stream(&self.path, range.clone())
            .await?;
        let mut reader = tokio::io::BufReader::new(reader);
        let mut buffers = Vec::with_capacity(requests.len());
        let mut position = range.start;
        while !requests.is_empty() {
            // Read overlapping requests together so the stream never needs to seek backwards.
            let mut useful = requests[0].1.clone();
            let mut count = 1;
            while count < requests.len() && requests[count].1.start < useful.end {
                useful.end = useful.end.max(requests[count].1.end);
                count += 1;
            }
            let gap = (useful.start - position) as u64;
            if gap > 0
                && tokio::io::copy_buf(&mut (&mut reader).take(gap), &mut tokio::io::sink()).await?
                    != gap
            {
                return Err(invalid_length());
            }
            let mut bytes = vec![0; useful.len()];
            reader.read_exact(&mut bytes).await.map_err(|error| {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    invalid_length()
                } else {
                    error
                }
            })?;
            let bytes = OwnedBytes::new(bytes);
            for (index, requested) in &requests[..count] {
                let result = if *requested == useful {
                    bytes.clone()
                } else {
                    // Copy proper subranges so cached results don't retain the entire
                    // overlapping-request buffer.
                    OwnedBytes::new(
                        bytes[requested.start - useful.start..requested.end - useful.start]
                            .to_vec(),
                    )
                };
                buffers.push((*index, result));
            }
            position = useful.end;
            requests = &requests[count..];
        }
        debug_assert_eq!(position, range.end);
        if reader.read(&mut [0]).await? != 0 {
            return Err(invalid_length());
        }
        Ok(buffers)
    }
}

/// Directory backed a quickwit `Storage` abstraction.
///
/// Explicit batches are coalesced here and fetched with at most five concurrent storage reads.
/// Separate read calls are not merged. Results preserve input order and duplicate ranges.
/// Downloads merging multiple requests stream through storage wrappers, retaining only requested
/// bytes and discarding gaps as they arrive. Unmerged reads keep using the buffered storage API.
///
/// It should not be used in a context outside quickwit, as it contains
/// several pitfalls:
/// Fetching data synchronously panics.
/// Writing data panics.
///
/// This directory is fetch slices of data to a possibly distant storage
/// everytime `read_bytes` is called.
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
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use quickwit_storage::{MockStorage, StorageErrorKind};
    use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

    use super::*;
    use crate::CachingDirectory;

    #[tokio::test]
    async fn test_batch_plans_all_ranges_and_preserves_order() {
        const MIB: usize = 1024 * 1024;
        let mut storage = MockStorage::default();
        let unmerged_bytes = OwnedBytes::new(vec![0, 1]);
        let unmerged_address = unmerged_bytes.as_ptr();
        storage
            .expect_get_slice()
            .times(1)
            .return_once(move |_, range| {
                assert_eq!(range, 10 * MIB..10 * MIB + 2);
                Ok(unmerged_bytes)
            });
        let largest_gap_read = Arc::new(AtomicUsize::new(0));
        let observed = largest_gap_read.clone();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(move |path, range| {
                assert_eq!(path, Path::new("file"));
                assert!([0..300_011, 5 * MIB..5 * MIB + 2].contains(&range));
                Ok(Box::new(GeneratedReader {
                    position: range.start,
                    end: range.end,
                    fail_at: None,
                    largest_gap_read: observed.clone(),
                }))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        assert!(
            handle
                .read_bytes_batch_async(Vec::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            handle
                .read_bytes_batch_async(vec![0..0, 9..9])
                .await
                .unwrap()
                .iter()
                .all(|bytes| bytes.is_empty())
        );
        let mut ranges: Vec<_> = [5 * MIB..5 * MIB + 2, 0..2, 1..3, 4..4, 5 * MIB..5 * MIB + 2]
            .into_iter()
            .cycle()
            .take(10_000)
            .collect();
        ranges.extend([
            1000..8000,
            4..16_393,
            4..16_393,
            300_000..300_011,
            10 * MIB..10 * MIB + 2,
        ]);
        let buffers = handle.read_bytes_batch_async(ranges.clone()).await.unwrap();
        assert_eq!(buffers.last().unwrap().as_ptr(), unmerged_address);
        let overlapping = &buffers[10_000..10_003];
        assert_eq!(overlapping[1].as_ptr(), overlapping[2].as_ptr());
        // Proper subranges own their bytes rather than retaining the overlapping buffer.
        assert_ne!(
            overlapping[0].as_ptr(),
            overlapping[1].as_ptr().wrapping_add(1000 - 4)
        );
        assert!((1..=64 * 1024).contains(&largest_gap_read.load(Ordering::Relaxed)));
        assert_eq!(buffers.len(), ranges.len());
        for (range, bytes) in ranges.into_iter().zip(buffers) {
            assert_eq!(
                bytes.as_slice(),
                range.map(|offset| offset as u8).collect::<Vec<_>>()
            );
        }
    }

    // Generate chunks without ever allocating the full response, including the gap.
    struct GeneratedReader {
        position: usize,
        end: usize,
        fail_at: Option<usize>,
        largest_gap_read: Arc<AtomicUsize>,
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
            if (16_393..300_000).contains(&self.position) {
                self.largest_gap_read
                    .fetch_max(buffer.remaining(), Ordering::Relaxed);
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
                        largest_gap_read: Arc::default(),
                    }))
                });
            let directory =
                CachingDirectory::new_unbounded(Arc::new(StorageDirectory::new(Arc::new(storage))));
            let handle = directory.get_file_handle(Path::new("file")).unwrap();
            assert_eq!(
                handle
                    .read_bytes_batch_async(vec![0..5, 16..20])
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
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
    async fn test_batch_limits_streams_and_drops_them_on_cancellation() {
        let writers = Arc::new(Mutex::new(Vec::new()));
        let opened = writers.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(5)
            .returning(move |_, _| {
                let (reader, writer) = tokio::io::duplex(1);
                opened.lock().unwrap().push(writer);
                Ok(Box::new(reader))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        let batch = tokio::spawn(async move {
            let ranges = (0..6)
                .flat_map(|index| {
                    let start = index * 5 * 1024 * 1024;
                    [start..start + 1, start + 2..start + 3]
                })
                .collect();
            handle.read_bytes_batch_async(ranges).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while writers.lock().unwrap().len() < 5 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        batch.abort();
        assert!(batch.await.unwrap_err().is_cancelled());
        let writers = std::mem::take(&mut *writers.lock().unwrap());
        assert_eq!(writers.len(), 5);
        for mut writer in writers {
            assert_eq!(
                writer.write_all(&[0]).await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
        }
    }
}
