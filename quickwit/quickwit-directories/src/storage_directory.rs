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
            .map(|range| async move {
                let bytes = self.read_bytes_async(range.clone()).await?;
                Ok::<_, io::Error>((range, bytes))
            })
            .buffer_unordered(5);
        while let Some((range, bytes)) = downloads.try_next().await? {
            if bytes.len() != range.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "download length differs from requested range",
                ));
            }
            let first = requests.partition_point(|(_, requested)| requested.start < range.start);
            for (index, requested) in requests[first..]
                .iter()
                .take_while(|(_, requested)| requested.start < range.end)
            {
                buffers[*index] = if *requested == range {
                    bytes.clone()
                } else {
                    // Copy proper subranges so cached results don't retain merged downloads.
                    OwnedBytes::new(
                        bytes[requested.start - range.start..requested.end - range.start].to_vec(),
                    )
                };
            }
        }
        Ok(buffers)
    }
}

/// Directory backed a quickwit `Storage` abstraction.
///
/// Explicit batches are coalesced here and fetched with at most five concurrent storage reads.
/// Separate read calls are not merged. Results preserve input order and duplicate ranges.
/// Storage wrappers beneath this directory receive the coalesced download ranges.
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quickwit_storage::{MockStorage, StorageErrorKind};

    use super::*;
    use crate::CachingDirectory;

    #[tokio::test]
    async fn test_batch_plans_all_ranges_and_preserves_order() {
        const MIB: usize = 1024 * 1024;
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice()
            .times(2)
            .returning(|path, range| {
                assert_eq!(path, Path::new("file"));
                assert!([0..3, 5 * MIB..5 * MIB + 2].contains(&range));
                Ok(OwnedBytes::new(
                    range.map(|offset| offset as u8).collect::<Vec<_>>(),
                ))
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
        let ranges: Vec<_> = [5 * MIB..5 * MIB + 2, 0..2, 1..3, 4..4, 5 * MIB..5 * MIB + 2]
            .into_iter()
            .cycle()
            .take(10_000)
            .collect();
        let buffers = handle.read_bytes_batch_async(ranges.clone()).await.unwrap();
        assert_eq!(buffers.len(), ranges.len());
        for (range, bytes) in ranges.into_iter().zip(buffers) {
            assert_eq!(
                bytes.as_slice(),
                range.map(|offset| offset as u8).collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn test_batch_payload_ownership() {
        for ranges in [[1..2, 4..5], [1..5, 1..5], [1..5, 2..3]] {
            let payload: Arc<[u8]> = Arc::from(&b"bcde"[..]);
            let weak = Arc::downgrade(&payload);
            let address = payload.as_ptr() as usize;
            let mut storage = MockStorage::default();
            storage
                .expect_get_slice()
                .times(1)
                .return_once(move |_, range| {
                    assert_eq!(range, 1..5);
                    Ok(OwnedBytes::new(payload))
                });
            let directory = StorageDirectory::new(Arc::new(storage));
            let handle = directory.get_file_handle(Path::new("file")).unwrap();
            let bytes = handle
                .read_bytes_batch_async(ranges.to_vec())
                .await
                .unwrap();
            for (range, bytes) in ranges.iter().zip(&bytes) {
                assert_eq!(bytes.as_slice(), &b"abcde"[range.clone()]);
                if *range == (1..5) {
                    assert_eq!(bytes.as_ptr() as usize, address);
                } else {
                    assert_ne!(bytes.as_ptr() as usize, address + range.start - 1);
                }
            }
            let [first, second]: [OwnedBytes; 2] = bytes.try_into().unwrap();
            drop(first);
            assert_eq!(weak.upgrade().is_some(), ranges[1] == (1..5));
            drop(second);
            assert!(weak.upgrade().is_none());
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)] // A batch containing one byte range.
    async fn test_failed_batch_is_not_cached() {
        let attempts = AtomicUsize::new(0);
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice()
            .times(4)
            .returning(move |_, range| {
                assert_eq!(range, 3..5);
                match attempts.fetch_add(1, Ordering::Relaxed) {
                    0 => {
                        Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing file")))
                    }
                    1 => Ok(OwnedBytes::new(vec![3])),
                    2 => Ok(OwnedBytes::new(vec![3, 4, 5])),
                    _ => Ok(OwnedBytes::new(vec![3, 4])),
                }
            });
        let directory =
            CachingDirectory::new_unbounded(Arc::new(StorageDirectory::new(Arc::new(storage))));
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::InvalidData,
            io::ErrorKind::InvalidData,
        ] {
            assert_eq!(
                handle
                    .read_bytes_batch_async(vec![3..5])
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
        for _ in 0..2 {
            let bytes = handle.read_bytes_batch_async(vec![3..5]).await.unwrap();
            assert_eq!(bytes[0].as_slice(), &[3, 4]);
        }
    }
}
