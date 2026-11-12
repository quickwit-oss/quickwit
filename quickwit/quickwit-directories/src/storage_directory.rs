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
            self.path, self.storage_directory
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
        ranges: Vec<Range<usize>>,
    ) -> io::Result<Vec<OwnedBytes>> {
        self.storage_directory
            .storage
            .get_slices(&self.path, ranges)
            .await
            .map_err(io::Error::from)
    }
}

/// Directory backed a quickwit `Storage` abstraction.
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

    use quickwit_storage::{CoalescingStorage, MockStorage, StorageErrorKind};
    use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

    use super::*;
    use crate::CachingDirectory;

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
            (Some(10), None, io::ErrorKind::UnexpectedEof),
            (Some(18), None, io::ErrorKind::UnexpectedEof),
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
    async fn test_batch_limits_downloads_refills_and_cancels() {
        let writers = Arc::new(Mutex::new(Vec::new()));
        let writers_for_storage = writers.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(6)
            .returning(move |_, range| {
                assert_eq!(range.len(), 3);
                let (reader, writer) = tokio::io::duplex(1);
                writers_for_storage.lock().unwrap().push(writer);
                Ok(Box::new(reader))
            });
        let storage = Arc::new(CoalescingStorage::new(Arc::new(storage)));
        let directory = StorageDirectory::new(storage);
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
        assert_eq!(writers.lock().unwrap().len(), 5);
        // Completing the fifth download admits the sixth while earlier downloads remain blocked.
        let mut completed_writer = writers.lock().unwrap().pop().unwrap();
        completed_writer.write_all(&[0, 1, 2]).await.unwrap();
        completed_writer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while writers.lock().unwrap().len() < 5 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!batch.is_finished());
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
