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

    async fn read_bytes_stream(
        &self,
        byte_range: Range<usize>,
    ) -> io::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        if byte_range.is_empty() {
            return Ok(Box::new(tokio::io::empty()));
        }
        self.storage_directory
            .storage
            .get_slice_stream(&self.path, byte_range)
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
    use std::task::{Context, Poll};

    use quickwit_storage::{MockStorage, StorageErrorKind};
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

    use super::*;

    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }
    }

    #[tokio::test]
    async fn test_empty_stream_skips_storage() {
        let directory = StorageDirectory::new(Arc::new(MockStorage::default()));
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        let mut stream = handle.read_bytes_stream(7..7).await.unwrap();
        assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_stream_propagates_open_and_read_errors() {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(|_, range| {
                if range.start == 0 {
                    return Err(
                        StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing file"))
                    );
                }
                Ok(Box::new(io::Cursor::new([5]).chain(FailingReader)))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let handle = directory.get_file_handle(Path::new("file")).unwrap();
        let error = handle.read_bytes_stream(0..1).await.err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        let mut stream = handle.read_bytes_stream(5..20).await.unwrap();
        assert_eq!(stream.read_u8().await.unwrap(), 5);
        assert_eq!(
            stream.read_u8().await.unwrap_err().kind(),
            io::ErrorKind::ConnectionReset
        );
    }
}
