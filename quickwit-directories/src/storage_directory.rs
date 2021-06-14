/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use async_trait::async_trait;
use quickwit_storage::Storage;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::OwnedBytes;
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::HasLen;
use tantivy::{AsyncIoResult, Directory};
use tracing::error;

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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> AsyncIoResult<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let object_bytes = self
            .storage_directory
            .get_slice(&self.path, byte_range)
            .await
            .map_err(Into::<io::Error>::into)?;
        Ok(OwnedBytes::new(object_bytes))
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageDirectory({:?})", self.uri())
    }
}

impl StorageDirectory {
    /// Creates a new StorageDirectory, backed by the given `storage`.
    pub fn new(storage: Arc<dyn Storage>) -> StorageDirectory {
        StorageDirectory { storage }
    }

    /// Fetches a slice of byte from a file asynchronously.
    pub async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<Vec<u8>> {
        let payload: Vec<u8> = self.storage.get_slice(path, range).await?;
        Ok(payload)
    }

    /// Fetches an entire file asynchronously.
    pub async fn get_all(&self, path: &Path) -> io::Result<Vec<u8>> {
        let payload: Vec<u8> = self.storage.get_all(path).await?;
        Ok(payload)
    }

    /// Returns the uri associated to the underlying storage.
    pub fn uri(&self) -> String {
        self.storage.uri()
    }
}

fn unsupported_operation(path: &Path) -> io::Error {
    let msg = "Unsupported operation. StorageDirectory only supports async reads";
    error!(path=?path, msg);
    io::Error::new(io::ErrorKind::Other, format!("{}: {:?}", msg, path))
}

impl Directory for StorageDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        Ok(Box::new(StorageDirectoryFileHandle {
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

    fn delete(&self, path: &std::path::Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: unsupported_operation(path),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            unsupported_operation(path),
            path.to_path_buf(),
        ))
    }

    fn open_write(&self, path: &std::path::Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            unsupported_operation(path),
            path.to_path_buf(),
        ))
    }

    fn atomic_write(&self, path: &std::path::Path, _data: &[u8]) -> io::Result<()> {
        Err(unsupported_operation(path))
    }

    fn watch(&self, _callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}
