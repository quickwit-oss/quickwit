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

use crate::StorageDirectory;
use async_trait::async_trait;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, io};
use tantivy::chrono::{DateTime, Utc};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::Directory;
use tantivy::HasLen;

/// A ReadOperation records meta data about a read operation.
/// It is recorded by the `DebugProxyDirectory`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReadOperation {
    /// Path that was read
    pub path: PathBuf,
    /// If fetching a range of data, the start offset, else 0.
    pub offset: usize,
    /// The number of bytes fetched
    pub num_bytes: usize,
    /// The date at which the operation was performed.
    pub start_date: DateTime<Utc>,
    /// The elapsed time to run the read operatioon.
    pub duration: Duration,
}

struct ReadOperationBuilder {
    start_date: DateTime<Utc>,
    start_instant: Instant,
    path: PathBuf,
    offset: usize,
}

impl ReadOperationBuilder {
    pub fn new(path: &Path) -> Self {
        let start_instant = Instant::now();
        let start_date = Utc::now();
        ReadOperationBuilder {
            start_date,
            start_instant,
            path: path.to_path_buf(),
            offset: 0,
        }
    }

    pub fn with_offset(self, offset: usize) -> Self {
        ReadOperationBuilder {
            start_date: self.start_date,
            start_instant: self.start_instant,
            path: self.path,
            offset,
        }
    }

    fn terminate(self, num_bytes: usize) -> ReadOperation {
        let duration = self.start_instant.elapsed();
        ReadOperation {
            path: self.path.clone(),
            offset: self.offset,
            num_bytes,
            start_date: self.start_date,
            duration,
        }
    }
}

/// The debug proxy wraps another directory and simply acts as a proxy
/// recording all of its read operations.
///
/// It has two purpose
/// - It is used when building our hotcache, to identify the file sections that
/// should be in the hotcache.
/// - It is used in the search-api to provide debugging/performance information.
#[derive(Debug)]
pub struct DebugProxyDirectory<D: Directory> {
    underlying: Arc<D>,
    operations_tx: flume::Sender<ReadOperation>,
    operations_rx: flume::Receiver<ReadOperation>,
}

impl<D: Directory> Clone for DebugProxyDirectory<D> {
    fn clone(&self) -> Self {
        DebugProxyDirectory {
            underlying: self.underlying.clone(),
            operations_tx: self.operations_tx.clone(),
            operations_rx: self.operations_rx.clone(),
        }
    }
}

impl<D: Directory> DebugProxyDirectory<D> {
    /// Wraps another directory to log all of its read operations.
    pub fn wrap(directory: D) -> Self {
        let (operations_tx, operations_rx) = flume::unbounded();
        DebugProxyDirectory {
            underlying: Arc::new(directory),
            operations_tx,
            operations_rx,
        }
    }

    /// Returns all of the existing read operations.
    ///
    /// Calling this "drains" the existing queue of operations.
    pub fn drain_read_operations(&self) -> impl Iterator<Item = ReadOperation> + '_ {
        self.operations_rx.drain()
    }

    /// Adds a new operation
    fn register(&self, read_op: ReadOperation) {
        let _ = self.operations_tx.send(read_op);
    }

    /// Adds a new operation in an async fashion.
    async fn register_async(&self, read_op: ReadOperation) {
        let _ = self.operations_tx.send_async(read_op).await;
    }
}

struct DebugProxyFileHandle<D: Directory> {
    directory: DebugProxyDirectory<D>,
    underlying: Box<dyn FileHandle>,
    path: PathBuf,
}

#[async_trait]
impl<D: Directory> FileHandle for DebugProxyFileHandle<D> {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        let read_operation_builder =
            ReadOperationBuilder::new(&self.path).with_offset(byte_range.start);
        let payload = self.underlying.read_bytes(byte_range)?;
        let read_operation = read_operation_builder.terminate(payload.len());
        self.directory.register(read_operation);
        Ok(payload)
    }

    fn get_physical_address(&self, range: Range<usize>) -> Option<String> {
        self.underlying.get_physical_address(range)
    }

    async fn read_bytes_async(
        &self,
        byte_range: Range<usize>,
    ) -> tantivy::AsyncIoResult<OwnedBytes> {
        let read_operation_builder =
            ReadOperationBuilder::new(&self.path).with_offset(byte_range.start);
        let payload = self.underlying.read_bytes_async(byte_range).await?;
        let read_operation = read_operation_builder.terminate(payload.len());
        self.directory.register_async(read_operation).await;
        Ok(payload)
    }
}

impl<D: Directory> fmt::Debug for DebugProxyFileHandle<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DebugProxyFileHandle({:?})", &self.underlying)
    }
}

impl<D: Directory> HasLen for DebugProxyFileHandle<D> {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

impl<D: Directory> Directory for DebugProxyDirectory<D> {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        let underlying = self.underlying.get_file_handle(path)?;
        Ok(Box::new(DebugProxyFileHandle {
            underlying,
            directory: self.clone(),
            path: path.to_owned(),
        }))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.underlying.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.underlying.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let read_operation_builder = ReadOperationBuilder::new(path);
        let payload = self.underlying.atomic_read(path)?;
        let read_operation = read_operation_builder.terminate(payload.len());
        self.register(read_operation);
        Ok(payload.to_vec())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &tantivy::directory::Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(|| {})))
    }
}

impl DebugProxyDirectory<StorageDirectory> {
    /// Fetches a slice of byte from a file asynchronously.
    pub async fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<Vec<u8>> {
        let read_operation_builder = ReadOperationBuilder::new(path);
        let payload: Vec<u8> = self.underlying.get_slice(path, range).await?;
        let read_operation = read_operation_builder.terminate(payload.len());
        self.register_async(read_operation).await;
        Ok(payload)
    }

    /// Fetches an entire file asynchronously.
    pub async fn get_all(&self, path: &Path) -> io::Result<Vec<u8>> {
        let read_operation_builder = ReadOperationBuilder::new(path);
        let payload: Vec<u8> = self.underlying.get_all(path).await?;
        let read_operation = read_operation_builder.terminate(payload.len());
        self.register_async(read_operation).await;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::DebugProxyDirectory;
    use std::io::Write;
    use std::path::Path;
    use tantivy::directory::{RAMDirectory, TerminatingWrite};
    use tantivy::Directory;

    const TEST_PATH: &str = "test.file";
    const TEST_PAYLOAD: &[u8] = b"hello happy tax payer";

    fn make_test_directory() -> tantivy::Result<RAMDirectory> {
        let ram_directory = RAMDirectory::create();
        let mut wrt = ram_directory.open_write(Path::new(TEST_PATH))?;
        wrt.write_all(TEST_PAYLOAD)?;
        wrt.flush()?;
        wrt.terminate()?;
        Ok(ram_directory)
    }

    #[test]
    fn test_debug_proxy_atomic_read() -> tantivy::Result<()> {
        let debug_proxy = DebugProxyDirectory::wrap(make_test_directory()?);
        let test_path = Path::new(TEST_PATH);
        let read_data = debug_proxy.atomic_read(test_path)?;
        assert_eq!(&read_data[..], TEST_PAYLOAD);
        let operations: Vec<crate::ReadOperation> = debug_proxy.drain_read_operations().collect();
        println!("operations {:?}", operations);
        assert_eq!(operations.len(), 1);
        let op0 = &operations[0];
        assert_eq!(op0.offset, 0);
        assert_eq!(op0.num_bytes, 21);
        assert_eq!(op0.path, test_path);
        Ok(())
    }

    #[test]
    fn test_debug_proxy_open_read_read_sync() -> tantivy::Result<()> {
        let test_path = Path::new(TEST_PATH);
        let debug_proxy = DebugProxyDirectory::wrap(make_test_directory()?);
        let read_data = debug_proxy.open_read(test_path)?;
        assert_eq!(read_data.read_bytes_slice(1..3)?.as_slice(), b"el");
        let operations: Vec<crate::ReadOperation> = debug_proxy.drain_read_operations().collect();
        assert_eq!(operations.len(), 1);
        let op = &operations[0];
        assert_eq!(op.path, test_path);
        assert_eq!(op.offset, 1);
        assert_eq!(op.num_bytes, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_debug_proxy_open_read_read_async() -> tantivy::Result<()> {
        let test_path = Path::new(TEST_PATH);
        let debug_proxy = DebugProxyDirectory::wrap(make_test_directory()?);
        let read_data = debug_proxy.open_read(test_path)?;
        assert_eq!(
            read_data.read_bytes_slice_async(1..3).await?.as_slice(),
            b"el"
        );
        let operations: Vec<crate::ReadOperation> = debug_proxy.drain_read_operations().collect();
        assert_eq!(operations.len(), 1);
        let op = &operations[0];
        assert_eq!(op.path, test_path);
        assert_eq!(op.offset, 1);
        assert_eq!(op.num_bytes, 2);
        Ok(())
    }
}
