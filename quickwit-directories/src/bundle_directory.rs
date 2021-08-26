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
use quickwit_storage::BundleStorageFileOffsets;
use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{fmt, io};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::directory::{FileSlice, OwnedBytes};
use tantivy::HasLen;
use tantivy::{AsyncIoResult, Directory};
use tracing::error;

struct BundleDirectoryFileHandle {
    bundle_directory: BundleDirectory,
    path: PathBuf,
}

impl HasLen for BundleDirectoryFileHandle {
    fn len(&self) -> usize {
        unimplemented!()
    }
}

impl fmt::Debug for BundleDirectoryFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BundleDirectoryFileHandle({:?}, dir={:?})",
            &self.path, self.bundle_directory
        )
    }
}

#[async_trait]
impl FileHandle for BundleDirectoryFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let object_bytes = self
            .bundle_directory
            .get_slice(&self.path, byte_range)
            .map_err(Into::<io::Error>::into)?;
        Ok(object_bytes)
    }

    async fn read_bytes_async(&self, _byte_range: Range<usize>) -> AsyncIoResult<OwnedBytes> {
        unimplemented!()
    }
}

/// BundleDirectory. Only read is supported.
#[derive(Clone)]
pub struct BundleDirectory {
    file: FileSlice,
    file_offsets: BundleStorageFileOffsets,
}

impl Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BundleDirectory")
    }
}

impl BundleDirectory {
    /// Creates a new BundleDirectory, backed by the given `FileSlice`.
    pub fn new(file: FileSlice) -> io::Result<BundleDirectory> {
        let file_offsets = BundleStorageFileOffsets::open_from_file_slice(file.clone())?;
        Ok(BundleDirectory { file, file_offsets })
    }

    /// Fetches a slice of bytes from the bundle file.
    pub fn get_slice(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
        let offsets = self.file_offsets.get(path)?;

        let data = self
            .file
            .slice(offsets.start + range.start..offsets.start + range.end)
            .read_bytes()?;
        Ok(data)
    }

    /// Fetches an entire file.
    pub fn get_all(&self, path: &Path) -> io::Result<OwnedBytes> {
        let offsets = self.file_offsets.get(path)?;

        let data = self.file.slice(offsets).read_bytes()?;
        Ok(data)
    }
}

fn unsupported_operation(path: &Path) -> io::Error {
    let msg = "Unsupported operation. BundleDirectory only supports reads";
    error!(path=?path, msg);
    io::Error::new(io::ErrorKind::Other, format!("{}: {:?}", msg, path))
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        Ok(Box::new(BundleDirectoryFileHandle {
            bundle_directory: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.get_all(path)
            .map(|bytes| bytes.as_ref().to_vec())
            .map_err(|err| OpenReadError::IoError {
                io_error: err,
                filepath: path.to_owned(),
            })
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: unsupported_operation(path),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        Ok(self.file_offsets.exists(path))
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

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Write,
    };

    use quickwit_storage::BundleStorageBuilder;

    use super::*;

    #[test]
    fn bundle_directory() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");
        let test_bundle_path = temp_dir.path().join("le_bundle");

        let mut bundle_file = File::create(test_bundle_path.to_owned())?;
        let mut bundle_builder = BundleStorageBuilder::new(&mut bundle_file)?;

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        bundle_builder.add_file(&test_filepath1)?;
        bundle_builder.add_file(&test_filepath2)?;
        bundle_builder.finalize()?;

        let buffer = fs::read(test_bundle_path)?;
        let bundle_file_slice = FileSlice::from(buffer);

        let bundle_dir = BundleDirectory::new(bundle_file_slice)?;

        let f1_data = bundle_dir.get_all(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.get_all(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }
}
