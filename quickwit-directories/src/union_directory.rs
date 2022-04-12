// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchHandle};
use tantivy::Directory;

/// A union directory takes a bunch of directories and stacks them, similarly to UnionFS.
/// The resulting directory is a virtual view of the union of the different directories.
///
/// If a path exists in all directories, the first of the list containing the path
/// will shadow the other directories.
///
/// The first directory of the list will receive all write operations.
/// Deletes on the other hand will be applied on all directories containing the file.
#[derive(Debug, Clone)]
pub struct UnionDirectory {
    directories: Arc<Vec<Box<dyn Directory>>>,
}

impl UnionDirectory {
    /// Creates a new union directory.
    pub fn union_of(directories: Vec<Box<dyn Directory>>) -> UnionDirectory {
        UnionDirectory {
            directories: Arc::new(directories),
        }
    }

    /// Helper function to find the first directory containing the given path.
    fn find_directory_for_path(&self, path: &Path) -> Result<&dyn Directory, OpenReadError> {
        for directory in self.directories.iter() {
            if directory.exists(path)? {
                return Ok(directory.as_ref());
            }
        }
        Err(OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

fn convert_open_to_delete_error(open_err: OpenReadError) -> DeleteError {
    match open_err {
        OpenReadError::FileDoesNotExist(path) => DeleteError::FileDoesNotExist(path),
        OpenReadError::IoError { io_error, filepath } => {
            DeleteError::IoError { io_error, filepath }
        }
        err @ OpenReadError::IncompatibleIndex(_) => DeleteError::IoError {
            io_error: io::Error::new(io::ErrorKind::Unsupported, err),
            filepath: PathBuf::from("/"),
        },
    }
}

impl Directory for UnionDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        let directory = self.find_directory_for_path(path)?;
        directory.get_file_handle(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        match self.find_directory_for_path(path) {
            Ok(_) => Ok(true),
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let directory = self.find_directory_for_path(path)?;
        directory.atomic_read(path)
    }

    fn open_write(&self, path: &Path) -> Result<tantivy::directory::WritePtr, OpenWriteError> {
        self.directories[0].open_write(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let mut found_file = false;
        for directory in self.directories.iter() {
            // We first check exist, in order to support read-only directories.
            match directory.exists(path) {
                Ok(true) => {
                    directory.delete(path)?;
                    found_file = true;
                }
                Ok(false) => {}
                Err(exist_err) => {
                    return Err(convert_open_to_delete_error(exist_err));
                }
            }
        }
        if !found_file {
            return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
        }
        Ok(())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.directories[0].atomic_write(path, data)
    }

    fn watch(&self, callback: tantivy::directory::WatchCallback) -> tantivy::Result<WatchHandle> {
        self.directories[0].watch(callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.directories[0].sync_directory()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tantivy::directory::{Directory, RamDirectory};

    use crate::UnionDirectory;

    #[test]
    fn test_union_directory_atomic_simple() -> anyhow::Result<()> {
        let dir1 = RamDirectory::create();
        let dir2 = RamDirectory::create();
        dir1.atomic_write(Path::new("path1"), &b"data1"[..])?;
        dir2.atomic_write(Path::new("path2"), &b"data2"[..])?;
        let union_directory = UnionDirectory::union_of(vec![Box::new(dir1), Box::new(dir2)]);
        {
            let payload_1 = union_directory.atomic_read(Path::new("path1"))?;
            assert_eq!(payload_1, b"data1");
        }
        {
            let payload_1 = union_directory
                .open_read(Path::new("path1"))?
                .read_bytes()?;
            assert_eq!(payload_1.as_slice(), b"data1");
        }
        {
            let payload_2 = union_directory.atomic_read(Path::new("path2"))?;
            assert_eq!(payload_2, b"data2");
        }
        {
            let payload_2 = union_directory
                .open_read(Path::new("path2"))?
                .read_bytes()?;
            assert_eq!(payload_2.as_slice(), b"data2");
        }
        Ok(())
    }

    #[test]
    fn test_union_directory_shadowing() -> anyhow::Result<()> {
        let dir1 = RamDirectory::create();
        let dir2 = RamDirectory::create();
        dir1.atomic_write(Path::new("shadowed_path"), &b"shadower"[..])?;
        dir2.atomic_write(Path::new("shadowed_path"), &b"shadowee"[..])?;
        let union_directory = UnionDirectory::union_of(vec![Box::new(dir1), Box::new(dir2)]);
        let payload = union_directory.atomic_read(Path::new("shadowed_path"))?;
        assert_eq!(payload, b"shadower");
        Ok(())
    }

    #[test]
    fn test_union_directory_exists() -> anyhow::Result<()> {
        let dir1 = RamDirectory::create();
        dir1.atomic_write(Path::new("path1"), &b"data1"[..])?;
        dir1.atomic_write(Path::new("shadowed_path"), &b"shadower"[..])?;

        let dir2 = RamDirectory::create();
        dir2.atomic_write(Path::new("path2"), &b"data2"[..])?;
        dir2.atomic_write(Path::new("shadowed_path"), &b"shadowee"[..])?;

        let union_directory = UnionDirectory::union_of(vec![Box::new(dir1), Box::new(dir2)]);
        assert!(union_directory.exists(Path::new("path1"))?);
        assert!(union_directory.exists(Path::new("path2"))?);
        assert!(union_directory.exists(Path::new("shadowed_path"))?);

        assert!(!union_directory.exists(Path::new("path3"))?);
        Ok(())
    }

    #[test]
    fn test_union_directory_delete() -> anyhow::Result<()> {
        let dir1 = RamDirectory::create();
        dir1.atomic_write(Path::new("path1"), &b"data1"[..])?;
        dir1.atomic_write(Path::new("shadowed_path"), &b"shadower"[..])?;

        let dir2 = RamDirectory::create();
        dir2.atomic_write(Path::new("path2"), &b"data2"[..])?;
        dir2.atomic_write(Path::new("shadowed_path"), &b"shadowee"[..])?;

        let union_directory = UnionDirectory::union_of(vec![Box::new(dir1), Box::new(dir2)]);

        union_directory.delete(Path::new("path1"))?;
        assert!(!union_directory.exists(Path::new("path1"))?);

        union_directory.delete(Path::new("path2"))?;
        assert!(!union_directory.exists(Path::new("path2"))?);

        union_directory.delete(Path::new("shadowed_path"))?;
        assert!(!union_directory.exists(Path::new("shadowed_path"))?);

        union_directory.delete(Path::new("path3")).unwrap_err();
        Ok(())
    }
    #[test]
    fn test_union_directory_write() -> anyhow::Result<()> {
        let dir1 = RamDirectory::create();
        dir1.atomic_write(Path::new("path1"), &b"data1"[..])?;

        let dir2 = RamDirectory::create();
        dir2.atomic_write(Path::new("path2"), &b"data2"[..])?;

        let union_directory = UnionDirectory::union_of(vec![Box::new(dir1), Box::new(dir2)]);
        union_directory.atomic_write(Path::new("path1"), &b"data1 data1"[..])?;
        union_directory.atomic_write(Path::new("path3"), &b"data3"[..])?;
        {
            let payload = union_directory.atomic_read(Path::new("path1"))?;
            assert_eq!(payload, b"data1 data1");
        }
        {
            let payload = union_directory.atomic_read(Path::new("path3"))?;
            assert_eq!(payload, b"data3");
        }
        Ok(())
    }
}
