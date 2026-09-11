// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, result};

use tantivy::Directory;
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::{
    AntiCallToken, DirectoryLock, FileHandle, Lock, MmapDirectory, TerminatingWrite, WatchCallback,
    WatchHandle, WritePtr,
};

/// A filesystem-backed Tantivy directory that does not synchronize its writes to disk.
///
/// Files can be read through mmap, but their contents are not guaranteed to survive a crash. This
/// directory is intended for indexes whose durable copy is uploaded to remote storage before they
/// are published.
#[derive(Clone)]
pub struct UnsyncedMmapDirectory {
    root_path: PathBuf,
    mmap_directory: MmapDirectory,
}

impl UnsyncedMmapDirectory {
    /// Opens an existing directory.
    pub fn open(directory_path: impl AsRef<Path>) -> Result<Self, OpenDirectoryError> {
        let directory_path = directory_path.as_ref();
        let mmap_directory = MmapDirectory::open(directory_path)?;
        let root_path = directory_path.canonicalize().map_err(|io_error| {
            OpenDirectoryError::wrap_io_error(io_error, directory_path.to_path_buf())
        })?;
        Ok(Self {
            root_path,
            mmap_directory,
        })
    }

    fn resolve_path(&self, relative_path: &Path) -> PathBuf {
        self.root_path.join(relative_path)
    }
}

impl fmt::Debug for UnsyncedMmapDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("UnsyncedMmapDirectory")
            .field(&self.root_path)
            .finish()
    }
}

struct UnsyncedFileWriter(File);

impl Write for UnsyncedFileWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for UnsyncedFileWriter {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

impl Directory for UnsyncedMmapDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.mmap_directory.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.mmap_directory.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.mmap_directory.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.resolve_path(path))
            .map_err(|io_error| {
                if io_error.kind() == io::ErrorKind::AlreadyExists {
                    OpenWriteError::FileAlreadyExists(path.to_path_buf())
                } else {
                    OpenWriteError::wrap_io_error(io_error, path.to_path_buf())
                }
            })?;
        Ok(BufWriter::new(Box::new(UnsyncedFileWriter(file))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.mmap_directory.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let parent_path = full_path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory")
        })?;
        let mut temp_file = tempfile::Builder::new().tempfile_in(parent_path)?;
        temp_file.write_all(data)?;
        temp_file
            .into_temp_path()
            .persist(full_path)
            .map_err(|persist_error| persist_error.error)?;
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.mmap_directory.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.mmap_directory.watch(watch_callback)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use tantivy::Directory;
    use tantivy::directory::TerminatingWrite;

    use super::UnsyncedMmapDirectory;

    #[test]
    fn test_unsynced_mmap_directory() -> anyhow::Result<()> {
        let temp_directory = tempfile::tempdir()?;
        let directory = UnsyncedMmapDirectory::open(temp_directory.path())?;

        let file_path = Path::new("file");
        let mut writer = directory.open_write(file_path)?;
        writer.write_all(b"file contents")?;
        writer.terminate()?;
        assert_eq!(
            directory.open_read(file_path)?.read_bytes()?.as_slice(),
            b"file contents"
        );

        let atomic_path = Path::new("atomic");
        directory.atomic_write(atomic_path, b"first")?;
        directory.atomic_write(atomic_path, b"second")?;
        assert_eq!(directory.atomic_read(atomic_path)?, b"second");
        directory.sync_directory()?;
        Ok(())
    }
}
