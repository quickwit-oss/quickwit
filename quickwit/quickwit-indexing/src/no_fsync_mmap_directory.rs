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

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::Directory;
use tantivy::directory::error::{
    DeleteError, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
};
use tantivy::directory::{
    Advice, AntiCallToken, DirectoryLock, FileHandle, Lock, MmapDirectory, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
};

/// Whether split scratch files (indexing and merge outputs) are fsynced.
///
/// Scratch files are discarded on restart, so fsync is skipped unless
/// `QW_ENABLE_SPLIT_SCRATCH_FSYNC` is set.
pub(crate) fn is_split_scratch_fsync_enabled() -> bool {
    quickwit_common::get_bool_from_env_cached!("QW_ENABLE_SPLIT_SCRATCH_FSYNC", false)
}

/// Opens the directory holding split scratch files, honoring
/// [`is_split_scratch_fsync_enabled`].
pub(crate) fn open_split_scratch_directory(
    directory_path: &Path,
    advice: Advice,
) -> Result<Box<dyn Directory>, OpenDirectoryError> {
    if is_split_scratch_fsync_enabled() {
        let mmap_directory = MmapDirectory::open_with_madvice(directory_path, advice)?;
        return Ok(Box::new(mmap_directory));
    }
    let no_fsync_mmap_directory = NoFsyncMmapDirectory::open_with_madvice(directory_path, advice)?;
    Ok(Box::new(no_fsync_mmap_directory))
}

/// An [`MmapDirectory`] that never calls fsync: written files and directory entries are not
/// durable across OS crashes or power losses.
///
/// Reads, locks, and watches are delegated to the underlying [`MmapDirectory`], while writes go
/// through regular files that are never synced.
#[derive(Clone, Debug)]
pub(crate) struct NoFsyncMmapDirectory {
    root_path: PathBuf,
    mmap_directory: MmapDirectory,
}

impl NoFsyncMmapDirectory {
    pub fn open_with_madvice(
        directory_path: &Path,
        advice: Advice,
    ) -> Result<Self, OpenDirectoryError> {
        let mmap_directory = MmapDirectory::open_with_madvice(directory_path, advice)?;
        // `MmapDirectory::open_with_madvice` has already checked that the path exists and is a
        // directory.
        let root_path = directory_path.canonicalize().map_err(|io_error| {
            OpenDirectoryError::wrap_io_error(io_error, directory_path.into())
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

impl Directory for NoFsyncMmapDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.mmap_directory.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
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
        Ok(BufWriter::new(Box::new(NoFsyncFileWriter(file))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.mmap_directory.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        // The temp file must live on the same filesystem as the target for the rename to be
        // atomic.
        let parent_path = full_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path `{}` has no parent directory", full_path.display()),
            )
        })?;
        let mut temp_file = tempfile::Builder::new().tempfile_in(parent_path)?;
        temp_file.write_all(data)?;
        temp_file.persist(&full_path).map_err(|error| error.error)?;
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

struct NoFsyncFileWriter(File);

impl Write for NoFsyncFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TerminatingWrite for NoFsyncFileWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.0.flush()
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{STORED, Schema};
    use tantivy::{IndexBuilder, TantivyDocument, doc};

    use super::*;

    #[test]
    fn test_no_fsync_mmap_directory_write_and_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let directory =
            NoFsyncMmapDirectory::open_with_madvice(temp_dir.path(), Advice::Normal).unwrap();
        let path = Path::new("file");

        let mut writer = directory.open_write(path).unwrap();
        writer.write_all(b"hello").unwrap();
        writer.terminate().unwrap();

        let file_slice = directory.open_read(path).unwrap();
        assert_eq!(file_slice.read_bytes().unwrap().as_slice(), b"hello");
        assert!(directory.exists(path).unwrap());

        let result = directory.open_write(path);
        assert!(matches!(result, Err(OpenWriteError::FileAlreadyExists(_))));

        directory.delete(path).unwrap();
        assert!(!directory.exists(path).unwrap());
    }

    #[test]
    fn test_no_fsync_mmap_directory_atomic_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let directory =
            NoFsyncMmapDirectory::open_with_madvice(temp_dir.path(), Advice::Normal).unwrap();
        let path = Path::new("meta.json");

        directory.atomic_write(path, b"foo").unwrap();
        assert_eq!(directory.atomic_read(path).unwrap(), b"foo");

        directory.atomic_write(path, b"bar").unwrap();
        assert_eq!(directory.atomic_read(path).unwrap(), b"bar");

        let num_entries = std::fs::read_dir(temp_dir.path()).unwrap().count();
        assert_eq!(num_entries, 1);
    }

    #[test]
    fn test_no_fsync_mmap_directory_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let directory =
            NoFsyncMmapDirectory::open_with_madvice(temp_dir.path(), Advice::Normal).unwrap();
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", STORED);
        let index = IndexBuilder::new()
            .schema(schema_builder.build())
            .open_or_create(directory)
            .unwrap();
        let mut index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        index_writer.add_document(doc!(id_field => 42u64)).unwrap();
        index_writer.commit().unwrap();

        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.num_docs(), 1);
        let doc: TantivyDocument = searcher.doc(tantivy::DocAddress::new(0, 0)).unwrap();
        assert_eq!(doc.len(), 1);
    }
}
