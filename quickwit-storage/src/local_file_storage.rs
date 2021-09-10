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

use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::warn;

use crate::{PutPayload, Storage, StorageErrorKind, StorageFactory, StorageResult};

/// File system compatible storage implementation.
#[derive(Clone)]
pub struct LocalFileStorage {
    root: PathBuf,
}

impl fmt::Debug for LocalFileStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalFileStorage(root={:?})", &self.root)
    }
}

impl LocalFileStorage {
    /// Creates a file storage instance given a uri
    pub fn from_uri(uri: &str) -> StorageResult<LocalFileStorage> {
        let root_pathbuf = Self::extract_root_path_from_uri(uri)?;
        Ok(Self { root: root_pathbuf })
    }

    /// Validates if the provided uri is of the correct protocol & extracts the root path.
    ///
    /// Both scheme `file:///{path}` and `file://{path}` are accepted.
    /// If uri starts with `file://`, a `/` is automatically added to ensure
    /// `path` starts from root.
    pub fn extract_root_path_from_uri(uri: &str) -> StorageResult<PathBuf> {
        if !uri.starts_with("file://") {
            let err_msg = anyhow::anyhow!(
                "{:?} is an invalid file storage uri. Only file:// is accepted.",
                uri
            );
            return Err(StorageErrorKind::DoesNotExist.with_error(err_msg));
        }

        let mut root_path = uri
            .split("://")
            .nth(1)
            .ok_or_else(|| {
                StorageErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Invalid root path: {}", uri))
            })?
            .to_string();
        if !root_path.starts_with('/') {
            root_path.insert(0, '/');
        }
        let pathbuf = PathBuf::from(root_path);
        if pathbuf
            .iter()
            .any(|segment| segment.to_string_lossy() == "..")
        {
            return Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Invalid uri, `..` is forbidden: {}", uri)));
        }
        Ok(pathbuf)
    }
}

/// Delete empty directories starting from `{root}/{path}` directory and stopping at `{root}`
/// directory. Note that the `{root}` directory is not deleted.
fn delete_all_dirs(root: PathBuf, path: &Path) -> BoxFuture<'_, std::io::Result<()>> {
    async move {
        let full_path = root.join(path);
        let path_entries_result = full_path.read_dir();
        if let Err(err) = &path_entries_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
        }

        let is_not_empty = path_entries_result?.next().is_some();
        if is_not_empty {
            return Ok(());
        }

        let delete_result = fs::remove_dir(full_path).await;
        if let Err(err) = &delete_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
            let _ = delete_result?;
        }

        match &path.parent() {
            Some(path) => {
                if path == &Path::new("") || path == &Path::new(".") {
                    return Ok(());
                }
                delete_all_dirs(root, path).await?;
            }
            _ => return Ok(()),
        }

        Ok(())
    }
    .boxed()
}

fn missing_file_is_ok(io_result: io::Result<()>) -> io::Result<()> {
    match io_result {
        Ok(()) => Ok(()),
        Err(io_err) if io_err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(io_err) => Err(io_err),
    }
}

#[async_trait]
impl Storage for LocalFileStorage {
    async fn put(&self, path: &Path, payload: PutPayload) -> crate::StorageResult<()> {
        let full_path = self.root.join(path);
        if let Some(parent_dir) = full_path.parent() {
            fs::create_dir_all(parent_dir).await?;
        }
        match payload {
            PutPayload::InMemory(data) => {
                fs::write(full_path, data).await?;
            }
            PutPayload::LocalFile(filepath) => {
                fs::copy(filepath, full_path).await?;
            }
        };
        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let full_path = self.root.join(path);
        fs::copy(full_path, output_path).await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        let full_path = self.root.join(path);
        let mut file = fs::File::open(full_path).await?;
        file.seek(SeekFrom::Start(range.start as u64)).await?;
        let mut content_bytes = vec![0u8; range.len()];
        file.read_exact(&mut content_bytes).await?;
        Ok(Bytes::from(content_bytes))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let full_path = self.root.join(path);
        missing_file_is_ok(fs::remove_file(full_path).await)?;
        let parent = path.parent();
        if parent.is_none() {
            return Ok(());
        }
        let delete_result = delete_all_dirs(self.root.to_path_buf(), parent.unwrap()).await;
        if delete_result.is_err() {
            warn!(path =% path.display(), "failed to clean the path");
        }
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        let full_path = self.root.join(path);
        let content_bytes = fs::read(full_path).await?;
        Ok(Bytes::from(content_bytes))
    }

    fn uri(&self) -> String {
        format!("file://{}", self.root.to_string_lossy())
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let full_path = self.root.join(path);
        match fs::metadata(full_path).await {
            Ok(metadata) => {
                if metadata.is_file() {
                    Ok(metadata.len())
                } else {
                    Err(StorageErrorKind::DoesNotExist
                        .with_error(anyhow::anyhow!("File {} is actually a directory")))
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    Err(StorageErrorKind::DoesNotExist.with_error(err))
                } else {
                    Err(err.into())
                }
            }
        }
    }
}

/// A File storage resolver
#[derive(Debug, Clone)]
pub struct LocalFileStorageFactory {}

impl Default for LocalFileStorageFactory {
    fn default() -> Self {
        LocalFileStorageFactory {}
    }
}

impl StorageFactory for LocalFileStorageFactory {
    fn protocol(&self) -> String {
        "file".to_string()
    }

    fn resolve(&self, uri: &str) -> StorageResult<Arc<dyn Storage>> {
        if !uri.starts_with("file://") {
            let err_msg = anyhow::anyhow!(
                "{:?} is an invalid file storage uri. Only file:// is accepted.",
                uri
            );
            return Err(StorageErrorKind::DoesNotExist.with_error(err_msg));
        }

        let storage = LocalFileStorage::from_uri(uri)?;
        Ok(Arc::new(storage))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::tests::storage_test_suite;
    use crate::StorageError;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let path_root = format!("file://{}", tempdir()?.path().to_string_lossy());
        let mut file_storage = LocalFileStorage::from_uri(&path_root)?;
        storage_test_suite(&mut file_storage).await?;
        Ok(())
    }

    #[test]
    fn test_storage_fail_if_uri_is_not_safe() {
        let storage = LocalFileStorage::from_uri("file:///tmp/../not_ok");
        assert!(storage.is_err());
        assert!(matches!(storage.unwrap_err(), StorageError { .. }));
    }

    #[test]
    fn test_storage_should_not_fail_if_dots_inside_directory_name() {
        LocalFileStorage::from_uri("file:///tmp/abc../").unwrap();
    }

    #[test]
    fn test_storage_should_automatically_start_from_root() -> anyhow::Result<()> {
        let storage = LocalFileStorage::from_uri("file://tmp/abc../")?;
        assert_eq!(storage.uri(), "file:///tmp/abc../");
        Ok(())
    }

    #[test]
    fn test_file_storage_factory() -> anyhow::Result<()> {
        let test_dir = tempfile::tempdir()?;
        let index_dir_uri = format!("file://{}/foo/bar", test_dir.path().display());
        let file_storage_factory = LocalFileStorageFactory::default();
        let storage = file_storage_factory.resolve(&index_dir_uri)?;
        assert_eq!(storage.uri().as_str(), index_dir_uri);

        let err = file_storage_factory
            .resolve("test://foo/bar")
            .err()
            .unwrap();
        assert_eq!(err.kind(), StorageErrorKind::DoesNotExist);

        let err = file_storage_factory.resolve("test://").err().unwrap();
        assert_eq!(err.kind(), StorageErrorKind::DoesNotExist);
        Ok(())
    }

    #[tokio::test]
    async fn test_try_delete_dir_all() -> anyhow::Result<()> {
        let path_root = tempdir()?.into_path();
        let dir_path = path_root.clone().join("foo/bar/baz");
        tokio::fs::create_dir_all(dir_path.clone()).await?;

        // check all empty directory
        assert_eq!(dir_path.exists(), true);
        delete_all_dirs(path_root.clone(), dir_path.as_path()).await?;
        assert_eq!(dir_path.exists(), false);
        assert_eq!(dir_path.parent().unwrap().exists(), false);

        // check with intermediate file
        tokio::fs::create_dir_all(dir_path.clone()).await?;
        let intermediate_file = dir_path.parent().unwrap().join("fizz.txt");
        tokio::fs::File::create(intermediate_file.clone()).await?;
        assert_eq!(dir_path.exists(), true);
        assert_eq!(intermediate_file.exists(), true);
        delete_all_dirs(path_root.clone(), dir_path.as_path()).await?;
        assert_eq!(dir_path.exists(), false);
        assert_eq!(dir_path.parent().unwrap().exists(), true);

        // make sure it does not go beyond the path
        tokio::fs::create_dir_all(path_root.join("home/foo/bar")).await?;
        delete_all_dirs(path_root.join("home/foo"), Path::new("bar")).await?;
        assert_eq!(path_root.join("home/foo").exists(), true);

        Ok(())
    }
}
