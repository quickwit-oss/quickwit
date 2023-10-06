// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use quickwit_common::ignore_error_kind;
use quickwit_common::uri::Uri;
use quickwit_config::StorageBackend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::warn;

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, DebouncedStorage, DeleteFailure, OwnedBytes, Storage, StorageError,
    StorageErrorKind, StorageFactory, StorageResolverError, StorageResult,
};

/// File system compatible storage implementation.
#[derive(Clone)]
pub struct LocalFileStorage {
    uri: Uri,
    root: PathBuf,
}

impl fmt::Debug for LocalFileStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("LocalFileStorage")
            .field("root", &self.root.display())
            .finish()
    }
}

impl LocalFileStorage {
    fn full_path(&self, relative_path: &Path) -> crate::StorageResult<PathBuf> {
        ensure_valid_relative_path(relative_path)?;
        Ok(self.root.join(relative_path))
    }

    /// Creates a local file storage instance given a URI.
    pub fn from_uri(uri: &Uri) -> Result<Self, StorageResolverError> {
        uri.filepath()
            .map(|root| Self {
                uri: uri.clone(),
                root: root.to_path_buf(),
            })
            .ok_or_else(|| {
                let message = format!("URI `{uri}` is not a valid file URI");
                StorageResolverError::InvalidUri(message)
            })
    }

    /// Moves a file from a source to a destination.
    /// from here is an external path, and to is an internal path.
    pub async fn move_into(&self, from_external: &Path, to: &Path) -> crate::StorageResult<()> {
        let to_full_path = self.full_path(to)?;
        tokio::fs::rename(from_external, to_full_path).await?;
        Ok(())
    }

    /// Moves a file from a source to a destination.
    /// from here is an internal path, and to is an external path.
    pub async fn move_out(&self, from_internal: &Path, to: &Path) -> crate::StorageResult<()> {
        let from_full_path = self.full_path(from_internal)?;
        tokio::fs::rename(from_full_path, to).await?;
        Ok(())
    }

    async fn delete_single_file(&self, relative_path: &Path) -> StorageResult<()> {
        let full_path = self.full_path(relative_path)?;
        ignore_error_kind!(ErrorKind::NotFound, tokio::fs::remove_file(full_path).await)?;
        Ok(())
    }
}

/// Ensure that the path given does not include any ".." for security reasons.
///
/// In order to reduce the attack surface, we want to make sure the `FileStorage`
/// only access/delete files that are children of its root_directory.
fn ensure_valid_relative_path(path: &Path) -> StorageResult<()> {
    for component in path.components() {
        match component {
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                // We forbid `Path` components that are breaking the assumption that
                // root.join(path) is a child of root (if we omit fs links).
                return Err(StorageErrorKind::Unauthorized.with_error(anyhow::anyhow!(
                    "path `{}` is forbidden. only simple relative path are allowed",
                    path.display()
                )));
            }
            Component::CurDir | Component::Normal(_) => {
                // we accept `./` and subdir/
            }
        }
    }
    Ok(())
}

/// Delete empty directories starting from `{root}/{path}` directory and stopping at `{root}`
/// directory. Note that the `{root}` directory is not deleted.
fn delete_all_dirs_if_empty<'a>(
    root: &'a Path,
    path: &'a Path,
) -> BoxFuture<'a, std::io::Result<()>> {
    async move {
        let full_path = root.join(path);
        let path_entries_result = full_path.read_dir();
        if let Err(err) = &path_entries_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurrent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
        }

        let is_not_empty = path_entries_result?.next().is_some();
        if is_not_empty {
            return Ok(());
        }

        let delete_result = tokio::fs::remove_dir(full_path).await;
        if let Err(err) = &delete_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurrent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
            delete_result?;
        }

        match &path.parent() {
            Some(path) => {
                if path == &Path::new("") || path == &Path::new(".") {
                    return Ok(());
                }
                delete_all_dirs_if_empty(root, path).await?;
            }
            _ => return Ok(()),
        }

        Ok(())
    }
    .boxed()
}

#[async_trait]
impl Storage for LocalFileStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if !self.root.try_exists()? {
            // By creating directories, we check if we have the right permissions.
            tokio::fs::create_dir_all(&self.root).await?
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        let full_path = self.full_path(path)?;
        let parent_dir = full_path.parent().ok_or_else(|| {
            let err = anyhow::anyhow!("no parent directory for {full_path:?}");
            StorageErrorKind::Internal.with_error(err)
        })?;

        tokio::fs::create_dir_all(parent_dir).await?;
        let mut reader = payload.byte_stream().await?.into_async_read();
        let named_temp_file = tempfile::NamedTempFile::new_in(parent_dir)?;
        let (temp_std_file, temp_filepath) = named_temp_file.into_parts();
        let mut temp_tokio_file = tokio::fs::File::from_std(temp_std_file);
        tokio::io::copy(&mut reader, &mut temp_tokio_file).await?;
        temp_tokio_file.flush().await?;
        temp_tokio_file.sync_data().await?;
        temp_filepath
            .persist(&full_path)
            .map_err(|err| StorageErrorKind::Io.with_error(err))?;
        // We also need to sync the parent directory to ensure it
        // the file move has been persisted on all file systems.
        tokio::fs::File::open(parent_dir).await?.sync_data().await?;
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let full_path = self.full_path(path)?;
        let mut file = tokio::fs::File::open(&full_path).await?;
        tokio::io::copy(&mut file, output).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let full_path = self.full_path(path)?;
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek};

            // we run these io in a spawn_blocking so there is no scheduling delay between each
            // step, as there would be if using tokio async File.
            let mut file = std::fs::File::open(full_path)?;
            file.seek(SeekFrom::Start(range.start as u64))?;
            let mut content_bytes: Vec<u8> = vec![0u8; range.len()];
            file.read_exact(&mut content_bytes)?;
            Ok(OwnedBytes::new(content_bytes))
        })
        .await
        .map_err(|_| {
            StorageErrorKind::Internal.with_error(anyhow::anyhow!("reading file panicked"))
        })?
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let full_path = self.full_path(path)?;
        let mut file = tokio::fs::File::open(&full_path).await?;
        file.seek(SeekFrom::Start(range.start as u64)).await?;
        Ok(Box::new(file.take(range.len() as u64)))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.delete_single_file(path).await?;
        if let Some(parent) = path.parent() {
            if let Err(error) = delete_all_dirs_if_empty(&self.root, parent).await {
                warn!(error=?error, path=%path.display(), "Failed to delete directory.");
            }
        }
        Ok(())
    }

    /// Deletes the files identified by `paths` concurrently, with a maximum of `10` syscalls at a
    /// time. Additionally, deletes the parent directories of `paths` if they are empty after the
    /// first round of deletions.
    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let mut successes = Vec::with_capacity(paths.len());
        let mut failures = HashMap::new();
        let mut parent_paths = BTreeSet::new();

        let remove_file_res_futures: Vec<_> = paths
            .iter()
            .map(|path| async move {
                let remove_file_res = self.delete_single_file(path).await;
                (path, remove_file_res)
            })
            .collect();

        let mut stream = futures::stream::iter(remove_file_res_futures).buffer_unordered(10);

        while let Some((path, remove_file_res)) = stream.next().await {
            match remove_file_res {
                Ok(_) => {
                    successes.push(path.to_path_buf());

                    if let Some(parent) = path.parent() {
                        parent_paths.insert(parent);
                    }
                }
                Err(error) => {
                    let failure = DeleteFailure {
                        error: Some(error),
                        ..Default::default()
                    };
                    failures.insert(path.to_path_buf(), failure);
                }
            }
        }
        // Delete parent directories of `paths` if they are empty.
        // Traverse the parent directories in reverse order, so that we delete the deepest ones
        // first.
        for parent_path in parent_paths.into_iter().rev() {
            if let Err(error) = delete_all_dirs_if_empty(&self.root, parent_path).await {
                warn!(error=?error, path=%parent_path.display(), "Failed to delete directory.");
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(BulkDeleteError {
            successes,
            failures,
            ..Default::default()
        })
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let full_path = self.full_path(path)?;
        let content_bytes = tokio::fs::read(full_path).await.map_err(|err| {
            StorageError::from(err).add_context(format!(
                "failed to read file {}/{}",
                self.uri(),
                path.to_string_lossy()
            ))
        })?;
        Ok(OwnedBytes::new(content_bytes))
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let full_path = self.full_path(path)?;
        match tokio::fs::metadata(full_path).await {
            Ok(metadata) => {
                if metadata.is_file() {
                    Ok(metadata.len())
                } else {
                    Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!(
                        "file `{}` is actually a directory",
                        path.display()
                    )))
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    Err(StorageErrorKind::NotFound.with_error(err))
                } else {
                    Err(err.into())
                }
            }
        }
    }
}

/// A File storage resolver
#[derive(Clone, Debug, Default)]
pub struct LocalFileStorageFactory;

#[async_trait]
impl StorageFactory for LocalFileStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::File
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = LocalFileStorage::from_uri(uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use super::*;
    use crate::test_suite::storage_test_suite;

    #[tokio::test]
    async fn test_local_file_storage() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let uri = Uri::from_str(&format!("{}", temp_dir.path().display())).unwrap();
        let mut local_file_storage = LocalFileStorage::from_uri(&uri)?;
        storage_test_suite(&mut local_file_storage).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_local_file_storage_forbids_double_dot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = Uri::from_str(&format!("{}", temp_dir.path().display())).unwrap();
        let local_file_storage = LocalFileStorage::from_uri(&uri).unwrap();
        assert_eq!(
            local_file_storage
                .exists(Path::new("hello/toto"))
                .await
                .unwrap(),
            false
        );
        let exist_error = local_file_storage
            .exists(Path::new("hello/../toto"))
            .await
            .unwrap_err();
        assert_eq!(exist_error.kind(), StorageErrorKind::Unauthorized);
    }

    #[tokio::test]
    async fn test_local_file_storage_factory() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let index_uri =
            Uri::from_well_formed(format!("file://{}/foo/bar", temp_dir.path().display()));
        let local_file_storage_factory = LocalFileStorageFactory;
        let local_file_storage = local_file_storage_factory.resolve(&index_uri).await?;
        assert_eq!(local_file_storage.uri(), &index_uri);

        let err = local_file_storage_factory
            .resolve(&Uri::from_well_formed("s3://foo/bar"))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let err = local_file_storage_factory
            .resolve(&Uri::from_well_formed("s3://"))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_local_file_storage_bulk_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(temp_dir.path().join("foo-dir"))
            .await
            .unwrap();
        tokio::fs::create_dir(temp_dir.path().join("bar-dir"))
            .await
            .unwrap();
        tokio::fs::File::create(temp_dir.path().join("foo-dir/foo"))
            .await
            .unwrap();

        let uri = Uri::from_str(&format!("{}", temp_dir.path().display())).unwrap();
        let local_file_storage = LocalFileStorage::from_uri(&uri).unwrap();
        let error = local_file_storage
            .bulk_delete(&[Path::new("foo-dir/foo"), Path::new("bar-dir")])
            .await
            .unwrap_err();
        assert_eq!(error.successes, [PathBuf::from("foo-dir/foo")]);

        let failure = error.failures.get(Path::new("bar-dir")).unwrap();
        assert_eq!(failure.error.as_ref().unwrap().kind(), StorageErrorKind::Io);

        assert!(!temp_dir.path().join("foo-dir").try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_try_delete_dir_all() -> anyhow::Result<()> {
        let path_root = tempfile::tempdir()?.into_path();
        let dir_path = path_root.clone().join("foo/bar/baz");
        tokio::fs::create_dir_all(dir_path.clone()).await?;

        // check all empty directory
        assert_eq!(dir_path.try_exists().unwrap(), true);
        delete_all_dirs_if_empty(&path_root, dir_path.as_path()).await?;
        assert_eq!(dir_path.try_exists().unwrap(), false);
        assert_eq!(dir_path.parent().unwrap().try_exists().unwrap(), false);

        // check with intermediate file
        tokio::fs::create_dir_all(dir_path.clone()).await?;
        let intermediate_file = dir_path.parent().unwrap().join("fizz.txt");
        tokio::fs::File::create(intermediate_file.clone()).await?;
        assert_eq!(dir_path.try_exists().unwrap(), true);
        assert_eq!(intermediate_file.try_exists().unwrap(), true);
        delete_all_dirs_if_empty(&path_root, dir_path.as_path()).await?;
        assert_eq!(dir_path.try_exists().unwrap(), false);
        assert_eq!(dir_path.parent().unwrap().try_exists().unwrap(), true);

        // make sure it does not go beyond the path
        tokio::fs::create_dir_all(path_root.join("home/foo/bar")).await?;
        delete_all_dirs_if_empty(&path_root.join("home/foo"), Path::new("bar")).await?;
        assert_eq!(path_root.join("home/foo").try_exists().unwrap(), true);

        Ok(())
    }
}
