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

use std::fmt;
use std::io::{self};
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tempfile::TempPath;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::error;

use crate::{BulkDeleteError, OwnedBytes, PutPayload, StorageErrorKind, StorageResult};

/// This trait is only used to make it build trait object with `AsyncWrite + Send + Unpin`.
pub trait SendableAsync: AsyncWrite + Send + Unpin {}
impl<W: AsyncWrite + Send + Unpin> SendableAsync for W {}

/// Storage meant to receive and serve quickwit's split.
///
/// Object storage are the primary target implementation of this trait,
/// and its interface is meant to allow for multipart download/upload.
///
/// Note that Storage does not have the notion of directory separators.
/// For underlying implementation where directory separator have meaning,
/// The implementation should treat directory separators as exactly the same way
/// object storage treat them. This means when directory separators a present
/// in the storage operation path, the storage implementation should create and remove transparently
/// these intermediate directories.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Storage: fmt::Debug + Send + Sync + 'static {
    /// Check storage connection if applicable
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Saves a file into the storage.
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()>;

    /// Copies the file associated to `Path` into an `AsyncWrite`.
    /// This function is required to call `.flush()` before it successfully returns.
    ///
    /// See also `copy_to_file`.
    ///
    /// async_trait Expansion of
    /// async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()>;
    ///
    /// Just putting the async form is breaking mockall.
    fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        path: &'life1 Path,
        output: &'life2 mut dyn SendableAsync,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = StorageResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait;

    /// Downloads an entire file and writes it into a local file.
    /// `output_path` is expected to be a file path (not a directory path)
    /// without any existing file yet.
    ///
    /// This function will attempt to download things to a temporary file
    /// in the same directory as the `output_path`, and then atomically move it
    /// to the actual `output_path`.
    ///
    /// In case of failure, `quickwit` (not the OS) will attempt to delete the file
    /// using some `Drop` mechanic.
    /// If quickwit is killed for instance, this may result in the temporary file not
    /// being deleted. It is important, upon started to identify these ".temp"
    /// files and delete them.
    ///
    /// See also `copy_to`.
    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<u64> {
        default_copy_to_file(self, path, output_path).await
    }

    /// Downloads a slice of a file from the storage, and returns an in memory buffer
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes>;

    /// Opens a stream handle on the file from the storage.
    ///
    /// Might panic, return an error or an empty stream if the range is empty.
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>>;

    /// Downloads the entire content of a "small" file, returns an in memory buffer.
    /// For large files prefer `copy_to_file`.
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes>;

    /// Deletes a file.
    ///
    /// This method should return Ok(()) if the file did not exist.
    async fn delete(&self, path: &Path) -> StorageResult<()>;

    /// Deletes multiple files at once.
    ///
    /// The implementation may call `[`Storage::delete`] in a loop if the underlying storage does
    /// not support deleting objects in bulk. The request can fail partially, i.e. some objects are
    /// successfully deleted while others are not.
    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError>;

    /// Returns whether a file exists or not.
    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        match self.file_num_bytes(path).await {
            Ok(_) => Ok(true),
            Err(storage_err) if storage_err.kind() == StorageErrorKind::NotFound => Ok(false),
            Err(other_storage_err) => Err(other_storage_err),
        }
    }

    /// Returns a file size.
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64>;

    /// Returns an URI identifying the storage
    fn uri(&self) -> &Uri;
}

async fn default_copy_to_file<S: Storage + ?Sized>(
    storage: &S,
    path: &Path,
    output_path: &Path,
) -> StorageResult<u64> {
    let mut download_temp_file =
        DownloadTempFile::with_target_path(output_path.to_path_buf()).await?;
    storage.copy_to(path, download_temp_file.as_mut()).await?;
    let num_bytes = download_temp_file.persist().await?;
    Ok(num_bytes)
}

struct DownloadTempFile {
    target_filepath: PathBuf,
    temp_filepath: PathBuf,
    file: File,
    has_attempted_deletion: bool,
}

impl DownloadTempFile {
    /// Creates or truncate temp file.
    pub async fn with_target_path(target_filepath: PathBuf) -> io::Result<DownloadTempFile> {
        let Some(filename) = target_filepath.file_name() else {
            return Err(io::Error::other(
                "Target filepath is not a directory path. Expected a filepath.",
            ));
        };
        let filename: &str = filename
            .to_str()
            .ok_or_else(|| io::Error::other("target filepath is not a valid UTF-8 string"))?;
        let mut temp_filepath = target_filepath.clone();
        temp_filepath.set_file_name(format!("{filename}.temp"));
        let file = tokio::fs::File::create(temp_filepath.clone()).await?;
        Ok(DownloadTempFile {
            target_filepath,
            temp_filepath,
            file,
            has_attempted_deletion: false,
        })
    }

    pub async fn persist(mut self) -> io::Result<u64> {
        TempPath::from_path(&self.temp_filepath).persist(&self.target_filepath)?;
        self.has_attempted_deletion = true;
        let num_bytes = std::fs::metadata(&self.target_filepath)?.len();
        Ok(num_bytes)
    }
}

impl Drop for DownloadTempFile {
    fn drop(&mut self) {
        if self.has_attempted_deletion {
            return;
        }
        let temp_filepath = self.temp_filepath.clone();
        self.has_attempted_deletion = true;
        tokio::task::spawn_blocking(move || {
            if let Err(io_error) = std::fs::remove_file(&temp_filepath) {
                error!(temp_filepath=%temp_filepath.display(), io_error=?io_error, "Failed to remove temporary file");
            }
        });
    }
}

impl AsMut<File> for DownloadTempFile {
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{RamStorage, StorageError};

    const CONTENT: &[u8] = b"hello world";

    #[tokio::test]
    async fn test_copy_to_file() {
        let ram_storage = RamStorage::default();
        let temp_dir = tempfile::tempdir().unwrap();
        let dest_filepath = temp_dir.path().join("bar");
        let path = Path::new("foo/bar");
        ram_storage
            .put(path, Box::new(CONTENT.to_owned()))
            .await
            .unwrap();
        let num_bytes = ram_storage
            .copy_to_file(path, &dest_filepath)
            .await
            .unwrap();
        assert_eq!(num_bytes, 11);
        let content = std::fs::read(&dest_filepath).unwrap();
        assert_eq!(&content, CONTENT);
    }

    #[tokio::test]
    async fn test_copy_to_file_deletes_tempfile_on_failure() {
        let mut storage = MockStorage::default();
        storage.expect_copy_to().return_once(|_, _| {
            Box::pin(futures::future::err(StorageError::from(io::Error::other(
                "fake storage error",
            ))))
        });
        let path = Path::new("foo/bar");
        let temp_dir = tempfile::tempdir().unwrap();
        let dest_filepath = temp_dir.path().join("bar");
        default_copy_to_file(&storage, path, &dest_filepath)
            .await
            .unwrap_err();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut read_dir = tokio::fs::read_dir(dest_filepath.parent().unwrap())
            .await
            .unwrap();
        let entry_opt = read_dir
            .next_entry()
            .await
            .unwrap()
            .map(|dir_entry| dir_entry.path());
        assert_eq!(entry_opt, None);
    }
}
