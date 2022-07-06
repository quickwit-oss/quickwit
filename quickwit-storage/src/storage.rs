// Copyright (C) 2022 Quickwit, Inc.
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

use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use quickwit_common::uri::Uri;

use crate::{OwnedBytes, PutPayload, StorageErrorKind, StorageResult};

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
pub trait Storage: Send + Sync + 'static {
    /// Check storage connection if applicable
    async fn check(&self) -> anyhow::Result<()>;

    /// Saves a file into the storage.
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()>;

    /// Downloads an entire file and writes it into a local file.
    /// `output_path` is expected to be a file path (not a directory path).
    /// TODO Change the API to support multipart download
    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()>;

    /// Downloads a slice of a file from the storage, and returns an in memory buffer
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes>;

    /// Downloads the entire content of a "small" file, returns an in memory buffer.
    /// For large files prefer `copy_to_file`.
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes>;

    /// Deletes a file.
    ///
    /// This method should return Ok(()) if the file did not exist.
    async fn delete(&self, path: &Path) -> StorageResult<()>;

    /// Returns whether a file exists or not.
    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        match self.file_num_bytes(path).await {
            Ok(_) => Ok(true),
            Err(storage_err) if storage_err.kind() == StorageErrorKind::DoesNotExist => Ok(false),
            Err(other_storage_err) => Err(other_storage_err),
        }
    }

    /// Returns a file size.
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64>;

    /// Returns an URI identifying the storage
    fn uri(&self) -> &Uri;
}
