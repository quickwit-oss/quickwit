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
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::StorageResult;

/// Payload argument of a put request.
#[derive(Clone)]
pub enum PutPayload {
    /// Put data from the local file.
    LocalFile(PathBuf),
    /// Put data from a local buffer
    InMemory(Arc<[u8]>),
}

impl PutPayload {
    // Returns the len of the payload expressed in number of bytes.
    pub(crate) async fn len(&self) -> io::Result<u64> {
        match self {
            Self::LocalFile(path) => {
                let metadata = tokio::fs::metadata(path).await?;
                Ok(metadata.len())
            }
            Self::InMemory(payload) => Ok(payload.len() as u64),
        }
    }
}

impl From<PathBuf> for PutPayload {
    fn from(file_path: PathBuf) -> Self {
        PutPayload::LocalFile(file_path)
    }
}

impl From<Vec<u8>> for PutPayload {
    fn from(payload_buffer: Vec<u8>) -> Self {
        PutPayload::InMemory(payload_buffer.into_boxed_slice().into())
    }
}

impl<'a> From<&'a [u8]> for PutPayload {
    fn from(payload_bytes: &[u8]) -> Self {
        From::from(payload_bytes.to_vec())
    }
}

/// Storage meant to receive and serve quickwit's split.
///
/// Object storage are the primary target implementation of this trait,
/// and its interface is meant to allow for multipart download/upload.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Saves a file into the storage.
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()>;

    /// Downloads an entire file and writes it into a local file.
    /// TODO Change the API to support multipart download
    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()>;

    /// Downloads a slice of a file from the storage, and returns an in memory buffer
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Vec<u8>>;

    /// Downloads the entire content of a "small" file, returns an in memory buffer.
    /// For large files prefer `copy_to_file`.
    async fn get_all(&self, path: &Path) -> StorageResult<Vec<u8>>;

    /// Delete file
    async fn delete(&self, path: &Path) -> StorageResult<()>;

    /// Returns whether a file exists or not.
    async fn exists(&self, path: &Path) -> StorageResult<bool>;

    /// Returns an URI identifying the storage
    fn uri(&self) -> String;
}
