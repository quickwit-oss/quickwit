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

use std::io::{self, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rusoto_core::ByteStream;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::{OwnedBytes, StorageErrorKind, StorageResult};

#[async_trait]
/// PutPayloadProvider is used to upload data and support multipart.
pub trait PutPayloadProvider: PutPayloadProviderClone + Send + Sync {
    /// Return the total length of the payload.
    async fn len(&self) -> io::Result<u64>;
    /// Retrieve bytestream for specified range.
    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream>;

    /// Retrieve complete bytestream.
    async fn byte_stream(&self) -> io::Result<ByteStream> {
        let total_len = self.len().await?;
        let range = 0..total_len;
        self.range_byte_stream(range).await
    }

    /// Load the whole Payload into memory.
    async fn read_all(&self) -> io::Result<OwnedBytes> {
        let total_len = self.len().await?;
        let range = 0..total_len;
        let mut reader = self.range_byte_stream(range).await?.into_async_read();

        let mut data: Vec<u8> = Vec::with_capacity(total_len as usize);
        tokio::io::copy(&mut reader, &mut data).await?;

        Ok(OwnedBytes::new(data))
    }
}

pub trait PutPayloadProviderClone {
    fn box_clone(&self) -> Box<dyn PutPayloadProvider>;
}

impl<T> PutPayloadProviderClone for T
where T: 'static + PutPayloadProvider + Clone
{
    fn box_clone(&self) -> Box<dyn PutPayloadProvider> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn PutPayloadProvider> {
    fn clone(&self) -> Box<dyn PutPayloadProvider> {
        self.box_clone()
    }
}

/// Payload argument of a put request.
#[derive(Clone)]
pub enum PutPayload {
    /// Put data from the local file.
    LocalFile(PathBuf),
    /// Put data from a local buffer
    InMemory(OwnedBytes),
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

#[async_trait]
impl PutPayloadProvider for PutPayload {
    async fn len(&self) -> io::Result<u64> {
        self.len().await
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
    match self {
        PutPayload::LocalFile(filepath) => {
            let mut file: tokio::fs::File = tokio::fs::File::open(&filepath).await?;
            file.seek(SeekFrom::Start(range.start)).await?;
            let reader_stream = ReaderStream::new(file.take(range.end - range.start));
            Ok(ByteStream::new(reader_stream))
        }
        PutPayload::InMemory(data) => Ok(ByteStream::from(
            (&data[range.start as usize..range.end as usize]).to_vec(),
        )),
    }
    }
}

impl From<PathBuf> for PutPayload {
    fn from(file_path: PathBuf) -> Self {
        PutPayload::LocalFile(file_path)
    }
}

impl From<OwnedBytes> for PutPayload {
    fn from(bytes: OwnedBytes) -> Self {
        PutPayload::InMemory(bytes)
    }
}

impl From<Vec<u8>> for PutPayload {
    fn from(bytes: Vec<u8>) -> Self {
        PutPayload::InMemory(OwnedBytes::new(bytes))
    }
}

impl From<&'static [u8]> for PutPayload {
    fn from(payload_bytes: &'static [u8]) -> Self {
        From::from(OwnedBytes::new(payload_bytes))
    }
}

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
    /// Saves a file into the storage.
    async fn put(&self, path: &Path, payload: Box<dyn PutPayloadProvider>) -> StorageResult<()>;

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
    fn uri(&self) -> String;
}
