// Copyright (C) 2024 Quickwit, Inc.
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

use std::fmt;
use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use bytesize::ByteSize;
use opendal::Operator;
use quickwit_common::uri::Uri;
use tokio::io::{AsyncRead, AsyncWriteExt};

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind,
    StorageResolverError, StorageResult,
};

/// OpenDAL based storage implementation.
/// # TODO
///
/// - Implement REQUEST_SEMAPHORE to control the concurrency.
/// - Implement STORAGE_METRICS for metrics.
/// - Add multipart_policy to control write at once or via multiple.
#[derive(Clone)]
pub struct OpendalStorage {
    uri: Uri,
    op: Operator,
}

impl fmt::Debug for OpendalStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("OpendalStorage")
            .field("operator", &self.op.info())
            .finish()
    }
}

impl OpendalStorage {
    /// Create a new google cloud storage.
    pub fn new_google_cloud_storage(
        uri: Uri,
        cfg: opendal::services::Gcs,
    ) -> Result<Self, StorageResolverError> {
        let op = Operator::new(cfg)?.finish();

        Ok(Self { uri, op })
    }
}

#[async_trait]
impl Storage for OpendalStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.op.check().await?;

        Ok(())
    }

    /// # TODO
    ///
    /// We can implement something like `multipart_policy` determine whether to use copy.
    /// If the payload is small enough, we can call `op.write()` at once.
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        let mut r = payload.byte_stream().await?.into_async_read();

        let mut w = self
            .op
            .writer_with(&path)
            .buffer(ByteSize::mb(8).as_u64() as usize)
            .await?;
        tokio::io::copy(&mut r, &mut w).await?;
        w.close().await?;

        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        let mut r = self.op.reader(&path).await?;
        tokio::io::copy(&mut r, output).await?;
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let range = range.start as u64..range.end as u64;
        let bs = self.op.read_with(&path).range(range).await?;

        Ok(OwnedBytes::new(bs))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let path = path.as_os_str().to_string_lossy();
        let range = range.start as u64..range.end as u64;
        let r = self.op.reader_with(&path).range(range).await?;

        Ok(Box::new(r))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let bs = self.op.read(&path).await?;

        Ok(OwnedBytes::new(bs))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        self.op.delete(&path).await?;

        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let paths = paths
            .iter()
            .map(|v| v.as_os_str().to_string_lossy().to_string())
            .collect();
        self.op.remove(paths).await.map_err(|err| BulkDeleteError {
            error: Some(err.into()),
            ..BulkDeleteError::default()
        })?;

        Ok(())
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let path = path.as_os_str().to_string_lossy();
        let meta = self.op.stat(&path).await?;
        Ok(meta.content_length())
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

impl From<opendal::Error> for StorageError {
    fn from(err: opendal::Error) -> Self {
        match err.kind() {
            opendal::ErrorKind::NotFound => StorageErrorKind::NotFound.with_error(err),
            opendal::ErrorKind::PermissionDenied => StorageErrorKind::Unauthorized.with_error(err),
            opendal::ErrorKind::ConfigInvalid => StorageErrorKind::Service.with_error(err),
            _ => StorageErrorKind::Io.with_error(err),
        }
    }
}

impl From<opendal::Error> for StorageResolverError {
    fn from(err: opendal::Error) -> Self {
        StorageResolverError::InvalidConfig(err.to_string())
    }
}
