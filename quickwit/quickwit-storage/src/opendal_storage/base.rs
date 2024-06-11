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
use futures::SinkExt;
use opendal::Operator;
use quickwit_common::uri::Uri;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;

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
        let mut payload_stream = payload.byte_stream().await?;

        let mut storage_sink = self
            .op
            .writer_with(&path)
            .chunk(ByteSize::mb(8).as_u64() as usize)
            .await?
            .into_bytes_sink();

        // Use bytes stream and sink to avoid extra memory copy.
        while let Some(bs) = payload_stream
            .try_next()
            .await
            .map_err(|err| StorageErrorKind::Io.with_error(err))?
        {
            storage_sink.feed(bs).await?;
        }
        storage_sink.close().await?;
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        let mut storage_reader = self
            .op
            .reader(&path)
            .await?
            .into_futures_async_read(..)
            .await?
            .compat();
        tokio::io::copy(&mut storage_reader, output).await?;
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let range = range.start as u64..range.end as u64;
        let storage_content = self.op.read_with(&path).range(range).await?.to_vec();

        Ok(OwnedBytes::new(storage_content))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let path = path.as_os_str().to_string_lossy();
        let range = range.start as u64..range.end as u64;
        let storage_reader = self
            .op
            .reader_with(&path)
            .await?
            .into_futures_async_read(range)
            .await?
            .compat();

        Ok(Box::new(storage_reader))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let storage_content = self.op.read(&path).await?.to_vec();

        Ok(OwnedBytes::new(storage_content))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        self.op.delete(&path).await?;

        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        // The mock service we used in integration testsuite doesn't support bucket delete.
        // Let's fallback to delete one by one in this case.
        #[cfg(feature = "integration-testsuite")]
        {
            let storage_info = self.op.info();
            if storage_info.name() == "sample-bucket"
                && storage_info.scheme() == opendal::Scheme::Gcs
            {
                let mut bulk_error = BulkDeleteError::default();
                for (index, path) in paths.iter().enumerate() {
                    let result = self.op.delete(&path.as_os_str().to_string_lossy()).await;
                    if let Err(err) = result {
                        let storage_error_kind = err.kind();
                        let storage_error: StorageError = err.into();
                        bulk_error.failures.insert(
                            path.to_path_buf(),
                            crate::DeleteFailure {
                                code: Some(storage_error_kind.to_string()),
                                message: Some(storage_error.to_string()),
                                error: Some(storage_error.clone()),
                            },
                        );
                        bulk_error.error = Some(storage_error);
                        for path in paths[index..].iter() {
                            bulk_error.unattempted.push(path.to_path_buf())
                        }
                        break;
                    } else {
                        bulk_error.successes.push(path.to_path_buf())
                    }
                }

                return if bulk_error.error.is_some() {
                    Err(bulk_error)
                } else {
                    Ok(())
                };
            }
        }

        let paths: Vec<String> = paths
            .iter()
            .map(|path| path.as_os_str().to_string_lossy().to_string())
            .collect();

        // OpenDAL will check the services' capability internally.
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
