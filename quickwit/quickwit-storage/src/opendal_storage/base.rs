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
use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use futures::AsyncWriteExt as FuturesAsyncWriteExt;
use opendal::{DeleteInput, IntoDeleteInput, Operator};
use quickwit_common::uri::Uri;
use tokio::io::{AsyncRead, AsyncWriteExt as TokioAsyncWriteExt};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use crate::metrics::object_storage_get_slice_in_flight_guards;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, MultiPartPolicy, OwnedBytes, PutPayload, Storage, StorageError,
    StorageErrorKind, StorageResolverError, StorageResult,
};

/// OpenDAL based storage implementation.
/// # TODO
///
/// - Implement REQUEST_SEMAPHORE to control the concurrency.
/// - Implement STORAGE_METRICS for metrics.
pub struct OpendalStorage {
    uri: Uri,
    op: Operator,
    multipart_policy: MultiPartPolicy,
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
        Ok(Self {
            uri,
            op,
            // limits are the same as on S3
            multipart_policy: MultiPartPolicy::default(),
        })
    }

    #[cfg(feature = "integration-testsuite")]
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }
}

#[async_trait]
impl Storage for OpendalStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.op.check().await?;
        Ok(())
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        crate::STORAGE_METRICS.object_storage_put_total.inc();
        let path = path.as_os_str().to_string_lossy();
        let mut payload_reader = payload.byte_stream().await?.into_async_read();

        let mut storage_writer = self
            .op
            .writer_with(&path)
            .chunk(self.multipart_policy.part_num_bytes(payload.len()) as usize)
            .await?
            .into_futures_async_write()
            .compat_write();
        tokio::io::copy(&mut payload_reader, &mut storage_writer).await?;
        storage_writer.get_mut().close().await?;
        crate::STORAGE_METRICS
            .object_storage_upload_num_bytes
            .inc_by(payload.len());
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
        let num_bytes_copied = tokio::io::copy(&mut storage_reader, output).await?;
        crate::STORAGE_METRICS
            .object_storage_download_num_bytes
            .inc_by(num_bytes_copied);
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let size = range.len();
        let range = range.start as u64..range.end as u64;
        // Unlike other object store implementations, in flight requests are
        // recorded before issuing the query to the object store.
        let _inflight_guards = object_storage_get_slice_in_flight_guards(size);
        crate::STORAGE_METRICS.object_storage_get_total.inc();
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
        crate::STORAGE_METRICS
            .object_storage_delete_requests_total
            .inc();
        let _timer = crate::STORAGE_METRICS
            .object_storage_delete_request_duration
            .start_timer();
        self.op.delete(&path).await?;
        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        // The mock service we used in integration testsuite doesn't support bulk delete.
        // Let's fallback to delete one by one in this case.
        #[cfg(feature = "integration-testsuite")]
        {
            let storage_info = self.op.info();
            if storage_info.name().starts_with("sample-bucket") && storage_info.scheme() == "gcs" {
                let mut bulk_error = BulkDeleteError::default();
                for (index, path) in paths.iter().enumerate() {
                    crate::STORAGE_METRICS
                        .object_storage_bulk_delete_requests_total
                        .inc();
                    let _timer = crate::STORAGE_METRICS
                        .object_storage_bulk_delete_request_duration
                        .start_timer();
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
        let delete_inputs: Vec<DeleteInput> = paths
            .iter()
            .map(|path| path.as_os_str().to_string_lossy().into_delete_input())
            .collect();

        self.op
            .delete_iter(delete_inputs)
            .await
            .map_err(|error| BulkDeleteError {
                error: Some(error.into()),
                ..Default::default()
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
