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

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use azure_core::error::ErrorKind;
use azure_core::{Pageable, StatusCode};
use azure_storage::Error as AzureError;
use azure_storage::prelude::*;
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::io::Error as FutureError;
use futures::stream::{StreamExt, TryStreamExt};
use md5::Digest;
use once_cell::sync::OnceCell;
use quickwit_common::retry::{RetryParams, Retryable, retry};
use quickwit_common::uri::Uri;
use quickwit_common::{chunk_range, ignore_error_kind, into_u64_range};
use quickwit_config::{AzureStorageConfig, StorageBackend};
use regex::Regex;
use tantivy::directory::OwnedBytes;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};

use crate::debouncer::DebouncedStorage;
use crate::metrics::object_storage_get_slice_in_flight_guards;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, DeleteFailure, MultiPartPolicy, PutPayload, STORAGE_METRICS, Storage,
    StorageError, StorageErrorKind, StorageFactory, StorageResolverError, StorageResult,
};

/// Azure object storage resolver.
pub struct AzureBlobStorageFactory {
    storage_config: AzureStorageConfig,
}

impl AzureBlobStorageFactory {
    /// Creates a new Azure blob storage factory.
    pub fn new(storage_config: AzureStorageConfig) -> Self {
        Self { storage_config }
    }
}

#[async_trait]
impl StorageFactory for AzureBlobStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Azure
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = AzureBlobStorage::from_uri(&self.storage_config, uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

/// Azure object storage implementation
pub struct AzureBlobStorage {
    container_client: ContainerClient,
    uri: Uri,
    prefix: PathBuf,
    multipart_policy: MultiPartPolicy,
    retry_params: RetryParams,
}

impl fmt::Debug for AzureBlobStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AzureBlobStorage")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl AzureBlobStorage {
    /// Creates a new [`AzureBlobStorage`] instance.
    pub fn new(
        storage_account_name: String,
        storage_credentials: StorageCredentials,
        uri: Uri,
        container_name: String,
    ) -> Self {
        let container_client = BlobServiceClient::new(storage_account_name, storage_credentials)
            .container_client(container_name);
        Self {
            container_client,
            uri,
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy {
                // Azure max part size is 100MB
                // https://azure.microsoft.com/en-us/blog/general-availability-larger-block-blobs-in-azure-storage/
                target_part_num_bytes: 100_000_000,
                multipart_threshold_num_bytes: 100_000_000,
                max_num_parts: 50_000, // Azure allows up to 50,000 blocks
                max_object_num_bytes: 4_770_000_000_000u64, // Azure allows up to 4.77TB objects
                max_concurrent_uploads: 100,
            },
            retry_params: RetryParams::aggressive(),
        }
    }

    /// Sets the prefix path.
    ///
    /// The existing prefix is overwritten.
    pub fn with_prefix(self, prefix: PathBuf) -> Self {
        Self {
            container_client: self.container_client,
            uri: self.uri,
            prefix,
            multipart_policy: self.multipart_policy,
            retry_params: self.retry_params,
        }
    }

    /// Creates an emulated storage for testing.
    #[cfg(feature = "integration-testsuite")]
    pub fn new_emulated(container: &str) -> Self {
        use std::str::FromStr;

        let container_client = ClientBuilder::emulator().container_client(container);
        let uri = Uri::from_str(&format!("azure://tester/{container}")).unwrap();

        Self {
            container_client,
            uri,
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::no_retries(),
        }
    }

    /// Sets the multipart policy.
    ///
    /// See `MultiPartPolicy`.
    #[cfg(feature = "integration-testsuite")]
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }

    /// Builds instance from URI.
    pub fn from_uri(
        azure_storage_config: &AzureStorageConfig,
        uri: &Uri,
    ) -> Result<AzureBlobStorage, StorageResolverError> {
        let storage_account_name =
            azure_storage_config.resolve_account_name().ok_or_else(|| {
                let message = format!(
                    "could not find Azure storage account name in environment variable `{}` or \
                     storage config",
                    AzureStorageConfig::AZURE_STORAGE_ACCOUNT_ENV_VAR
                );
                StorageResolverError::InvalidConfig(message)
            })?;
        let storage_credentials = if let Some(access_key) =
            azure_storage_config.resolve_access_key()
        {
            StorageCredentials::access_key(storage_account_name.clone(), access_key)
        } else if let Ok(credential) = azure_identity::create_credential() {
            StorageCredentials::token_credential(credential)
        } else {
            return Err(StorageResolverError::InvalidConfig(
                "could not find Azure storage account credentials using the following credential \
                 providers: environment, managed identity, and storage account access key"
                    .to_string(),
            ));
        };
        let (container_name, prefix) = parse_azure_uri(uri).ok_or_else(|| {
            let message = format!("failed to extract container name from Azure URI `{uri}`");
            StorageResolverError::InvalidUri(message)
        })?;
        let azure_blob_storage = AzureBlobStorage::new(
            storage_account_name,
            storage_credentials,
            uri.clone(),
            container_name,
        );
        Ok(azure_blob_storage.with_prefix(prefix))
    }

    /// Returns the blob name (a.k.a blob key).
    fn blob_name(&self, relative_path: &Path) -> String {
        let key_path = self.prefix.join(relative_path);
        key_path.to_string_lossy().to_string()
    }

    /// Downloads a blob as vector of bytes.
    async fn get_to_vec(
        &self,
        path: &Path,
        range_opt: Option<Range<usize>>,
    ) -> StorageResult<Vec<u8>> {
        let name = self.blob_name(path);
        let capacity = range_opt.as_ref().map(Range::len).unwrap_or(0);
        retry(&self.retry_params, || async {
            let (mut response_stream, _in_flight_guards) = if let Some(range) = range_opt.as_ref() {
                let stream = self
                    .container_client
                    .blob_client(&name)
                    .get()
                    .range(range.clone())
                    .into_stream();
                // only record ranged get request as being in flight
                let in_flight_guards = object_storage_get_slice_in_flight_guards(capacity);
                (stream, Some(in_flight_guards))
            } else {
                let stream = self.container_client.blob_client(&name).get().into_stream();
                (stream, None)
            };
            let mut buf: Vec<u8> = Vec::with_capacity(capacity);
            download_all(&mut response_stream, &mut buf).await?;

            Result::<_, AzureErrorWrapper>::Ok(buf)
        })
        .await
        .map_err(StorageError::from)
    }

    /// Performs a single part upload.
    async fn put_single_part<'a>(
        &'a self,
        name: &'a str,
        payload: Box<dyn crate::PutPayload>,
    ) -> StorageResult<()> {
        crate::STORAGE_METRICS.object_storage_put_parts.inc();
        crate::STORAGE_METRICS
            .object_storage_upload_num_bytes
            .inc_by(payload.len());
        retry(&self.retry_params, || async {
            let data = Bytes::from(payload.read_all().await?.to_vec());
            let hash = azure_storage_blobs::prelude::Hash::from(md5::compute(&data[..]).0);
            self.container_client
                .blob_client(name)
                .put_block_blob(data)
                .hash(hash)
                .into_future()
                .await?;
            Result::<(), AzureErrorWrapper>::Ok(())
        })
        .await?;
        Ok(())
    }

    /// Performs a multipart upload.
    async fn put_multi_part<'a>(
        &'a self,
        name: &'a str,
        payload: Box<dyn PutPayload>,
        part_len: u64,
        total_len: u64,
    ) -> StorageResult<()> {
        assert!(total_len > 0);
        let multipart_ranges =
            chunk_range(0..total_len as usize, part_len as usize).map(into_u64_range);

        let blob_client = self.container_client.blob_client(name);
        let upload_blocks_stream = tokio_stream::iter(multipart_ranges.enumerate())
            .map(|(num, range)| {
                let moved_blob_client = blob_client.clone();
                let moved_payload = payload.clone();
                crate::STORAGE_METRICS.object_storage_put_parts.inc();
                crate::STORAGE_METRICS
                    .object_storage_upload_num_bytes
                    .inc_by(range.end - range.start);
                async move {
                    retry(&self.retry_params, || async {
                        // zero pad block ids to make them sortable as strings
                        let block_id = format!("block:{:05}", num);
                        let (data, hash_digest) =
                            extract_range_data_and_hash(moved_payload.box_clone(), range.clone())
                                .await?;
                        let hash = azure_storage_blobs::prelude::Hash::from(hash_digest.0);
                        moved_blob_client
                            .put_block(block_id.clone(), data)
                            .hash(hash)
                            .into_future()
                            .await?;
                        Result::<_, AzureErrorWrapper>::Ok(block_id)
                    })
                    .await
                }
            })
            .buffer_unordered(self.multipart_policy.max_concurrent_uploads());

        // Collect and sort block ids to preserve part order for put_block_list.
        // Azure docs: "The put block list operation enforces the order in which blocks
        // are to be combined to create a blob".
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        let mut block_ids: Vec<String> = upload_blocks_stream
            .try_collect()
            .await
            .map_err(StorageError::from)?;
        block_ids.sort_unstable();

        let block_list = BlockList {
            blocks: block_ids
                .into_iter()
                .map(BlobBlockType::new_uncommitted)
                .collect(),
        };

        // Commit all uploaded blocks.
        blob_client
            .put_block_list(block_list)
            .into_future()
            .await
            .map_err(AzureErrorWrapper::from)?;

        Ok(())
    }
}

#[async_trait]
impl Storage for AzureBlobStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if let Some(first_blob_result) = self
            .container_client
            .list_blobs()
            .max_results(NonZeroU32::new(1u32).expect("1 is always non-zero."))
            .into_stream()
            .next()
            .await
        {
            let _ = first_blob_result?;
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        crate::STORAGE_METRICS.object_storage_put_total.inc();
        let name = self.blob_name(path);
        let total_len = payload.len();
        let part_num_bytes = self.multipart_policy.part_num_bytes(total_len);

        if part_num_bytes >= total_len {
            self.put_single_part(&name, payload).await?;
        } else {
            self.put_multi_part(&name, payload, part_num_bytes, total_len)
                .await?;
        }
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let name = self.blob_name(path);
        let mut output_stream = self.container_client.blob_client(name).get().into_stream();

        while let Some(chunk_result) = output_stream.next().await {
            let chunk_response = chunk_result.map_err(AzureErrorWrapper::from)?;
            let chunk_response_body_stream = chunk_response
                .data
                .map_err(FutureError::other)
                .into_async_read()
                .compat();
            let mut body_stream_reader = BufReader::new(chunk_response_body_stream);
            let num_bytes_copied = tokio::io::copy_buf(&mut body_stream_reader, output).await?;
            STORAGE_METRICS
                .object_storage_download_num_bytes
                .inc_by(num_bytes_copied);
        }
        output.flush().await?;
        Ok(())
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let blob_name = self.blob_name(path);
        let delete_res: Result<_, StorageError> = self
            .container_client
            .blob_client(blob_name)
            .delete()
            .into_future()
            .await
            .map_err(|err| AzureErrorWrapper::from(err).into());
        ignore_error_kind!(StorageErrorKind::NotFound, delete_res)?;
        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        // See https://github.com/Azure/azure-sdk-for-rust/issues/1068
        warn!(
            num_files = paths.len(),
            "`AzureBlobStorage` does not support batch delete. Falling back to sequential delete, \
             which might be slow and issue many requests."
        );
        let mut successes = Vec::with_capacity(paths.len());
        let mut failures = HashMap::new();

        let futures = paths
            .iter()
            .map(|path| async move {
                let delete_res = self.delete(path).await;
                (path, delete_res)
            })
            .collect::<Vec<_>>();
        let mut stream = futures::stream::iter(futures).buffer_unordered(100);

        while let Some((path, delete_res)) = stream.next().await {
            match delete_res {
                Ok(_) => successes.push(path.to_path_buf()),
                Err(error) => {
                    let failure = DeleteFailure {
                        error: Some(error),
                        ..Default::default()
                    };
                    failures.insert(path.to_path_buf(), failure);
                }
            };
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(BulkDeleteError {
                successes,
                failures,
                ..Default::default()
            })
        }
    }

    #[instrument(level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_to_vec(path, Some(range.clone()))
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!(
                    "failed to fetch slice {:?} for object: {}/{}",
                    range,
                    self.uri,
                    path.display(),
                ))
            })
    }

    #[instrument(level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        retry(&self.retry_params, || async {
            let range = range.clone();
            let name = self.blob_name(path);
            let page_stream = self
                .container_client
                .blob_client(name)
                .get()
                .range(range)
                .into_stream();
            let mut bytes_stream = page_stream
                .map(|page_res| page_res.map(|page| page.data).map_err(FutureError::other))
                .try_flatten()
                .map(|bytes_res| bytes_res.map_err(FutureError::other));
            // Peek into the stream so that any early error can be retried
            let first_chunk = bytes_stream.next().await;
            let reader: Box<dyn AsyncRead + Send + Unpin> = if let Some(res) = first_chunk {
                let first_chunk = res.map_err(AzureErrorWrapper::from)?;
                let reconstructed_stream =
                    Box::pin(futures::stream::once(async { Ok(first_chunk) }).chain(bytes_stream));
                Box::new(StreamReader::new(reconstructed_stream))
            } else {
                Box::new(tokio::io::empty())
            };
            Result::<Box<dyn AsyncRead + Send + Unpin>, AzureErrorWrapper>::Ok(reader)
        })
        .await
        .map_err(|e| e.into())
    }

    #[instrument(level = "debug", skip(self), fields(fetched_bytes_len))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let data = self
            .get_to_vec(path, None)
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!(
                    "failed to fetch object: {}/{}",
                    self.uri,
                    path.display()
                ))
            })?;
        tracing::Span::current().record("fetched_bytes_len", data.len());
        Ok(data)
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let name = self.blob_name(path);
        let properties_result = self
            .container_client
            .blob_client(name)
            .get_properties()
            .into_future()
            .await;
        match properties_result {
            Ok(response) => Ok(response.blob.properties.content_length),
            Err(err) => Err(StorageError::from(AzureErrorWrapper::from(err))),
        }
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

/// Copy range of payload into `Bytes` and return the computed md5.
async fn extract_range_data_and_hash(
    payload: Box<dyn PutPayload>,
    range: Range<u64>,
) -> io::Result<(Bytes, Digest)> {
    let mut reader = payload
        .range_byte_stream(range.clone())
        .await?
        .into_async_read();
    let mut buf: Vec<u8> = Vec::with_capacity(range.count());
    tokio::io::copy(&mut reader, &mut buf).await?;
    let data = Bytes::from(buf);
    let hash = md5::compute(&data[..]);
    Ok((data, hash))
}

pub fn parse_azure_uri(uri: &Uri) -> Option<(String, PathBuf)> {
    // Ex: azure://container/prefix.
    static URI_PTN: OnceCell<Regex> = OnceCell::new();

    let captures = URI_PTN
        .get_or_init(|| {
            Regex::new(r"azure(\+[^:]+)?://(?P<container>[^/]+)(/(?P<prefix>.+))?")
                .expect("The regular expression should compile.")
        })
        .captures(uri.as_str())?;

    let container = captures.name("container")?.as_str().to_string();
    let prefix = captures
        .name("prefix")
        .map(|prefix_match| PathBuf::from(prefix_match.as_str()))
        .unwrap_or_default();
    Some((container, prefix))
}

/// Collect a download stream into an output buffer.
async fn download_all(
    chunk_stream: &mut Pageable<GetBlobResponse, AzureError>,
    output: &mut Vec<u8>,
) -> Result<(), AzureErrorWrapper> {
    output.clear();
    while let Some(chunk_result) = chunk_stream.next().await {
        let chunk_response = chunk_result?;
        let chunk_response_body_stream = chunk_response
            .data
            .map_err(FutureError::other)
            .into_async_read()
            .compat();
        let mut body_stream_reader = BufReader::new(chunk_response_body_stream);
        let num_bytes_copied = tokio::io::copy_buf(&mut body_stream_reader, output).await?;
        crate::STORAGE_METRICS
            .object_storage_download_num_bytes
            .inc_by(num_bytes_copied);
    }
    // When calling `get_all`, the Vec capacity is not properly set.
    output.shrink_to_fit();
    Ok(())
}

#[derive(Error, Debug)]
#[error("Azure error wrapper(inner={inner})")]
struct AzureErrorWrapper {
    inner: AzureError,
}

impl Retryable for AzureErrorWrapper {
    fn is_retryable(&self) -> bool {
        match self.inner.kind() {
            ErrorKind::HttpResponse { status, .. } => !matches!(
                status,
                StatusCode::NotFound
                    | StatusCode::Unauthorized
                    | StatusCode::BadRequest
                    | StatusCode::Forbidden
            ),
            ErrorKind::Io => true,
            _ => false,
        }
    }
}

impl From<AzureError> for AzureErrorWrapper {
    fn from(err: AzureError) -> Self {
        AzureErrorWrapper { inner: err }
    }
}

impl From<io::Error> for AzureErrorWrapper {
    fn from(err: io::Error) -> Self {
        AzureErrorWrapper {
            inner: AzureError::new(ErrorKind::Io, err),
        }
    }
}

impl From<AzureErrorWrapper> for StorageError {
    fn from(err: AzureErrorWrapper) -> Self {
        match err.inner.kind() {
            ErrorKind::HttpResponse { status, .. } => match status {
                StatusCode::NotFound => StorageErrorKind::NotFound.with_error(err),
                _ => StorageErrorKind::Service.with_error(err),
            },
            ErrorKind::Io => StorageErrorKind::Io.with_error(err),
            ErrorKind::Credential => StorageErrorKind::Unauthorized.with_error(err),
            _ => StorageErrorKind::Internal.with_error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_common::uri::Uri;

    use crate::object_storage::azure_blob_storage::parse_azure_uri;

    #[test]
    fn test_parse_azure_uri() {
        assert!(parse_azure_uri(&Uri::for_test("azure://")).is_none());

        let (container, prefix) =
            parse_azure_uri(&Uri::for_test("azure://test-container")).unwrap();
        assert_eq!(container, "test-container");
        assert!(prefix.to_str().unwrap().is_empty());

        let (container, prefix) =
            parse_azure_uri(&Uri::for_test("azure://test-container/")).unwrap();
        assert_eq!(container, "test-container");
        assert!(prefix.to_str().unwrap().is_empty());

        let (container, prefix) =
            parse_azure_uri(&Uri::for_test("azure://test-container/indexes")).unwrap();
        assert_eq!(container, "test-container");
        assert_eq!(prefix.to_str().unwrap(), "indexes");
    }
}
