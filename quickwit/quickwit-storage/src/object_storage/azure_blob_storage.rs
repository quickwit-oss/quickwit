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
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use std::{fmt, io};

use async_trait::async_trait;
use azure_core::Error as AzureError;
use azure_core::error::ErrorKind;
use azure_core::http::{HttpClientOptions, RequestContent, StatusCode, Transport, new_http_client};
use azure_storage_blob::models::{
    BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    HttpRange,
};
use azure_storage_blob::{BlobContainerClient, BlockBlobClient};
use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use futures::io::Error as FutureError;
use futures::stream::{StreamExt, TryStreamExt};
use quickwit_common::retry::{RetryParams, Retryable, retry};
use quickwit_common::uri::Uri;
use quickwit_common::{chunk_range, ignore_error_kind, into_u64_range};
use quickwit_config::{AzureStorageConfig, StorageBackend};
use quickwit_metrics::HistogramTimer;
use regex::Regex;
use tantivy::directory::OwnedBytes;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader};
use tokio::sync::OnceCell;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};
use ulid::Ulid;

use crate::debouncer::DebouncedStorage;
use crate::metrics::object_storage_get_slice_in_flight_guards;
use crate::object_storage::azure_credentials::{
    AzureCredential, build_container_client, resolve_credential,
};
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, DeleteFailure, MultiPartPolicy, PutPayload, Storage, StorageError,
    StorageErrorKind, StorageFactory, StorageResolverError, StorageResult,
};

/// Azure object storage resolver.
pub struct AzureBlobStorageFactory {
    storage_config: AzureStorageConfig,
    credential: OnceCell<AzureCredential>,
    transport: OnceLock<Transport>,
}

impl AzureBlobStorageFactory {
    /// Creates a new Azure blob storage factory.
    pub fn new(storage_config: AzureStorageConfig) -> Self {
        Self {
            storage_config,
            credential: OnceCell::new(),
            transport: OnceLock::new(),
        }
    }
}

#[async_trait]
impl StorageFactory for AzureBlobStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Azure
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let credential = self
            .credential
            .get_or_try_init(|| async { resolve_credential(&self.storage_config) })
            .await?
            .clone();
        let transport = self
            .transport
            .get_or_init(|| {
                // As the SDK's default transport does: decompression breaks ranged downloads.
                Transport::new(new_http_client(Some(HttpClientOptions {
                    automatic_decompression: false,
                })))
            })
            .clone();
        let storage = AzureBlobStorage::from_uri_and_credential(
            &self.storage_config,
            uri,
            credential,
            Some(transport),
        )?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

/// Azure object storage implementation
pub struct AzureBlobStorage {
    container_client: BlobContainerClient,
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
    pub(crate) fn new(
        storage_account_name: &str,
        credential: AzureCredential,
        transport: Option<Transport>,
        blob_service_uri: Option<String>,
        uri: Uri,
        container_name: &str,
    ) -> Result<Self, StorageResolverError> {
        let container_client = build_container_client(
            storage_account_name,
            credential,
            transport,
            blob_service_uri,
            container_name,
        )?;
        Ok(Self {
            container_client,
            uri,
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy {
                // Azure max part size is 100MB
                // https://azure.microsoft.com/en-us/blog/general-availability-larger-block-blobs-in-azure-storage/
                target_part_num_bytes: ByteSize::mb(100),
                multipart_threshold_num_bytes: ByteSize::mb(100),
                max_num_parts: 50_000, // Azure allows up to 50,000 blocks
                max_object_num_bytes: ByteSize::b(4_770_000_000_000), /* Azure allows up to
                                        * 4.77TB objects */
                max_concurrent_uploads: 100,
            },
            retry_params: RetryParams::aggressive(),
        })
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
    ///
    /// The well-known account and key are documented at:
    /// <https://learn.microsoft.com/azure/storage/common/storage-use-azurite>.
    #[cfg(feature = "integration-testsuite")]
    pub fn new_emulated(container: &str) -> Self {
        use std::str::FromStr;

        let container_client = build_emulated_container_client(container);
        let uri = Uri::from_str(&format!("azure://tester/{container}")).unwrap();

        Self {
            container_client,
            uri,
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::no_retries(),
        }
    }

    /// Creates the container of an emulated storage.
    #[cfg(feature = "integration-testsuite")]
    pub async fn create_emulated_container(container: &str) -> anyhow::Result<()> {
        build_emulated_container_client(container)
            .create(None)
            .await?;
        Ok(())
    }

    /// Deletes the container of an emulated storage.
    #[cfg(feature = "integration-testsuite")]
    pub async fn delete_emulated_container(container: &str) -> anyhow::Result<()> {
        build_emulated_container_client(container)
            .delete(None)
            .await?;
        Ok(())
    }

    /// Sets the multipart policy.
    ///
    /// See `MultiPartPolicy`.
    #[cfg(feature = "integration-testsuite")]
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }

    fn from_uri_and_credential(
        azure_storage_config: &AzureStorageConfig,
        uri: &Uri,
        credential: AzureCredential,
        transport: Option<Transport>,
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
        let (container_name, prefix) = parse_azure_uri(uri).ok_or_else(|| {
            let message = format!("failed to extract container name from Azure URI `{uri}`");
            StorageResolverError::InvalidUri(message)
        })?;
        let blob_service_uri = azure_storage_config.resolve_blob_service_uri(&storage_account_name);
        let azure_blob_storage = AzureBlobStorage::new(
            &storage_account_name,
            credential,
            transport,
            blob_service_uri,
            uri.clone(),
            &container_name,
        )?;
        Ok(azure_blob_storage.with_prefix(prefix))
    }

    /// Returns the blob name (a.k.a blob key).
    fn blob_name(&self, relative_path: &Path) -> String {
        let key_path = self.prefix.join(relative_path);
        // The 0.21 SDK trimmed slashes, so keep doing it for existing blobs to resolve.
        key_path.to_string_lossy().trim_matches('/').to_string()
    }

    /// Downloads a blob as `Bytes`.
    async fn get_to_bytes(
        &self,
        path: &Path,
        range_opt: Option<Range<usize>>,
    ) -> StorageResult<Bytes> {
        if is_empty_range(range_opt.as_ref())? {
            // Still a request, so that a missing blob is `NotFound`.
            self.file_num_bytes(path).await?;
            return Ok(Bytes::new());
        }
        let name = self.blob_name(path);
        let range_num_bytes = range_opt.as_ref().map(Range::len).unwrap_or(0);
        retry(&self.retry_params, || async {
            let _timer = HistogramTimer::new(&crate::metrics::OBJECT_STORAGE_GET_OBJECT_DURATION);
            // only record ranged get request as being in flight
            let _in_flight_guards = range_opt
                .as_ref()
                .map(|_| object_storage_get_slice_in_flight_guards(range_num_bytes));
            let download_response = self
                .container_client
                .blob_client(&name)
                .download(Some(download_options(range_opt.as_ref())))
                .await?;
            let capacity = if range_opt.is_some() {
                range_num_bytes
            } else {
                download_response
                    .properties
                    .content_length
                    .and_then(|num_bytes| usize::try_from(num_bytes).ok())
                    .unwrap_or(0)
            };
            let mut body = download_response.body;
            let mut bytes = BytesMut::with_capacity(capacity);
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            crate::metrics::OBJECT_STORAGE_DOWNLOAD_NUM_BYTES.inc_by(bytes.len() as u64);
            Result::<_, AzureErrorWrapper>::Ok(bytes.freeze())
        })
        .await
        .map_err(StorageError::from)
    }

    async fn get_to_reader(
        &self,
        path: &Path,
        range_opt: Option<Range<usize>>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        if is_empty_range(range_opt.as_ref())? {
            self.file_num_bytes(path).await?;
            return Ok(Box::new(tokio::io::empty()));
        }
        let name = self.blob_name(path);
        retry(&self.retry_params, || async {
            let download_response = self
                .container_client
                .blob_client(&name)
                .download(Some(download_options(range_opt.as_ref())))
                .await?;
            let mut bytes_stream = download_response
                .body
                .map(|bytes_res| bytes_res.map_err(FutureError::other));
            // Peek into the stream so that any early error can be retried
            let first_chunk = bytes_stream.next().await;
            let reader: Box<dyn AsyncRead + Send + Unpin> = match first_chunk {
                Some(bytes_res) => {
                    let first_chunk = bytes_res.map_err(AzureErrorWrapper::from)?;
                    let reconstructed_stream = Box::pin(
                        futures::stream::once(async { Ok(first_chunk) }).chain(bytes_stream),
                    );
                    Box::new(StreamReader::new(reconstructed_stream))
                }
                None => Box::new(tokio::io::empty()),
            };
            Result::<Box<dyn AsyncRead + Send + Unpin>, AzureErrorWrapper>::Ok(reader)
        })
        .await
        .map_err(|err| err.into())
    }

    /// Performs a single part upload.
    async fn put_single_part<'a>(
        &'a self,
        name: &'a str,
        payload: Box<dyn crate::PutPayload>,
    ) -> StorageResult<()> {
        crate::metrics::OBJECT_STORAGE_PUT_PARTS.inc();
        crate::metrics::OBJECT_STORAGE_UPLOAD_NUM_BYTES.inc_by(payload.len());
        let _timer = HistogramTimer::new(&crate::metrics::OBJECT_STORAGE_PUT_OBJECT_DURATION);
        let block_blob_client = self.container_client.blob_client(name).block_blob_client();
        let mut block_ids = Vec::new();
        // `Put Block` rejects empty blocks: an empty blob commits an empty block list.
        if payload.len() > 0 {
            let block_id = format_block_id(Ulid::new(), 0);
            retry(&self.retry_params, || async {
                let data = Bytes::from(payload.read_all().await?.to_vec());
                stage_block(&block_blob_client, &block_id, data).await
            })
            .await?;
            block_ids.push(block_id);
        }
        self.commit_block_list(&block_blob_client, block_ids).await
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

        let block_blob_client = &self.container_client.blob_client(name).block_blob_client();
        let upload_id = Ulid::new();
        let upload_blocks_stream = tokio_stream::iter(multipart_ranges.enumerate())
            .map(|(num, range)| {
                let moved_payload = payload.clone();
                crate::metrics::OBJECT_STORAGE_PUT_PARTS.inc();
                crate::metrics::OBJECT_STORAGE_UPLOAD_NUM_BYTES.inc_by(range.end - range.start);
                async move {
                    let _timer =
                        HistogramTimer::new(&crate::metrics::OBJECT_STORAGE_UPLOAD_PART_DURATION);
                    retry(&self.retry_params, || async {
                        let block_id = format_block_id(upload_id, num);
                        let data =
                            extract_range_data(moved_payload.box_clone(), range.clone()).await?;
                        stage_block(block_blob_client, &block_id, data).await?;
                        Result::<_, AzureErrorWrapper>::Ok(block_id)
                    })
                    .await
                }
            })
            .buffer_unordered(self.multipart_policy.max_concurrent_uploads());

        // Collect and sort block ids to preserve part order for the block list.
        // Azure docs: "The put block list operation enforces the order in which blocks
        // are to be combined to create a blob".
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        let mut block_ids: Vec<String> = upload_blocks_stream
            .try_collect()
            .await
            .map_err(StorageError::from)?;
        block_ids.sort_unstable();
        self.commit_block_list(block_blob_client, block_ids).await
    }

    /// Lists blocks as `latest`, not `uncommitted`, so that retrying a commit that already
    /// succeeded finds them among the committed blocks.
    async fn commit_block_list(
        &self,
        block_blob_client: &BlockBlobClient,
        block_ids: Vec<String>,
    ) -> StorageResult<()> {
        let block_lookup_list = BlockLookupList {
            latest: Some(block_ids.into_iter().map(String::into_bytes).collect()),
            ..Default::default()
        };
        retry(&self.retry_params, || async {
            let block_list_content = RequestContent::try_from(block_lookup_list.clone())?;
            block_blob_client
                .commit_block_list(block_list_content, None)
                .await?;
            Result::<(), AzureErrorWrapper>::Ok(())
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Storage for AzureBlobStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        let list_blobs_options = BlobContainerClientListBlobsOptions {
            maxresults: Some(1),
            ..Default::default()
        };
        let mut blob_pager = self.container_client.list_blobs(Some(list_blobs_options))?;
        if let Some(first_page_result) = blob_pager.next().await {
            let _ = first_page_result?;
        }
        Ok(())
    }

    #[instrument(name = "storage.azure.put", level = "debug", skip(self, payload), fields(payload_len = payload.len()))]
    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        crate::metrics::OBJECT_STORAGE_PUT_TOTAL.inc();
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

    #[instrument(name = "storage.azure.copy_to", level = "debug", skip(self, output))]
    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let reader = self.get_to_reader(path, None).await?;
        let mut body_stream_reader = BufReader::new(reader);
        let num_bytes_copied = tokio::io::copy_buf(&mut body_stream_reader, output).await?;
        crate::metrics::OBJECT_STORAGE_DOWNLOAD_NUM_BYTES.inc_by(num_bytes_copied);
        output.flush().await?;
        Ok(())
    }

    #[instrument(name = "storage.azure.delete", level = "debug", skip(self))]
    async fn delete(&self, path: &Path) -> StorageResult<()> {
        // The Azure SDK has no batch delete, so bulk_delete also funnels through
        // here one blob at a time.
        crate::metrics::OBJECT_STORAGE_DELETE_REQUESTS_TOTAL.inc();
        let _timer = HistogramTimer::new(&crate::metrics::OBJECT_STORAGE_DELETE_REQUEST_DURATION);
        let blob_name = self.blob_name(path);
        let delete_res: Result<_, StorageError> = self
            .container_client
            .blob_client(&blob_name)
            .delete(None)
            .await
            .map_err(|err| AzureErrorWrapper::from(err).into());
        ignore_error_kind!(StorageErrorKind::NotFound, delete_res)?;
        Ok(())
    }

    #[instrument(name = "storage.azure.bulk_delete", level = "debug", skip(self, paths), fields(num_paths = paths.len()))]
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

    #[instrument(name = "storage.azure.get_slice", level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_to_bytes(path, Some(range.clone()))
            .await
            .map(into_owned_bytes)
            .map_err(|err| {
                err.add_context(format!(
                    "failed to fetch slice {:?} for object: {}/{}",
                    range,
                    self.uri,
                    path.display(),
                ))
            })
    }

    #[instrument(name = "storage.azure.get_slice_stream", level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.get_to_reader(path, Some(range)).await
    }

    #[instrument(
        name = "storage.azure.get_all",
        level = "debug",
        skip(self),
        fields(fetched_bytes_len)
    )]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let data = self
            .get_to_bytes(path, None)
            .await
            .map(into_owned_bytes)
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

    #[instrument(name = "storage.azure.file_num_bytes", level = "debug", skip(self))]
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let name = self.blob_name(path);
        let response = self
            .container_client
            .blob_client(&name)
            .get_properties(None)
            .await
            .map_err(AzureErrorWrapper::from)?;
        let content_length_opt = response.content_length().map_err(AzureErrorWrapper::from)?;
        content_length_opt.ok_or_else(|| {
            StorageErrorKind::Service.with_error(anyhow::anyhow!(
                "blob properties of `{name}` have no content length"
            ))
        })
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

/// Block ids carry the upload's ULID, have the fixed length Azure requires within a blob, and
/// sort by part.
///
/// Concurrent uploads of the same blob are not supported: a commit discards the other upload's
/// uncommitted blocks, so its own commit then fails.
fn format_block_id(upload_id: Ulid, part_num: usize) -> String {
    format!("{upload_id}:{part_num:05}")
}

/// Within `i64::MAX` so that the `Range` header stays a signed 64-bit offset.
const WHOLE_BLOB_PARTITION_SIZE: NonZeroUsize = NonZeroUsize::new(i64::MAX as usize).unwrap();

/// One streamed request per download. The SDK default fans out up to 96 buffered 4 MiB requests.
fn download_options(range_opt: Option<&Range<usize>>) -> BlobClientDownloadOptions<'static> {
    let mut download_options = BlobClientDownloadOptions {
        parallel: Some(NonZeroUsize::MIN),
        partition_size: Some(WHOLE_BLOB_PARTITION_SIZE),
        ..Default::default()
    };
    if let Some(range) = range_opt {
        download_options.range = Some(HttpRange::from(range.clone()));
        download_options.partition_size = NonZeroUsize::new(range.len());
    }
    download_options
}

/// The SDK refuses to download an empty range.
fn is_empty_range(range_opt: Option<&Range<usize>>) -> StorageResult<bool> {
    let Some(range) = range_opt else {
        return Ok(false);
    };
    if range.start > range.end {
        return Err(StorageErrorKind::Internal.with_error(anyhow::anyhow!(
            "invalid byte range {range:?}: the start is after the end"
        )));
    }
    Ok(range.is_empty())
}

/// Uploads stage and commit blocks because `upload()` takes no checksum the service verifies.
async fn stage_block(
    block_blob_client: &BlockBlobClient,
    block_id: &str,
    data: Bytes,
) -> Result<(), AzureErrorWrapper> {
    let stage_block_options = BlockBlobClientStageBlockOptions {
        transactional_content_crc64: Some(crc64_nvme(&data)),
        ..Default::default()
    };
    let content_length = data.len() as u64;
    block_blob_client
        .stage_block(
            block_id.as_bytes(),
            content_length,
            data.into(),
            Some(stage_block_options),
        )
        .await?;
    Ok(())
}

#[cfg(feature = "integration-testsuite")]
fn build_emulated_container_client(container: &str) -> BlobContainerClient {
    const EMULATOR_ACCOUNT: &str = "devstoreaccount1";
    const EMULATOR_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const EMULATOR_BLOB_SERVICE_URI: &str = "http://127.0.0.1:10000/devstoreaccount1";

    build_container_client(
        EMULATOR_ACCOUNT,
        AzureCredential::SharedKey(EMULATOR_ACCOUNT_KEY.to_string()),
        None,
        Some(EMULATOR_BLOB_SERVICE_URI.to_string()),
        container,
    )
    .expect("the emulator endpoint should be a valid URL")
}

/// `x-ms-content-crc64` is CRC-64/NVME, little endian.
fn crc64_nvme(data: &[u8]) -> Vec<u8> {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, data)
        .to_le_bytes()
        .to_vec()
}

/// Copy range of payload into `Bytes`.
async fn extract_range_data(payload: Box<dyn PutPayload>, range: Range<u64>) -> io::Result<Bytes> {
    let mut reader = payload
        .range_byte_stream(range.clone())
        .await?
        .into_async_read();
    let mut buf: Vec<u8> = Vec::with_capacity(range.count());
    tokio::io::copy(&mut reader, &mut buf).await?;
    Ok(Bytes::from(buf))
}

pub fn parse_azure_uri(uri: &Uri) -> Option<(String, PathBuf)> {
    // Ex: azure://container/prefix.
    static URI_PTN: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"azure(\+[^:]+)?://(?P<container>[^/]+)(/(?P<prefix>.+))?")
            .expect("The regular expression should compile.")
    });

    let captures = URI_PTN.captures(uri.as_str())?;

    let container = captures.name("container")?.as_str().to_string();
    let prefix = captures
        .name("prefix")
        .map(|prefix_match| PathBuf::from(prefix_match.as_str()))
        .unwrap_or_default();
    Some((container, prefix))
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
            ErrorKind::Io | ErrorKind::Connection => true,
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
            ErrorKind::Io | ErrorKind::Connection => StorageErrorKind::Io.with_error(err),
            ErrorKind::Credential => StorageErrorKind::Unauthorized.with_error(err),
            _ => StorageErrorKind::Internal.with_error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use quickwit_common::uri::Uri;

    use super::{AzureBlobStorage, AzureCredential, crc64_nvme, parse_azure_uri};

    #[test]
    fn test_crc64_nvme_matches_the_service_vector() {
        assert_eq!(crc64_nvme(b"hello"), vec![87, 66, 82, 6, 112, 133, 119, 51]);
    }

    #[test]
    fn test_blob_name_trims_slashes() {
        let storage = AzureBlobStorage::new(
            "devstoreaccount1",
            AzureCredential::SharedKey(
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/\
                 KBHBeksoGMGw=="
                    .to_string(),
            ),
            None,
            Some("http://127.0.0.1:10000/devstoreaccount1".to_string()),
            Uri::for_test("azure://container"),
            "container",
        )
        .unwrap()
        .with_prefix(PathBuf::from("/index/"));
        assert_eq!(
            storage.blob_name(Path::new("split.split")),
            "index/split.split"
        );
    }

    #[cfg(feature = "integration-testsuite")]
    #[tokio::test]
    #[cfg_attr(not(feature = "ci-test"), ignore)]
    async fn test_retrying_a_successful_commit_succeeds() -> anyhow::Result<()> {
        use bytes::Bytes;
        use quickwit_common::rand::append_random_suffix;
        use ulid::Ulid;

        use super::{format_block_id, stage_block};
        use crate::Storage;

        let container_name = append_random_suffix("quickwit-commit-retry").to_lowercase();
        AzureBlobStorage::create_emulated_container(&container_name).await?;
        let storage = AzureBlobStorage::new_emulated(&container_name);
        let block_blob_client = storage
            .container_client
            .blob_client("blob")
            .block_blob_client();
        let block_id = format_block_id(Ulid::new(), 0);
        stage_block(&block_blob_client, &block_id, Bytes::from_static(b"hello")).await?;

        storage
            .commit_block_list(&block_blob_client, vec![block_id.clone()])
            .await?;
        storage
            .commit_block_list(&block_blob_client, vec![block_id])
            .await?;
        let content = storage.get_all(Path::new("blob")).await?;
        assert_eq!(content.as_slice(), b"hello");

        AzureBlobStorage::delete_emulated_container(&container_name).await?;
        Ok(())
    }

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
