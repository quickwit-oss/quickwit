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

use std::num::NonZeroU32;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use azure_core::error::ErrorKind;
use azure_core::{Pageable, StatusCode};
use azure_storage::core::prelude::*;
use azure_storage::Error as AzureError;
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::stream::StreamExt;
use md5::Digest;
use once_cell::sync::OnceCell;
use quickwit_aws::retry::{retry, RetryParams, Retryable};
use quickwit_common::{chunk_range, into_u64_range};
use regex::Regex;
use tantivy::directory::OwnedBytes;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use crate::{
    MultiPartPolicy, PutPayload, Storage, StorageError, StorageErrorKind, StorageFactory,
    StorageResolverError, StorageResult,
};

/// Azure object storage URI resolver.
#[derive(Default)]
pub struct AzureCompatibleBlobStorageFactory;

impl StorageFactory for AzureCompatibleBlobStorageFactory {
    fn protocol(&self) -> String {
        "azure".to_string()
    }

    fn resolve(&self, uri: &str) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = AzureCompatibleBlobStorage::from_uri(uri)?;
        Ok(Arc::new(storage))
    }
}

/// Azure object storage implementation
pub struct AzureCompatibleBlobStorage {
    container_client: ContainerClient,
    account: String,
    container: String,
    prefix: PathBuf,
    multipart_policy: MultiPartPolicy,
    retry_params: RetryParams,
}

impl fmt::Debug for AzureCompatibleBlobStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AzureCompatibleBlobStorage")
            .field("container", &self.container)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl AzureCompatibleBlobStorage {
    /// Creates an object storage.
    pub fn new(account: &str, access_key: &str, container: &str) -> AzureCompatibleBlobStorage {
        let container_client =
            StorageClient::new_access_key(account, access_key).container_client(container);
        AzureCompatibleBlobStorage {
            container_client,
            account: account.to_string(),
            container: container.to_string(),
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams {
                max_attempts: 3,
                ..Default::default()
            },
        }
    }

    /// Sets the prefix path.
    ///
    /// The existing prefix is overwritten.
    pub fn with_prefix(self, prefix: &Path) -> Self {
        AzureCompatibleBlobStorage {
            container_client: self.container_client,
            account: self.account,
            container: self.container,
            prefix: prefix.to_path_buf(),
            multipart_policy: self.multipart_policy,
            retry_params: self.retry_params,
        }
    }

    /// Creates an emulated storage for testing.
    #[cfg(feature = "testsuite")]
    pub fn new_emulated(container: &str) -> Self {
        let container_client = StorageClient::new_emulator_default().container_client(container);
        AzureCompatibleBlobStorage {
            container_client,
            account: container.to_string(),
            container: container.to_string(),
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams {
                max_attempts: 3,
                ..Default::default()
            },
        }
    }

    /// Sets the multipart policy.
    ///
    /// See `MultiPartPolicy`.
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }

    /// Construct instance from uri.
    pub fn from_uri(uri: &str) -> Result<AzureCompatibleBlobStorage, StorageResolverError> {
        let (account_name, container, path) =
            parse_uri(uri).ok_or_else(|| StorageResolverError::InvalidUri {
                message: format!("Invalid uri: {}", uri),
            })?;
        let access_key = std::env::var("QW_AZURE_ACCESS_KEY")
            .expect("The `QW_AZURE_ACCESS_KEY` environment variable must be defined.");
        let azure_compatible_storage =
            AzureCompatibleBlobStorage::new(&account_name, &access_key, &container);
        Ok(azure_compatible_storage.with_prefix(&path))
    }

    /// Returns the blob name (a.k.a blob key)
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
            let mut response_stream = if let Some(range) = range_opt.as_ref() {
                self.container_client
                    .blob_client(&name)
                    .get()
                    .range(range.clone())
                    .into_stream()
            } else {
                self.container_client.blob_client(&name).get().into_stream()
            };

            let mut buf: Vec<u8> = Vec::with_capacity(capacity);
            download_all(&mut response_stream, &mut buf).await?;

            Result::<_, AzureErrorWrapper>::Ok(buf)
        })
        .await
        .map_err(StorageError::from)
    }

    /// Returns the absolute uri of a path for this storage.
    fn uri(&self, relative_path: &Path) -> String {
        format!(
            "azure://{}/{}/{}",
            self.account,
            self.container,
            self.blob_name(relative_path)
        )
    }

    /// Performs a single part upload.
    async fn put_single_part<'a>(
        &'a self,
        name: &'a str,
        payload: Box<dyn crate::PutPayload>,
    ) -> StorageResult<()> {
        retry(&self.retry_params, || async {
            let data = Bytes::from(payload.read_all().await?.to_vec());
            let hash = md5::compute(&data[..]);
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
        let mut upload_blocks_stream_result = tokio_stream::iter(multipart_ranges.enumerate())
            .map(|(num, range)| {
                let moved_blob_client = blob_client.clone();
                let moved_payload = payload.clone();
                async move {
                    retry(&self.retry_params, || async {
                        let block_id = format!("block:{}", num);
                        let (data, hash) =
                            extract_range_data_and_hash(moved_payload.box_clone(), range.clone())
                                .await?;
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
            .buffer_unordered(self.multipart_policy.max_concurrent_upload());

        // Concurrently upload block with limit.
        let mut block_list = BlockList::default();
        while let Some(put_block_result) = upload_blocks_stream_result.next().await {
            match put_block_result {
                Ok(block_id) => block_list
                    .blocks
                    .push(BlobBlockType::new_uncommitted(block_id)),
                Err(error) => return Err(error.into()),
            }
        }

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
impl Storage for AzureCompatibleBlobStorage {
    async fn check(&self) -> anyhow::Result<()> {
        if let Some(first_blob_result) = self
            .container_client
            .list_blobs()
            .max_results(NonZeroU32::new(1u32).unwrap())
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

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let name = self.blob_name(path);
        let mut output_stream = self.container_client.blob_client(name).get().into_stream();

        let mut dest_file = File::create(output_path).await?;
        while let Some(result) = output_stream.next().await {
            let chunk = result.map_err(AzureErrorWrapper::from)?.data;
            dest_file.write_all(&chunk).await?;
        }
        dest_file.flush().await?;
        Ok(())
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let name = self.blob_name(path);
        let delete_result: Result<_, StorageError> = self
            .container_client
            .blob_client(name.clone())
            .delete()
            .into_future()
            .await
            .map_err(|err| AzureErrorWrapper::from(err).into());
        if let Err(error) = delete_result {
            return match error.kind() {
                StorageErrorKind::DoesNotExist => Ok(()),
                _ => Err(error),
            };
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_to_vec(path, Some(range.clone()))
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!(
                    "Failed to fetch slice {:?} for object: {}",
                    range,
                    self.uri(path)
                ))
            })
    }

    #[instrument(level = "debug", skip(self), fields(fetched_bytes_len))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let data = self
            .get_to_vec(path, None)
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!("Failed to fetch object: {}", self.uri(path)))
            })?;
        tracing::Span::current().record("fetched_bytes_len", &data.len());
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

    fn uri(&self) -> String {
        format!(
            "azure://{}/{}/{}",
            self.account,
            self.container,
            self.prefix.to_string_lossy()
        )
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

pub fn parse_uri(uri: &str) -> Option<(String, String, PathBuf)> {
    // ex. azure://account/container/prefix
    static URI_PTN: OnceCell<Regex> = OnceCell::new();
    URI_PTN
        .get_or_init(|| {
            Regex::new(
                r"azure(\+[^:]+)?://(?P<account>[^/]+)(/(?P<container>[^/]+))(/(?P<path>.+))?",
            )
            .unwrap()
        })
        .captures(uri)
        .and_then(|cap| {
            cap.name("account").and_then(|account_match| {
                cap.name("container").map(|container_match| {
                    (
                        account_match.as_str().to_string(),
                        container_match.as_str().to_string(),
                        cap.name("path").map_or_else(
                            || PathBuf::from(""),
                            |path_match| PathBuf::from(path_match.as_str()),
                        ),
                    )
                })
            })
        })
}

/// Collect a download stream into an output buffer.
async fn download_all(
    chunk_stream: &mut Pageable<GetBlobResponse, AzureError>,
    output: &mut Vec<u8>,
) -> Result<(), AzureErrorWrapper> {
    output.clear();
    let object_storage_download_num_bytes = crate::STORAGE_METRICS
        .object_storage_download_num_bytes
        .clone();
    while let Some(chunk_result) = chunk_stream.next().await {
        let chunk_response = chunk_result.map_err(AzureErrorWrapper::from)?;
        let chuck_data = chunk_response.data;
        object_storage_download_num_bytes.inc_by(chuck_data.len() as u64);
        output.extend(chuck_data.as_ref());
    }
    output.shrink_to_fit();
    Ok(())
}

#[derive(Error, Debug)]
#[error("AzureErrorWrapper(inner={inner})")]
struct AzureErrorWrapper {
    inner: AzureError,
}

impl Retryable for AzureErrorWrapper {
    fn is_retryable(&self) -> bool {
        true // TODO
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
                StatusCode::NotFound => StorageErrorKind::DoesNotExist.with_error(err),
                _ => StorageErrorKind::Service.with_error(err),
            },
            ErrorKind::Io => StorageErrorKind::Io.with_error(err),
            ErrorKind::Credential => StorageErrorKind::Unauthorized.with_error(err),
            _ => StorageErrorKind::InternalError.with_error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::object_storage::azure_compatible_storage::parse_uri;

    #[test]
    fn test_parse_uri() {
        let (account, container, path) = parse_uri("azure://quickwit/indexes/wiki").unwrap();

        assert_eq!(account, "quickwit");
        assert_eq!(container, "indexes");
        assert_eq!(path.to_string_lossy().to_string(), "wiki");
    }
}
