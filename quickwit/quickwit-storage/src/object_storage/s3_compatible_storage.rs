// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fmt, io};

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use aws_smithy_http::byte_stream::ByteStream;
use base64::prelude::{Engine, BASE64_STANDARD};
use futures::{stream, StreamExt};
use once_cell::sync::{Lazy, OnceCell};
use quickwit_aws::get_aws_config;
use quickwit_aws::retry::{retry, Retry, RetryParams, Retryable};
use quickwit_common::uri::Uri;
use quickwit_common::{chunk_range, into_u64_range};
use quickwit_config::S3StorageConfig;
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;
use tracing::{info, instrument, warn};

use crate::object_storage::MultiPartPolicy;
use crate::storage::{BulkDeleteError, DeleteFailure, SendableAsync};
use crate::{
    OwnedBytes, Storage, StorageError, StorageErrorKind, StorageResolverError, StorageResult,
    STORAGE_METRICS,
};

/// Semaphore to limit the number of concurent requests to the object store. Some object stores
/// (R2, SeaweedFs...) return errors when too many concurent requests are emitted.
static REQUEST_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| {
    let num_permits: usize = env::var("QW_S3_MAX_CONCURRENCY")
        .as_deref()
        .unwrap_or("10000")
        .parse()
        .expect("QW_S3_MAX_CONCURRENCY value should be a number.");
    Semaphore::new(num_permits)
});

/// S3-compatible object storage implementation.
pub struct S3CompatibleObjectStorage {
    s3_client: S3Client,
    uri: Uri,
    bucket: String,
    prefix: PathBuf,
    multipart_policy: MultiPartPolicy,
    retry_params: RetryParams,
    disable_multi_object_delete_requests: bool,
}

impl fmt::Debug for S3CompatibleObjectStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("S3CompatibleObjectStorage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

async fn create_s3_client(s3_storage_config: &S3StorageConfig) -> S3Client {
    let aws_config = get_aws_config().await;
    let mut s3_config = aws_sdk_s3::Config::builder().region(aws_config.region().cloned());

    s3_config.set_retry_config(aws_config.retry_config().cloned());
    s3_config.set_credentials_provider(aws_config.credentials_provider().cloned());
    s3_config.set_http_connector(aws_config.http_connector().cloned());
    s3_config.set_timeout_config(aws_config.timeout_config().cloned());
    s3_config.set_credentials_cache(aws_config.credentials_cache().cloned());
    s3_config.set_sleep_impl(Some(Arc::new(quickwit_aws::TokioSleep::default())));
    s3_config.set_force_path_style(s3_storage_config.force_path_style_access());

    if let Some(endpoint) = s3_storage_config.endpoint() {
        info!(endpoint=%endpoint, "Using custom S3 endpoint.");
        s3_config.set_endpoint_url(Some(endpoint));
    }
    S3Client::from_conf(s3_config.build())
}

impl S3CompatibleObjectStorage {
    /// Creates an object storage given a region and a bucket name.
    pub async fn new(
        s3_storage_config: &S3StorageConfig,
        uri: Uri,
        bucket: String,
    ) -> Result<Self, StorageResolverError> {
        let s3_client = create_s3_client(s3_storage_config).await;
        let retry_params = RetryParams {
            max_attempts: 3,
            ..Default::default()
        };
        Ok(Self {
            s3_client,
            uri,
            bucket,
            prefix: PathBuf::new(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params,
            disable_multi_object_delete_requests: s3_storage_config
                .disable_multi_object_delete_requests,
        })
    }

    /// Creates an object storage given a region and an uri.
    pub async fn from_uri(
        s3_storage_config: &S3StorageConfig,
        uri: &Uri,
    ) -> Result<Self, StorageResolverError> {
        let (bucket, prefix) = parse_s3_uri(uri).ok_or_else(|| {
            let message = format!("Failed to extract bucket name from S3 URI: {uri}");
            StorageResolverError::InvalidUri(message)
        })?;
        let storage = Self::new(s3_storage_config, uri.clone(), bucket).await?;
        Ok(storage.with_prefix(prefix))
    }

    /// Sets a specific for all buckets.
    ///
    /// This method overrides any existing prefix. (It does NOT
    /// append the argument to any existing prefix.)
    pub fn with_prefix(self, prefix: PathBuf) -> Self {
        Self {
            s3_client: self.s3_client,
            uri: self.uri,
            bucket: self.bucket,
            prefix,
            multipart_policy: self.multipart_policy,
            retry_params: self.retry_params,
            disable_multi_object_delete_requests: self.disable_multi_object_delete_requests,
        }
    }

    /// Sets the multipart policy.
    ///
    /// See `MultiPartPolicy`.
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }
}

pub fn parse_s3_uri(uri: &Uri) -> Option<(String, PathBuf)> {
    static S3_URI_PTN: OnceCell<Regex> = OnceCell::new();

    let captures = S3_URI_PTN
        .get_or_init(|| {
            // s3://bucket/path/to/object
            Regex::new(r"s3(\+[^:]+)?://(?P<bucket>[^/]+)(/(?P<prefix>.+))?")
                .expect("The regular expression should compile.")
        })
        .captures(uri.as_str())?;

    let bucket = captures.name("bucket")?.as_str().to_string();
    let prefix = captures
        .name("prefix")
        .map(|prefix_match| PathBuf::from(prefix_match.as_str()))
        .unwrap_or_default();
    Some((bucket, prefix))
}

#[derive(Clone, Debug)]
struct MultipartUploadId(pub String);

#[derive(Clone, Debug)]
struct Part {
    pub part_number: usize,
    pub range: Range<u64>,
    pub md5: md5::Digest,
}

impl Part {
    fn len(&self) -> u64 {
        self.range.end - self.range.start
    }
}

const MD5_CHUNK_SIZE: usize = 1_000_000;

async fn compute_md5<T: AsyncRead + std::marker::Unpin>(mut read: T) -> io::Result<md5::Digest> {
    let mut checksum = md5::Context::new();
    let mut buf = vec![0; MD5_CHUNK_SIZE];
    loop {
        let read_len = read.read(&mut buf).await?;
        checksum.consume(&buf[..read_len]);
        if read_len == 0 {
            return Ok(checksum.compute());
        }
    }
}

impl S3CompatibleObjectStorage {
    fn key(&self, relative_path: &Path) -> String {
        // FIXME: This may not work on Windows.
        let key_path = self.prefix.join(relative_path);
        key_path.to_string_lossy().to_string()
    }

    fn relative_path(&self, key: &str) -> PathBuf {
        // FIXME: This may not work on Windows.
        Path::new(key)
            .strip_prefix(&self.prefix)
            .expect("The prefix should have been prepended to the key before this method call.")
            .to_path_buf()
    }

    async fn put_single_part_single_try<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        payload: Box<dyn crate::PutPayload>,
        len: u64,
    ) -> Result<(), Retry<StorageError>> {
        let body = payload
            .byte_stream()
            .await
            .map_err(|io_error| Retry::Permanent(StorageError::from(io_error)))?;
        self.s3_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .content_length(len as i64)
            .send()
            .await
            .map_err(|sdk_error| {
                if sdk_error.is_retryable() {
                    Retry::Transient(StorageError::from(sdk_error))
                } else {
                    Retry::Permanent(StorageError::from(sdk_error))
                }
            })?;

        crate::STORAGE_METRICS.object_storage_put_parts.inc();
        crate::STORAGE_METRICS
            .object_storage_upload_num_bytes
            .inc_by(len);
        Ok(())
    }

    async fn put_single_part<'a>(
        &'a self,
        key: &'a str,
        payload: Box<dyn crate::PutPayload>,
        len: u64,
    ) -> StorageResult<()> {
        let bucket = &self.bucket;
        retry(&self.retry_params, || async {
            self.put_single_part_single_try(bucket, key, payload.clone(), len)
                .await
        })
        .await
        .map_err(|error| error.into_inner())?;
        Ok(())
    }

    async fn create_multipart_upload(&self, key: &str) -> StorageResult<MultipartUploadId> {
        let upload_id = retry(&self.retry_params, || async {
            self.s3_client
                .create_multipart_upload()
                .bucket(self.bucket.clone())
                .key(key)
                .send()
                .await
        })
        .await?
        .upload_id
        .ok_or_else(|| {
            StorageErrorKind::InternalError
                .with_error(anyhow!("The returned multipart upload id was null."))
        })?;
        Ok(MultipartUploadId(upload_id))
    }

    async fn create_multipart_requests(
        &self,
        payload: Box<dyn crate::PutPayload>,
        len: u64,
        part_len: u64,
    ) -> io::Result<Vec<Part>> {
        assert!(len > 0);
        let multipart_ranges = chunk_range(0..len as usize, part_len as usize)
            .map(into_u64_range)
            .collect::<Vec<_>>();

        let mut parts = Vec::with_capacity(multipart_ranges.len());

        for (multipart_id, multipart_range) in multipart_ranges.into_iter().enumerate() {
            let read = payload
                .range_byte_stream(multipart_range.clone())
                .await?
                .into_async_read();
            let md5 = compute_md5(read).await?;

            let part = Part {
                part_number: multipart_id + 1, // parts are 1-indexed
                range: multipart_range,
                md5,
            };
            parts.push(part);
        }
        Ok(parts)
    }

    async fn upload_part<'a>(
        &'a self,
        upload_id: MultipartUploadId,
        key: &'a str,
        part: Part,
        payload: Box<dyn crate::PutPayload>,
    ) -> Result<CompletedPart, Retry<StorageError>> {
        let byte_stream = payload
            .range_byte_stream(part.range.clone())
            .await
            .map_err(StorageError::from)
            .map_err(Retry::Permanent)?;
        let md5 = BASE64_STANDARD.encode(part.md5.0);
        crate::STORAGE_METRICS.object_storage_put_parts.inc();
        crate::STORAGE_METRICS
            .object_storage_upload_num_bytes
            .inc_by(part.len());

        let upload_part_output = self
            .s3_client
            .upload_part()
            .bucket(self.bucket.clone())
            .key(key)
            .body(byte_stream)
            .content_length(part.len() as i64)
            .content_md5(md5)
            .part_number(part.part_number as i32)
            .upload_id(upload_id.0)
            .send()
            .await
            .map_err(|s3_err| {
                if s3_err.is_retryable() {
                    Retry::Transient(StorageError::from(s3_err))
                } else {
                    Retry::Permanent(StorageError::from(s3_err))
                }
            })?;

        let completed_part = CompletedPart::builder()
            .set_e_tag(upload_part_output.e_tag().map(|tag| tag.to_string()))
            .part_number(part.part_number as i32)
            .build();
        Ok(completed_part)
    }

    async fn put_multi_part<'a>(
        &'a self,
        key: &'a str,
        payload: Box<dyn crate::PutPayload>,
        part_len: u64,
        total_len: u64,
    ) -> StorageResult<()> {
        let upload_id = self.create_multipart_upload(key).await?;
        let parts = self
            .create_multipart_requests(payload.clone(), total_len, part_len)
            .await?;
        let max_concurrent_upload = self.multipart_policy.max_concurrent_uploads();
        let completed_parts_res: StorageResult<Vec<CompletedPart>> =
            stream::iter(parts.into_iter().map(|part| {
                let payload = payload.clone();
                let upload_id = upload_id.clone();
                retry(&self.retry_params, move || {
                    self.upload_part(upload_id.clone(), key, part.clone(), payload.clone())
                })
            }))
            .buffered(max_concurrent_upload)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|res| res.map_err(|e| e.into_inner()))
            .collect();
        match completed_parts_res {
            Ok(completed_parts) => {
                self.complete_multipart_upload(key, completed_parts, &upload_id.0)
                    .await
            }
            Err(upload_error) => {
                let abort_multipart_upload_res: StorageResult<()> =
                    self.abort_multipart_upload(key, &upload_id.0).await;
                if let Err(abort_error) = abort_multipart_upload_res {
                    warn!(
                        key = %key,
                        error = ?abort_error,
                        "Failed to abort multipart upload."
                    );
                }
                Err(upload_error)
            }
        }
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        completed_parts: Vec<CompletedPart>,
        upload_id: &str,
    ) -> StorageResult<()> {
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        retry(&self.retry_params, || async {
            self.s3_client
                .complete_multipart_upload()
                .bucket(self.bucket.clone())
                .key(key)
                .multipart_upload(completed_upload.clone())
                .upload_id(upload_id)
                .send()
                .await
        })
        .await?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> StorageResult<()> {
        retry(&self.retry_params, || async {
            self.s3_client
                .abort_multipart_upload()
                .bucket(self.bucket.clone())
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
        })
        .await?;
        Ok(())
    }

    async fn create_get_object_request(
        &self,
        path: &Path,
        range_opt: Option<Range<usize>>,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
        let key = self.key(path);
        let range_str = range_opt.map(|range| format!("bytes={}-{}", range.start, range.end - 1));
        crate::STORAGE_METRICS.object_storage_get_total.inc();

        let get_object_output = self
            .s3_client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .set_range(range_str)
            .send()
            .await?;
        Ok(get_object_output)
    }

    async fn get_to_vec(
        &self,
        path: &Path,
        range_opt: Option<Range<usize>>,
    ) -> StorageResult<Vec<u8>> {
        let cap = range_opt.as_ref().map(Range::len).unwrap_or(0);
        let get_object_output = retry(&self.retry_params, || {
            self.create_get_object_request(path, range_opt.clone())
        })
        .await?;
        let mut buf: Vec<u8> = Vec::with_capacity(cap);
        download_all(get_object_output.body, &mut buf).await?;
        Ok(buf)
    }

    /// Bulk delete implementation based on the DeleteObject API:
    /// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
    async fn bulk_delete_single<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
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

    /// Bulk delete implementation based on the DeleteObjects API, also called Multi-Object Delete
    /// API: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    async fn bulk_delete_multi<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let mut error = None;
        let mut successes = Vec::with_capacity(paths.len());
        let mut failures = HashMap::new();
        let mut unattempted = Vec::new();

        #[cfg(test)]
        const MAX_NUM_KEYS: usize = 3;

        #[cfg(not(test))]
        const MAX_NUM_KEYS: usize = 1_000;

        for chunk in paths.chunks(MAX_NUM_KEYS) {
            if error.is_some() {
                unattempted.extend(chunk.iter().map(|path| path.to_path_buf()));
                continue;
            }
            let objects: Vec<ObjectIdentifier> = chunk
                .iter()
                .map(|path| ObjectIdentifier::builder().key(self.key(path)).build())
                .collect();
            let delete = Delete::builder().set_objects(Some(objects)).build();
            let delete_objects_res = retry(&self.retry_params, || async {
                self.s3_client
                    .delete_objects()
                    .bucket(self.bucket.clone())
                    .delete(delete.clone())
                    .send()
                    .await
            })
            .await;

            match delete_objects_res {
                Ok(delete_objects_output) => {
                    if let Some(deleted_objects) = delete_objects_output.deleted {
                        for deleted_object in deleted_objects {
                            if let Some(key) = deleted_object.key {
                                let path = self.relative_path(&key);
                                successes.push(path);
                            }
                        }
                    }
                    if let Some(s3_errors) = delete_objects_output.errors {
                        for s3_error in s3_errors {
                            if let Some(key) = s3_error.key {
                                let path = self.relative_path(&key);
                                match s3_error.code {
                                    Some(code) if code == "NoSuchKey" => {
                                        successes.push(path);
                                    }
                                    _ => {
                                        let failure = DeleteFailure {
                                            code: s3_error.code,
                                            message: s3_error.message,
                                            ..Default::default()
                                        };
                                        failures.insert(path, failure);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(delete_objects_error) => {
                    error = Some(delete_objects_error.into());
                    unattempted.extend(chunk.iter().map(|path| path.to_path_buf()));
                }
            }
        }
        if error.is_none() && failures.is_empty() {
            Ok(())
        } else {
            Err(BulkDeleteError {
                error,
                successes,
                failures,
                unattempted,
            })
        }
    }
}

async fn download_all(byte_stream: ByteStream, output: &mut Vec<u8>) -> io::Result<()> {
    output.clear();
    let mut body_stream_reader = BufReader::new(byte_stream.into_async_read());
    let num_bytes_copied = tokio::io::copy_buf(&mut body_stream_reader, output).await?;
    STORAGE_METRICS
        .object_storage_download_num_bytes
        .inc_by(num_bytes_copied);
    // When calling `get_all`, the Vec capacity is not properly set.
    output.shrink_to_fit();
    Ok(())
}

#[async_trait]
impl Storage for S3CompatibleObjectStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        // we ignore error as we never close the semaphore
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        self.s3_client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .max_keys(1)
            .send()
            .await?;
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        crate::STORAGE_METRICS.object_storage_put_total.inc();
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let key = self.key(path);
        let total_len = payload.len();
        let part_num_bytes = self.multipart_policy.part_num_bytes(total_len);
        if part_num_bytes >= total_len {
            self.put_single_part(&key, payload, total_len).await?;
        } else {
            self.put_multi_part(&key, payload, part_num_bytes, total_len)
                .await?;
        }
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let get_object_output = retry(&self.retry_params, || {
            self.create_get_object_request(path, None)
        })
        .await?;
        let mut body_read = BufReader::new(get_object_output.body.into_async_read());
        let num_bytes_copied = tokio::io::copy_buf(&mut body_read, output).await?;
        STORAGE_METRICS
            .object_storage_download_num_bytes
            .inc_by(num_bytes_copied);
        output.flush().await?;
        Ok(())
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let bucket = self.bucket.clone();
        let key = self.key(path);
        let delete_res = retry(&self.retry_params, || async {
            self.s3_client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
        })
        .await;

        match delete_res {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Some("NoSuchKey") => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        if self.disable_multi_object_delete_requests {
            self.bulk_delete_single(paths).await
        } else {
            self.bulk_delete_multi(paths).await
        }
    }

    #[instrument(level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        self.get_to_vec(path, Some(range.clone()))
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!(
                    "Failed to fetch slice {:?} for object: {}/{}",
                    range,
                    self.uri,
                    path.display(),
                ))
            })
    }

    #[instrument(level = "debug", skip(self), fields(num_bytes_fetched))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let bytes = self
            .get_to_vec(path, None)
            .await
            .map(OwnedBytes::new)
            .map_err(|err| {
                err.add_context(format!(
                    "Failed to fetch object: {}/{}",
                    self.uri,
                    path.display()
                ))
            })?;
        tracing::Span::current().record("num_bytes_fetched", bytes.len());
        Ok(bytes)
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let bucket = self.bucket.clone();
        let key = self.key(path);
        let head_object_output = retry(&self.retry_params, || async {
            self.s3_client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
        })
        .await?;

        Ok(head_object_output.content_length() as u64)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_client::test_connection::TestConnection;
    use bytes::Bytes;
    use hyper::{http, Body};
    use quickwit_common::chunk_range;
    use quickwit_common::uri::Uri;

    use super::*;
    use crate::{MultiPartPolicy, S3CompatibleObjectStorage};

    #[tokio::test]
    async fn test_md5_calc() -> std::io::Result<()> {
        let data = (0..1_500_000).map(|el| el as u8).collect::<Vec<_>>();
        let md5 = compute_md5(data.as_slice()).await?;
        assert_eq!(md5, md5::compute(data));

        Ok(())
    }

    #[test]
    fn test_split_range_into_chunks_inexact() {
        assert_eq!(
            chunk_range(0..11, 3).collect::<Vec<_>>(),
            vec![0..3, 3..6, 6..9, 9..11]
        );
    }
    #[test]
    fn test_split_range_into_chunks_exact() {
        assert_eq!(
            chunk_range(0..9, 3).collect::<Vec<_>>(),
            vec![0..3, 3..6, 6..9]
        );
    }

    #[test]
    fn test_split_range_empty() {
        assert!(chunk_range(0..0, 1).collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_parse_uri() {
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("s3://bucket/path/to/object")),
            Some(("bucket".to_string(), PathBuf::from("path/to/object")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("s3://bucket/path")),
            Some(("bucket".to_string(), PathBuf::from("path")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("s3://bucket/path/to/object")),
            Some(("bucket".to_string(), PathBuf::from("path/to/object")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("s3://bucket/")),
            Some(("bucket".to_string(), PathBuf::from("")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("s3://bucket")),
            Some(("bucket".to_string(), PathBuf::from("")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::from_well_formed("ram://path/to/file")),
            None
        );
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_relative_path() {
        let sdk_config = aws_config::load_from_env().await;
        let s3_client = S3Client::new(&sdk_config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();
        let prefix = PathBuf::new();

        let mut s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::default(),
            disable_multi_object_delete_requests: false,
        };
        assert_eq!(
            s3_storage.relative_path("indexes/foo"),
            PathBuf::from("indexes/foo")
        );

        s3_storage.prefix = PathBuf::from("indexes");

        assert_eq!(
            s3_storage.relative_path("indexes/foo"),
            PathBuf::from("foo")
        );
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_single() {
        let client = TestConnection::new(vec![
            (
                http::Request::builder()
                    .body(SdkBody::from(Body::empty()))
                    .unwrap(),
                http::Response::builder()
                    .body(SdkBody::from(Body::empty()))
                    .unwrap(),
            ),
            (
                http::Request::builder()
                    .body(SdkBody::from(Body::empty()))
                    .unwrap(),
                http::Response::builder()
                    .body(SdkBody::from(Body::empty()))
                    .unwrap(),
            ),
        ]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .region(Some(Region::new("Foo")))
            .http_connector(client.clone())
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();
        let prefix = PathBuf::new();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::default(),
            disable_multi_object_delete_requests: true,
        };
        let _ = s3_storage
            .bulk_delete(&[Path::new("foo"), Path::new("bar")])
            .await;

        let requests = client.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0]
            .actual
            .uri()
            .to_string()
            .ends_with("DeleteObject"));
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_multi() {
        let client = TestConnection::new(vec![(
            http::Request::builder()
                .body(SdkBody::from(Body::empty()))
                .unwrap(),
            http::Response::builder()
                .body(SdkBody::from(Body::empty()))
                .unwrap(),
        )]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .region(Some(Region::new("Foo")))
            .http_connector(client.clone())
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();
        let prefix = PathBuf::new();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::default(),
            disable_multi_object_delete_requests: false,
        };
        let _ = s3_storage
            .bulk_delete(&[Path::new("foo"), Path::new("bar")])
            .await;

        let requests = client.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]
            .actual
            .uri()
            .to_string()
            .ends_with("DeleteObjects"));
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_multi_errors() {
        let client = TestConnection::new(vec![
            (
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder().body(SdkBody::from(Body::empty())).unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(Body::from(Bytes::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                            <Deleted>
                                <Key>foo</Key>
                            </Deleted>
                            <Error>
                                <Key>bar</Key>
                                <Code>NoSuchKey</Code>
                                <Message>The specified key does not exist</Message>
                            </Error>
                            <Error>
                                <Key>baz</Key>
                                <Code>AccessDenied</Code>
                                <Message>Access Denied</Message>
                            </Error>
                        </DeleteResult>"#
                    ))))
                    .unwrap()
            ),
            (
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder().body(SdkBody::from(Body::empty())).unwrap(),
                http::Response::builder()
                    .status(400)
                    .body(SdkBody::from(Body::from(Bytes::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>MalformedXML</Code>
                            <Message>The XML you provided was not well-formed or did not validate against our published schema.</Message>
                            <RequestId>264A17BF16E9E80A</RequestId>
                            <HostId>P3xqrhuhYxlrefdw3rEzmJh8z5KDtGzb+/FB7oiQaScI9Yaxd8olYXc7d1111ab+</HostId>
                        </Error>"#
                    ))))
                    .unwrap()
            ),
        ]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .region(Some(Region::new("Foo")))
            .http_connector(client)
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();
        let prefix = PathBuf::new();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::default(),
            disable_multi_object_delete_requests: false,
        };
        let bulk_delete_error = s3_storage
            .bulk_delete(&[
                Path::new("foo"),
                Path::new("bar"),
                Path::new("baz"),
                Path::new("foobar"),
                Path::new("foobaz"),
                Path::new("barfoo"),
                Path::new("barbaz"),
            ])
            .await
            .unwrap_err();

        assert_eq!(
            bulk_delete_error.successes,
            [PathBuf::from("foo"), PathBuf::from("bar")]
        );
        let failure = bulk_delete_error.failures.get(Path::new("baz")).unwrap();
        assert_eq!(failure.code.as_ref().unwrap(), "AccessDenied");
        assert_eq!(failure.message.as_ref().unwrap(), "Access Denied");
        assert!(failure.error.is_none());

        assert_eq!(
            bulk_delete_error.unattempted,
            [
                PathBuf::from("foobar"),
                PathBuf::from("foobaz"),
                PathBuf::from("barfoo"),
                PathBuf::from("barbaz")
            ]
        );
        let delete_objects_error = bulk_delete_error.error.unwrap();
        assert!(delete_objects_error.to_string().contains("MalformedXML"));
    }
}
