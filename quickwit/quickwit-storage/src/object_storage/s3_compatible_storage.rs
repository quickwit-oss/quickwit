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
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, io};

use anyhow::{anyhow, Context as AnyhhowContext};
use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::builders::ObjectIdentifierBuilder;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client as S3Client;
use base64::prelude::{Engine, BASE64_STANDARD};
use futures::{stream, StreamExt};
use once_cell::sync::{Lazy, OnceCell};
use quickwit_aws::get_aws_config;
use quickwit_aws::retry::{aws_retry, AwsRetryable};
use quickwit_common::retry::{Retry, RetryParams};
use quickwit_common::uri::Uri;
use quickwit_common::{chunk_range, into_u64_range};
use quickwit_config::S3StorageConfig;
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::Semaphore;
use tracing::{info, instrument, warn};

use crate::object_storage::MultiPartPolicy;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, DeleteFailure, OwnedBytes, Storage, StorageError, StorageErrorKind,
    StorageResolverError, StorageResult, STORAGE_METRICS,
};

/// Semaphore to limit the number of concurrent requests to the object store. Some object stores
/// (R2, SeaweedFs...) return errors when too many concurrent requests are emitted.
static REQUEST_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| {
    let num_permits: usize = quickwit_common::get_from_env("QW_S3_MAX_CONCURRENCY", 10_000usize);
    Semaphore::new(num_permits)
});

/// Wrap the async read handle together with a permit to keep the permit alive
/// until the handle is dropped
struct S3AsyncRead<T: AsyncRead + Send + Unpin> {
    pub read: T,
    pub _permit: Result<tokio::sync::SemaphorePermit<'static>, tokio::sync::AcquireError>,
}

impl<T: AsyncRead + Send + Unpin> AsyncRead for S3AsyncRead<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let self_unpin = self.get_mut();
        Pin::new(&mut self_unpin.read).poll_read(cx, buf)
    }
}

/// S3-compatible object storage implementation.
pub struct S3CompatibleObjectStorage {
    s3_client: S3Client,
    uri: Uri,
    bucket: String,
    prefix: String,
    multipart_policy: MultiPartPolicy,
    retry_params: RetryParams,
    disable_multi_object_delete: bool,
    disable_multipart_upload: bool,
    // If 0, we don't have any prefix
    hash_prefix_cardinality: usize,
}

impl fmt::Debug for S3CompatibleObjectStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("S3CompatibleObjectStorage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("hash_prefix_cardinality", &self.hash_prefix_cardinality)
            .finish()
    }
}

fn get_credentials_provider(
    s3_storage_config: &S3StorageConfig,
) -> Option<SharedCredentialsProvider> {
    match (
        &s3_storage_config.access_key_id,
        &s3_storage_config.secret_access_key,
    ) {
        (Some(access_key_id), Some(secret_access_key)) => {
            info!("using S3 credentials defined in storage config");
            let credentials = Credentials::from_keys(access_key_id, secret_access_key, None);
            let credentials_provider = SharedCredentialsProvider::new(credentials);
            Some(credentials_provider)
        }
        _ => None,
    }
}

fn get_region(s3_storage_config: &S3StorageConfig) -> Option<Region> {
    s3_storage_config.region.clone().map(|region| {
        info!(region=%region, "using S3 region defined in storage config");
        Region::new(region)
    })
}

pub async fn create_s3_client(s3_storage_config: &S3StorageConfig) -> S3Client {
    let aws_config = get_aws_config().await;
    let credentials_provider =
        get_credentials_provider(s3_storage_config).or(aws_config.credentials_provider());
    let region = get_region(s3_storage_config).or(aws_config.region().cloned());
    let mut s3_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(region);

    if let Some(identity_cache) = aws_config.identity_cache() {
        s3_config.set_identity_cache(identity_cache);
    }
    s3_config.set_credentials_provider(credentials_provider);
    s3_config.set_force_path_style(s3_storage_config.force_path_style_access());
    s3_config.set_http_client(aws_config.http_client());
    s3_config.set_retry_config(aws_config.retry_config().cloned());
    s3_config.set_sleep_impl(aws_config.sleep_impl());
    s3_config.set_stalled_stream_protection(aws_config.stalled_stream_protection());
    s3_config.set_timeout_config(aws_config.timeout_config().cloned());

    if let Some(endpoint) = s3_storage_config.endpoint() {
        info!(endpoint=%endpoint, "using S3 endpoint defined in storage config or environment variable");
        s3_config.set_endpoint_url(Some(endpoint));
    }
    S3Client::from_conf(s3_config.build())
}

impl S3CompatibleObjectStorage {
    /// Creates an object storage given a region and an uri.
    pub async fn from_uri(
        s3_storage_config: &S3StorageConfig,
        uri: &Uri,
    ) -> Result<Self, StorageResolverError> {
        let s3_client = create_s3_client(s3_storage_config).await;
        Self::from_uri_and_client(s3_storage_config, uri, s3_client).await
    }

    /// Creates an object storage given a region, an uri and an S3 client.
    pub async fn from_uri_and_client(
        s3_storage_config: &S3StorageConfig,
        uri: &Uri,
        s3_client: S3Client,
    ) -> Result<Self, StorageResolverError> {
        let (bucket, prefix) = parse_s3_uri(uri).ok_or_else(|| {
            let message = format!("failed to extract bucket name from S3 URI: {uri}");
            StorageResolverError::InvalidUri(message)
        })?;
        let retry_params = RetryParams::aggressive();
        let disable_multi_object_delete = s3_storage_config.disable_multi_object_delete;
        let disable_multipart_upload = s3_storage_config.disable_multipart_upload;
        Ok(Self {
            s3_client,
            uri: uri.clone(),
            bucket,
            prefix: prefix.to_string_lossy().to_string(),
            multipart_policy: MultiPartPolicy::default(),
            retry_params,
            disable_multi_object_delete,
            disable_multipart_upload,
            hash_prefix_cardinality: s3_storage_config.hash_prefix_cardinality,
        })
    }

    /// Sets a specific for all buckets.
    ///
    /// This method overrides any existing prefix. (It does NOT
    /// append the argument to any existing prefix.)
    pub fn with_prefix(self, prefix: String) -> Self {
        Self {
            s3_client: self.s3_client,
            uri: self.uri,
            bucket: self.bucket,
            prefix,
            multipart_policy: self.multipart_policy,
            retry_params: self.retry_params,
            disable_multi_object_delete: self.disable_multi_object_delete,
            disable_multipart_upload: self.disable_multipart_upload,
            hash_prefix_cardinality: self.hash_prefix_cardinality,
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
const HEX_ALPHABET: [u8; 16] = *b"0123456789abcdef";
const UNINITIALIZED_HASH_PREFIX: &str = "00000000";

fn build_key(prefix: &str, relative_path: &str, hash_prefix_cardinality: usize) -> String {
    let mut key = String::with_capacity(
        UNINITIALIZED_HASH_PREFIX.len() + 1 + prefix.len() + 1 + relative_path.len(),
    );
    if hash_prefix_cardinality > 1 {
        key.push_str(UNINITIALIZED_HASH_PREFIX);
        key.push('/');
    }
    key.push_str(prefix);
    if key.as_bytes().last().copied() != Some(b'/') {
        key.push('/');
    }
    key.push_str(relative_path);
    // We then set up the prefix.
    if hash_prefix_cardinality > 1 {
        let key_without_prefix = &key.as_bytes()[UNINITIALIZED_HASH_PREFIX.len() + 1..];
        let mut prefix_hash: usize =
            murmurhash32::murmurhash3(key_without_prefix) as usize % hash_prefix_cardinality;
        unsafe {
            let prefix_buf: &mut [u8] = &mut key.as_bytes_mut()[..UNINITIALIZED_HASH_PREFIX.len()];
            for prefix_byte in prefix_buf {
                let hex: u8 = HEX_ALPHABET[(prefix_hash % 16) as usize];
                *prefix_byte = hex;
                if prefix_hash < 16 {
                    break;
                }
                prefix_hash /= 16;
            }
        }
    }
    key
}

impl S3CompatibleObjectStorage {
    fn key(&self, relative_path: &Path) -> String {
        build_key(
            &self.prefix,
            relative_path.to_string_lossy().as_ref(),
            self.hash_prefix_cardinality,
        )
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

        crate::STORAGE_METRICS.object_storage_put_parts.inc();
        crate::STORAGE_METRICS
            .object_storage_upload_num_bytes
            .inc_by(len);

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
        Ok(())
    }

    async fn put_single_part<'a>(
        &'a self,
        key: &'a str,
        payload: Box<dyn crate::PutPayload>,
        len: u64,
    ) -> StorageResult<()> {
        let bucket = &self.bucket;
        aws_retry(&self.retry_params, || async {
            self.put_single_part_single_try(bucket, key, payload.clone(), len)
                .await
        })
        .await
        .map_err(|error| error.into_inner())?;
        Ok(())
    }

    async fn create_multipart_upload(&self, key: &str) -> StorageResult<MultipartUploadId> {
        let upload_id = aws_retry(&self.retry_params, || async {
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
            StorageErrorKind::Internal
                .with_error(anyhow!("the returned multipart upload id was null"))
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

    fn build_delete_batch_requests<'a>(
        &self,
        delete_paths: &'a [&'a Path],
    ) -> anyhow::Result<Vec<(&'a [&'a Path], Delete)>> {
        #[cfg(test)]
        const MAX_NUM_KEYS: usize = 3;

        #[cfg(not(test))]
        const MAX_NUM_KEYS: usize = 1_000;

        let path_chunks = delete_paths.chunks(MAX_NUM_KEYS);
        let num_delete_requests = path_chunks.len();
        let mut delete_requests: Vec<(&[&Path], Delete)> = Vec::with_capacity(num_delete_requests);

        for path_chunk in path_chunks {
            let object_ids: Vec<ObjectIdentifier> = path_chunk
                .iter()
                .map(|path| {
                    let key = self.key(path);
                    ObjectIdentifierBuilder::default()
                        .key(key)
                        .build()
                        .context("failed to build object identifier")
                })
                .collect::<anyhow::Result<_>>()?;
            let delete = Delete::builder()
                .set_objects(Some(object_ids))
                .build()
                .context("failed to build delete request")?;
            delete_requests.push((path_chunk, delete));
        }
        Ok(delete_requests)
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
            .set_e_tag(upload_part_output.e_tag)
            .part_number(part.part_number as i32)
            .build();
        Ok(completed_part)
    }

    async fn put_multipart<'a>(
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
                aws_retry(&self.retry_params, move || {
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
        aws_retry(&self.retry_params, || async {
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
        aws_retry(&self.retry_params, || async {
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

    async fn get_object(
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
        let get_object_output = aws_retry(&self.retry_params, || {
            self.get_object(path, range_opt.clone())
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

        let delete_requests: Vec<(&[&Path], Delete)> = self
            .build_delete_batch_requests(paths)
            .map_err(|error: anyhow::Error| {
                let unattempted = paths.iter().copied().map(Path::to_path_buf).collect();
                BulkDeleteError {
                    error: Some(StorageErrorKind::Internal.with_error(error)),
                    successes: Default::default(),
                    failures: Default::default(),
                    unattempted,
                }
            })?;

        let mut error = None;
        let mut successes = Vec::with_capacity(paths.len());
        let mut failures = HashMap::new();
        let mut unattempted = Vec::new();

        let mut delete_requests_it = delete_requests.iter();

        for (path_chunk, delete) in &mut delete_requests_it {
            let delete_objects_res: StorageResult<DeleteObjectsOutput> =
                aws_retry(&self.retry_params, || async {
                    crate::STORAGE_METRICS
                        .object_storage_bulk_delete_requests_total
                        .inc();
                    let _timer = crate::STORAGE_METRICS
                        .object_storage_bulk_delete_request_duration
                        .start_timer();
                    self.s3_client
                        .delete_objects()
                        .bucket(self.bucket.clone())
                        .delete(delete.clone())
                        .send()
                        .await
                })
                .await
                .map_err(Into::into);

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
                    error = Some(delete_objects_error);
                    unattempted.extend(path_chunk.iter().copied().map(PathBuf::from));
                    break;
                }
            }
        }

        if error.is_none() && failures.is_empty() {
            return Ok(());
        }

        // Do we have remaining requests?
        for (path_chunk, _) in delete_requests_it {
            unattempted.extend(path_chunk.iter().copied().map(PathBuf::from));
        }

        Err(BulkDeleteError {
            error,
            successes,
            failures,
            unattempted,
        })
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
        if self.disable_multipart_upload || part_num_bytes >= total_len {
            self.put_single_part(&key, payload, total_len).await?;
        } else {
            self.put_multipart(&key, payload, part_num_bytes, total_len)
                .await?;
        }
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let _permit = REQUEST_SEMAPHORE.acquire().await;
        let get_object_output =
            aws_retry(&self.retry_params, || self.get_object(path, None)).await?;
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
        let delete_res = aws_retry(&self.retry_params, || async {
            crate::STORAGE_METRICS
                .object_storage_delete_requests_total
                .inc();
            let _timer = crate::STORAGE_METRICS
                .object_storage_delete_request_duration
                .start_timer();
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
        if self.disable_multi_object_delete {
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
    ) -> crate::StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let permit = REQUEST_SEMAPHORE.acquire().await;
        let get_object_output = aws_retry(&self.retry_params, || {
            self.get_object(path, Some(range.clone()))
        })
        .await?;
        Ok(Box::new(S3AsyncRead {
            read: get_object_output.body.into_async_read(),
            _permit: permit,
        }))
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
                    "failed to fetch object: {}/{}",
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
        let head_object_output = aws_retry(&self.retry_params, || async {
            self.s3_client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
        })
        .await?;

        Ok(head_object_output.content_length().unwrap_or(0) as u64)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
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
            parse_s3_uri(&Uri::for_test("s3://bucket/path/to/object")),
            Some(("bucket".to_string(), PathBuf::from("path/to/object")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::for_test("s3://bucket/path")),
            Some(("bucket".to_string(), PathBuf::from("path")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::for_test("s3://bucket/path/to/object")),
            Some(("bucket".to_string(), PathBuf::from("path/to/object")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::for_test("s3://bucket/")),
            Some(("bucket".to_string(), PathBuf::from("")))
        );
        assert_eq!(
            parse_s3_uri(&Uri::for_test("s3://bucket")),
            Some(("bucket".to_string(), PathBuf::from("")))
        );
        assert_eq!(parse_s3_uri(&Uri::for_test("ram://path/to/file")), None);
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_relative_path() {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::v2024_03_28())
            .load()
            .await;
        let s3_client = S3Client::new(&sdk_config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();

        let mut s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix: String::new(),
            hash_prefix_cardinality: 0,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::for_test(),
            disable_multi_object_delete: false,
            disable_multipart_upload: false,
        };
        assert_eq!(
            s3_storage.relative_path("indexes/foo"),
            PathBuf::from("indexes/foo")
        );

        s3_storage.prefix = "indexes".to_string();

        assert_eq!(
            s3_storage.relative_path("indexes/foo"),
            PathBuf::from("foo")
        );
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_single() {
        let client = StaticReplayClient::new(vec![
            ReplayEvent::new(
                http::Request::builder()
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
                http::Response::builder()
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
            ),
            ReplayEvent::new(
                http::Request::builder()
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
                http::Response::builder()
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
            ),
        ]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Some(Region::new("Foo")))
            .http_client(client.clone())
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix: String::new(),
            hash_prefix_cardinality: 0,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::for_test(),
            disable_multi_object_delete: true,
            disable_multipart_upload: false,
        };
        let _ = s3_storage
            .bulk_delete(&[Path::new("foo"), Path::new("bar")])
            .await;

        let requests = client.actual_requests().collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].uri().to_string().ends_with("DeleteObject"));
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_multi() {
        let client = StaticReplayClient::new(vec![ReplayEvent::new(
            http::Request::builder()
                .body(SdkBody::from_body_0_4(Body::empty()))
                .unwrap(),
            http::Response::builder()
                .body(SdkBody::from_body_0_4(Body::empty()))
                .unwrap(),
        )]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Some(Region::new("Foo")))
            .http_client(client.clone())
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix: String::new(),
            hash_prefix_cardinality: 0,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::for_test(),
            disable_multi_object_delete: false,
            disable_multipart_upload: false,
        };
        let _ = s3_storage
            .bulk_delete(&[Path::new("foo"), Path::new("bar")])
            .await;

        let requests = client.actual_requests().collect::<Vec<_>>();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].uri().to_string().ends_with("delete"));
    }

    #[tokio::test]
    async fn test_s3_compatible_storage_bulk_delete_multi_errors() {
        let client = StaticReplayClient::new(vec![
            ReplayEvent::new(
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder().body(SdkBody::empty()).unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from_body_0_4(Body::from(Bytes::from(
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
            ReplayEvent::new(
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder().body(SdkBody::from_body_0_4(Body::empty())).unwrap(),
                http::Response::builder()
                    .status(400)
                    .body(SdkBody::from_body_0_4(Body::from(Bytes::from(
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
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Some(Region::new("Foo")))
            .http_client(client)
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix: String::new(),
            hash_prefix_cardinality: 0,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::for_test(),
            disable_multi_object_delete: false,
            disable_multipart_upload: false,
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

    #[tokio::test]
    async fn test_s3_compatible_storage_retry_put() {
        let client = StaticReplayClient::new(vec![
            ReplayEvent::new(
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder().body(SdkBody::empty()).unwrap(),
                http::Response::builder()
                    .status(429)
                    .body(SdkBody::from_body_0_4(Body::from(Bytes::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                          <Code>SlowDown</Code>
                          <Message>message</Message>
                          <Resource>/my-path</Resource>
                          <RequestId>4442587FB7D0A2F9</RequestId>
                        </Error>"#,
                    ))))
                    .unwrap(),
            ),
            ReplayEvent::new(
                // This is quite fragile, currently this is *not* validated by the SDK
                // but may in future, that being said, there is no way to know what the
                // request should look like until it raises an error in reality as this
                // is up to how the validation is implemented.
                http::Request::builder()
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from_body_0_4(Body::empty()))
                    .unwrap(),
            ),
        ]);
        let credentials = Credentials::new("mock_key", "mock_secret", None, None, "mock_provider");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Some(Region::new("Foo")))
            .http_client(client)
            .credentials_provider(credentials)
            .build();
        let s3_client = S3Client::from_conf(config);
        let uri = Uri::for_test("s3://bucket/indexes");
        let bucket = "bucket".to_string();

        let s3_storage = S3CompatibleObjectStorage {
            s3_client,
            uri,
            bucket,
            prefix: String::new(),
            hash_prefix_cardinality: 0,
            multipart_policy: MultiPartPolicy::default(),
            retry_params: RetryParams::for_test(),
            disable_multi_object_delete: false,
            disable_multipart_upload: false,
        };
        s3_storage
            .put(Path::new("my-path"), Box::new(vec![1, 2, 3]))
            .await
            .unwrap();
    }

    #[test]
    fn test_build_key() {
        assert_eq!(build_key("hello", "coucou", 0), "hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 0), "hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 1), "hello/coucou");
        assert_eq!(build_key("hello", "coucou", 1), "hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 2), "10000000/hello/coucou");
        assert_eq!(build_key("hello", "coucou", 2), "10000000/hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 16), "d0000000/hello/coucou");
        assert_eq!(build_key("hello", "coucou", 16), "d0000000/hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 17), "50000000/hello/coucou");
        assert_eq!(build_key("hello", "coucou", 17), "50000000/hello/coucou");
        assert_eq!(build_key("hello/", "coucou", 70), "f0000000/hello/coucou");
    }
}
