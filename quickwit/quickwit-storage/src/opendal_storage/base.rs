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

use std::ops::Range;
use std::path::Path;
use std::{fmt, io};

use async_trait::async_trait;
use futures::AsyncWriteExt as FuturesAsyncWriteExt;
use opendal::{DeleteInput, IntoDeleteInput, Operator};
use quickwit_common::uri::Uri;
use quickwit_metrics::HistogramTimer;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt as TokioAsyncWriteExt};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use tracing::instrument;

use crate::metrics::object_storage_get_slice_in_flight_guards;
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, MultiPartPolicy, OwnedBytes, PutPayload, Storage, StorageError,
    StorageErrorKind, StorageResolverError, StorageResult,
};

/// OpenDAL based storage implementation.
/// # TODO
///
/// - Implement REQUEST_SEMAPHORE to control the concurrency.
/// - Implement object storage metrics.
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
        Ok(Self::from_operator(uri, op))
    }

    fn from_operator(uri: Uri, op: Operator) -> Self {
        Self {
            uri,
            op,
            // limits are the same as on S3
            multipart_policy: MultiPartPolicy::default(),
        }
    }

    #[cfg(test)]
    // Lets local HTTPS tests trust a private CA without changing global trust,
    // while still using Quickwit's GCS storage construction and read path.
    pub(super) fn new_google_cloud_storage_with_http_client_for_test(
        uri: Uri,
        cfg: opendal::services::Gcs,
        http_client: opendal::raw::HttpClient,
    ) -> Result<Self, StorageResolverError> {
        let op = Operator::new(cfg)?
            .layer(opendal::layers::HttpClientLayer::new(http_client))
            .finish();
        Ok(Self::from_operator(uri, op))
    }

    #[cfg(feature = "integration-testsuite")]
    pub fn set_policy(&mut self, multipart_policy: MultiPartPolicy) {
        self.multipart_policy = multipart_policy;
    }
}

/// We spotted a ever growing usage of RAM in the opendal GCS implementation.
/// We are using the same fix as
/// https://github.com/apache/opendal/pull/7217/
async fn copy_read_write_loop<R, W>(input: &mut R, output: &mut W) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    // Only flush to `output` once we have accumulated at least `WRITE_THRESHOLD`
    // bytes, to avoid issuing many tiny writes when `read` returns small chunks.
    // The final (possibly smaller) chunk is flushed on EOF.
    const WRITE_THRESHOLD: usize = 8 * 1024;
    let mut buf = [0u8; 2 * WRITE_THRESHOLD];
    let mut filled = 0;
    loop {
        debug_assert!(buf[filled..].len() > WRITE_THRESHOLD);
        let n = input.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled > 0 {
                output.write_all(&buf[..filled]).await?;
            }
            return Ok(());
        }
        filled += n;
        if filled >= WRITE_THRESHOLD {
            output.write_all(&buf[..filled]).await?;
            filled = 0;
        }
    }
}

#[async_trait]
impl Storage for OpendalStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.op.check().await?;
        Ok(())
    }

    #[instrument(name = "storage.gcs.put", level = "debug", skip(self, payload), fields(payload_len = payload.len()))]
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        crate::metrics::OBJECT_STORAGE_PUT_TOTAL.inc();
        let path = path.as_os_str().to_string_lossy();
        let mut payload_reader = payload.byte_stream().await?.into_async_read();

        let mut storage_writer = self
            .op
            .writer_with(&path)
            .chunk(self.multipart_policy.part_num_bytes(payload.len()) as usize)
            .await?
            .into_futures_async_write()
            .compat_write();
        // Avoid `tokio::io::copy`'s pending-read flush path and keep buffering policy explicit.
        copy_read_write_loop(&mut payload_reader, &mut storage_writer).await?;
        storage_writer.get_mut().close().await?;
        crate::metrics::OBJECT_STORAGE_UPLOAD_NUM_BYTES.inc_by(payload.len());
        Ok(())
    }

    #[instrument(name = "storage.gcs.copy_to", level = "debug", skip(self, output))]
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
        crate::metrics::OBJECT_STORAGE_DOWNLOAD_NUM_BYTES.inc_by(num_bytes_copied);
        output.flush().await?;
        Ok(())
    }

    #[instrument(name = "storage.gcs.get_slice", level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let size = range.len();
        let range = range.start as u64..range.end as u64;
        // Unlike other object store implementations, in flight requests are
        // recorded before issuing the query to the object store.
        let _inflight_guards = object_storage_get_slice_in_flight_guards(size);
        crate::metrics::OBJECT_STORAGE_GET_TOTAL.inc();
        // `Buffer::to_bytes` is zero-copy when the underlying buffer is contiguous, and coalesces
        // into a single `Bytes` otherwise — avoiding the extra `Vec<u8>` round-trip `to_vec` would
        // perform.
        let storage_content = self.op.read_with(&path).range(range).await?.to_bytes();
        Ok(into_owned_bytes(storage_content))
    }

    #[instrument(name = "storage.gcs.get_slice_stream", level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
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

    #[instrument(name = "storage.gcs.get_all", level = "debug", skip(self))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let path = path.as_os_str().to_string_lossy();
        let storage_content = self.op.read(&path).await?.to_bytes();
        Ok(into_owned_bytes(storage_content))
    }

    #[instrument(name = "storage.gcs.delete", level = "debug", skip(self))]
    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let path = path.as_os_str().to_string_lossy();
        crate::metrics::OBJECT_STORAGE_DELETE_REQUESTS_TOTAL.inc();
        let _timer = HistogramTimer::new(&crate::metrics::OBJECT_STORAGE_DELETE_REQUEST_DURATION);
        self.op.delete(&path).await?;
        Ok(())
    }

    #[instrument(name = "storage.gcs.bulk_delete", level = "debug", skip(self, paths), fields(num_paths = paths.len()))]
    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        // The mock service we used in integration testsuite doesn't support bulk delete.
        // Let's fallback to delete one by one in this case.
        #[cfg(feature = "integration-testsuite")]
        {
            let storage_info = self.op.info();
            if storage_info.name().starts_with("sample-bucket") && storage_info.scheme() == "gcs" {
                let mut bulk_error = BulkDeleteError::default();
                for (index, path) in paths.iter().enumerate() {
                    crate::metrics::OBJECT_STORAGE_BULK_DELETE_REQUESTS_TOTAL.inc();
                    let _timer = HistogramTimer::new(
                        &crate::metrics::OBJECT_STORAGE_BULK_DELETE_REQUEST_DURATION,
                    );
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

    #[instrument(name = "storage.gcs.file_num_bytes", level = "debug", skip(self))]
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use super::*;

    /// `AsyncRead` that returns a predefined sequence of chunks, one chunk per
    /// `poll_read` call. This lets us simulate a source that returns small
    /// payloads at each read.
    struct ChunkedReader {
        chunks: VecDeque<Vec<u8>>,
    }

    impl ChunkedReader {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into(),
            }
        }
    }

    impl AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                return Poll::Ready(Ok(()));
            };
            let to_copy = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..to_copy]);
            if to_copy < chunk.len() {
                let rest = chunk.split_off(to_copy);
                self.chunks.push_front(rest);
            }
            Poll::Ready(Ok(()))
        }
    }

    /// `AsyncWrite` that records the size of each `poll_write` invocation and
    /// accumulates all bytes written, so tests can assert on both content and
    /// batching granularity.
    #[derive(Default)]
    struct RecordingWriter {
        writes: Vec<usize>,
        data: Vec<u8>,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.push(buf.len());
            self.data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    const WRITE_THRESHOLD: usize = 8 * 1024;

    #[tokio::test]
    async fn test_copy_read_write_loop_batches_small_reads() {
        // 100 chunks of 100 bytes = 10_000 bytes total. Each `read` returns at
        // most 100 bytes, so without batching we'd issue 100 `write_all` calls.
        let chunks: Vec<Vec<u8>> = (0..100u8).map(|i| vec![i; 100]).collect();
        let expected: Vec<u8> = chunks.iter().flatten().copied().collect();

        let mut reader = ChunkedReader::new(chunks);
        let mut writer = RecordingWriter::default();

        copy_read_write_loop(&mut reader, &mut writer)
            .await
            .unwrap();

        assert_eq!(writer.data, expected);
        assert!(!writer.writes.is_empty());
        // Every write except possibly the last (EOF flush) must meet the
        // batching threshold.
        let last_idx = writer.writes.len() - 1;
        for (i, &n) in writer.writes.iter().enumerate() {
            if i < last_idx {
                assert!(
                    n >= WRITE_THRESHOLD,
                    "intermediate write #{i} was only {n} bytes (expected >= {WRITE_THRESHOLD})"
                );
            }
        }
        assert_eq!(writer.writes.iter().sum::<usize>(), expected.len());
        // Sanity check: we are batching, not writing one chunk at a time.
        assert!(
            writer.writes.len() < 10,
            "expected a small number of batched writes, got {writes:?}",
            writes = writer.writes
        );
    }

    #[tokio::test]
    async fn test_copy_read_write_loop_empty_input() {
        let mut reader = ChunkedReader::new(vec![]);
        let mut writer = RecordingWriter::default();
        copy_read_write_loop(&mut reader, &mut writer)
            .await
            .unwrap();
        assert!(writer.data.is_empty());
        assert!(writer.writes.is_empty());
    }

    #[tokio::test]
    async fn test_copy_read_write_loop_below_threshold_produces_single_write() {
        // 5 chunks of 100 bytes = 500 bytes, well below the 8 KB threshold.
        // The single flush should happen on EOF.
        let chunks = vec![vec![42u8; 100]; 5];
        let mut reader = ChunkedReader::new(chunks);
        let mut writer = RecordingWriter::default();
        copy_read_write_loop(&mut reader, &mut writer)
            .await
            .unwrap();
        assert_eq!(writer.data, vec![42u8; 500]);
        assert_eq!(writer.writes, vec![500]);
    }

    #[tokio::test]
    async fn test_copy_read_write_loop_exact_threshold_does_not_emit_empty_tail() {
        // Total input is an exact multiple of the threshold: ensure we don't
        // emit a spurious empty final write when EOF aligns with a flush.
        let chunk = vec![7u8; WRITE_THRESHOLD];
        let mut reader = ChunkedReader::new(vec![chunk.clone(), chunk.clone()]);
        let mut writer = RecordingWriter::default();
        copy_read_write_loop(&mut reader, &mut writer)
            .await
            .unwrap();
        assert_eq!(writer.data.len(), 2 * WRITE_THRESHOLD);
        for &n in &writer.writes {
            assert!(n >= WRITE_THRESHOLD, "unexpected small write of {n} bytes");
        }
    }

    #[tokio::test]
    async fn test_copy_read_write_loop_large_single_read() {
        // A single `read` returning ~16 KB should flow through as one write.
        let chunk = vec![3u8; 2 * WRITE_THRESHOLD];
        let mut reader = ChunkedReader::new(vec![chunk.clone()]);
        let mut writer = RecordingWriter::default();
        copy_read_write_loop(&mut reader, &mut writer)
            .await
            .unwrap();
        assert_eq!(writer.data, chunk);
        assert_eq!(writer.writes, vec![2 * WRITE_THRESHOLD]);
    }
}
