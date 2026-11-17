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

use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use quickwit_common::uri::Uri;
use quickwit_common::{rate_limited_info, rate_limited_warn};
use quickwit_config::StorageTimeoutPolicy;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;
use tokio::time::{Instant, timeout_at};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, ListObjectsStream, PutPayload, Storage, StorageErrorKind, StorageResult,
};

/// Storage proxy that implements a retry operation if the underlying storage
/// takes too long.
///
/// This is useful in order to ensure a low latency on S3.
/// Retrying agressively is recommended for S3.
/// Streamed retries resume after emitted bytes and require immutable objects.
///
/// <https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/timeouts-and-retries-for-latency-sensitive-applications.html>
#[derive(Clone, Debug)]
pub struct TimeoutAndRetryStorage {
    underlying: Arc<dyn Storage>,
    storage_timeout_policy: StorageTimeoutPolicy,
}

impl TimeoutAndRetryStorage {
    /// Creates a new `TimeoutAndRetryStorage`.
    ///
    /// See [StorageTimeoutPolicy] for more information.
    pub fn new(storage: Arc<dyn Storage>, storage_timeout_policy: StorageTimeoutPolicy) -> Self {
        TimeoutAndRetryStorage {
            underlying: storage,
            storage_timeout_policy,
        }
    }
}

#[async_trait]
impl Storage for TimeoutAndRetryStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.underlying.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.underlying.put(path, payload).await
    }

    fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        path: &'life1 Path,
        output: &'life2 mut dyn SendableAsync,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = StorageResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.underlying.copy_to(path, output)
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<u64> {
        self.underlying.copy_to_file(path, output_path).await
    }

    /// Downloads a slice of a file from the storage, and returns an in memory buffer
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let num_bytes = range.len();
        for (attempt_id, timeout_duration) in self
            .storage_timeout_policy
            .compute_timeout(num_bytes)
            .enumerate()
        {
            let get_slice_fut = self.underlying.get_slice(path, range.clone());
            // TODO test avoid aborting timed out requests. #5468
            match tokio::time::timeout(timeout_duration, get_slice_fut).await {
                Ok(Ok(bytes)) => {
                    match attempt_id {
                        0 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_0_TIMEOUT.inc(),
                        1 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_1_TIMEOUT.inc(),
                        _ => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_2_PLUS_TIMEOUT.inc(),
                    }
                    return Ok(bytes);
                }
                Ok(Err(error)) => return Err(error),
                Err(_elapsed) => {
                    rate_limited_info!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), timeout_secs=timeout_duration.as_secs_f32(), "get timeout elapsed");
                }
            }
        }
        rate_limited_warn!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), "all get_slice attempts timeouted");
        crate::metrics::GET_SLICE_TIMEOUT_ALL_TIMEOUTS.inc();
        return Err(
            StorageErrorKind::Timeout.with_error(anyhow::anyhow!("internal timeout on get_slice"))
        );
    }

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        let num_bytes = range.len();
        for (attempt_id, timeout_duration) in self
            .storage_timeout_policy
            .compute_timeout(num_bytes)
            .enumerate()
        {
            // The timeout covers the whole body read. Each retry overwrites the same buffer,
            // and cancellation drops the previous stream.
            let download = self.underlying.get_slice_into(path, range.clone(), buffer);
            match tokio::time::timeout(timeout_duration, download).await {
                Ok(Ok(())) => {
                    match attempt_id {
                        0 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_0_TIMEOUT.inc(),
                        1 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_1_TIMEOUT.inc(),
                        _ => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_2_PLUS_TIMEOUT.inc(),
                    }
                    return Ok(());
                }
                Ok(Err(error)) => return Err(error),
                Err(_elapsed) => {
                    rate_limited_info!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), timeout_secs=timeout_duration.as_secs_f32(), "get timeout elapsed");
                }
            }
        }
        rate_limited_warn!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), "all get_slice attempts timeouted");
        crate::metrics::GET_SLICE_TIMEOUT_ALL_TIMEOUTS.inc();
        Err(StorageErrorKind::Timeout.with_error(anyhow::anyhow!("internal timeout on get_slice")))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let mut timeouts = self.storage_timeout_policy.compute_timeout(range.len());
        let (reader, deadline) =
            open_stream_with_retries(self.underlying.as_ref(), path, &range, &mut timeouts)
                .await?
                .ok_or_else(|| {
                    StorageErrorKind::Timeout
                        .with_error(anyhow::anyhow!("internal timeout on get_slice_stream"))
                })?;
        let storage = self.underlying.clone();
        let path = Arc::new(path.to_owned());
        let state = (range, timeouts, ReaderStream::new(reader), deadline);
        let chunks = stream::try_unfold(
            state,
            move |(mut range, mut timeouts, mut reader, mut deadline)| {
                let storage = storage.clone();
                let path = path.clone();
                async move {
                    loop {
                        match timeout_at(deadline, reader.next()).await {
                            Ok(Some(result)) => {
                                let bytes = result?;
                                if bytes.len() > range.len() {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "get_slice_stream returned more bytes than requested",
                                    ));
                                }
                                range.start += bytes.len();
                                return Ok(Some((bytes, (range, timeouts, reader, deadline))));
                            }
                            Ok(None) => return Ok(None),
                            Err(_) => {}
                        }
                        // Drop the timed-out reader before reopening.
                        drop(reader);
                        // A timeout while verifying EOF cannot be retried with an empty range.
                        let reopened = if range.is_empty() {
                            None
                        } else {
                            open_stream_with_retries(storage.as_ref(), &path, &range, &mut timeouts)
                                .await?
                        };
                        let (new_reader, new_deadline) = reopened.ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "internal timeout on get_slice_stream",
                            )
                        })?;
                        reader = ReaderStream::new(new_reader);
                        deadline = new_deadline;
                    }
                }
            },
        );
        Ok(Box::new(StreamReader::new(Box::pin(chunks))))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.underlying.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.underlying.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.underlying.bulk_delete(paths).await
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.underlying.list(prefix)
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.underlying.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.underlying.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.underlying.uri()
    }
}

/// Opens a stream using the remaining timeout attempts. The deadline also covers its body.
/// Returns `None` when all attempts time out. Storage errors are returned immediately.
async fn open_stream_with_retries(
    storage: &dyn Storage,
    path: &Path,
    range: &Range<usize>,
    timeouts: &mut impl Iterator<Item = Duration>,
) -> StorageResult<Option<(Box<dyn AsyncRead + Send + Unpin>, Instant)>> {
    for duration in timeouts {
        let deadline = Instant::now() + duration;
        let opening = storage.get_slice_stream(path, range.clone());
        if let Ok(result) = timeout_at(deadline, opening).await {
            return Ok(Some((result?, deadline)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {

    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::Instant;

    use super::*;
    use crate::{CoalescingStorage, MockStorage};

    fn stream_storage(storage: impl Storage) -> TimeoutAndRetryStorage {
        TimeoutAndRetryStorage::new(
            Arc::new(storage),
            StorageTimeoutPolicy {
                min_throughtput_bytes_per_secs: 0,
                timeout_millis: 2_000,
                max_num_retries: 1,
            },
        )
    }

    fn delayed_reader(
        chunks: Vec<(Duration, io::Result<&'static [u8]>)>,
    ) -> Box<dyn AsyncRead + Send + Unpin> {
        let chunks = stream::iter(chunks).then(|(delay, result)| async move {
            tokio::time::sleep(delay).await;
            result.map(Bytes::from_static)
        });
        Box::new(StreamReader::new(Box::pin(chunks)))
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_body_timeout_resumes_emitted_offset() {
        for coalesced in [false, true] {
            let mut underlying = MockStorage::new();
            let mut sequence = mockall::Sequence::new();
            underlying
                .expect_get_slice_stream()
                .withf(|path, range| path == Path::new("object") && *range == (10..18))
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|_, _| {
                    Ok(delayed_reader(vec![
                        (Duration::from_secs(1), Ok(b"abcd")),
                        (Duration::from_millis(1_500), Ok(b"wrong")),
                    ]))
                });
            underlying
                .expect_get_slice_stream()
                .withf(|_, range| *range == (14..18))
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|_, _| Ok(Box::new(&b"efgh"[..])));
            let storage = stream_storage(underlying);
            if coalesced {
                let storage = CoalescingStorage::new(Arc::new(storage));
                let (first, second) = tokio::try_join!(
                    storage.get_slice(Path::new("object"), 10..12),
                    storage.get_slice(Path::new("object"), 16..18)
                )
                .unwrap();
                assert_eq!(first.as_slice(), b"ab");
                assert_eq!(second.as_slice(), b"gh");
            } else {
                let start = Instant::now();
                let mut reader = storage
                    .get_slice_stream(Path::new("object"), 10..18)
                    .await
                    .unwrap();
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"abcdefgh");
                assert_eq!(start.elapsed(), Duration::from_secs(2));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_body_timeout_retries_only_unread_bytes() {
        // Retry when no bytes arrived. An EOF timeout after all bytes arrived must not reopen.
        for (emitted, attempts) in [(&b""[..], 2), (&b"abcdefgh"[..], 1)] {
            let mut underlying = MockStorage::new();
            underlying
                .expect_get_slice_stream()
                .withf(|_, range| *range == (10..18))
                .times(attempts)
                .returning(move |_, _| {
                    Ok(delayed_reader(vec![
                        (Duration::ZERO, Ok(emitted)),
                        (Duration::from_secs(3), Ok(b"")),
                    ]))
                });
            let storage = stream_storage(underlying);
            let start = Instant::now();
            let mut reader = storage
                .get_slice_stream(Path::new("object"), 10..18)
                .await
                .unwrap();
            let mut bytes = Vec::new();
            let error = reader.read_to_end(&mut bytes).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            assert_eq!(bytes, emitted);
            assert_eq!(start.elapsed(), Duration::from_secs(2 * attempts as u64));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_preserves_body_failures() {
        for (body, expected_error) in [
            (Ok(&b"short"[..]), None),
            (Ok(&b"too many bytes"[..]), Some(io::ErrorKind::InvalidData)),
            (
                Err(io::Error::other("body failure")),
                Some(io::ErrorKind::Other),
            ),
        ] {
            let mut underlying = MockStorage::new();
            underlying
                .expect_get_slice_stream()
                .times(1)
                .return_once(|_, _| Ok(delayed_reader(vec![(Duration::ZERO, body)])));
            let storage = stream_storage(underlying);
            let mut reader = storage
                .get_slice_stream(Path::new("object"), 10..18)
                .await
                .unwrap();
            let mut bytes = Vec::new();
            let result = reader.read_to_end(&mut bytes).await;
            assert_eq!(result.err().map(|error| error.kind()), expected_error);
            if expected_error.is_none() {
                assert_eq!(bytes, b"short"); // Caller can detect truncation from the length.
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_cancellation_drops_reader() {
        let (reader, mut writer) = tokio::io::duplex(8);
        let mut underlying = MockStorage::new();
        underlying
            .expect_get_slice_stream()
            .times(1)
            .return_once(|_, _| Ok(Box::new(reader)));
        let storage = stream_storage(underlying);
        let mut reader = storage
            .get_slice_stream(Path::new("object"), 10..18)
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let mut reading = Box::pin(reader.read_to_end(&mut bytes));
        assert!(futures::poll!(&mut reading).is_pending());
        drop(reading);
        drop(reader);
        assert_eq!(
            writer.write_all(b"abcd").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[derive(Debug)]
    struct StorageWithDelay {
        // Each attempt specifies its delay and the byte used to fill its response.
        attempts: Mutex<Vec<(Duration, u8)>>,
    }

    impl StorageWithDelay {
        pub fn new(mut attempts: Vec<(Duration, u8)>) -> StorageWithDelay {
            attempts.reverse();
            StorageWithDelay {
                attempts: Mutex::new(attempts),
            }
        }
    }

    #[async_trait]
    impl Storage for StorageWithDelay {
        fn uri(&self) -> &Uri {
            todo!();
        }

        async fn check_connectivity(&self) -> anyhow::Result<()> {
            todo!()
        }
        async fn put(&self, _path: &Path, _payload: Box<dyn PutPayload>) -> StorageResult<()> {
            todo!();
        }
        async fn copy_to(
            &self,
            _path: &Path,
            _output: &mut dyn SendableAsync,
        ) -> StorageResult<()> {
            todo!();
        }

        async fn get_slice(&self, _path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
            let attempt = self.attempts.lock().unwrap().pop();
            let Some((delay, fill_byte)) = attempt else {
                return Err(
                    StorageErrorKind::Internal.with_error(anyhow::anyhow!("internal error"))
                );
            };
            tokio::time::sleep(delay).await;
            let buf = vec![fill_byte; range.len()];
            Ok(OwnedBytes::new(buf))
        }
        async fn get_slice_into(
            &self,
            path: &Path,
            range: Range<usize>,
            buffer: &mut [u8],
        ) -> StorageResult<()> {
            crate::default_get_slice_into(self, path, range, buffer).await
        }
        async fn get_slice_stream(
            &self,
            _path: &Path,
            range: Range<usize>,
        ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
            let (delay, fill_byte) = self.attempts.lock().unwrap().pop().ok_or_else(|| {
                StorageErrorKind::Internal.with_error(anyhow::anyhow!("internal error"))
            })?;
            // Opening and the first byte succeed immediately; the remaining body stalls.
            let body = futures::stream::once(async move {
                Ok::<_, std::io::Error>(Bytes::from(vec![fill_byte]))
            })
            .chain(futures::stream::once(async move {
                tokio::time::sleep(delay).await;
                Ok::<_, std::io::Error>(Bytes::from(vec![fill_byte; range.len() - 1]))
            }))
            .boxed();
            Ok(Box::new(tokio_util::io::StreamReader::new(body)))
        }
        async fn get_all(&self, _path: &Path) -> StorageResult<OwnedBytes> {
            todo!();
        }
        async fn delete(&self, _path: &Path) -> StorageResult<()> {
            todo!();
        }
        async fn bulk_delete<'a>(&self, _paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
            todo!();
        }
        async fn exists(&self, _path: &Path) -> StorageResult<bool> {
            todo!()
        }
        async fn file_num_bytes(&self, _path: &Path) -> StorageResult<u64> {
            todo!();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_read_timeouts_and_retries() {
        let timeout_policy = StorageTimeoutPolicy {
            min_throughtput_bytes_per_secs: 100_000,
            timeout_millis: 2_000,
            max_num_retries: 1,
        };
        for into_buffer in [false, true] {
            for (second_delay, succeeds) in [(1, true), (5, false)] {
                // Different bytes ensure the retry overwrites the first attempt's partial write.
                let retry_byte = b'b';
                let lower = Arc::new(StorageWithDelay::new(vec![
                    (Duration::from_secs(5), b'a'),
                    (Duration::from_secs(second_delay), retry_byte),
                ]));
                let storage = TimeoutAndRetryStorage::new(lower.clone(), timeout_policy.clone());
                let mut buffer = [99; 90];
                let address = buffer.as_ptr();
                let start = Instant::now();
                let path = Path::new("foo/bar");
                let result = if into_buffer {
                    let result = storage.get_slice_into(path, 10..100, &mut buffer).await;
                    assert_eq!(buffer.as_ptr(), address);
                    result.map(|()| OwnedBytes::new(buffer.to_vec()))
                } else {
                    storage.get_slice(path, 10..100).await
                };
                assert!(
                    lower.attempts.lock().unwrap().is_empty(),
                    "both attempts must run"
                );
                if succeeds {
                    assert_eq!(result.unwrap().as_slice(), &[retry_byte; 90]);
                    // Tokio timers round deadlines to millisecond ticks.
                    assert!(start.elapsed().as_millis().abs_diff(3_000) < 10);
                } else {
                    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Timeout);
                    assert!(start.elapsed().as_millis().abs_diff(4_000) < 10);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_storage_errors_are_returned_without_retry() {
        let mut backend = crate::MockStorage::new();
        backend.expect_get_slice().times(1).returning(|_, _| {
            Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing object")))
        });
        backend
            .expect_get_slice_into()
            .times(1)
            .returning(|_, _, _| {
                Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing object")))
            });
        let storage = TimeoutAndRetryStorage::new(
            Arc::new(backend),
            StorageTimeoutPolicy {
                min_throughtput_bytes_per_secs: 100_000,
                timeout_millis: 2_000,
                max_num_retries: 1,
            },
        );
        let path = Path::new("missing");
        let error = storage.get_slice(path, 0..3).await.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        let error = storage
            .get_slice_into(path, 0..3, &mut [0; 3])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
}
