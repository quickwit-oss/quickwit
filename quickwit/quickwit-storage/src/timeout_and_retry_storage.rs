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
                Ok(result) => {
                    match attempt_id {
                        0 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_0_TIMEOUT.inc(),
                        1 => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_1_TIMEOUT.inc(),
                        _ => crate::metrics::GET_SLICE_TIMEOUT_SUCCESS_AFTER_2_PLUS_TIMEOUT.inc(),
                    }
                    return result;
                }
                Err(_elapsed) => {
                    rate_limited_info!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), timeout_secs=timeout_duration.as_secs_f32(), "get timeout elapsed");
                    continue;
                }
            }
        }
        rate_limited_warn!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), "all get_slice attempts timeouted");
        crate::metrics::GET_SLICE_TIMEOUT_ALL_TIMEOUTS.inc();
        return Err(
            StorageErrorKind::Timeout.with_error(anyhow::anyhow!("internal timeout on get_slice"))
        );
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let mut timeouts = self.storage_timeout_policy.compute_timeout(range.len());
        let (reader, deadline) = loop {
            let Some(duration) = timeouts.next() else {
                return Err(StorageErrorKind::Timeout
                    .with_error(anyhow::anyhow!("internal timeout on get_slice_stream")));
            };
            let deadline = Instant::now() + duration;
            let opening = self.underlying.get_slice_stream(path, range.clone());
            if let Ok(result) = timeout_at(deadline, opening).await {
                break (result?, deadline);
            }
        };
        let storage = self.underlying.clone();
        let path = Arc::new(path.to_owned());
        let state = (range, timeouts, Some((ReaderStream::new(reader), deadline)));
        let chunks = stream::try_unfold(state, move |(mut range, mut timeouts, mut body)| {
            let storage = storage.clone();
            let path = path.clone();
            async move {
                loop {
                    if let Some((reader, deadline)) = body.as_mut() {
                        match timeout_at(*deadline, reader.next()).await {
                            Ok(Some(result)) => {
                                let bytes = result?;
                                if bytes.len() > range.len() {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "get_slice_stream returned more bytes than requested",
                                    ));
                                }
                                range.start += bytes.len();
                                return Ok(Some((bytes, (range, timeouts, body))));
                            }
                            Ok(None) => return Ok(None),
                            Err(_) => body = None, // Drop the timed-out reader before reopening.
                        }
                    }
                    // A timeout while verifying EOF cannot be retried with an empty range.
                    let duration =
                        timeouts
                            .next()
                            .filter(|_| !range.is_empty())
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "internal timeout on get_slice_stream",
                                )
                            })?;
                    let deadline = Instant::now() + duration;
                    let opening = storage.get_slice_stream(&path, range.clone());
                    if let Ok(result) = timeout_at(deadline, opening).await {
                        body = Some((
                            ReaderStream::new(result.map_err(io::Error::from)?),
                            deadline,
                        ));
                    }
                }
            }
        });
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

#[cfg(test)]
mod tests {

    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::MockStorage;

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

    #[tokio::test(start_paused = true)]
    async fn test_stream_body_timeouts_exhaust_retries() {
        let mut underlying = MockStorage::new();
        underlying
            .expect_get_slice_stream()
            .withf(|_, range| *range == (10..18))
            .times(2)
            .returning(|_, _| {
                Ok(delayed_reader(vec![(
                    Duration::from_secs(3),
                    Ok(b"abcdefgh"),
                )]))
            });
        let storage = stream_storage(underlying);
        let start = Instant::now();
        let mut reader = storage
            .get_slice_stream(Path::new("object"), 10..18)
            .await
            .unwrap();
        let error = reader.read_to_end(&mut Vec::new()).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(start.elapsed(), Duration::from_secs(4));
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
        delays: Mutex<Vec<Duration>>,
    }

    impl StorageWithDelay {
        pub fn new(mut delays: Vec<Duration>) -> StorageWithDelay {
            delays.reverse();
            StorageWithDelay {
                delays: Mutex::new(delays),
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
        fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            _path: &'life1 Path,
            _output: &'life2 mut dyn SendableAsync,
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
            todo!();
        }

        async fn get_slice(&self, _path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
            let duration_opt = self.delays.lock().unwrap().pop();
            let Some(delay) = duration_opt else {
                return Err(
                    StorageErrorKind::Internal.with_error(anyhow::anyhow!("internal error"))
                );
            };
            tokio::time::sleep(delay).await;
            let buf = vec![0u8; range.len()];
            Ok(OwnedBytes::new(buf))
        }
        async fn get_slice_stream(
            &self,
            _path: &Path,
            range: Range<usize>,
        ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
            let delay = self.delays.lock().unwrap().pop().unwrap();
            tokio::time::sleep(delay).await;
            Ok(Box::new(std::io::Cursor::new(vec![0; range.len()])))
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
    async fn test_timeout_and_retry_storage() {
        let timeout_policy = StorageTimeoutPolicy {
            min_throughtput_bytes_per_secs: 100_000,
            timeout_millis: 2_000,
            max_num_retries: 1,
        };
        let path = Path::new("foo/bar");
        for streamed in [false, true] {
            for (second_delay, expected_error, elapsed_millis) in [
                (3, Some(StorageErrorKind::Timeout), 4_000),
                (1, None, 3_000),
            ] {
                let now = Instant::now();
                let underlying = StorageWithDelay::new(vec![
                    Duration::from_secs(5),
                    Duration::from_secs(second_delay),
                ]);
                let storage =
                    TimeoutAndRetryStorage::new(Arc::new(underlying), timeout_policy.clone());
                let result = if streamed {
                    storage.get_slice_stream(path, 10..100).await.map(|_| ())
                } else {
                    storage.get_slice(path, 10..100).await.map(|_| ())
                };
                assert_eq!(result.err().map(|error| error.kind()), expected_error);
                assert!(now.elapsed().as_millis().abs_diff(elapsed_millis) < 100);
            }
        }
    }
}
