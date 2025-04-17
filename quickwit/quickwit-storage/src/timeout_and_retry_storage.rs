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
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_common::{rate_limited_info, rate_limited_warn};
use quickwit_config::StorageTimeoutPolicy;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, PutPayload, Storage, StorageErrorKind, StorageResult};

/// Storage proxy that implements a retry operation if the underlying storage
/// takes too long.
///
/// This is useful in order to ensure a low latency on S3.
/// Retrying agressively is recommended for S3.
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
                    crate::STORAGE_METRICS
                        .get_slice_timeout_successes
                        .get(attempt_id)
                        .or(crate::STORAGE_METRICS.get_slice_timeout_successes.last())
                        .unwrap()
                        .inc();
                    return result;
                }
                Err(_elapsed) => {
                    rate_limited_info!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), timeout_secs=timeout_duration.as_secs_f32(), "get timeout elapsed");
                    continue;
                }
            }
        }
        rate_limited_warn!(limit_per_min=60, num_bytes=num_bytes, path=%path.display(), "all get_slice attempts timeouted");
        crate::STORAGE_METRICS.get_slice_timeout_all_timeouts.inc();
        return Err(
            StorageErrorKind::Timeout.with_error(anyhow::anyhow!("internal timeout on get_slice"))
        );
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.underlying.get_slice_stream(path, range).await
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

    use tokio::time::Instant;

    use super::*;

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
            _range: Range<usize>,
        ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
            todo!()
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

    #[tokio::test]
    async fn test_timeout_and_retry_storage() {
        tokio::time::pause();

        let timeout_policy = StorageTimeoutPolicy {
            min_throughtput_bytes_per_secs: 100_000,
            timeout_millis: 2_000,
            max_num_retries: 1,
        };

        let path = Path::new("foo/bar");

        {
            let now = Instant::now();
            let storage_with_delay =
                StorageWithDelay::new(vec![Duration::from_secs(5), Duration::from_secs(3)]);
            let storage =
                TimeoutAndRetryStorage::new(Arc::new(storage_with_delay), timeout_policy.clone());
            assert_eq!(
                storage.get_slice(path, 10..100).await.unwrap_err().kind,
                StorageErrorKind::Timeout
            );
            let elapsed = now.elapsed().as_millis();
            assert!(elapsed.abs_diff(2 * 2_000) < 100);
        }
        {
            let now = Instant::now();
            let storage_with_delay =
                StorageWithDelay::new(vec![Duration::from_secs(5), Duration::from_secs(1)]);
            let storage = TimeoutAndRetryStorage::new(Arc::new(storage_with_delay), timeout_policy);
            assert!(storage.get_slice(path, 10..100).await.is_ok(),);
            let elapsed = now.elapsed().as_millis();
            assert!(elapsed.abs_diff(2_000 + 1_000) < 100);
        }
    }
}
