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

//! The single thin async layer of the download manager.
//!
//! Everything here is entered only on a cache miss. It folds together what used
//! to be three separate `Storage` decorators: request debouncing
//! ([`crate::AsyncDebouncer`]), the on-disk split cache fill, and the aggressive
//! per-attempt timeout + retry. The actual object-store concurrency limiting and
//! AWS retry still live in the backend, so they are not re-implemented here.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use quickwit_common::uri::Uri;
use quickwit_common::{rate_limited_info, rate_limited_warn};
use quickwit_config::StorageTimeoutPolicy;

use super::{DownloadKey, DownloadPlan, SearchDownloadCaches, SplitDownloadView};
use crate::split_cache::split_id_from_path;
use crate::{OwnedBytes, SplitCache, Storage, StorageErrorKind, StorageResult};

impl SplitDownloadView {
    /// The thin async layer. Entered only on a [`super::Resolution::Miss`].
    ///
    /// Translates the logical (file-relative) range to an absolute range in the
    /// physical split, fetches it (debounced, via the on-disk split cache then the
    /// backend with the timeout policy), and writes the result back into the
    /// ephemeral and — for `.fast` files — long-term caches so that subsequent
    /// resolves are synchronous hits.
    pub(crate) async fn download(&self, plan: DownloadPlan) -> StorageResult<OwnedBytes> {
        let file_offsets = self
            .bundle_offsets
            .get(&plan.file_in_split)
            .ok_or_else(|| {
                StorageErrorKind::NotFound.with_error(anyhow::anyhow!(
                    "missing file `{}` in split `{}`",
                    plan.file_in_split.display(),
                    self.split_path.display(),
                ))
            })?;
        let abs_range = file_offsets.start as usize + plan.byte_range.start
            ..file_offsets.start as usize + plan.byte_range.end;

        let key = DownloadKey {
            split_path: self.split_path.clone(),
            file_in_split: plan.file_in_split.clone(),
            byte_range: plan.byte_range.clone(),
        };
        let backend = self.backend.clone();
        let split_cache_opt = self.split_cache_opt.clone();
        let timeout_policy = self.timeout_policy.clone();
        let split_path = self.split_path.clone();
        // The builder runs while the debouncer lock is held, so it must be cheap:
        // it only constructs the future (an `async fn` call), it does not poll it.
        let download_fut = self.inflight.get_or_create(key, move || {
            fetch_slice(
                backend,
                split_cache_opt,
                split_path,
                abs_range,
                timeout_policy,
            )
        });
        let bytes = download_fut.await?;

        let cache_tag = self.cache_tag(&plan.file_in_split);
        if let Some(byte_range_cache) = &self.byte_range_cache {
            byte_range_cache.put_slice(cache_tag.clone(), plan.byte_range.clone(), bytes.clone());
        }
        if plan.is_fast {
            self.fast_field_cache
                .put_slice(cache_tag, plan.byte_range, bytes.clone());
        }
        Ok(bytes)
    }
}

/// Fetches the footer byte range of a split, with request dedup and the optional
/// aggressive-timeout policy (mirroring the historical timeout+retry on the footer
/// fetch). The footer is addressed by its absolute range in the physical `.split`
/// file and does not go through the on-disk split cache.
pub async fn fetch_split_footer(
    caches: &SearchDownloadCaches,
    backend: Arc<dyn Storage>,
    timeout_policy: Option<StorageTimeoutPolicy>,
    split_path: PathBuf,
    footer_range: Range<usize>,
) -> StorageResult<OwnedBytes> {
    let key = DownloadKey {
        split_path: split_path.clone(),
        file_in_split: PathBuf::new(),
        byte_range: footer_range.clone(),
    };
    let download_fut = caches.inflight.get_or_create(key, move || async move {
        download_slice_with_timeout(
            backend.as_ref(),
            &split_path,
            footer_range,
            timeout_policy.as_ref(),
        )
        .await
    });
    download_fut.await
}

/// Fetches an absolute byte range of a physical `.split` file: first the on-disk
/// split cache (if configured), then the backend with the timeout policy.
async fn fetch_slice(
    backend: Arc<dyn Storage>,
    split_cache_opt: Option<Arc<SplitCache>>,
    split_path: PathBuf,
    abs_range: Range<usize>,
    timeout_policy: Option<StorageTimeoutPolicy>,
) -> StorageResult<OwnedBytes> {
    if let Some(split_cache) = &split_cache_opt {
        let from_split_cache =
            read_from_split_cache(split_cache, backend.uri(), &split_path, abs_range.clone()).await;
        record_split_cache_outcome(from_split_cache.as_ref());
        if let Some(bytes) = from_split_cache {
            return Ok(bytes);
        }
    }
    download_slice_with_timeout(
        backend.as_ref(),
        &split_path,
        abs_range,
        timeout_policy.as_ref(),
    )
    .await
}

/// Reads an absolute range from the on-disk split cache, if the split is present.
async fn read_from_split_cache(
    split_cache: &SplitCache,
    storage_uri: &Uri,
    split_path: &Path,
    abs_range: Range<usize>,
) -> Option<OwnedBytes> {
    let split_id = split_id_from_path(split_path)?;
    let split_file = split_cache.get_split_file(split_id, storage_uri).await?;
    split_file.get_range(abs_range).await.ok()
}

/// Records a hit/miss against the `searcher_split` cache metrics, preserving the
/// behavior of the former `SplitCacheBackingStorage::record_hit_metrics`.
fn record_split_cache_outcome(result_opt: Option<&OwnedBytes>) {
    let split_metrics = &crate::metrics::SEARCHER_SPLIT_CACHE.cache_metrics;
    if let Some(result) = result_opt {
        split_metrics.hits_num_items.inc();
        split_metrics.hits_num_bytes.inc_by(result.len() as u64);
    } else {
        split_metrics.misses_num_items.inc();
    }
}

/// Downloads a slice from the backend, applying the aggressive per-attempt
/// timeout + retry policy if one is configured. Ported from the former
/// `TimeoutAndRetryStorage::get_slice` (the backend still owns AWS retry + the
/// concurrency semaphore, so neither is re-implemented here).
async fn download_slice_with_timeout(
    backend: &dyn Storage,
    split_path: &Path,
    range: Range<usize>,
    timeout_policy: Option<&StorageTimeoutPolicy>,
) -> StorageResult<OwnedBytes> {
    let Some(timeout_policy) = timeout_policy else {
        return backend.get_slice(split_path, range).await;
    };
    let num_bytes = range.len();
    for (attempt_id, timeout_duration) in timeout_policy.compute_timeout(num_bytes).enumerate() {
        let get_slice_fut = backend.get_slice(split_path, range.clone());
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
                rate_limited_info!(limit_per_min = 60, num_bytes = num_bytes, path = %split_path.display(), timeout_secs = timeout_duration.as_secs_f32(), "get timeout elapsed");
                continue;
            }
        }
    }
    rate_limited_warn!(limit_per_min = 60, num_bytes = num_bytes, path = %split_path.display(), "all get_slice attempts timeouted");
    crate::metrics::GET_SLICE_TIMEOUT_ALL_TIMEOUTS.inc();
    Err(StorageErrorKind::Timeout.with_error(anyhow::anyhow!("internal timeout on get_slice")))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use quickwit_common::uri::Uri;
    use quickwit_config::StorageTimeoutPolicy;
    use tokio::io::AsyncRead;
    use tokio::time::Instant;

    use super::*;
    use crate::storage::SendableAsync;
    use crate::{BulkDeleteError, PutPayload, StorageErrorKind};

    /// Test backend that returns after a configurable per-call delay (consumed in
    /// order). Used to exercise the timeout + retry loop in
    /// [`download_slice_with_timeout`].
    #[derive(Debug)]
    struct StorageWithDelay {
        delays: Mutex<Vec<Duration>>,
    }

    impl StorageWithDelay {
        fn new(mut delays: Vec<Duration>) -> StorageWithDelay {
            delays.reverse();
            StorageWithDelay {
                delays: Mutex::new(delays),
            }
        }
    }

    #[async_trait::async_trait]
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
    async fn test_download_slice_with_timeout() {
        tokio::time::pause();
        let timeout_policy = StorageTimeoutPolicy {
            min_throughtput_bytes_per_secs: 100_000,
            timeout_millis: 2_000,
            max_num_retries: 1,
        };
        let path = Path::new("foo/bar");
        {
            // Both attempts exceed the 2s timeout -> overall timeout after 2 * 2s.
            let now = Instant::now();
            let backend =
                StorageWithDelay::new(vec![Duration::from_secs(5), Duration::from_secs(3)]);
            let err = download_slice_with_timeout(&backend, path, 10..100, Some(&timeout_policy))
                .await
                .unwrap_err();
            assert_eq!(err.kind(), StorageErrorKind::Timeout);
            let elapsed = now.elapsed().as_millis();
            assert!(elapsed.abs_diff(2 * 2_000) < 100);
        }
        {
            // First attempt times out, retry succeeds after 1s.
            let now = Instant::now();
            let backend =
                StorageWithDelay::new(vec![Duration::from_secs(5), Duration::from_secs(1)]);
            assert!(
                download_slice_with_timeout(&backend, path, 10..100, Some(&timeout_policy))
                    .await
                    .is_ok()
            );
            let elapsed = now.elapsed().as_millis();
            assert!(elapsed.abs_diff(2_000 + 1_000) < 100);
        }
    }
}
