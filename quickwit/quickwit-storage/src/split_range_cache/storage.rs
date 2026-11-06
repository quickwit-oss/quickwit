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

use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use bytes::Bytes;
use feuer::{AlignedBuffer, GetOrFetchError};
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::error;

use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind, StorageResult,
};

pub(super) async fn get_or_fetch<F, Fut>(
    cache: &feuer::Cache,
    object_uri: String,
    byte_range: Range<usize>,
    fetch: F,
) -> StorageResult<Bytes>
where
    F: FnOnce(Range<usize>) -> Fut + Send + 'static,
    Fut: Future<Output = StorageResult<AlignedBuffer>> + Send + 'static,
{
    // Each exact split slice is a complete object to Feuer. Version the key so
    // persisted entries from the range API cannot be mistaken for whole objects.
    let cache_key = split_slice_key(&object_uri, &byte_range);
    cache
        .get_or_fetch(cache_key, || async move {
            let buffer = fetch(byte_range.clone()).await?;
            if buffer.as_ref().len() != byte_range.len() {
                return Err(StorageErrorKind::Internal.with_error(anyhow::anyhow!(
                    "range download returned {} bytes for {byte_range:?}",
                    buffer.as_ref().len()
                )));
            }
            Ok(buffer)
        })
        .await
        .map_err(|error| match error {
            GetOrFetchError::Callback(error) => error,
        })
}

/// Read-only storage decorator caching exact slices of immutable split objects.
struct FeuerSplitRangeStorage {
    inner: Arc<dyn Storage>,
    cache: Arc<feuer::Cache>,
}

/// Wrap `storage` with the supplied cache.
/// Cache hits require matching object URIs and byte ranges.
pub fn wrap_storage_with_split_range_cache(
    cache: Arc<feuer::Cache>,
    storage: Arc<dyn Storage>,
) -> Arc<dyn Storage> {
    Arc::new(FeuerSplitRangeStorage {
        inner: storage,
        cache,
    })
}

impl fmt::Debug for FeuerSplitRangeStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeuerSplitRangeStorage")
            .field("uri", self.inner.uri())
            .finish()
    }
}

fn unsupported_operation(paths: &[&Path]) -> StorageError {
    let msg = "Unsupported operation. FeuerSplitRangeStorage only supports async reads";
    error!(paths=?paths, msg);
    io::Error::other(format!("{msg}: {paths:?}")).into()
}

#[async_trait]
impl Storage for FeuerSplitRangeStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, _payload: Box<dyn PutPayload>) -> StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.inner.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let object_uri = self
            .inner
            .uri()
            .join(path)
            .map_err(|error| StorageErrorKind::Internal.with_error(error))?
            .into_string();
        let inner = self.inner.clone();
        let owned_path = path.to_owned();
        let cache = self.cache.clone();
        get_or_fetch(
            &self.cache,
            object_uri,
            byte_range,
            move |fetch_range| async move {
                let mut buffer = cache.allocate_buffer(fetch_range.len())?;
                inner
                    .get_slice_into(&owned_path, fetch_range, buffer.as_mut_slice())
                    .await?;
                Ok(buffer)
            },
        )
        .await
        .map(into_owned_bytes)
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.inner.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.inner.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(unsupported_operation(paths)),
            ..Default::default()
        })
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}

// Bounds precede the URI so colons within a URI cannot alias another slice key.
fn split_slice_key(object_uri: &str, byte_range: &Range<usize>) -> String {
    format!(
        "split-slice-v1:{}:{}:{object_uri}",
        byte_range.start, byte_range.end
    )
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::split_range_cache::buffer_for_test;
    use crate::split_range_cache::metrics::QuickwitMetricsRegistry;

    async fn memory_cache() -> feuer::Cache {
        crate::open_split_range_cache(
            &quickwit_config::StorageCacheConfig {
                path: None,
                disk_capacity: bytesize::ByteSize::b(0),
                memory_capacity: bytesize::ByteSize::mib(8),
            },
            &crate::FeuerIoQueues::default(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_concurrent_ranges_fetch_exact_slices_independently() {
        for ranges in [[1..2, 4..5], [1..4, 2..3]] {
            let cache = Arc::new(memory_cache().await);
            let mut lower = crate::MockStorage::new();
            lower
                .expect_uri()
                .return_const(Uri::for_test("ram://splits"));
            for expected in ranges.clone() {
                lower
                    .expect_get_slice_into()
                    .withf(move |path, range, _| path == Path::new("a.split") && range == &expected)
                    .times(1)
                    .returning(|_, range, buffer| {
                        assert_eq!(buffer.as_ptr() as usize % 4096, 0);
                        buffer.copy_from_slice(&b"abcde"[range]);
                        Ok(())
                    });
            }
            let storage = wrap_storage_with_split_range_cache(cache, Arc::new(lower));
            let path = Path::new("a.split");
            let (first, second) = tokio::join!(
                storage.get_slice(path, ranges[0].clone()),
                storage.get_slice(path, ranges[1].clone())
            );
            for (range, bytes) in ranges.into_iter().zip([first.unwrap(), second.unwrap()]) {
                assert_eq!(bytes.as_slice(), &b"abcde"[range.clone()]);
                let hit = storage.get_slice(path, range).await.unwrap();
                assert_eq!(hit.as_slice().as_ptr(), bytes.as_slice().as_ptr());
            }
        }
    }

    #[tokio::test]
    async fn test_identical_misses_share_fetch_and_waiter_takes_over_after_cancellation() {
        for cancel in [false, true] {
            let cache = memory_cache().await;
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let mut first = Box::pin(get_or_fetch(
                &cache,
                "object".into(),
                0..4,
                move |_| async move {
                    release_rx.await.unwrap();
                    Ok(buffer_for_test(b"abcd"))
                },
            ));
            assert!(futures::poll!(first.as_mut()).is_pending());
            let mut waiter = Box::pin(get_or_fetch(
                &cache,
                "object".into(),
                0..4,
                move |_| async move {
                    assert!(cancel, "identical misses must share a successful fetch");
                    Ok(buffer_for_test(b"abcd"))
                },
            ));
            assert!(futures::poll!(waiter.as_mut()).is_pending());
            if cancel {
                drop(first);
                assert!(release_tx.send(()).is_err(), "source must be canceled");
            } else {
                release_tx.send(()).unwrap();
                assert_eq!(first.await.unwrap().as_ref(), b"abcd");
            }
            assert_eq!(waiter.await.unwrap().as_ref(), b"abcd");
        }
    }

    #[tokio::test]
    async fn test_slice_ownership_and_memory_charge() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = ::metrics::set_default_local_recorder(&recorder);
        let cache_name = "slice_ownership";
        let registry: mixtrics::metrics::BoxedRegistry =
            Box::new(QuickwitMetricsRegistry { cache_name });
        // Exclude idle buffers so this metric measures only cached slice charges.
        let config = feuer::CacheConfig::new("", 0, 8 * 1024 * 1024)
            .unwrap()
            .with_idle_buffer_pool_percent(0)
            .unwrap();
        let cache = feuer::Cache::builder(config)
            .with_metrics(&registry)
            .open()
            .await
            .unwrap();
        let mut buffer = cache.allocate_buffer(4).unwrap();
        buffer.as_mut_slice().copy_from_slice(b"bcde");
        let source_address = buffer.as_ref().as_ptr() as usize;
        let capacity = buffer.capacity();
        assert!(capacity > 4);
        let bytes = get_or_fetch(&cache, "object".into(), 1..5, move |range| async move {
            assert_eq!(range, 1..5);
            Ok(buffer)
        })
        .await
        .unwrap();
        assert_eq!(&bytes[..], b"bcde");
        assert_eq!(bytes.as_ptr() as usize, source_address);
        let hit = get_or_fetch(&cache, "object".into(), 1..5, |_| async {
            panic!("exact slice must hit memory")
        })
        .await
        .unwrap();
        assert_eq!(hit.as_ptr(), bytes.as_ptr());
        let metrics = snapshotter.snapshot().into_vec();
        let charged_bytes = &metrics
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "feuer_memory_used_bytes"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "cache" && label.value() == cache_name)
            })
            .unwrap()
            .3;
        assert_eq!(charged_bytes, &DebugValue::Gauge((capacity as f64).into()));
    }

    #[tokio::test]
    async fn test_failed_downloads_are_not_cached() {
        let cache = memory_cache().await;
        for payload in [b"ab".as_slice(), b"abcde".as_slice()] {
            let error = get_or_fetch(&cache, "object".into(), 0..4, move |_| async move {
                Ok(buffer_for_test(payload))
            })
            .await
            .unwrap_err();
            assert_eq!(error.kind(), StorageErrorKind::Internal);
        }
        let error = get_or_fetch(&cache, "object".into(), 0..4, |_| async {
            Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing")))
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        let bytes = get_or_fetch(&cache, "object".into(), 0..4, |_| async {
            Ok(buffer_for_test(b"abcd"))
        })
        .await
        .unwrap();
        assert_eq!(bytes.as_ref(), b"abcd");
    }

    #[test]
    fn test_split_slice_keys_include_uri_and_both_bounds() {
        let key = split_slice_key("s3://bucket/a.split", &(1..4));
        assert_eq!(key, "split-slice-v1:1:4:s3://bucket/a.split");
        assert_eq!(key, split_slice_key("s3://bucket/a.split", &(1..4)));
        for (uri, range) in [
            ("s3://other/a.split", 1..4),
            ("s3://bucket/a.split", 0..4),
            ("s3://bucket/a.split", 1..5),
            ("s3://bucket/a.split:1:4", 1..4),
        ] {
            assert_ne!(key, split_slice_key(uri, &range));
        }
        assert_ne!(key, "s3://bucket/a.split");
    }
}
