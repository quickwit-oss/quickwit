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
use fail::fail_point;
use feuer::{Download, GetOrFetchError};
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::error;

use super::FeuerSplitRangeCache;
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind, StorageResult,
};

impl FeuerSplitRangeCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        object_uri: String,
        byte_range: Range<usize>,
        fetch: F,
    ) -> StorageResult<Bytes>
    where
        F: FnOnce(Range<usize>) -> Fut + Send + 'static,
        Fut: Future<Output = StorageResult<Download>> + Send + 'static,
    {
        // Each exact split slice is a complete object to Feuer. Version the key so
        // persisted entries from the range API cannot be mistaken for whole objects.
        let cache_key = split_slice_key(&object_uri, &byte_range);
        self.cache
            .get_or_fetch_object_with_allocation_charge(cache_key, || async {
                let (downloaded_range, download) = self
                    .downloads
                    .get_or_create(object_uri, byte_range.clone(), fetch)
                    .await?;
                // The coalescer validates the merged download's length. Each entry exposes
                // only this slice but retains and charges for the full backing allocation.
                let start = byte_range.start - downloaded_range.start;
                let end = byte_range.end - downloaded_range.start;
                Ok::<_, StorageError>((
                    download.bytes().slice(start..end),
                    download.allocation_charge(),
                ))
            })
            .await
            .map_err(|error| match error {
                GetOrFetchError::Callback(error) => error,
            })
    }
}

/// Read-only storage decorator caching exact slices of immutable split objects.
#[derive(Clone)]
pub struct FeuerSplitRangeStorage {
    inner: Arc<dyn Storage>,
    cache: Arc<FeuerSplitRangeCache>,
}

/// Wrap `storage` with a process-wide cache keyed by full object URI and exact byte range.
/// Covering and overlapping cached slices do not serve different ranges.
pub fn wrap_storage_with_split_range_cache(
    cache: Arc<FeuerSplitRangeCache>,
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
        if should_bypass_cache() {
            return self.inner.get_slice(path, byte_range).await;
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
        self.cache
            .get_or_fetch(object_uri, byte_range, move |fetch_range| async move {
                // The coalescer has finalized the range before acquiring one buffer.
                let mut buffer = cache.cache.allocate_buffer(fetch_range.len())?;
                inner
                    .get_slice_into(&owned_path, fetch_range, buffer.as_mut_slice())
                    .await?;
                Ok(buffer.into_download())
            })
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
    use super::split_slice_key;

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

fn should_bypass_cache() -> bool {
    fail_point!("split-range-cache-before-get", |_| true);
    false
}
