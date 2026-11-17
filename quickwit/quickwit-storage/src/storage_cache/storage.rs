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
use std::sync::{Arc, LazyLock, Mutex};

use async_trait::async_trait;
use feuer::GetOrFetchError;
use quickwit_common::get_from_env;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tokio::sync::Semaphore;
use tracing::error;

use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind, StorageResult,
};

static FALLTHROUGH_MIN_BYTES: LazyLock<usize> =
    LazyLock::new(|| get_from_env("QW_STORAGE_CACHE_FALLTHROUGH_MIN_BYTES", 20_000_000, false));
static FALLTHROUGH_DOWNLOADS: LazyLock<Semaphore> = LazyLock::new(|| {
    Semaphore::new(get_from_env(
        "QW_STORAGE_CACHE_FALLTHROUGH_MAX_IN_FLIGHT",
        32,
        false,
    ))
});

/// Read-only storage decorator caching exact slices of immutable split objects.
#[derive(Debug)]
struct FeuerStorage {
    inner: Arc<dyn Storage>,
    cache: feuer::Cache,
}

/// Wrap `storage` with the supplied cache.
/// Cache hits require matching object URIs and byte ranges.
pub fn wrap_storage_with_storage_cache(
    cache: feuer::Cache,
    storage: Arc<dyn Storage>,
) -> Arc<dyn Storage> {
    Arc::new(FeuerStorage {
        inner: storage,
        cache,
    })
}

fn unsupported_operation(paths: &[&Path]) -> StorageError {
    let msg = "Unsupported operation. FeuerStorage only supports async reads";
    error!(paths=?paths, msg);
    io::Error::other(format!("{msg}: {paths:?}")).into()
}

#[async_trait]
impl Storage for FeuerStorage {
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
        let cache_key = split_slice_key(&object_uri, &byte_range);
        let cache = self.cache.clone();
        let storage = self.inner.clone();
        let path = path.to_owned();
        let download_size = byte_range.len();
        let permit_slot = Arc::new(Mutex::new(None));
        let decision_slot = permit_slot.clone();
        self.cache
            .get_or_fetch_with_disk_fallthrough(
                &cache_key,
                move || {
                    if download_size < *FALLTHROUGH_MIN_BYTES {
                        return false;
                    }
                    let Ok(permit) = FALLTHROUGH_DOWNLOADS.try_acquire() else {
                        return false;
                    };
                    *decision_slot.lock().unwrap() = Some(permit);
                    true
                },
                move || async move {
                    // Ordinary misses have no fallthrough permit.
                    let _permit = permit_slot.lock().unwrap().take();
                    let mut buffer = cache.allocate_buffer(download_size)?;
                    storage
                        .get_slice_into(&path, byte_range, buffer.as_mut())
                        .await?;
                    Ok(buffer)
                },
            )
            .await
            .map(into_owned_bytes)
            .map_err(|GetOrFetchError::Callback(error)| error)
    }

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        // Feuer owns the cached allocation, so copy from it into the caller's buffer.
        let bytes = self.get_slice(path, range).await?;
        buffer.copy_from_slice(&bytes);
        Ok(())
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

fn split_slice_key<'a>(
    object_uri: &'a str,
    byte_range: &Range<usize>,
) -> (&'static str, &'a str, usize, usize) {
    (
        "split-slice-v1",
        object_uri,
        byte_range.start,
        byte_range.end,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_miss_does_not_wait_for_fallthrough_capacity() {
        let mut permits = Vec::new();
        while let Ok(permit) = FALLTHROUGH_DOWNLOADS.try_acquire() {
            permits.push(permit);
        }
        let payload = vec![42; 20_000_000];
        let storage = wrap_storage_with_storage_cache(
            feuer::Cache::builder()
                .memory_capacity(1024)
                .open()
                .unwrap(),
            Arc::new(
                crate::RamStorageBuilder::default()
                    .put("split", &payload)
                    .build(),
            ),
        );
        let bytes = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            storage.get_slice(Path::new("split"), 0..payload.len()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(bytes.as_slice(), payload);
    }

    #[test]
    fn test_split_slice_keys_include_uri_and_both_bounds() {
        let cache = feuer::Cache::builder()
            .memory_capacity(1024)
            .open()
            .unwrap();
        let hash_key =
            |uri: &str, range: &Range<usize>| cache.hash_key(&split_slice_key(uri, range));
        let key = hash_key("s3://bucket/a.split", &(1..4));
        assert_eq!(key, hash_key("s3://bucket/a.split", &(1..4)));
        for (uri, range) in [
            ("s3://other/a.split", 1..4),
            ("s3://bucket/a.split", 0..4),
            ("s3://bucket/a.split", 1..5),
            ("s3://bucket/a.split:1:4", 1..4),
        ] {
            assert_ne!(key, hash_key(uri, &range));
        }
        assert_ne!(key, cache.hash_key("s3://bucket/a.split"));
    }
}
