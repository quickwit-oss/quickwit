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
use feuer::GetOrFetchError;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::error;

use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind, StorageResult,
};

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
        self.cache
            .get_or_fetch(&cache_key, move || async move {
                let mut buffer = cache.allocate_buffer(byte_range.len())?;
                storage
                    .get_slice_into(&path, byte_range, buffer.as_mut())
                    .await?;
                Ok(buffer)
            })
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

    #[test]
    fn test_split_slice_keys_include_uri_and_both_bounds() {
        let hash_key = |uri: &str, range: &Range<usize>| {
            feuer::ObjectKeyHash::from_hash(&split_slice_key(uri, range))
        };
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
        assert_ne!(key, feuer::ObjectKeyHash::from_hash("s3://bucket/a.split"));
    }
}
