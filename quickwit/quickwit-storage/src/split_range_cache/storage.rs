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

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::Code;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::warn;

use super::{FoyerSplitRangeCache, SplitRangeCacheKey};
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, Storage, StorageError, StorageErrorKind, StorageResult,
};

/// Foyer hybrid-cache entry header size in the 0.22.3 block engine.
pub(crate) const FOYER_ENTRY_HEADER_SIZE: usize = 36;
/// Foyer blob index reserved at the end of each block.
pub(crate) const FOYER_BLOB_INDEX_SIZE: usize = 4 * 1024;
/// Foyer disk page size used to align encoded entries.
pub(crate) const FOYER_PAGE_SIZE: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum AdmissionBypass {
    MaxEntrySize,
    EncodedTooLarge,
}

pub(crate) fn admission_bypass_reason(
    key_size: usize,
    value: &Bytes,
    max_entry_size: usize,
    block_size: usize,
) -> Option<AdmissionBypass> {
    if value.len() > max_entry_size {
        return Some(AdmissionBypass::MaxEntrySize);
    }
    let encoded_len = FOYER_ENTRY_HEADER_SIZE + key_size + Bytes::estimated_size(value);
    let aligned_len = encoded_len.div_ceil(FOYER_PAGE_SIZE) * FOYER_PAGE_SIZE;
    if aligned_len > block_size - FOYER_BLOB_INDEX_SIZE {
        return Some(AdmissionBypass::EncodedTooLarge);
    }
    None
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct LowerStorageError(StorageError);

pub(crate) enum CacheFetchError {
    Lower(StorageError),
    Foyer,
}

impl FoyerSplitRangeCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        key: SplitRangeCacheKey,
        fetch: F,
    ) -> Result<Bytes, CacheFetchError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = StorageResult<Bytes>> + Send + 'static,
    {
        let key_size = key.estimated_size();
        let max_entry_size = self.max_entry_size;
        let block_size = self.block_size;
        match self
            .cache
            .get_or_fetch(&key, || async move {
                let bytes = fetch().await.map_err(LowerStorageError)?;
                let properties =
                    if admission_bypass_reason(key_size, &bytes, max_entry_size, block_size)
                        .is_some()
                    {
                        foyer::HybridCacheProperties::default()
                            .with_location(foyer::Location::InMem)
                    } else {
                        foyer::HybridCacheProperties::default()
                    };
                Ok::<_, LowerStorageError>((bytes, properties))
            })
            .await
        {
            Ok(entry) => Ok(entry.value().clone()),
            Err(error) => {
                if let Some(lower_error) = error.downcast_ref::<LowerStorageError>() {
                    Err(CacheFetchError::Lower(lower_error.0.clone()))
                } else {
                    warn!(
                        error = ?error,
                        "split range cache fetch failed, reading from storage"
                    );
                    Err(CacheFetchError::Foyer)
                }
            }
        }
    }
}

/// Read-only [`Storage`] decorator that caches exact split byte-range payloads.
#[derive(Clone)]
pub struct FoyerSplitRangeStorage {
    inner: Arc<dyn Storage>,
    cache: Arc<FoyerSplitRangeCache>,
}

impl FoyerSplitRangeStorage {
    /// Wraps `inner` so [`Storage::get_slice`] is served from `cache` on an exact
    /// `{object URI, byte range}` key.
    pub fn new(inner: Arc<dyn Storage>, cache: Arc<FoyerSplitRangeCache>) -> Self {
        Self { inner, cache }
    }

    /// Process-wide cache behind this decorator.
    pub fn cache(&self) -> &Arc<FoyerSplitRangeCache> {
        &self.cache
    }
}

impl fmt::Debug for FoyerSplitRangeStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FoyerSplitRangeStorage")
            .field("uri", self.inner.uri())
            .finish()
    }
}

fn read_only_error() -> StorageError {
    StorageErrorKind::Internal.with_error(anyhow::anyhow!("split range cache storage is read-only"))
}

#[async_trait]
impl Storage for FoyerSplitRangeStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, _path: &Path, _payload: Box<dyn PutPayload>) -> StorageResult<()> {
        Err(read_only_error())
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
        let key = SplitRangeCacheKey {
            object_uri,
            byte_range: byte_range.clone(),
        };
        let inner = self.inner.clone();
        let owned_path = path.to_owned();
        let fetch_range = byte_range.clone();
        let fetch_result = self
            .cache
            .get_or_fetch(key, move || async move {
                inner
                    .get_slice(&owned_path, fetch_range)
                    .await
                    .map(Bytes::from_owner)
            })
            .await;
        match fetch_result {
            Ok(bytes) => Ok(into_owned_bytes(bytes)),
            Err(CacheFetchError::Lower(storage_error)) => Err(storage_error),
            Err(CacheFetchError::Foyer) => self.inner.get_slice(path, byte_range).await,
        }
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

    async fn delete(&self, _path: &Path) -> StorageResult<()> {
        Err(read_only_error())
    }

    async fn bulk_delete<'a>(&self, _paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(read_only_error()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admission_bypass_pinned_foyer_block_format() {
        let key_size = 40;
        let block_size = 2 * FOYER_PAGE_SIZE;
        // encoded = 36 + 40 + (usize_len + value_len) = 84 + value_len on 64-bit.
        // 4012 => encoded 4096, one page, fits in block_size - blob index.
        // 4013 => encoded 4097, two pages, exceeds that slot.
        assert_eq!(
            admission_bypass_reason(
                key_size,
                &Bytes::from(vec![0; 4012]),
                usize::MAX,
                block_size
            ),
            None
        );
        assert_eq!(
            admission_bypass_reason(
                key_size,
                &Bytes::from(vec![0; 4013]),
                usize::MAX,
                block_size
            ),
            Some(AdmissionBypass::EncodedTooLarge)
        );
        assert_eq!(
            admission_bypass_reason(key_size, &Bytes::from(vec![0; 101]), 100, 4 * 1024 * 1024),
            Some(AdmissionBypass::MaxEntrySize)
        );
    }
}
