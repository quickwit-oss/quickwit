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
use foyer::Code;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::{error, warn};

use super::metrics::{FetchOutcome, record_admission_bypass, record_fail_open, record_request};
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
    // `max_entry_size < block_size` is not enough: the disk slot is
    // `block_size - blob index` after header, key, and page alignment.
    let encoded_len = FOYER_ENTRY_HEADER_SIZE + key_size + Bytes::estimated_size(value);
    let aligned_len = encoded_len.div_ceil(FOYER_PAGE_SIZE) * FOYER_PAGE_SIZE;
    let Some(available_block_size) = block_size.checked_sub(FOYER_BLOB_INDEX_SIZE) else {
        return Some(AdmissionBypass::EncodedTooLarge);
    };
    if aligned_len > available_block_size {
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
        let requested_num_bytes = (key.byte_range.end - key.byte_range.start) as u64;
        let max_entry_size = self.max_entry_size;
        let block_size = self.block_size;
        match self
            .cache
            .get_or_fetch(&key, || async move {
                let bytes = fetch().await.map_err(LowerStorageError)?;
                if let Some(reason) =
                    admission_bypass_reason(key_size, &bytes, max_entry_size, block_size)
                {
                    record_admission_bypass(reason);
                    // Foyer keeps this tag on the RAM entry and skips disk enqueue
                    // on eviction (write-on-eviction).
                    Ok::<_, LowerStorageError>((
                        bytes,
                        foyer::HybridCacheProperties::default()
                            .with_location(foyer::Location::InMem),
                    ))
                } else {
                    Ok((bytes, foyer::HybridCacheProperties::default()))
                }
            })
            .await
        {
            Ok(entry) => {
                let outcome = match entry.source() {
                    foyer::Source::Memory => FetchOutcome::MemoryHit,
                    foyer::Source::Disk => FetchOutcome::DiskHit,
                    foyer::Source::Outer => FetchOutcome::RemoteMiss,
                };
                let bytes = entry.value().clone();
                record_request(outcome, bytes.len() as u64);
                Ok(bytes)
            }
            Err(error) => {
                record_request(FetchOutcome::Error, requested_num_bytes);
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

/// Wraps `storage` so [`Storage::get_slice`] is served from `cache` on an exact
/// `{object URI, byte range}` key.
pub fn wrap_storage_with_split_range_cache(
    cache: Arc<FoyerSplitRangeCache>,
    storage: Arc<dyn Storage>,
) -> Arc<dyn Storage> {
    Arc::new(FoyerSplitRangeStorage {
        inner: storage,
        cache,
    })
}

impl fmt::Debug for FoyerSplitRangeStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FoyerSplitRangeStorage")
            .field("uri", self.inner.uri())
            .finish()
    }
}

fn unsupported_operation(paths: &[&Path]) -> StorageError {
    let msg = "Unsupported operation. FoyerSplitRangeStorage only supports async reads";
    error!(paths=?paths, msg);
    io::Error::other(format!("{msg}: {paths:?}")).into()
}

#[async_trait]
impl Storage for FoyerSplitRangeStorage {
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
            Err(CacheFetchError::Foyer) => {
                record_fail_open();
                self.inner.get_slice(path, byte_range).await
            }
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

fn should_bypass_cache() -> bool {
    fail_point!("split-range-cache-before-get", |_| true);
    false
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
        // 5 KiB < max_entry_size 7 KiB < block_size 8 KiB, but the disk slot is
        // only 4 KiB after the blob index.
        assert_eq!(
            admission_bypass_reason(
                key_size,
                &Bytes::from(vec![0; 5 * 1024]),
                7 * 1024,
                8 * 1024
            ),
            Some(AdmissionBypass::EncodedTooLarge)
        );
    }

    #[test]
    fn test_admission_bypasses_block_smaller_than_blob_index() {
        assert_eq!(
            admission_bypass_reason(
                1,
                &Bytes::from_static(b"value"),
                usize::MAX,
                FOYER_BLOB_INDEX_SIZE - 1,
            ),
            Some(AdmissionBypass::EncodedTooLarge)
        );
    }
}
