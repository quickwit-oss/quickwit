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
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;
use tracing::{error, warn};

use super::metrics::{FetchOutcome, record_request};
use super::{FoyerSplitRangeCache, SplitRangeCacheKey};
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, ListObjectsStream, OwnedBytes, PutPayload, Storage, StorageError,
    StorageErrorKind, StorageResult,
};

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
        let requested_num_bytes = (key.byte_range.end - key.byte_range.start) as u64;
        let max_entry_size = self.max_entry_size;
        match self
            .cache
            .get_or_fetch(&key, || async move {
                let bytes = fetch().await.map_err(LowerStorageError)?;
                if bytes.len() > max_entry_size {
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

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(unsupported_operation(paths)),
            ..Default::default()
        })
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.inner.list(prefix)
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}
