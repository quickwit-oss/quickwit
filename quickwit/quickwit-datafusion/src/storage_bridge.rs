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

//! Adapter from `quickwit_storage::Storage` to `object_store::ObjectStore`.
//!
//! The adapter is **lazy**: it holds a `StorageResolver` + target `Uri` and
//! resolves the underlying `Storage` inside its own async methods the first
//! time DataFusion asks for data. Construction is cheap and synchronous, which
//! lets the sibling [`crate::object_store_registry::QuickwitObjectStoreRegistry`]
//! build wrappers directly from its sync `ObjectStoreRegistry::get_store` hook
//! without needing to pre-resolve storages. Subsequent calls reuse the cached
//! handle; resolution errors are not memoised so a transient metastore blip
//! does not poison the store.
//!
//! This mirrors how `quickwit-search` uses the resolver:
//! `storage_resolver.resolve(&uri).await` per request, no global registry.
//!
//! Only `get_opts` is implemented. All write, delete, copy, and list
//! operations return `NotSupported` — DataFusion only reads parquet files
//! through this store.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use quickwit_common::uri::Uri;
use quickwit_storage::{
    OwnedBytes, Storage, StorageCache, StorageResolver, wrap_storage_with_cache,
};
use tokio::sync::OnceCell;

/// Adapts Quickwit's `Storage` trait to DataFusion's `ObjectStore` interface.
///
/// Construction is sync and cheap: a `Uri` plus a `StorageResolver` handle
/// (resolver is `Clone`). The underlying `Arc<dyn Storage>` is materialised
/// on the first async method call and cached for the wrapper's lifetime.
pub struct QuickwitObjectStore {
    index_uri: Uri,
    storage_resolver: StorageResolver,
    storage_cache: Option<Arc<dyn StorageCache>>,
    storage: OnceCell<Arc<dyn Storage>>,
}

impl QuickwitObjectStore {
    pub fn new(index_uri: Uri, storage_resolver: StorageResolver) -> Self {
        Self {
            index_uri,
            storage_resolver,
            storage_cache: None,
            storage: OnceCell::new(),
        }
    }

    pub fn with_storage_cache(mut self, storage_cache: Arc<dyn StorageCache>) -> Self {
        self.storage_cache = Some(storage_cache);
        self
    }

    /// Returns the handle to the underlying `Storage`, resolving it via the
    /// `StorageResolver` if this is the first call.
    async fn storage(&self) -> ObjectStoreResult<&Arc<dyn Storage>> {
        self.storage
            .get_or_try_init(|| async {
                let storage = self
                    .storage_resolver
                    .resolve(&self.index_uri)
                    .await
                    .map_err(|err| object_store::Error::Generic {
                        store: "QuickwitObjectStore",
                        source: Box::new(err),
                    })?;
                Ok(match &self.storage_cache {
                    Some(cache) => wrap_storage_with_cache(
                        Arc::new(ScopedStorageCache::new(
                            self.index_uri.as_str().to_string(),
                            Arc::clone(cache),
                        )),
                        storage,
                    ),
                    None => storage,
                })
            })
            .await
    }
}

struct ScopedStorageCache {
    scope: String,
    inner: Arc<dyn StorageCache>,
}

impl ScopedStorageCache {
    fn new(scope: String, inner: Arc<dyn StorageCache>) -> Self {
        Self { scope, inner }
    }

    fn scoped_path(&self, path: &Path) -> PathBuf {
        PathBuf::from(format!("{}#{}", self.scope, path.to_string_lossy()))
    }
}

#[async_trait]
impl StorageCache for ScopedStorageCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        self.inner.get(&self.scoped_path(path), byte_range).await
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        self.inner.get_all(&self.scoped_path(path)).await
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.inner
            .put(self.scoped_path(&path), byte_range, bytes)
            .await
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        self.inner.put_all(self.scoped_path(&path), bytes).await
    }
}

impl std::fmt::Debug for QuickwitObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitObjectStore")
            .field("index_uri", &self.index_uri.as_str())
            .field("resolved", &self.storage.initialized())
            .finish()
    }
}

impl std::fmt::Display for QuickwitObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QuickwitObjectStore({})", self.index_uri.as_str())
    }
}

fn to_object_store_error(err: quickwit_storage::StorageError, path: &str) -> object_store::Error {
    use quickwit_storage::StorageErrorKind;
    match err.kind() {
        StorageErrorKind::NotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        _ => object_store::Error::Generic {
            store: "QuickwitObjectStore",
            source: Box::new(err),
        },
    }
}

fn object_path_to_std(location: &ObjectPath) -> std::path::PathBuf {
    std::path::PathBuf::from(location.as_ref())
}

#[async_trait]
impl ObjectStore for QuickwitObjectStore {
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let storage = self.storage().await?;
        let path = object_path_to_std(location);
        let location_str = location.as_ref();
        let map_err = |err| to_object_store_error(err, location_str);

        let (bytes, byte_range) = match &options.range {
            Some(GetRange::Bounded(r)) => {
                let usize_range = r.start as usize..r.end as usize;
                let data = storage
                    .get_slice(&path, usize_range)
                    .await
                    .map_err(map_err)?;
                let b = Bytes::copy_from_slice(data.as_ref());
                let len = b.len() as u64;
                // The storage may return fewer bytes than requested if the range
                // extends past the end of the file, so derive the actual end from
                // the number of bytes returned.
                (b, r.start..r.start + len)
            }
            Some(GetRange::Suffix(n)) => {
                let file_size = storage.file_num_bytes(&path).await.map_err(map_err)?;
                let start = file_size.saturating_sub(*n);
                let usize_range = start as usize..file_size as usize;
                let data = storage
                    .get_slice(&path, usize_range)
                    .await
                    .map_err(map_err)?;
                let b = Bytes::copy_from_slice(data.as_ref());
                let len = b.len() as u64;
                (b, start..start + len)
            }
            Some(GetRange::Offset(start)) => {
                let file_size = storage.file_num_bytes(&path).await.map_err(map_err)?;
                let start = *start;
                let usize_range = start as usize..file_size as usize;
                let data = storage
                    .get_slice(&path, usize_range)
                    .await
                    .map_err(map_err)?;
                let b = Bytes::copy_from_slice(data.as_ref());
                let len = b.len() as u64;
                (b, start..start + len)
            }
            None => {
                let data = storage.get_all(&path).await.map_err(map_err)?;
                let b = Bytes::copy_from_slice(data.as_ref());
                let len = b.len() as u64;
                (b, 0..len)
            }
        };

        let size = byte_range.end;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        };
        options.check_preconditions(&meta)?;

        let payload = if options.head {
            GetResultPayload::Stream(Box::pin(futures::stream::empty()))
        } else {
            GetResultPayload::Stream(Box::pin(futures::stream::once(async { Ok(bytes) })))
        };

        Ok(GetResult {
            payload,
            meta,
            range: byte_range,
            attributes: Default::default(),
        })
    }

    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        locations
            .map(|location| match location {
                Ok(_) => Err(object_store::Error::NotSupported {
                    source: "QuickwitObjectStore is read-only".into(),
                }),
                Err(err) => Err(err),
            })
            .boxed()
    }

    fn list(
        &self,
        _prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        Box::pin(futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "QuickwitObjectStore does not support listing".into(),
            })
        }))
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore does not support listing".into(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct RecordingStorageCache {
        paths: Mutex<Vec<PathBuf>>,
    }

    impl RecordingStorageCache {
        fn paths(&self) -> Vec<PathBuf> {
            self.paths.lock().unwrap().clone()
        }

        fn record(&self, path: &Path) {
            self.paths.lock().unwrap().push(path.to_path_buf());
        }
    }

    #[async_trait]
    impl StorageCache for RecordingStorageCache {
        async fn get(&self, path: &Path, _byte_range: Range<usize>) -> Option<OwnedBytes> {
            self.record(path);
            None
        }

        async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
            self.record(path);
            None
        }

        async fn put(&self, path: PathBuf, _byte_range: Range<usize>, _bytes: OwnedBytes) {
            self.record(&path);
        }

        async fn put_all(&self, path: PathBuf, _bytes: OwnedBytes) {
            self.record(&path);
        }
    }

    #[tokio::test]
    async fn scoped_storage_cache_prefixes_cache_keys() {
        let inner = Arc::new(RecordingStorageCache::default());
        let inner_cache: Arc<dyn StorageCache> = inner.clone();
        let cache = ScopedStorageCache::new("s3://metrics-bucket".to_string(), inner_cache);

        cache
            .get(Path::new("indexes/metrics.parquet"), 10..20)
            .await;
        cache
            .put(
                Path::new("indexes/metrics.parquet").to_path_buf(),
                10..20,
                OwnedBytes::new(&b"bytes"[..]),
            )
            .await;

        let paths = inner.paths();
        assert_eq!(
            paths,
            vec![
                PathBuf::from("s3://metrics-bucket#indexes/metrics.parquet"),
                PathBuf::from("s3://metrics-bucket#indexes/metrics.parquet"),
            ]
        );
    }
}
