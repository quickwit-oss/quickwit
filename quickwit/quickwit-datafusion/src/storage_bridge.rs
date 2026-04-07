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
//! ## Why this adapter exists
//!
//! `quickwit_storage::Storage` and `object_store::ObjectStore` are both
//! object-storage interfaces but have incompatible method signatures, error
//! types, and path representations.  DataFusion's `ParquetSource` requires
//! `ObjectStore`; Quickwit's split pipeline produces `Arc<dyn Storage>`.
//!
//! The long-term fix is for `quickwit-storage` types to implement `ObjectStore`
//! directly.  Until then, `QuickwitObjectStore` is the bridge.
//!
//! ## What is and is not implemented
//!
//! Only read operations (`get_opts`, `get_range`, `head`) are implemented.
//! All write and list operations return `NotSupported` — DataFusion only
//! reads parquet files through this store.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use quickwit_storage::Storage;

/// Adapts Quickwit's `Storage` trait to DataFusion's `ObjectStore` interface.
///
/// Only read operations are implemented since DataFusion only needs to read
/// parquet files.
#[derive(Debug)]
pub struct QuickwitObjectStore {
    storage: Arc<dyn Storage>,
}

impl QuickwitObjectStore {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

impl std::fmt::Display for QuickwitObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QuickwitObjectStore({})", self.storage.uri())
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
        let path = object_path_to_std(location);
        let location_str = location.as_ref();
        let map_err = |err| to_object_store_error(err, location_str);

        let (bytes, byte_range) = match options.range {
            Some(GetRange::Bounded(r)) => {
                let usize_range = r.start as usize..r.end as usize;
                let data = self
                    .storage
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
                let file_size = self
                    .storage
                    .file_num_bytes(&path)
                    .await
                    .map_err(map_err)?;
                let start = file_size.saturating_sub(n);
                let usize_range = start as usize..file_size as usize;
                let data = self
                    .storage
                    .get_slice(&path, usize_range)
                    .await
                    .map_err(map_err)?;
                let b = Bytes::copy_from_slice(data.as_ref());
                let len = b.len() as u64;
                (b, start..start + len)
            }
            // GetRange::Offset(start) and None both fall back to reading everything
            // from `start` (or 0) to the end via get_all.
            _ => {
                let data = self
                    .storage
                    .get_all(&path)
                    .await
                    .map_err(map_err)?;
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
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async {
                Ok(bytes)
            }))),
            meta,
            range: byte_range,
            attributes: Default::default(),
        })
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<u64>,
    ) -> ObjectStoreResult<Bytes> {
        let path = object_path_to_std(location);
        let usize_range = range.start as usize..range.end as usize;
        let data = self
            .storage
            .get_slice(&path, usize_range)
            .await
            .map_err(|err| to_object_store_error(err, location.as_ref()))?;
        Ok(Bytes::copy_from_slice(data.as_ref()))
    }

    async fn head(&self, location: &ObjectPath) -> ObjectStoreResult<ObjectMeta> {
        let path = object_path_to_std(location);
        let size = self
            .storage
            .file_num_bytes(&path)
            .await
            .map_err(|err| to_object_store_error(err, location.as_ref()))?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
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

    async fn delete(&self, _location: &ObjectPath) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
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

    async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
    }

    async fn copy_if_not_exists(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        Err(object_store::Error::NotSupported {
            source: "QuickwitObjectStore is read-only".into(),
        })
    }
}
