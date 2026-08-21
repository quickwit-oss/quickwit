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

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::StorageBackend;
use tokio::io::AsyncRead;

use crate::local_file_storage::{LocalFileStorage, LocalFileStorageFactory};
use crate::prefix_storage::PrefixStorage;
use crate::ram_storage::{RamStorage, RamStorageFactory};
use crate::storage::SendableAsync;
#[cfg(feature = "azure")]
use crate::{AzureBlobStorage, AzureBlobStorageFactory};
use crate::{
    BulkDeleteError, DebouncedStorage, ListObjectsStream, OwnedBytes, PutPayload,
    S3CompatibleObjectStorage, S3CompatibleObjectStorageFactory, Storage, StorageGetSlice,
    StorageResolverError, StorageResult,
};
#[cfg(feature = "gcs")]
use crate::{GoogleCloudStorageFactory, opendal_storage::OpendalStorage};

/// A registered factory for one of Quickwit's supported storage backends.
///
/// Keeping the heterogeneous registry as an enum lets each factory return its concrete storage
/// type. Type erasure is deferred until the complete storage stack is shared with a caller.
pub enum StorageFactory {
    /// Local filesystem storage.
    File(LocalFileStorageFactory),
    /// In-memory storage.
    Ram(RamStorageFactory),
    /// S3-compatible object storage.
    S3(S3CompatibleObjectStorageFactory),
    /// Azure Blob Storage.
    #[cfg(feature = "azure")]
    Azure(AzureBlobStorageFactory),
    /// Google Cloud Storage.
    #[cfg(feature = "gcs")]
    Google(GoogleCloudStorageFactory),
    /// A backend unavailable in this build.
    Unsupported(UnsupportedStorage),
}

impl StorageFactory {
    /// Returns the backend targeted by this factory.
    pub fn backend(&self) -> StorageBackend {
        match self {
            Self::File(_) => StorageBackend::File,
            Self::Ram(_) => StorageBackend::Ram,
            Self::S3(_) => StorageBackend::S3,
            #[cfg(feature = "azure")]
            Self::Azure(_) => StorageBackend::Azure,
            #[cfg(feature = "gcs")]
            Self::Google(_) => StorageBackend::Google,
            Self::Unsupported(factory) => factory.backend,
        }
    }

    pub(crate) async fn resolve(&self, uri: &Uri) -> Result<ResolvedStorage, StorageResolverError> {
        let storage = match self {
            Self::File(factory) => ResolvedStorageInner::File(factory.resolve(uri)?),
            Self::Ram(factory) => ResolvedStorageInner::Ram(factory.resolve(uri)?),
            Self::S3(factory) => ResolvedStorageInner::S3(factory.resolve(uri).await?),
            #[cfg(feature = "azure")]
            Self::Azure(factory) => ResolvedStorageInner::Azure(factory.resolve(uri)?),
            #[cfg(feature = "gcs")]
            Self::Google(factory) => ResolvedStorageInner::Google(factory.resolve(uri)?),
            Self::Unsupported(factory) => {
                return Err(StorageResolverError::UnsupportedBackend(
                    factory.message.to_string(),
                ));
            }
        };
        Ok(ResolvedStorage { storage })
    }
}

impl From<LocalFileStorageFactory> for StorageFactory {
    fn from(factory: LocalFileStorageFactory) -> Self {
        Self::File(factory)
    }
}

impl From<RamStorageFactory> for StorageFactory {
    fn from(factory: RamStorageFactory) -> Self {
        Self::Ram(factory)
    }
}

impl From<S3CompatibleObjectStorageFactory> for StorageFactory {
    fn from(factory: S3CompatibleObjectStorageFactory) -> Self {
        Self::S3(factory)
    }
}

#[cfg(feature = "azure")]
impl From<AzureBlobStorageFactory> for StorageFactory {
    fn from(factory: AzureBlobStorageFactory) -> Self {
        Self::Azure(factory)
    }
}

#[cfg(feature = "gcs")]
impl From<GoogleCloudStorageFactory> for StorageFactory {
    fn from(factory: GoogleCloudStorageFactory) -> Self {
        Self::Google(factory)
    }
}

impl From<UnsupportedStorage> for StorageFactory {
    fn from(factory: UnsupportedStorage) -> Self {
        Self::Unsupported(factory)
    }
}

/// A concrete storage returned by [`crate::StorageResolver`].
///
/// The backend variants are intentionally private. Callers operate on this type through the
/// [`Storage`] implementation and can keep composing generic storage layers before introducing an
/// erased trait object at their outermost consumer boundary.
#[derive(Debug)]
pub struct ResolvedStorage {
    storage: ResolvedStorageInner,
}

#[derive(Debug)]
enum ResolvedStorageInner {
    File(DebouncedStorage<LocalFileStorage>),
    Ram(PrefixStorage<Arc<RamStorage>>),
    S3(DebouncedStorage<S3CompatibleObjectStorage>),
    #[cfg(feature = "azure")]
    Azure(DebouncedStorage<AzureBlobStorage>),
    #[cfg(feature = "gcs")]
    Google(DebouncedStorage<OpendalStorage>),
}

macro_rules! dispatch_storage {
    ($self:expr, $storage:ident => $expression:expr) => {
        match &$self.storage {
            ResolvedStorageInner::File($storage) => $expression,
            ResolvedStorageInner::Ram($storage) => $expression,
            ResolvedStorageInner::S3($storage) => $expression,
            #[cfg(feature = "azure")]
            ResolvedStorageInner::Azure($storage) => $expression,
            #[cfg(feature = "gcs")]
            ResolvedStorageInner::Google($storage) => $expression,
        }
    };
}

#[async_trait]
impl Storage for ResolvedStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        dispatch_storage!(self, storage => storage.check_connectivity().await)
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        dispatch_storage!(self, storage => storage.put(path, payload).await)
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        dispatch_storage!(self, storage => storage.copy_to(path, output).await)
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<u64> {
        dispatch_storage!(self, storage => storage.copy_to_file(path, output_path).await)
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        dispatch_storage!(self, storage => storage.get_slice(path, range).await)
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        dispatch_storage!(self, storage => storage.get_slice_stream(path, range).await)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        dispatch_storage!(self, storage => storage.get_all(path).await)
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        dispatch_storage!(self, storage => storage.delete(path).await)
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        dispatch_storage!(self, storage => storage.bulk_delete(paths).await)
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        dispatch_storage!(self, storage => storage.list(prefix))
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        dispatch_storage!(self, storage => storage.exists(path).await)
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        dispatch_storage!(self, storage => storage.file_num_bytes(path).await)
    }

    fn uri(&self) -> &Uri {
        dispatch_storage!(self, storage => storage.uri())
    }
}

impl StorageGetSlice for ResolvedStorage {
    async fn get_slice_unboxed(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        dispatch_storage!(self, storage => storage.get_slice_unboxed(path, range).await)
    }
}

/// A storage factory for handling unsupported or unavailable storage backends.
#[derive(Debug, Clone)]
pub struct UnsupportedStorage {
    backend: StorageBackend,
    message: &'static str,
}

impl UnsupportedStorage {
    /// Creates a new [`UnsupportedStorage`].
    pub fn new(backend: StorageBackend, message: &'static str) -> Self {
        Self { backend, message }
    }
}
