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
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;

use crate::cache::StorageCache;
use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, OwnedBytes, Storage, StorageResult};

/// Use with care, StorageWithCache is read-only.
pub struct StorageWithCache {
    pub storage: Arc<dyn Storage>,
    pub cache: Arc<dyn StorageCache>,
}

impl fmt::Debug for StorageWithCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageWithCache").finish()
    }
}

/// A read-only view of another storage, with the objects it reads kept in a cache.
///
/// Hidden contract: this wrapper does not forward the conditional-write methods. `put` is not
/// implemented either, and forwarding a conditional write would be worse than refusing it: the
/// write would land in the inner storage while the cache this wrapper serves reads from kept the
/// version the write path never observed. The trait's default implementations answer
/// [`StorageErrorKind::Unsupported`], and
/// `conditional_writes_stay_unsupported_on_a_read_only_wrapper` pins both the refusal and the fact
/// that nothing reached the inner storage.
#[async_trait]
impl Storage for StorageWithCache {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(
        &self,
        path: &Path,
        _payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        unimplemented!("StorageWithCache is readonly. Failed to put {:?}", path)
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
        if let Some(bytes) = self.cache.get(path, byte_range.clone()).await {
            Ok(bytes)
        } else {
            let bytes = self.storage.get_slice(path, byte_range.clone()).await?;
            self.cache
                .put(path.to_owned(), byte_range, bytes.clone())
                .await;
            Ok(bytes)
        }
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        _range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        unimplemented!(
            "StorageWithCache does not support streamed read yet. Failed to get {:?}",
            path
        )
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        if let Some(bytes) = self.cache.get_all(path).await {
            Ok(bytes)
        } else {
            let bytes = self.storage.get_all(path).await?;
            self.cache.put_all(path.to_owned(), bytes.clone()).await;
            Ok(bytes)
        }
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        unimplemented!("Failed to delete file `{path:?}`. `StorageWithCache` is read-only.")
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        unimplemented!("Failed to delete files `{paths:?}`. `StorageWithCache` is read-only.")
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.storage.list(prefix)
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        MockStorage, MockStorageCache, ObjectVersion, OwnedBytes, RamStorage, StorageErrorKind,
        wrap_storage_with_cache,
    };

    /// A read-only wrapper must not let a conditional write reach the inner storage: the reads it
    /// serves come from its cache, which a write through the wrapper would not update.
    /// `Unsupported` is the answer; returning success, or writing on the side, would both be
    /// wrong.
    #[tokio::test]
    async fn conditional_writes_stay_unsupported_on_a_read_only_wrapper() {
        let inner_storage = Arc::new(RamStorage::default());
        let storage =
            wrap_storage_with_cache(Arc::new(MockStorageCache::default()), inner_storage.clone());
        let path = Path::new("a-file");

        let error = storage
            .put_if_absent(path, Box::new(b"payload".to_vec()))
            .await
            .expect_err("a read-only wrapper must not accept a conditional write");
        assert_eq!(error.kind(), StorageErrorKind::Unsupported);

        let error = storage
            .put_if_version_matches(
                path,
                Box::new(b"payload".to_vec()),
                &ObjectVersion::new("version-1"),
            )
            .await
            .expect_err("a read-only wrapper must not accept a conditional write");
        assert_eq!(error.kind(), StorageErrorKind::Unsupported);

        let error = storage
            .get_all_with_version(path)
            .await
            .expect_err("a read-only wrapper must not answer a versioned read");
        assert_eq!(error.kind(), StorageErrorKind::Unsupported);

        assert!(
            !inner_storage.exists(path).await.unwrap(),
            "nothing may reach the inner storage through a read-only wrapper"
        );
    }

    #[tokio::test]
    async fn put_in_cache_test() {
        let mut mock_storage = MockStorage::default();
        let mut mock_cache = MockStorageCache::default();
        let actual_cache: Arc<Mutex<HashMap<PathBuf, OwnedBytes>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let cache1 = actual_cache.clone();
        mock_cache
            .expect_get_all()
            .times(2)
            .returning(move |path| cache1.lock().unwrap().get(path).cloned());
        mock_cache
            .expect_put_all()
            .times(1)
            .returning(move |path, data| {
                let actual_cache = actual_cache.clone();
                actual_cache.lock().unwrap().insert(path, data);
            });

        mock_storage
            .expect_get_all()
            .times(1)
            .returning(|_path| Ok(OwnedBytes::new(vec![1, 2, 3])));

        let storage_with_cache = StorageWithCache {
            storage: Arc::new(mock_storage),
            cache: Arc::new(mock_cache),
        };

        let data1 = storage_with_cache
            .get_all(Path::new("cool_file"))
            .await
            .unwrap();
        // hitting the cache
        let data2 = storage_with_cache
            .get_all(Path::new("cool_file"))
            .await
            .unwrap();
        assert_eq!(data1, data2);
    }
}
