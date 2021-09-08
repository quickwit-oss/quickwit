//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use crate::Cache;
use crate::{PutPayload, Storage, StorageFactory, StorageResult};

/// Use with care, StorageWithCache is read-only.
struct StorageWithCache {
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Cache>,
}

#[async_trait]
impl Storage for StorageWithCache {
    async fn put(&self, path: &Path, _payload: PutPayload) -> StorageResult<()> {
        unimplemented!("StorageWithCache is readonly. Failed to put {:?}", path)
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        self.storage.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<Bytes> {
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

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.get_all(path).await {
            Ok(bytes)
        } else {
            let bytes = self.storage.get_all(path).await?;
            self.cache.put_all(path.to_owned(), bytes.clone()).await;
            Ok(bytes)
        }
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        unimplemented!("StorageWithCache is readonly. Failed to delete {:?}", path)
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }
}

/// A StorageFactory that wraps all Storage that are produced with a cache.
///
/// The cache is shared with all of the storage instances.
pub struct StorageWithCacheFactory {
    storage_factory: Arc<dyn StorageFactory>,
    cache: Arc<dyn Cache>,
}

impl StorageWithCacheFactory {
    /// Creates a new StorageFactory with the given cache.
    pub fn new(storage_factory: Arc<dyn StorageFactory>, cache: Arc<dyn Cache>) -> Self {
        StorageWithCacheFactory {
            storage_factory,
            cache,
        }
    }
}

impl StorageFactory for StorageWithCacheFactory {
    fn protocol(&self) -> String {
        self.storage_factory.protocol()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<Arc<dyn Storage>> {
        let storage = self.storage_factory.resolve(uri)?;
        Ok(Arc::new(StorageWithCache {
            storage,
            cache: self.cache.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::MockCache;
    use crate::MockStorage;

    #[tokio::test]
    async fn put_in_cache_test() {
        let mut mock_storage = MockStorage::default();
        let mut mock_cache = MockCache::default();
        let actual_cache: Arc<Mutex<HashMap<PathBuf, Bytes>>> =
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
            .returning(|_path| Box::pin(async { Ok(Bytes::from_static(&[1, 2, 3])) }));

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
