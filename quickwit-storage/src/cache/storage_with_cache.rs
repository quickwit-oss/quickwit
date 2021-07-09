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

use crate::cache::Cache;
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
            self.storage.get_slice(path, byte_range).await
        }
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.get_all(&path).await {
            Ok(bytes)
        } else {
            self.storage.get_all(path).await
        }
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        unimplemented!("StorageWithCache is readonly. Failed to delete {:?}", path)
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
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
