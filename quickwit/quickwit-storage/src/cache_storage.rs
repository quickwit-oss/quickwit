// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{CacheStorageConfig, StorageBackend};
use quickwit_proto::cache_storage::SplitsChangeNotification;
use serde::{Deserialize, Serialize};

use crate::cached_splits_registry::CachedSplitRegistry;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, SendableAsync, Storage, StorageFactory,
    StorageResolver, StorageResolverError, StorageResult,
};

/// Storage that wraps two storages using one of them as a cache for another
#[derive(Clone)]
pub struct CacheStorage {
    uri: Uri,
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Storage>,
    cache_split_registry: CachedSplitRegistry,
}

impl fmt::Debug for CacheStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("CacheStorage")
            .field("storage", &self.storage)
            .field("cache", &self.cache)
            .finish()
    }
}

impl CacheStorage {
    /// Create a resolver that can uses ram storage for both cache and the upstream storage
    #[cfg(test)]
    pub async fn for_test() -> CacheStorage {
        let storage_resolver = StorageResolver::for_test();
        let storage_config = CacheStorageConfig::for_test();
        let cache_split_registry = CachedSplitRegistry::new(storage_config);
        let storage = storage_resolver
            .resolve(&Uri::for_test("ram://data"))
            .await
            .unwrap();
        let cache = storage_resolver
            .resolve(&Uri::for_test("ram://cache"))
            .await
            .unwrap();
        CacheStorage {
            uri: Uri::for_test("cache://ram://cache"),
            storage,
            cache,
            cache_split_registry,
        }
    }

    fn counters(&self) -> &AtomicCacheStorageCounters {
        self.cache_split_registry.counters()
    }
}

#[async_trait]
impl Storage for CacheStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.cache.check_connectivity().await?;
        self.storage.check_connectivity().await?;
        Ok(())
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.storage.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        if let Some(results) = self
            .cache_split_registry
            .get_slice(self.cache.clone(), path, range.clone())
            .await
        {
            self.counters().num_hits.fetch_add(1, Ordering::Relaxed);
            results
        } else {
            self.counters().num_misses.fetch_add(1, Ordering::Relaxed);
            self.storage.get_slice(path, range).await
        }
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.storage.bulk_delete(paths).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        if let Some(results) = self
            .cache_split_registry
            .get_all(self.cache.clone(), path)
            .await
        {
            self.counters().num_hits.fetch_add(1, Ordering::Relaxed);
            results
        } else {
            self.counters().num_misses.fetch_add(1, Ordering::Relaxed);
            self.storage.get_all(path).await
        }
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }
}

/// Cache storage stats
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheStorageCounters {
    /// number of splits that should be cached
    pub num_cached_splits: usize,
    /// number of splits that are downloaded
    pub num_downloaded_splits: usize,
    /// number of cache hits
    pub num_hits: usize,
    /// number of cache misses
    pub num_misses: usize,
}

#[derive(Default)]
pub(crate) struct AtomicCacheStorageCounters {
    /// number of splits that should be cached
    pub num_cached_splits: AtomicUsize,
    /// number of splits that are downloaded
    pub num_downloaded_splits: AtomicUsize,
    /// number of cache hits
    pub num_hits: AtomicUsize,
    /// number of cache misses
    pub num_misses: AtomicUsize,
}

impl AtomicCacheStorageCounters {
    pub fn as_counters(&self) -> CacheStorageCounters {
        CacheStorageCounters {
            num_cached_splits: self.num_cached_splits.load(Ordering::Relaxed),
            num_downloaded_splits: self.num_downloaded_splits.load(Ordering::Relaxed),
            num_hits: self.num_hits.load(Ordering::Relaxed),
            num_misses: self.num_misses.load(Ordering::Relaxed),
        }
    }
}

/// Storage resolver for [`CacheStorage`].
#[derive(Clone)]
pub struct CacheStorageFactory {
    inner: Arc<InnerCacheStorageFactory>,
}

struct InnerCacheStorageFactory {
    storage_config: CacheStorageConfig,
    cache_split_registry: CachedSplitRegistry,
}

impl CacheStorageFactory {
    /// Create a new storage factory
    pub fn new(storage_config: CacheStorageConfig) -> Self {
        Self {
            inner: Arc::new(InnerCacheStorageFactory {
                storage_config: storage_config.clone(),
                cache_split_registry: CachedSplitRegistry::new(storage_config),
            }),
        }
    }

    /// Returns the cache storage stats
    pub fn counters(&self) -> CacheStorageCounters {
        self.inner.cache_split_registry.counters().as_counters()
    }

    /// Update all split caches on the node
    pub async fn update_split_cache(
        &self,
        storage_resolver: &StorageResolver,
        notifications: Vec<SplitsChangeNotification>,
    ) -> anyhow::Result<()> {
        let mut splits: Vec<(String, String, Uri)> = Vec::new();
        for notification in notifications {
            splits.push((
                notification.split_id,
                notification.index_id,
                notification.storage_uri.parse()?,
            ));
        }
        self.inner
            .cache_split_registry
            .bulk_update(storage_resolver, &splits)
            .await;
        Ok(())
    }
}

#[async_trait]
impl StorageFactory for CacheStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Cache
    }

    async fn resolve(
        &self,
        storage_resolver: &StorageResolver,
        uri: &Uri,
    ) -> Result<Arc<dyn Storage>, StorageResolverError> {
        if uri.protocol().is_cache() {
            // TODO: Prevent stack overflow here if cache uri is also cache
            let cache_uri = self.inner.storage_config.cache_uri().ok_or_else(|| {
                StorageResolverError::InvalidConfig("Expected cache uri in config.".to_string())
            })?;
            let cache = storage_resolver.resolve(&cache_uri).await?;
            let upstream_uri = uri
                .scheme_specific_part()
                .ok_or_else(|| {
                    StorageResolverError::InvalidConfig(
                        "Expected cache uri with child part in index config".to_string(),
                    )
                })?
                .parse::<Uri>()
                .map_err(|err| {
                    let message =
                        format!("Cannot parse index storage uri `{:?}`.", err.to_string());
                    StorageResolverError::InvalidConfig(message)
                })?;
            let storage = storage_resolver.resolve(&upstream_uri).await?;
            let cache_storage = CacheStorage {
                uri: uri.clone(),
                storage,
                cache,
                cache_split_registry: self.inner.cache_split_registry.clone(),
            };
            Ok(Arc::new(cache_storage))
        } else {
            let message = format!("URI `{uri}` is not a valid Cache URI.");
            Err(StorageResolverError::InvalidUri(message))
        }
    }

    fn as_cache_storage_factory(&self) -> Option<CacheStorageFactory> {
        Some(self.clone())
    }
}

#[cfg(test)]
mod tests {

    use quickwit_config::CacheStorageConfig;

    use super::*;
    use crate::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let mut ram_storage = CacheStorage::for_test().await;
        storage_test_suite(&mut ram_storage).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_storage_factory() {
        let storage_resolver = StorageResolver::ram_for_test();
        let storage_config = CacheStorageConfig::for_test();
        let cache_storage_factory = CacheStorageFactory::new(storage_config);
        let cache_uri = Uri::from_well_formed("s3:///foo");
        let err = cache_storage_factory
            .resolve(&storage_resolver, &cache_uri)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let data_uri = Uri::from_well_formed("cache://ram://");
        let data_storage = cache_storage_factory
            .resolve(&storage_resolver, &data_uri)
            .await
            .unwrap();

        let data_storage_two = cache_storage_factory
            .resolve(&storage_resolver, &data_uri)
            .await
            .unwrap();
        assert_eq!(data_storage.uri(), data_storage_two.uri());
    }
}
