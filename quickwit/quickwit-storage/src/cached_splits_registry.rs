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

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use quickwit_common::uri::Uri;
use quickwit_common::{split_file, split_id};
use quickwit_config::CacheStorageConfig;
use tantivy::directory::OwnedBytes;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tracing::error;

use crate::cache_storage::AtomicCacheStorageCounters;
use crate::{Storage, StorageResolver, StorageResult};

enum SplitState {
    Initializing,
    Preparing,
    Ready,
    Deleting,
}

struct SplitInfo {
    index_id: String,
    state_lock: Arc<RwLock<SplitState>>,
}

#[derive(Clone)]
pub struct CachedSplitRegistry {
    inner: Arc<InnerCachedSplitRegistry>,
}

struct InnerCachedSplitRegistry {
    storage_config: CacheStorageConfig,
    counters: AtomicCacheStorageCounters,
    splits: RwLock<HashMap<String, SplitInfo>>,
    cache_storage: tokio::sync::OnceCell<Option<Arc<dyn Storage>>>,
}

impl CachedSplitRegistry {
    pub(crate) fn new(storage_config: CacheStorageConfig) -> Self {
        Self {
            inner: Arc::new(InnerCachedSplitRegistry {
                storage_config,
                counters: Default::default(),
                splits: RwLock::new(HashMap::new()),
                cache_storage: Default::default(),
            }),
        }
    }

    pub(crate) fn counters(&self) -> &AtomicCacheStorageCounters {
        &self.inner.counters
    }

    // TODO why `Option`
    async fn cache_storage(&self, storage_resolver: &StorageResolver) -> Option<Arc<dyn Storage>> {
        self.inner
            .cache_storage
            .get_or_init(|| async {
                let cache_uri = self.inner.storage_config.cache_uri()?;
                match storage_resolver.resolve(&cache_uri).await {
                    Ok(cache) => Some(cache),
                    Err(err) => {
                        error!("Failed to resolve cache storage. {:?}", err);
                        None
                    }
                }
            })
            .await
            .clone()
    }

    #[allow(dead_code)]
    pub async fn load(
        &self,
        storage_resolver: &StorageResolver,
        split_id: &str,
        index_id: &str,
        storage_uri: &Uri,
    ) {
        let mut splits_guard = self.inner.splits.write().await;
        self.inner_load(
            &mut splits_guard,
            storage_resolver,
            split_id,
            index_id,
            storage_uri,
        )
        .await;
    }

    async fn inner_load(
        &self,
        splits_guard: &mut RwLockWriteGuard<'_, HashMap<String, SplitInfo>>,
        storage_resolver: &StorageResolver,
        split_id: &str,
        index_id: &str,
        storage_uri: &Uri,
    ) {
        if splits_guard.contains_key(split_id) {
            // No need to do anything: the split is already here.
            return;
        }
        let state_lock = Arc::new(RwLock::new(SplitState::Initializing));
        let split = SplitInfo {
            index_id: index_id.to_string(),
            state_lock: state_lock.clone(),
        };
        splits_guard.insert(split_id.to_string(), split);
        tokio::spawn({
            let self_clone = self.clone();
            let split_id = split_id.to_string();
            let index_id = index_id.to_string();
            let storage_uri = storage_uri.clone();
            let state_lock = state_lock;
            let storage_resolver = storage_resolver.clone();
            async move {
                let mut state_guard = state_lock.write().await;
                match *state_guard {
                    SplitState::Initializing => {
                        *state_guard = SplitState::Preparing;
                        let file_name = split_file(&split_id);
                        let path = Path::new(&file_name);
                        let output_path = Path::new(&index_id).join(split_file(&split_id));
                        if let Err(err) = self_clone
                            .copy_split(&storage_resolver, &storage_uri, path, &output_path)
                            .await
                        {
                            error!(error=?err, "Failed to copy split to cache.");
                            self_clone.inner.splits.write().await.remove(&split_id);
                        } else {
                            *state_guard = SplitState::Ready;
                            self_clone
                                .inner
                                .counters
                                .num_downloaded_splits
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    SplitState::Deleting => {
                        // The resource was deleted while we were trying to initialize it
                        if self_clone
                            .inner
                            .splits
                            .write()
                            .await
                            .remove(&split_id)
                            .is_some()
                        {}
                    }
                    _ => {
                        // Shouldn't be here.
                    }
                };
            }
        });
    }

    async fn copy_split(
        &self,
        storage_resolver: &StorageResolver,
        storage_uri: &Uri,
        path: &Path,
        output_path: &Path,
    ) -> anyhow::Result<()> {
        // TODO: Figure out an ealier way to handle these issues
        if let Some(cache_storage) = self.cache_storage(storage_resolver).await {
            let storage = storage_resolver.resolve(storage_uri).await?;
            storage
                .copy_to_storage(path, cache_storage, output_path)
                .await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn delete(&self, storage_resolver: &StorageResolver, split_id: &String) {
        let splits_guard = self.inner.splits.write().await;
        self.inner_delete(&splits_guard, storage_resolver, split_id)
            .await;
    }

    async fn inner_delete(
        &self,
        splits_guard: &RwLockWriteGuard<'_, HashMap<String, SplitInfo>>,
        storage_resolver: &StorageResolver,
        split_id: &String,
    ) {
        if let Some(value) = splits_guard.get(split_id) {
            let mut state_guard = value.state_lock.write().await;
            if !matches!(*state_guard, SplitState::Deleting) {
                *state_guard = SplitState::Deleting;
                self.inner
                    .counters
                    .num_downloaded_splits
                    .fetch_sub(1, Ordering::Relaxed);
                tokio::spawn({
                    let self_clone = self.clone();
                    let split_id = split_id.clone();
                    let split_index = value.index_id.clone();
                    let path = Path::new(&split_index).join(split_file(&split_id));
                    let storage_resolver = storage_resolver.clone();
                    async move {
                        if let Err(err) = self_clone.delete_split(&storage_resolver, &path).await {
                            error!(error=?err, "Failed to deleted cached resource.");
                        }
                        self_clone.inner.splits.write().await.remove(&split_id);
                    }
                });
            }
        }
    }

    async fn delete_split(
        &self,
        storage_resolver: &StorageResolver,
        path: &Path,
    ) -> anyhow::Result<()> {
        // TODO: This should've been handle earlier, but we need have a more graceful way of dealing
        // with possible issues
        if let Some(cache_storage) = self.cache_storage(storage_resolver).await {
            cache_storage.delete(path).await?;
        }
        Ok(())
    }

    pub async fn bulk_update(
        &self,
        storage_resolver: &StorageResolver,
        splits: &[(String, String, Uri)],
    ) {
        let mut splits_guard = self.inner.splits.write().await;
        let new: HashSet<String> = splits.iter().map(|(key, _, _)| key).cloned().collect();
        let current: HashSet<String> = splits_guard.keys().cloned().collect();
        let deleted = current.difference(&new);

        // Process Additions
        for (split_id, index_id, origin_uri) in splits {
            self.inner_load(
                &mut splits_guard,
                storage_resolver,
                split_id,
                index_id,
                origin_uri,
            )
            .await;
        }

        // Process Delitions
        for split_id in deleted {
            self.inner_delete(&splits_guard, storage_resolver, split_id)
                .await;
        }
    }

    pub async fn get_slice(
        &self,
        cache: Arc<dyn Storage>,
        path: &Path,
        range: Range<usize>,
    ) -> Option<StorageResult<OwnedBytes>> {
        let splits_guard = self.inner.splits.read().await;
        if let Some(split_id) = split_id(path) {
            if let Some(split) = splits_guard.get(split_id) {
                let state_guard = split.state_lock.write().await;
                if matches!(*state_guard, SplitState::Ready) {
                    let path = Path::new(&split.index_id).join(path);
                    // Only return cache if it is ready
                    return Some(cache.get_slice(&path, range).await);
                }
            }
        }
        None
    }

    pub async fn get_all(
        &self,
        cache: Arc<dyn Storage>,
        path: &Path,
    ) -> Option<StorageResult<OwnedBytes>> {
        let splits_guard = self.inner.splits.read().await;
        if let Some(split_id) = split_id(path) {
            if let Some(split) = splits_guard.get(split_id) {
                let state_guard = split.state_lock.write().await;
                if matches!(*state_guard, SplitState::Ready) {
                    let path = Path::new(&split.index_id).join(path);
                    // Only return cache if it is ready
                    return Some(cache.get_all(&path).await);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use quickwit_common::test_utils::wait_until_predicate;

    use crate::cached_splits_registry::*;
    use crate::*;

    #[tokio::test]
    async fn test_basic_workflow() {
        let storage_resolver = StorageResolver::ram_for_test();
        let config = CacheStorageConfig::for_test();
        let registry = CachedSplitRegistry::new(config.clone());
        let storage = storage_resolver
            .resolve(&Uri::for_test("ram://data"))
            .await
            .unwrap();
        let cache = storage_resolver
            .resolve(&config.cache_uri().unwrap())
            .await
            .unwrap();
        let split_id = "abcd".to_string();
        let index_id = "my_index".to_string();
        let path = PathBuf::new().join("abcd.split");
        assert_eq!(registry.counters().as_counters().num_downloaded_splits, 0);
        storage
            .put(&path, Box::new(b"abcdefg"[..].to_vec()))
            .await
            .unwrap();

        registry
            .load(&storage_resolver, &split_id, &index_id, storage.uri())
            .await;

        let registry_clone = registry.clone();
        let cache_clone = cache.clone();
        let path_clone = path.clone();
        wait_until_predicate(
            {
                move || {
                    test_get_all(
                        registry_clone.clone(),
                        cache_clone.clone(),
                        path_clone.clone(),
                        b"abcdefg",
                    )
                }
            },
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        assert_eq!(registry.counters().as_counters().num_downloaded_splits, 1);

        registry.delete(&storage_resolver, &split_id).await;

        let registry_clone = registry.clone();
        let cache_clone = cache.clone();
        let path_clone = path.clone();
        assert!(wait_until_predicate(
            {
                move || {
                    test_get_all(
                        registry_clone.clone(),
                        cache_clone.clone(),
                        path_clone.clone(),
                        b"abcdefg",
                    )
                }
            },
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await
        .is_err());

        assert_eq!(registry.counters().as_counters().num_downloaded_splits, 0);
    }

    async fn test_get_all(
        registry: CachedSplitRegistry,
        cache: Arc<dyn Storage>,
        path: PathBuf,
        expected: &[u8],
    ) -> bool {
        if let Some(Ok(actual)) = registry.get_all(cache, &path).await {
            actual.eq(expected)
        } else {
            false
        }
    }
}
