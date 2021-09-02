/*
* Copyright (C) 2021 Quickwit Inc.
*
* Quickwit is offered under the AGPL v3.0 and as commercial software.
* For commercial licensing, contact us at hello@quickwit.io.
*
* AGPL:
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{PutPayload, Storage, StorageErrorKind, StorageResult};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;

use super::{local_storage_cache::LocalStorageCache, Cache, FULL_SLICE};

/// A storage with a backing [`LocalFileCache`].
pub struct StorageWithLocalStorageCache {
    remote_storage: Arc<dyn Storage>,
    cache: Mutex<Box<dyn Cache>>,
    //TODO: solve in-fligh requests will be solved with waiting on channel
    //subscirber
}

impl StorageWithLocalStorageCache {

    /// Create an instance of [`StorageWithLocalFileCache`]
    ///
    /// It needs to create both the remote and local Storage to work with.
    /// max_item_count and max_num_bytes are used for the cache [`Capacity`].
    pub fn create(
        remote_storage: Arc<dyn Storage>,
        local_storage: Arc<dyn Storage>,
        max_item_count: usize,
        max_num_bytes: usize,
    ) -> StorageResult<Self> {
        let _local_storage_root = local_storage.root().ok_or_else(|| {
            StorageErrorKind::InternalError
                .with_error(anyhow!("The local storage need to have valid root path."))
        })?;
        Ok(Self {
            remote_storage,
            cache: Mutex::new(Box::new(LocalStorageCache::new(
                local_storage,
                max_num_bytes,
                max_item_count,
            ))),
        })
    }
}

#[async_trait]
impl Storage for StorageWithLocalStorageCache {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.remote_storage.put(path, payload.clone()).await?;
        let mut looked_cache = self.cache.lock().await;
        looked_cache.put(path, payload).await?;
        looked_cache.save_state(self.remote_storage.uri()).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        if self
            .cache
            .lock()
            .await
            .copy_to_file(path, output_path)
            .await?
        {
            return Ok(());
        }

        self.remote_storage.copy_to_file(path, output_path).await?;
        let mut looked_cache = self.cache.lock().await;
        looked_cache
            .put(path, PutPayload::LocalFile(output_path.to_path_buf()))
            .await?;
        looked_cache.save_state(self.remote_storage.uri()).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.lock().await.get(path, range.clone()).await? {
            return Ok(bytes);
        }

        //TODO find ways around copying all bytes in RAM
        //copy_to_file maybe?
        let all_bytes = self.remote_storage.get_all(path).await?;
        let data = Bytes::copy_from_slice(&all_bytes[range.start..range.end]);
        let mut looked_cache = self.cache.lock().await;
        looked_cache
            .put(path, PutPayload::InMemory(all_bytes))
            .await?;
        looked_cache.save_state(self.remote_storage.uri()).await?;
        Ok(data)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.lock().await.get(path, FULL_SLICE).await? {
            return Ok(bytes);
        }

        let all_bytes = self.remote_storage.get_all(path).await?;

        let mut looked_cache = self.cache.lock().await;
        looked_cache
            .put(path, PutPayload::InMemory(all_bytes.clone()))
            .await?;
        looked_cache.save_state(self.remote_storage.uri()).await?;
        Ok(all_bytes)
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.remote_storage.delete(path).await?;
        let mut looked_cache = self.cache.lock().await;
        if looked_cache.delete(path).await? {
            return looked_cache.save_state(self.remote_storage.uri()).await;
        }
        Ok(())
    }

    async fn file_num_bytes(&self, path: &Path) -> crate::StorageResult<u64> {
        self.remote_storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.remote_storage.uri()
    }

    fn root(&self) -> Option<PathBuf> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_bis::CacheState;
    use crate::cache_bis::Capacity;
    use crate::cache_bis::CACHE_STATE_FILE_NAME;
    use crate::cache_bis::FULL_SLICE;
    use crate::MockStorage;
    use crate::Storage;
    use anyhow::Context;
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::{tempdir, TempDir};

    use crate::PutPayload;

    use super::StorageWithLocalStorageCache;

    fn create_test_storages() -> anyhow::Result<(TempDir, MockStorage, MockStorage)> {
        let local_dir = tempdir()?;
        let remote_storage = MockStorage::default();
        let local_storage = MockStorage::default();
        Ok((local_dir, remote_storage, local_storage))
    }

    #[tokio::test]
    async fn test_put_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_test_storages()?;
        let path = local_dir.path().to_path_buf();

        remote_storage
            .expect_uri()
            .times(2)
            .returning(|| "s3://remote".to_string());
        remote_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Ok(())
            });
        remote_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("bar"));
            Ok(Bytes::from(b"data".to_vec()))
        });

        local_storage
            .expect_uri()
            .times(2)
            .returning(|| "file://mock".to_string());
        local_storage
            .expect_root()
            .times(3)
            .returning(move || Some(path.clone()));
        local_storage
            .expect_put()
            .times(2)
            .returning(|path, _payload| {
                assert!(path == Path::new("foo") || path == Path::new("bar"));
                Ok(())
            });
        local_storage
            .expect_get_slice()
            .times(1)
            .returning(|path, range| {
                assert_eq!(path, Path::new("foo"));
                assert_eq!(range, FULL_SLICE);
                Ok(Bytes::from(b"data".to_vec()))
            });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            10,
            10,
        )?;
        cached_storage
            .put(
                Path::new("foo"),
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;
        assert_eq!(
            cached_storage.get_all(Path::new("bar")).await?,
            &b"data"[..]
        );
        assert_eq!(
            cached_storage.get_all(Path::new("foo")).await?,
            &b"data"[..]
        );

        //check cache state is good
        {
            let state_file_path = local_dir.path().join(CACHE_STATE_FILE_NAME);
            let json_file = std::fs::File::open(state_file_path)?;
            let reader = std::io::BufReader::new(json_file);
            let cache_state: CacheState = serde_json::from_reader(reader)
                .with_context(|| "Could not deserialise state".to_string())?;

            assert_eq!(cache_state.remote_storage_uri, String::from("s3://remote"));
            assert_eq!(cache_state.local_storage_uri, String::from("file://mock"));
            assert_eq!(cache_state.item_count, 2);
            assert_eq!(cache_state.num_bytes, 7);
            assert_eq!(
                cache_state.capacity,
                Capacity {
                    max_num_bytes: 10,
                    max_item_count: 10
                }
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_test_storages()?;
        let path = local_dir.path().to_path_buf();

        remote_storage
            .expect_uri()
            .times(1)
            .returning(|| "s3://remote".to_string());
        remote_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Ok(Bytes::from(b"foo".to_vec()))
        });

        local_storage
            .expect_uri()
            .times(1)
            .returning(|| "file://mock".to_string());
        local_storage
            .expect_root()
            .times(2)
            .returning(move || Some(path.clone()));
        local_storage
            .expect_get_slice()
            .times(2)
            .returning(|path, range| {
                assert_eq!(path, Path::new("foo"));
                assert_eq!(range, FULL_SLICE);
                Ok(Bytes::from(b"foo".to_vec()))
            });
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Ok(())
            });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            5,
            5,
        )?;
        assert_eq!(cached_storage.get_all(Path::new("foo")).await?, &b"foo"[..]);
        assert_eq!(cached_storage.get_all(Path::new("foo")).await?, &b"foo"[..]);
        assert_eq!(cached_storage.get_all(Path::new("foo")).await?, &b"foo"[..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_only_calls_remote_storage_for_unknow_cache_item() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_test_storages()?;
        let path = local_dir.path().to_path_buf();

        remote_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Ok(())
        });

        local_storage
            .expect_root()
            .times(1)
            .returning(move || Some(path.clone()));

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            5,
            5,
        )?;
        cached_storage.delete(Path::new("foo")).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_test_storages()?;
        let path = local_dir.path().to_path_buf();

        remote_storage
            .expect_uri()
            .times(2)
            .returning(|| "s3://remote".to_string());
        remote_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Ok(())
            });
        remote_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Ok(())
        });

        local_storage
            .expect_uri()
            .times(2)
            .returning(|| "file://mock".to_string());
        local_storage
            .expect_root()
            .times(3)
            .returning(move || Some(path.clone()));
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert!(path == Path::new("foo") || path == Path::new("bar"));
                Ok(())
            });
        local_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Ok(())
        });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            10,
            10,
        )?;
        cached_storage
            .put(
                Path::new("foo"),
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;

        cached_storage.delete(Path::new("foo")).await?;

        //verify cache state
        let state_file_path = local_dir.path().join(CACHE_STATE_FILE_NAME);
        let json_file = std::fs::File::open(state_file_path)?;
        let reader = std::io::BufReader::new(json_file);
        let cache_state: CacheState = serde_json::from_reader(reader)
            .with_context(|| "Could not deserialise state".to_string())?;
        assert_eq!(cache_state.remote_storage_uri, String::from("s3://remote"));
        assert_eq!(cache_state.local_storage_uri, String::from("file://mock"));
        assert_eq!(cache_state.item_count, 0);
        assert_eq!(cache_state.num_bytes, 0);
        assert_eq!(
            cache_state.capacity,
            Capacity {
                max_num_bytes: 10,
                max_item_count: 10
            }
        );

        Ok(())
    }
}
