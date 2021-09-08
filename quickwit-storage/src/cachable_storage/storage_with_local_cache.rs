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
    collections::HashSet,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{PutPayload, Storage, StorageErrorKind, StorageResult, StorageUriResolver};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{
        broadcast::{self, Sender},
        Mutex,
    },
};

use super::{
    local_storage_cache::LocalStorageCache, CacheState, StorageCache, CACHE_STATE_FILE_NAME,
};

/// A storage with a backing [`Cache`].
pub struct StorageWithLocalStorageCache {
    remote_storage: Arc<dyn Storage>,
    cache: Mutex<Box<dyn StorageCache>>,
    in_flight_download_requests: Mutex<HashSet<PathBuf>>,
    notification_sender: Sender<(PathBuf, Bytes)>,
}

impl StorageWithLocalStorageCache {
    /// Create an instance of [`StorageWithLocalStorageCache`]
    ///
    /// It needs both the remote and local Storage to work with.
    /// max_item_count and max_num_bytes are used for the cache [`Capacity`].
    pub fn create(
        remote_storage: Arc<dyn Storage>,
        local_storage: Arc<dyn Storage>,
        ram_max_num_bytes: usize,
        max_num_files: usize,
        max_num_bytes: usize,
    ) -> StorageResult<Self> {
        let _local_storage_root = local_storage.root().ok_or_else(|| {
            StorageErrorKind::InternalError.with_error(anyhow!(
                "The local storage needs to have valid file system root path."
            ))
        })?;

        let (notification_sender, _) = broadcast::channel(10);
        Ok(Self {
            remote_storage,
            cache: Mutex::new(Box::new(LocalStorageCache::new(
                local_storage,
                ram_max_num_bytes,
                max_num_files,
                max_num_bytes,
            ))),
            in_flight_download_requests: Mutex::new(HashSet::new()),
            notification_sender,
        })
    }

    /// Create an instance of [`StorageWithLocalStorageCache`] from a path.
    ///
    /// It needs a folder `path` previously used by an instance of [`StorageWithLocalStorageCache`].
    /// Lastly, the file at `{path}/[`CACHE_STATE_FILE_NAME`]` should be present and valid.
    // TODO: this constructor should move into a factory.
    pub fn from_path(
        remote_storage: Arc<dyn Storage>,
        storage_uri_resolver: &StorageUriResolver,
        path: &Path,
    ) -> StorageResult<Self> {
        let cache_state = CacheState::from_path(path)?;

        let local_storage = storage_uri_resolver
            .resolve(&cache_state.local_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        let num_files = cache_state.items.len();
        let num_bytes: usize = cache_state.items.iter().map(|(_, size)| *size).sum();
        let mut local_storage_cache = LocalStorageCache::from_state(
            local_storage,
            cache_state.ram_capacity,
            cache_state.disk_capacity,
            cache_state.items,
        );
        local_storage_cache.num_files = num_files;
        local_storage_cache.num_bytes = num_bytes;

        let (notification_sender, _) = broadcast::channel(10);
        Ok(Self {
            remote_storage,
            cache: Mutex::new(Box::new(local_storage_cache)),
            in_flight_download_requests: Mutex::new(HashSet::new()),
            notification_sender,
        })
    }

    /// Helper method to register a `get_all` request,
    /// & put subsequent requests for this entry on hold.
    async fn get_all_from_remote_storage(&self, path: &Path) -> StorageResult<(Bytes, bool)> {
        let is_currently_downloading = self
            .in_flight_download_requests
            .lock()
            .await
            .get(&path.to_path_buf())
            .is_some();

        // Wait for download to complete
        if is_currently_downloading {
            let mut receiver = self.notification_sender.subscribe();
            while let Ok((downloaded_path, data)) = receiver.recv().await {
                if downloaded_path == path {
                    return Ok((data, false));
                }
            }
        }

        self.in_flight_download_requests
            .lock()
            .await
            .insert(path.to_path_buf());

        // Donwload & notify
        let data = self.remote_storage.get_all(path).await?;
        let _send_result = self
            .notification_sender
            .send((path.to_path_buf(), data.clone()));

        self.in_flight_download_requests
            .lock()
            .await
            .remove(&path.to_path_buf());

        Ok((data, true))
    }

    /// Helper method to register a `copy_to_file` request,
    /// & put subsequent requests for this entry on hold.
    async fn copy_to_from_remote_storage(
        &self,
        path: &Path,
        output_path: &Path,
    ) -> StorageResult<(Bytes, bool)> {
        let is_currently_downloading = self
            .in_flight_download_requests
            .lock()
            .await
            .get(&path.to_path_buf())
            .is_some();

        // Wait for download to complete
        if is_currently_downloading {
            let mut receiver = self.notification_sender.subscribe();
            while let Ok((downloaded_path, data)) = receiver.recv().await {
                if downloaded_path == path {
                    return Ok((data, false));
                }
            }
        }

        self.in_flight_download_requests
            .lock()
            .await
            .insert(path.to_path_buf());

        // Donwload & notify
        self.remote_storage.copy_to_file(path, output_path).await?;
        let data = read_all(output_path).await.map_err(|err| {
            StorageErrorKind::Io.with_error(anyhow!("Could not read from file: {}.", err))
        })?;

        self.notification_sender
            .send((path.to_path_buf(), data.clone()))
            .map_err(|err| {
                StorageErrorKind::InternalError
                    .with_error(anyhow!("Could not send notification: {}.", err))
            })?;

        self.in_flight_download_requests
            .lock()
            .await
            .remove(&path.to_path_buf());
        Ok((data, true))
    }
}

#[async_trait]
impl Storage for StorageWithLocalStorageCache {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.remote_storage.put(path, payload.clone()).await?;
        let mut locked_cache = self.cache.lock().await;
        locked_cache.put(path, payload).await?;
        locked_cache.save_state(self.remote_storage.uri()).await
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

        let (data, is_the_downloader) = self.copy_to_from_remote_storage(path, output_path).await?;
        if !is_the_downloader {
            write_all(output_path, data).await.map_err(|err| {
                StorageErrorKind::Io.with_error(anyhow!("Could not write to file `{}`.", err))
            })?;
            return Ok(());
        }

        let mut locked_cache = self.cache.lock().await;
        locked_cache
            .put(path, PutPayload::LocalFile(output_path.to_path_buf()))
            .await?;
        locked_cache.save_state(self.remote_storage.uri()).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        if let Some(bytes) = self
            .cache
            .lock()
            .await
            .get_slice(path, range.clone())
            .await?
        {
            return Ok(bytes);
        }

        //TODO: optimize this to avoid copying whole data in RAM (copy_to_file maybe?)
        let (all_bytes, is_the_downloader) = self.get_all_from_remote_storage(path).await?;
        let data = Bytes::copy_from_slice(&all_bytes[range.start..range.end]);
        if !is_the_downloader {
            return Ok(data);
        }

        let mut locked_cache = self.cache.lock().await;
        locked_cache
            .put(path, PutPayload::InMemory(all_bytes))
            .await?;
        locked_cache.save_state(self.remote_storage.uri()).await?;
        Ok(data)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.lock().await.get(path).await? {
            return Ok(bytes);
        }

        let (all_bytes, is_the_downloader) = self.get_all_from_remote_storage(path).await?;
        if !is_the_downloader {
            return Ok(all_bytes);
        }

        let mut locked_cache = self.cache.lock().await;
        locked_cache
            .put(path, PutPayload::InMemory(all_bytes.clone()))
            .await?;
        locked_cache.save_state(self.remote_storage.uri()).await?;
        Ok(all_bytes)
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.remote_storage.delete(path).await?;
        let mut locked_cache = self.cache.lock().await;
        if locked_cache.delete(path).await? {
            return locked_cache.save_state(self.remote_storage.uri()).await;
        }
        Ok(())
    }

    async fn file_num_bytes(&self, path: &Path) -> crate::StorageResult<u64> {
        {
            let mut locked_cache = self.cache.lock().await;
            if let Some(file_num_bytes) = locked_cache.file_num_bytes(path).await? {
                return Ok(file_num_bytes as u64);
            }
        }
        self.remote_storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.remote_storage.uri()
    }

    fn root(&self) -> Option<PathBuf> {
        None
    }
}

/// Helper function to read a file content.
async fn read_all(path: &Path) -> anyhow::Result<Bytes> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(Bytes::from(buffer))
}

/// Helper function to write a file content
async fn write_all(path: &Path, data: Bytes) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::create(path).await?;
    file.write_all(&data.to_vec()).await?;
    file.flush().await?;
    Ok(())
}

/// A struct embedding the cache parameters.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    ram_max_num_bytes: usize,
    max_num_files: usize,
    max_num_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ram_max_num_bytes: 1_000_000_000, // 1GB
            max_num_files: 1000,
            max_num_bytes: 10_000_000_000, // 10GB
        }
    }
}

/// TODO doc
pub fn create_cachable_storage(
    remote_storage: Arc<dyn Storage>,
    storage_uri_resolver: &StorageUriResolver,
    local_path: &Path,
    config: CacheConfig,
) -> crate::StorageResult<Arc<dyn Storage>> {
    let cache_state_file_path = local_path.join(CACHE_STATE_FILE_NAME);
    let storage = if cache_state_file_path.exists() {
        StorageWithLocalStorageCache::from_path(remote_storage, storage_uri_resolver, local_path)?
    } else {
        let local_storage_uri = format!("file://{}", local_path.to_string_lossy());
        let local_storage = storage_uri_resolver
            .resolve(&local_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        StorageWithLocalStorageCache::create(
            remote_storage,
            local_storage,
            config.ram_max_num_bytes,
            config.max_num_files,
            config.max_num_bytes,
        )?
    };

    Ok(Arc::new(storage))
}

#[cfg(test)]
mod tests {
    use crate::cachable_storage::CacheState;
    use crate::cachable_storage::DiskCapacity;
    use crate::cachable_storage::CACHE_STATE_FILE_NAME;
    use crate::LocalFileStorage;
    use crate::MockStorage;
    use crate::Storage;
    use anyhow::Context;
    use futures::future;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tempfile::{tempdir, TempDir};

    use super::StorageWithLocalStorageCache;
    use crate::tests::storage_test_suite;
    use crate::PutPayload;

    fn create_test_storages() -> anyhow::Result<(TempDir, MockStorage, MockStorage)> {
        let local_dir = tempdir()?;
        let remote_storage = MockStorage::default();
        let local_storage = MockStorage::default();
        Ok((local_dir, remote_storage, local_storage))
    }

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let remote_dir = tempdir()?;
        let remote_storage = Arc::new(LocalFileStorage::from_uri(&format!(
            "file://{}",
            remote_dir.path().to_string_lossy()
        ))?);
        let local_dir = tempdir()?;
        let local_storage = Arc::new(LocalFileStorage::from_uri(&format!(
            "file://{}",
            local_dir.path().to_string_lossy()
        ))?);

        let mut storage_with_cache = StorageWithLocalStorageCache::create(
            remote_storage,
            local_storage,
            3_000_000,
            500,
            3_000_000,
        )?;
        storage_test_suite(&mut storage_with_cache).await?;
        Ok(())
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
                Box::pin(async { Ok(()) })
            });
        remote_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("bar"));
            Box::pin(async { Ok(Bytes::from(b"data".to_vec())) })
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
                Box::pin(async { Ok(()) })
            });
        local_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(Bytes::from(b"data".to_vec())) })
        });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            500,
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
            assert_eq!(cache_state.items.len(), 2);
            assert_eq!(
                cache_state
                    .items
                    .iter()
                    .map(|(_, size)| *size)
                    .sum::<usize>(),
                7
            );
            assert_eq!(
                cache_state.disk_capacity,
                DiskCapacity {
                    max_num_files: 10,
                    max_num_bytes: 10
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
            Box::pin(async { Ok(Bytes::from(b"foo".to_vec())) })
        });

        local_storage
            .expect_uri()
            .times(1)
            .returning(|| "file://mock".to_string());
        local_storage
            .expect_root()
            .times(2)
            .returning(move || Some(path.clone()));
        local_storage.expect_get_all().times(2).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(Bytes::from(b"foo".to_vec())) })
        });
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Box::pin(async { Ok(()) })
            });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            500,
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
            Box::pin(async { Ok(()) })
        });

        local_storage
            .expect_root()
            .times(1)
            .returning(move || Some(path.clone()));

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            500,
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
                Box::pin(async { Ok(()) })
            });
        remote_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(()) })
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
                Box::pin(async { Ok(()) })
            });
        local_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(()) })
        });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            500,
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
        assert_eq!(cache_state.items.len(), 0);
        assert_eq!(
            cache_state
                .items
                .iter()
                .map(|(_, size)| *size)
                .sum::<usize>(),
            0
        );
        assert_eq!(
            cache_state.disk_capacity,
            DiskCapacity {
                max_num_files: 10,
                max_num_bytes: 10
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_does_not_call_remote_storage_concurrently_one_same_entry(
    ) -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_test_storages()?;
        let path = local_dir.path().to_path_buf();

        remote_storage
            .expect_uri()
            .times(1)
            .returning(|| "s3://remote".to_string());
        remote_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(Bytes::from(b"foo".to_vec()))
            })
        });

        local_storage
            .expect_uri()
            .times(1)
            .returning(|| "file://mock".to_string());
        local_storage
            .expect_root()
            .times(2)
            .returning(move || Some(path.clone()));
        // get_slice should not be called since all
        // requests are served from the single download
        local_storage.expect_get_slice().never();
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Box::pin(async { Ok(()) })
            });

        let cached_storage = StorageWithLocalStorageCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            500,
            10,
            10,
        )?;

        let get_task_1 = cached_storage.get_all(Path::new("foo"));
        let get_task_2 = cached_storage.get_all(Path::new("foo"));
        let (response1, response2) = future::join(get_task_1, get_task_2).await;

        assert_eq!(response1?, &b"foo"[..]);
        assert_eq!(response2?, &b"foo"[..]);

        Ok(())
    }
}
