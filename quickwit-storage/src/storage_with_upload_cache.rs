// Copyright (C) 2021 Quickwit, Inc.
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

use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use tempfile::Builder;
use tokio::sync::{Mutex, MutexGuard};

use crate::{
    LocalFileStorage, PutPayload, Storage, StorageErrorKind, StorageResult, StorageUriResolver,
};

const CACHE_STATE_FILE_NAME: &str = "cache-sate.json";

/// Capacity encapsulates the maximum number of items a cache can hold.
/// We need to account for the number of items as well as the size of each item.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiskCapacity {
    /// Maximum of number of files.
    max_num_files: usize,
    /// Maximum size in bytes.
    max_num_bytes: usize,
}

impl DiskCapacity {
    pub fn exceeds_capacity(&self, num_bytes: usize, num_files: usize) -> bool {
        self.max_num_bytes < num_bytes || self.max_num_files < num_files
    }
}

/// CacheState is a struct for serializing/deserializing the cache state.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CacheState {
    /// Uri of the local storage.
    pub local_storage_uri: String,
    /// The disk capacity
    pub disk_capacity: DiskCapacity,
    /// The list of items in the cache.
    pub items: Vec<(PathBuf, usize)>,
}

impl CacheState {
    /// Construct an instance of [`CacheState`] from a persisted cache state file.
    pub fn from_path(path: &Path) -> StorageResult<Self> {
        let file_path = path.to_path_buf().join(CACHE_STATE_FILE_NAME);
        let json_file = std::fs::File::open(file_path)?;
        let reader = std::io::BufReader::new(json_file);
        serde_json::from_reader(reader)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))
    }
}

/// A struct to wrap the lru cache and its info.
pub struct WrappedCache {
    /// Underlying L.R.U cache.
    pub lru: LruCache<PathBuf, usize>,
    /// Current number of bytes in the cache.
    pub num_bytes: usize,
    /// Current number of files in the cache.
    pub num_files: usize,
}

/// A storage with a backing file system cache.
pub struct StorageWithUploadCache {
    remote_storage: Arc<dyn Storage>,
    local_storage: Arc<dyn Storage>,
    local_storage_root: PathBuf,
    disk_capacity: DiskCapacity,
    disk_cache: Mutex<WrappedCache>,
}

impl StorageWithUploadCache {
    /// Create an instance of [`StorageWithUploadCache`]
    ///
    /// It needs both the remote and local storage to work with.
    /// max_item_count and max_num_bytes are used for the cache [`Capacity`].
    pub fn create(
        remote_storage: Arc<dyn Storage>,
        local_storage: Arc<dyn Storage>,
        max_num_files: usize,
        max_num_bytes: usize,
    ) -> StorageResult<Self> {
        // Extract the local storage root path, also validate that local storage
        // is of type [`LocalFileStorage]
        let local_storage_root_path =
            LocalFileStorage::extract_root_path_from_uri(&local_storage.uri())?;

        Ok(Self {
            remote_storage,
            local_storage,
            local_storage_root: local_storage_root_path,
            disk_capacity: DiskCapacity {
                max_num_files,
                max_num_bytes,
            },
            disk_cache: Mutex::new(WrappedCache {
                lru: LruCache::unbounded(),
                num_bytes: 0,
                num_files: 0,
            }),
        })
    }

    /// Create an instance of [`StorageWithUploadCache`] from a path.
    ///
    /// It needs a folder `path` previously used by an instance of [`StorageWithUploadCache`].
    /// Lastly, the file at `{path}/[`CACHE_STATE_FILE_NAME`]` should be present with valid content.
    pub fn from_path(
        remote_storage: Arc<dyn Storage>,
        storage_uri_resolver: &StorageUriResolver,
        path: &Path,
    ) -> StorageResult<Self> {
        let cache_state = CacheState::from_path(path)?;
        let local_storage = storage_uri_resolver
            .resolve(&cache_state.local_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        // Verify that the provided `root_path` and extracted `root_path` match.
        let local_storage_root_path =
            LocalFileStorage::extract_root_path_from_uri(&cache_state.local_storage_uri)?;
        if local_storage_root_path != path.to_path_buf() {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Invalid root path: {}, but {} expected ",
                path.to_string_lossy(),
                local_storage_root_path.to_string_lossy()
            )));
        }

        let num_files = cache_state.items.len();
        let num_bytes = cache_state.items.iter().map(|(_, size)| *size).sum();
        if cache_state
            .disk_capacity
            .exceeds_capacity(num_bytes, num_files)
        {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial cache items should not exceed cache capacity.",
            )));
        }

        let mut disk_cache = LruCache::unbounded();
        for (path, size) in cache_state.items {
            disk_cache.put(path, size);
        }

        Ok(Self {
            remote_storage,
            local_storage,
            local_storage_root: local_storage_root_path,
            disk_capacity: cache_state.disk_capacity,
            disk_cache: Mutex::new(WrappedCache {
                lru: LruCache::unbounded(),
                num_bytes,
                num_files,
            }),
        })
    }

    /// Check if an incomming item will exceed the capacity once inserted.
    fn exceeds_capacity(&self, num_bytes: usize, num_files: usize) -> bool {
        self.disk_capacity.exceeds_capacity(num_bytes, num_files)
    }

    /// Persist the state of the entire cache.
    async fn save_state<'a>(
        &self,
        wrapped_cache: &MutexGuard<'_, WrappedCache>,
    ) -> StorageResult<()> {
        let mut cache_items = vec![];
        for (path, size) in wrapped_cache.lru.iter() {
            cache_items.push((path.clone(), *size));
        }

        let cache_state = CacheState {
            local_storage_uri: self.local_storage.uri(),
            disk_capacity: self.disk_capacity,
            items: cache_items,
        };

        let file_path = self.local_storage_root.join(CACHE_STATE_FILE_NAME);
        let content: Vec<u8> = serde_json::to_vec(&cache_state)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        atomic_write(&file_path, &content)?;
        Ok(())
    }
}

#[async_trait]
impl Storage for StorageWithUploadCache {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.remote_storage.put(path, payload.clone()).await?;

        let mut locked_disk_cache = self.disk_cache.lock().await;
        let payload_length = payload.len().await? as usize;
        if self.exceeds_capacity(payload_length, locked_disk_cache.num_files + 1) {
            return Ok(());
        }

        if let Some(item_num_bytes) = locked_disk_cache.lru.pop(&path.to_path_buf()) {
            if self.local_storage.delete(path).await.is_err() {
                return Ok(());
            }
            locked_disk_cache.num_bytes -= item_num_bytes;
            locked_disk_cache.num_files -= 1;
        }

        while self.exceeds_capacity(
            locked_disk_cache.num_bytes + payload_length,
            locked_disk_cache.num_files + 1,
        ) {
            if let Some((item_path, item_num_bytes)) = locked_disk_cache.lru.pop_lru() {
                if self
                    .local_storage
                    .delete(item_path.as_path())
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                locked_disk_cache.num_bytes -= item_num_bytes;
                locked_disk_cache.num_files -= 1;
            }
        }

        self.local_storage.put(path, payload).await?;
        locked_disk_cache.num_bytes += payload_length;
        locked_disk_cache.num_files += 1;
        locked_disk_cache
            .lru
            .put(path.to_path_buf(), payload_length);
        let _ = self.save_state(&locked_disk_cache).await;
        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let mut locked_disk_cache = self.disk_cache.lock().await;
        if locked_disk_cache.lru.get(&path.to_path_buf()).is_none() {
            return self.remote_storage.copy_to_file(path, output_path).await;
        }

        let _ = self.save_state(&locked_disk_cache).await;
        self.local_storage.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        let mut locked_disk_cache = self.disk_cache.lock().await;
        if locked_disk_cache.lru.get(&path.to_path_buf()).is_none() {
            return self.remote_storage.get_slice(path, range).await;
        }

        let _ = self.save_state(&locked_disk_cache).await;
        self.local_storage.get_slice(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        let mut locked_disk_cache = self.disk_cache.lock().await;
        if locked_disk_cache.lru.get(&path.to_path_buf()).is_none() {
            return self.remote_storage.get_all(path).await;
        }

        let _ = self.save_state(&locked_disk_cache).await;
        self.local_storage.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.remote_storage.delete(path).await?;

        let mut locked_disk_cache = self.disk_cache.lock().await;
        if let Some(item_num_bytes) = locked_disk_cache.lru.pop(&path.to_path_buf()) {
            if self.local_storage.delete(path).await.is_err() {
                return Ok(());
            }
            locked_disk_cache.num_bytes -= item_num_bytes;
            locked_disk_cache.num_files -= 1;
            let _ = self.save_state(&locked_disk_cache).await;
        }
        Ok(())
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.remote_storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.remote_storage.uri()
    }
}

/// Writes a file in an atomic manner.
// This code was copied from tantivy.
fn atomic_write(path: &Path, content: &[u8]) -> io::Result<()> {
    // We create the temporary file in the same directory as the target file.
    // Indeed the canonical temp directory and the target file might sit in different
    // filesystem, in which case the atomic write may actually not work.
    let parent_path = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path {:?} does not have parent directory.",
        )
    })?;
    let mut tempfile = Builder::new().tempfile_in(&parent_path)?;
    tempfile.write_all(content)?;
    tempfile.flush()?;
    tempfile.into_temp_path().persist(path)?;
    Ok(())
}

/// A struct embedding the cache parameters.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// The maximum number of files for the local file cache
    pub max_num_files: usize,
    /// The maximum size in bytes allowed for the local file cache.
    pub max_num_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_num_files: 1000,
            max_num_bytes: 10_000_000_000, // 10GB
        }
    }
}

/// Creates an instance of [`StorageWithUploadCache`].
///
/// It tries to construct an instance from a previously saved state if it exists,
/// otherwise it will construct a new instance from the given parameters.
pub fn create_storage_with_upload_cache(
    remote_storage: Arc<dyn Storage>,
    storage_uri_resolver: &StorageUriResolver,
    local_storage_root_path: &Path,
    config: CacheConfig,
) -> crate::StorageResult<Arc<dyn Storage>> {
    let cache_state_file_path = local_storage_root_path.join(CACHE_STATE_FILE_NAME);
    let storage = if cache_state_file_path.exists() {
        StorageWithUploadCache::from_path(
            remote_storage,
            storage_uri_resolver,
            local_storage_root_path,
        )?
    } else {
        let local_storage_uri = format!("file://{}", local_storage_root_path.to_string_lossy());
        let local_storage = storage_uri_resolver
            .resolve(&local_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        StorageWithUploadCache::create(
            remote_storage,
            local_storage,
            config.max_num_files,
            config.max_num_bytes,
        )?
    };

    Ok(Arc::new(storage))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use anyhow::Context;
    use bytes::Bytes;
    use tempfile::{tempdir, TempDir};

    use super::{CacheState, DiskCapacity, StorageWithUploadCache, CACHE_STATE_FILE_NAME};
    use crate::tests::storage_test_suite;
    use crate::{
        quickwit_storage_uri_resolver, LocalFileStorage, MockStorage, PutPayload, RamStorage,
        Storage,
    };

    struct TestEnv {
        #[allow(dead_code)]
        local_dir: TempDir,
        local_storage_uri: String,
        local_storage_root: PathBuf,
        cache: StorageWithUploadCache,
    }

    fn create_mock_storages() -> anyhow::Result<(TempDir, MockStorage, MockStorage)> {
        let local_dir = tempdir()?;
        let remote_storage = MockStorage::default();
        let local_storage = MockStorage::default();
        Ok((local_dir, remote_storage, local_storage))
    }

    async fn create_test_env(max_num_file: usize, max_num_bytes: usize) -> anyhow::Result<TestEnv> {
        let storage_resolver = quickwit_storage_uri_resolver().clone();

        let local_dir = tempdir()?;
        let local_storage_root = local_dir.path().to_path_buf();
        let local_storage_uri = format!("file://{}", local_storage_root.to_string_lossy());
        let local_storage = storage_resolver.resolve(&local_storage_uri)?;

        let remote_storage = Arc::new(RamStorage::default());
        remote_storage
            .put(
                Path::new("abc"),
                PutPayload::InMemory(Bytes::from(b"abc".to_vec())),
            )
            .await?;
        remote_storage
            .put(
                Path::new("def"),
                PutPayload::InMemory(Bytes::from(b"def".to_vec())),
            )
            .await?;

        let cache = StorageWithUploadCache::create(
            remote_storage,
            local_storage,
            max_num_file,
            max_num_bytes,
        )?;

        Ok(TestEnv {
            local_dir,
            local_storage_uri,
            local_storage_root,
            cache,
        })
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

        let mut storage_with_cache =
            StorageWithUploadCache::create(remote_storage, local_storage, 500, 3_000_000)?;
        storage_test_suite(&mut storage_with_cache).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_put_and_get_all_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_mock_storages()?;
        let local_storage_uri = format!("file://{}", local_dir.path().to_string_lossy());
        let moved_local_storage_uri = local_storage_uri.clone();

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
            .times(3)
            .returning(move || moved_local_storage_uri.clone());
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Box::pin(async { Ok(()) })
            });
        local_storage.expect_get_all().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(Bytes::from(b"data".to_vec())) })
        });

        let cached_storage = StorageWithUploadCache::create(
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

        // Check cache state is good.
        {
            let state_file_path = local_dir.path().join(CACHE_STATE_FILE_NAME);
            let json_file = std::fs::File::open(state_file_path)?;
            let reader = std::io::BufReader::new(json_file);
            let cache_state: CacheState = serde_json::from_reader(reader)
                .with_context(|| "Could not deserialise state".to_string())?;

            assert_eq!(cache_state.local_storage_uri, local_storage_uri);
            assert_eq!(cache_state.items.len(), 1);
            assert_eq!(
                cache_state
                    .items
                    .iter()
                    .map(|(_, size)| *size)
                    .sum::<usize>(),
                3
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
    async fn test_get_slice_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_mock_storages()?;
        let local_strage_uri = format!("file://{}", local_dir.path().to_string_lossy());

        remote_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Box::pin(async { Ok(()) })
            });
        remote_storage
            .expect_get_slice()
            .times(1)
            .returning(|path, _range| {
                assert_eq!(path, Path::new("bar"));
                Box::pin(async { Ok(Bytes::from(b"data".to_vec())) })
            });

        local_storage
            .expect_uri()
            .times(5)
            .returning(move || local_strage_uri.clone());
        local_storage
            .expect_get_slice()
            .times(3)
            .returning(|path, _range| {
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

        let cached_storage = StorageWithUploadCache::create(
            Arc::new(remote_storage),
            Arc::new(local_storage),
            5,
            5,
        )?;
        cached_storage
            .put(
                Path::new("foo"),
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;

        assert_eq!(
            cached_storage.get_slice(Path::new("foo"), 0..3).await?,
            &b"foo"[..]
        );
        assert_eq!(
            cached_storage.get_slice(Path::new("foo"), 0..3).await?,
            &b"foo"[..]
        );
        assert_eq!(
            cached_storage.get_slice(Path::new("foo"), 0..3).await?,
            &b"foo"[..]
        );
        assert_eq!(
            cached_storage.get_slice(Path::new("bar"), 0..4).await?,
            &b"data"[..]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_only_calls_remote_storage_for_unknow_cache_item() -> anyhow::Result<()> {
        let (local_dir, mut remote_storage, mut local_storage) = create_mock_storages()?;
        let local_strage_uri = format!("file://{}", local_dir.path().to_string_lossy());

        remote_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(()) })
        });

        local_storage
            .expect_uri()
            .times(1)
            .returning(move || local_strage_uri.clone());

        let cached_storage = StorageWithUploadCache::create(
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
        let (local_dir, mut remote_storage, mut local_storage) = create_mock_storages()?;
        let local_strage_uri = format!("file://{}", local_dir.path().to_string_lossy());
        let moved_local_strage_uri = local_strage_uri.clone();

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
            .times(3)
            .returning(move || moved_local_strage_uri.clone());
        local_storage
            .expect_put()
            .times(1)
            .returning(|path, _payload| {
                assert_eq!(path, Path::new("foo"));
                Box::pin(async { Ok(()) })
            });
        local_storage.expect_delete().times(1).returning(|path| {
            assert_eq!(path, Path::new("foo"));
            Box::pin(async { Ok(()) })
        });

        let cached_storage = StorageWithUploadCache::create(
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

        // Verify cache state.
        let state_file_path = local_dir.path().join(CACHE_STATE_FILE_NAME);
        let json_file = std::fs::File::open(state_file_path)?;
        let reader = std::io::BufReader::new(json_file);
        let cache_state: CacheState = serde_json::from_reader(reader)
            .with_context(|| "Could not deserialise state".to_string())?;
        assert_eq!(cache_state.local_storage_uri, local_strage_uri);
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
    async fn test_get_should_hit_remote_storage() -> anyhow::Result<()> {
        let test_env = create_test_env(5, 10).await?;

        assert_eq!(test_env.cache.get_all(Path::new("abc")).await?, &b"abc"[..]);
        assert_eq!(test_env.cache.get_all(Path::new("def")).await?, &b"def"[..]);
        assert!(test_env.cache.get_all(Path::new("ghi")).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_item_larger_than_cache_should_be_stored() -> anyhow::Result<()> {
        let test_env = create_test_env(5, 10).await?;
        let payload = PutPayload::InMemory(Bytes::from(b"abcdefghijklmnop".to_vec()));
        test_env.cache.put(Path::new("large"), payload).await?;
        test_env
            .cache
            .put(
                Path::new("four"),
                PutPayload::InMemory(Bytes::from(b"four".to_vec())),
            )
            .await?;

        let state = CacheState::from_path(test_env.local_storage_root.as_path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.local_storage_uri, test_env.local_storage_uri);
        assert_eq!(
            state.disk_capacity,
            DiskCapacity {
                max_num_bytes: 10,
                max_num_files: 5
            }
        );

        assert_eq!(
            test_env.cache.get_all(Path::new("large")).await?,
            &b"abcdefghijklmnop"[..]
        );
        assert_eq!(
            test_env.cache.get_all(Path::new("four")).await?,
            &b"four"[..]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_edge_condition() -> anyhow::Result<()> {
        let test_env = create_test_env(5, 5).await?;
        {
            let payload = PutPayload::InMemory(Bytes::from(b"abc".to_vec()));
            test_env.cache.put(Path::new("3"), payload).await?;
            assert_eq!(test_env.cache.get_all(Path::new("3")).await?, &b"abc"[..]);
            assert_eq!(
                test_env.cache.get_slice(Path::new("3"), 0..3).await?,
                &b"abc"[..]
            );
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"de".to_vec()));
            test_env.cache.put(Path::new("2"), payload).await?;
            // our first entry should still be in cache.
            let state = CacheState::from_path(test_env.local_storage_root.as_path()).unwrap();
            assert_eq!(state.items.len(), 2);
            assert!(state.items.contains(&(PathBuf::from("3"), 3)));
            assert!(state.items.contains(&(PathBuf::from("2"), 2)));
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"fghij".to_vec()));
            test_env.cache.put(Path::new("5"), payload).await?;
            assert_eq!(test_env.cache.get_all(Path::new("5")).await?, &b"fghij"[..]);
            assert_eq!(
                test_env.cache.get_slice(Path::new("5"), 1..4).await?,
                &b"ghi"[..]
            );
            // our first two entries should have been removed from the cache
            let state = CacheState::from_path(test_env.local_storage_root.as_path()).unwrap();
            assert_eq!(state.items.len(), 1);
            assert_eq!(state.items, vec![(PathBuf::from("5"), 5)]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_item_from_cache() -> anyhow::Result<()> {
        let test_env = create_test_env(5, 5).await?;
        test_env
            .cache
            .put(
                Path::new("1"),
                PutPayload::InMemory(Bytes::from(b"a".to_vec())),
            )
            .await?;
        test_env
            .cache
            .put(
                Path::new("2"),
                PutPayload::InMemory(Bytes::from(b"bc".to_vec())),
            )
            .await?;
        test_env
            .cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"def".to_vec())),
            )
            .await?;

        test_env.cache.delete(Path::new("2")).await?;
        test_env.cache.delete(Path::new("3")).await?;
        let state = CacheState::from_path(test_env.local_storage_root.as_path()).unwrap();
        assert_eq!(state.items.len(), 0);

        // first evicted item should still be in the storage
        assert!(test_env.cache.get_all(Path::new("1")).await.is_ok());

        // deleted item should not be found
        assert!(test_env.cache.get_all(Path::new("3")).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_item_from_cache() -> anyhow::Result<()> {
        let test_env = create_test_env(5, 5).await?;
        test_env
            .cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"abc".to_vec())),
            )
            .await?;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("3");

        assert!(test_env
            .cache
            .copy_to_file(Path::new("not_found"), output_path.as_path())
            .await
            .is_err());

        assert!(test_env
            .cache
            .copy_to_file(Path::new("3"), output_path.as_path())
            .await
            .is_ok());

        let metadata = tokio::fs::metadata(output_path.as_path()).await?;
        assert_eq!(metadata.len(), 3);
        Ok(())
    }
}
