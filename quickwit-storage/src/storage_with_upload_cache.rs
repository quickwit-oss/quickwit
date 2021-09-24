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

use std::collections::HashMap;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::{LocalFileStorage, PutPayload, Storage, StorageErrorKind, StorageResult};

/// An intermediate folder created at `cache_dir/INTERNAL_CACHE_DIR_NAME`
/// to hold the local files.
const INTERNAL_CACHE_DIR_NAME: &str = "quickwit-cache";

/// An extension of temporary file names.
/// It's used to denote that a file is being worked on.
const CACHE_TEMP_FILE_EXTENSION: &str = "_quickwit_cache_temp_file";

/// CacheParams encapsulates the various contraints of the cache.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CacheParams {
    /// Maximum number of files allowed in the cache.
    max_num_files: usize,
    /// Maximum size in bytes allowed in the cache.
    max_num_bytes: usize,
    /// Maximum size allowed per file.
    max_file_size: usize,
}

impl Default for CacheParams {
    fn default() -> Self {
        Self {
            max_num_files: 1000,
            max_num_bytes: 10_000_000_000, // 10GB
            max_file_size: 2_500_000_000,  // 2.5GB
        }
    }
}

/// A storage that keeps files around on the local disk.
///
/// This storage is meant to be used during indexing and merging where the `put` and `copy_to_file`
/// methods are the most utilized.
/// As a result, only the methods `put`, `copy_to_file`, and `delete` interact with the local file
/// system.
/// Other operations, such as `get_all` or `get_slice,` directly interact with the underlying remote
/// storage.
pub struct StorageWithUploadCache {
    /// The remote storage.
    remote_storage: Arc<dyn Storage>,
    /// The backing local storage.
    local_storage: LocalFileStorage,
    // The underlying cache.
    cache_items: Mutex<HashMap<PathBuf, usize>>,
    /// The parameters of the cache.
    cache_params: CacheParams,
}

impl StorageWithUploadCache {
    /// Create an instance of [`StorageWithUploadCache`]
    ///
    /// It needs the remote storage to work with.
    /// Tries to create a folder `cache_dir/INTERNAL_CACHE_DIR_NAME` if it does not exist.
    /// List the content of `cache_dir/INTERNAL_CACHE_DIR_NAME` as initial cache content.
    /// Removes and ignores all files ending with `CACHE_TEMP_FILE_EXTENSION`
    pub fn create(
        remote_storage: Arc<dyn Storage>,
        cache_dir: &Path,
        cache_params: CacheParams,
    ) -> StorageResult<Self> {
        let local_storage_root = cache_dir.join(INTERNAL_CACHE_DIR_NAME);
        fs::create_dir_all(local_storage_root.as_path())?;
        let mut file_entries = HashMap::new();
        let mut total_size_in_bytes: usize = 0;
        for dir_entry_result in fs::read_dir(local_storage_root.as_path())? {
            let dir_entry = dir_entry_result?;
            let path = dir_entry.path();
            if path.is_file() {
                // Remove and ignore temp files.
                if path.to_string_lossy().ends_with(CACHE_TEMP_FILE_EXTENSION) {
                    fs::remove_file(path)?;
                    continue;
                }

                let file_size = dir_entry.metadata()?.len() as usize;
                if file_size > cache_params.max_file_size {
                    warn!("An existing file size exceeds the maximum size per file allowed.");
                }

                let relative_file_path = path
                    .strip_prefix(local_storage_root.as_path())
                    .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
                total_size_in_bytes += file_size;
                file_entries.insert(relative_file_path.to_path_buf(), file_size);
            }
        }

        let local_storage_uri = format!("file://{}", local_storage_root.to_string_lossy());
        let local_storage = LocalFileStorage::from_uri(&local_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        if file_entries.len() > cache_params.max_num_files {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial number of files exceeds the maximum number of files allowed.",
            )));
        }

        if total_size_in_bytes > cache_params.max_num_bytes {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial cache size exceeds the maximum size in bytes allowed.",
            )));
        }

        Ok(Self {
            remote_storage,
            local_storage,
            cache_items: Mutex::new(file_entries),
            cache_params,
        })
    }

    /// Takes a snapshot of the cache view (only used for testing).
    #[cfg(test)]
    async fn inspect(&self) -> HashMap<PathBuf, usize> {
        self.cache_items.lock().await.clone()
    }
}

#[async_trait]
impl Storage for StorageWithUploadCache {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.remote_storage.put(path, payload.clone()).await?;

        // Ignore if path ends with `CACHE_TEMP_FILE_EXTENSION`.
        if path.to_string_lossy().ends_with(CACHE_TEMP_FILE_EXTENSION) {
            return Ok(());
        }

        let (num_entries, size_in_bytes_entries) = {
            let locked_cache_item = self.cache_items.lock().await;
            (
                locked_cache_item.len(),
                locked_cache_item
                    .iter()
                    .map(|(_, size)| *size)
                    .sum::<usize>(),
            )
        };

        // Avoid storing in the cache when the maximum number of cached files is reached.
        if num_entries + 1 > self.cache_params.max_num_files {
            warn!("Failed to cache file: maximum number of files exceeded.");
            return Ok(());
        }

        let payload_length = payload.len().await? as usize;
        // Ignore storing a file whose size exceeds the max file size.
        if payload_length > self.cache_params.max_file_size {
            warn!("Failed to cache file: maximum size in bytes per file exceeded.");
            return Ok(());
        }

        // Ignore storing a file that cannot fit in the cache.
        if payload_length + size_in_bytes_entries > self.cache_params.max_num_bytes {
            warn!("Failed to cache file: maximum size in bytes of cache exceeded.");
            return Ok(());
        }

        // Safely copy using intermediate temp file.
        let temp_file_path = format!("{}{}", path.to_string_lossy(), CACHE_TEMP_FILE_EXTENSION);
        self.local_storage
            .put(Path::new(&temp_file_path), payload)
            .await?;
        self.local_storage
            .move_to(Path::new(&temp_file_path), path)
            .await?;

        let mut locked_cache_items = self.cache_items.lock().await;
        locked_cache_items.insert(path.to_path_buf(), payload_length);
        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        // Ignore cache if path ends with `CACHE_TEMP_FILE_EXTENSION`.
        if path.to_string_lossy().ends_with(CACHE_TEMP_FILE_EXTENSION) {
            return self.remote_storage.copy_to_file(path, output_path).await;
        }

        let is_file_exist_in_cache = self
            .cache_items
            .lock()
            .await
            .contains_key(&path.to_path_buf());
        if is_file_exist_in_cache {
            let copy_to_result = self.local_storage.copy_to_file(path, output_path).await;
            match copy_to_result {
                Ok(_) => return Ok(()),
                Err(error) => {
                    error!(file_path = %path.to_string_lossy(), error = %error, "Could not copy file from local storage.");
                }
            }
        }
        self.remote_storage.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        self.remote_storage.get_slice(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        self.remote_storage.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.remote_storage.delete(path).await?;

        let mut locked_cache_item = self.cache_items.lock().await;
        if locked_cache_item.contains_key(&path.to_path_buf()) {
            if let Err(error) = self.local_storage.delete(path).await {
                warn!(file_path = %path.to_string_lossy(), error = %error, "Could not remove file from local storage.");
            }
            locked_cache_item.remove(&path.to_path_buf());
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

/// Creates an instance of [`StorageWithUploadCache`].
pub fn create_storage_with_upload_cache(
    remote_storage: Arc<dyn Storage>,
    cache_dir: &Path,
    cache_params: CacheParams,
) -> crate::StorageResult<Arc<dyn Storage>> {
    let storage = StorageWithUploadCache::create(remote_storage, cache_dir, cache_params)?;
    Ok(Arc::new(storage))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;
    use tokio::fs;

    use super::{
        CacheParams, StorageWithUploadCache, CACHE_TEMP_FILE_EXTENSION, INTERNAL_CACHE_DIR_NAME,
    };
    use crate::tests::storage_test_suite;
    use crate::{
        create_storage_with_upload_cache, PutPayload, RamStorage, Storage, StorageError,
        StorageErrorKind,
    };

    fn list_files(path: PathBuf) -> Vec<PathBuf> {
        std::fs::read_dir(path)
            .unwrap()
            .filter_map(|result| match result {
                Ok(entry) if entry.path().is_file() => Some(entry.path()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());
        let mut storage_with_cache = StorageWithUploadCache::create(
            remote_storage,
            local_dir.path(),
            CacheParams::default(),
        )?;

        storage_test_suite(&mut storage_with_cache).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_should_error_with_wrong_num_files() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let root_path = local_dir.path().join(INTERNAL_CACHE_DIR_NAME);
        fs::create_dir_all(root_path.to_path_buf()).await?;
        fs::write(root_path.join("a.split"), b"a").await?;
        fs::write(root_path.join("b.split"), b"b").await?;
        fs::write(root_path.join("c.split"), b"c").await?;

        let cache_params = CacheParams {
            max_num_files: 2,
            max_num_bytes: 10,
            max_file_size: 10,
        };
        let remote_storage = Arc::new(RamStorage::default());
        let result =
            create_storage_with_upload_cache(remote_storage, local_dir.path(), cache_params);
        assert!(matches!(
            result,
            Err(StorageError {
                kind: StorageErrorKind::InternalError,
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_should_error_with_wrong_num_bytes() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let root_path = local_dir.path().join(INTERNAL_CACHE_DIR_NAME);
        fs::create_dir_all(root_path.to_path_buf()).await?;
        fs::write(root_path.join("a.split"), b"abcdefgh").await?;
        fs::write(root_path.join("b.split"), b"abcdefgh").await?;

        let cache_params = CacheParams {
            max_num_files: 4,
            max_num_bytes: 10,
            max_file_size: 4,
        };
        let remote_storage = Arc::new(RamStorage::default());
        let result =
            create_storage_with_upload_cache(remote_storage, local_dir.path(), cache_params);
        assert!(matches!(
            result,
            Err(StorageError {
                kind: StorageErrorKind::InternalError,
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_should_accept_a_file_size_exeeding_constraint() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let root_path = local_dir.path().join(INTERNAL_CACHE_DIR_NAME);
        fs::create_dir_all(root_path.to_path_buf()).await?;
        fs::write(root_path.join("b.split"), b"abcd").await?;
        fs::write(root_path.join("a.split"), b"abcdefgh").await?;

        let cache_params = CacheParams {
            max_num_files: 100,
            max_num_bytes: 100,
            max_file_size: 4,
        };
        let remote_storage = Arc::new(RamStorage::default());
        let result =
            create_storage_with_upload_cache(remote_storage, local_dir.path(), cache_params);
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_should_fill_cache_with_dir_entries() -> anyhow::Result<()> {
        // setup dir
        let local_dir = tempdir()?;
        let root_path = local_dir.path().join(INTERNAL_CACHE_DIR_NAME);
        let foo_temp_file = root_path.join(format!("foo.split{}", CACHE_TEMP_FILE_EXTENSION));
        let bar_temp_file = root_path.join(format!("bar.split{}", CACHE_TEMP_FILE_EXTENSION));

        {
            let remote_storage = Arc::new(RamStorage::default());
            let cache = StorageWithUploadCache::create(
                remote_storage.clone(),
                local_dir.path(),
                CacheParams::default(),
            )?;
            assert_eq!(cache.uri(), remote_storage.uri());

            cache
                .put(
                    root_path.join("a5.split").as_path(),
                    PutPayload::InMemory(Bytes::from(b"aaaaa".to_vec())),
                )
                .await?;
            cache
                .put(
                    root_path.join("b3.split").as_path(),
                    PutPayload::InMemory(Bytes::from(b"bbb".to_vec())),
                )
                .await?;
            cache
                .put(
                    root_path.join("c6.split").as_path(),
                    PutPayload::InMemory(Bytes::from(b"cccccc".to_vec())),
                )
                .await?;

            fs::write(foo_temp_file.as_path(), b"corrupted").await?;
            fs::write(bar_temp_file.as_path(), b"corrupted").await?;
        }

        {
            // directory should contain temp file
            let files = list_files(root_path.clone());
            assert_eq!(files.len(), 5);
            assert!(files.contains(&foo_temp_file));
            assert!(files.contains(&bar_temp_file));

            let remote_storage = Arc::new(RamStorage::default());
            let cache = StorageWithUploadCache::create(
                remote_storage,
                local_dir.path(),
                CacheParams::default(),
            )?;

            // directory should not contain temp files
            let files = list_files(root_path);
            assert_eq!(files.len(), 3);
            assert!(!files.contains(&foo_temp_file));
            assert!(!files.contains(&bar_temp_file));

            let output_dir = tempdir()?;
            cache
                .copy_to_file(
                    Path::new("b3.split"),
                    output_dir.path().join("b3_copy.split").as_path(),
                )
                .await?;

            let cache_items_snapshot = cache.inspect().await;
            assert_eq!(cache_items_snapshot.len(), 3);
            assert_eq!(
                cache_items_snapshot.get(Path::new("a5.split")),
                Some(&5usize)
            );
            assert_eq!(
                cache_items_snapshot.get(Path::new("b3.split")),
                Some(&3usize)
            );
            assert_eq!(
                cache_items_snapshot.get(Path::new("c6.split")),
                Some(&6usize)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_put_should_not_hit_cache_when_max_num_files_reached() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());

        let cache_params = CacheParams {
            max_num_files: 2,
            max_num_bytes: 10,
            max_file_size: 10,
        };
        let cache = StorageWithUploadCache::create(remote_storage, local_dir.path(), cache_params)?;
        cache
            .put(
                Path::new("a.split"),
                PutPayload::InMemory(Bytes::from(b"a".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("b.split"),
                PutPayload::InMemory(Bytes::from(b"b".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("c.split"),
                PutPayload::InMemory(Bytes::from(b"c".to_vec())),
            )
            .await?;

        let cache_items_snapshot = cache.inspect().await;
        assert_eq!(
            cache_items_snapshot.get(Path::new("a.split")),
            Some(&1usize)
        );
        assert_eq!(
            cache_items_snapshot.get(Path::new("b.split")),
            Some(&1usize)
        );
        assert_eq!(cache_items_snapshot.get(Path::new("c.split")), None);
        Ok(())
    }

    #[tokio::test]
    async fn test_put_should_not_hit_cache_when_max_file_size_reached() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());

        let cache_params = CacheParams {
            max_num_files: 10,
            max_num_bytes: 10,
            max_file_size: 10,
        };
        let cache = StorageWithUploadCache::create(remote_storage, local_dir.path(), cache_params)?;
        cache
            .put(
                Path::new("a.split"),
                PutPayload::InMemory(Bytes::from(b"abcdefghijkl".to_vec())),
            )
            .await?;

        let cache_items_snapshot = cache.inspect().await;
        assert_eq!(cache_items_snapshot.get(Path::new("a.split")), None);
        Ok(())
    }

    #[tokio::test]
    async fn test_put_should_not_hit_cache_when_max_num_bytes_reached() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());

        let cache_params = CacheParams {
            max_num_files: 10,
            max_num_bytes: 10,
            max_file_size: 10,
        };
        let cache = StorageWithUploadCache::create(remote_storage, local_dir.path(), cache_params)?;
        cache
            .put(
                Path::new("a.split"),
                PutPayload::InMemory(Bytes::from(b"abcd".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("b.split"),
                PutPayload::InMemory(Bytes::from(b"efgh".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("c.split"),
                PutPayload::InMemory(Bytes::from(b"ijkl".to_vec())),
            )
            .await?;

        let cache_items_snapshot = cache.inspect().await;
        assert_eq!(
            cache_items_snapshot.get(Path::new("a.split")),
            Some(&4usize)
        );
        assert_eq!(
            cache_items_snapshot.get(Path::new("b.split")),
            Some(&4usize)
        );
        assert_eq!(cache_items_snapshot.get(Path::new("c.split")), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_put_should_not_hit_cache_when_item_name_ends_with_special_temp_extension(
    ) -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());

        let cache = StorageWithUploadCache::create(
            remote_storage,
            local_dir.path(),
            CacheParams::default(),
        )?;
        let file_name = format!("foo.seg{}", CACHE_TEMP_FILE_EXTENSION);
        cache
            .put(
                Path::new(&file_name),
                PutPayload::InMemory(Bytes::from(b"abcd".to_vec())),
            )
            .await?;

        let cache_items_snapshot = cache.inspect().await;
        assert_eq!(cache_items_snapshot.get(Path::new(&file_name)), None);

        // item should exist in remote storage
        assert!(cache.exists(Path::new(&file_name)).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_to_file_calls_both_storages_appropriately() -> anyhow::Result<()> {
        let foo_split = Path::new("foo.split");
        let bar_split = Path::new("bar.split");
        let special_split_file_name = format!("foo.split{}", CACHE_TEMP_FILE_EXTENSION);
        let special_split = Path::new(&special_split_file_name);

        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());
        remote_storage
            .put(
                foo_split,
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;

        let cache = create_storage_with_upload_cache(
            remote_storage,
            local_dir.path(),
            CacheParams::default(),
        )?;
        cache
            .put(
                bar_split,
                PutPayload::InMemory(Bytes::from(b"bar".to_vec())),
            )
            .await?;
        cache
            .put(
                special_split,
                PutPayload::InMemory(Bytes::from(b"special".to_vec())),
            )
            .await?;

        let copy_foo_file = local_dir.path().join("copy_foo");
        cache.copy_to_file(foo_split, &copy_foo_file).await?;
        assert_eq!(fs::read(copy_foo_file).await?, b"foo".to_vec());

        let copy_bar_file = local_dir.path().join("copy_bar");
        cache.copy_to_file(foo_split, &copy_bar_file).await?;
        assert_eq!(fs::read(copy_bar_file).await?, b"foo".to_vec());

        let copy_special_file = local_dir.path().join("copy_special");
        cache
            .copy_to_file(special_split, &copy_special_file)
            .await?;
        assert_eq!(fs::read(copy_special_file).await?, b"special".to_vec());

        let copy_result = cache
            .copy_to_file(
                Path::new("unknown.split"),
                &local_dir.path().join("copy_unknown"),
            )
            .await;
        matches!(
            copy_result,
            Err(StorageError {
                kind: StorageErrorKind::DoesNotExist,
                ..
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_should_remove_from_both_storage() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());

        let split_file = Path::new("a.split");
        let local_store_file = local_dir
            .path()
            .join(INTERNAL_CACHE_DIR_NAME)
            .join("a.split");

        let cache = StorageWithUploadCache::create(
            remote_storage.clone(),
            local_dir.path(),
            CacheParams::default(),
        )?;
        cache
            .put(
                split_file,
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;

        assert!(local_store_file.exists());
        assert_eq!(cache.inspect().await.get(split_file), Some(&3usize));

        assert!(cache.exists(split_file).await?);
        cache.delete(split_file).await?;

        // file should be removed from remote storage
        assert!(!remote_storage.exists(split_file).await?);

        // file should be removed from local storage
        assert!(!local_store_file.exists());

        // file should be removed from cache state
        assert_eq!(cache.inspect().await.get(split_file), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_should_remove_from_remote_storage() -> anyhow::Result<()> {
        let local_dir = tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());
        let split_file = Path::new("a.split");
        remote_storage
            .put(
                split_file,
                PutPayload::InMemory(Bytes::from(b"foo".to_vec())),
            )
            .await?;

        let cache = StorageWithUploadCache::create(
            remote_storage.clone(),
            local_dir.path(),
            CacheParams::default(),
        )?;
        assert_eq!(cache.inspect().await.get(split_file), None);

        // file should exist in cache since it's in remote storage
        assert!(cache.exists(split_file).await?);
        cache.delete(split_file).await?;

        // file should be removed from remote storage
        assert!(!remote_storage.exists(split_file).await?);
        assert_eq!(cache.inspect().await.get(split_file), None);

        Ok(())
    }
}
