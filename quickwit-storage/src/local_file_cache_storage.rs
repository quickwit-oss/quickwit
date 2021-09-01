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
    io::{self, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    LocalFileStorage, PutPayload, Storage, StorageErrorKind, StorageResult, StorageUriResolver,
};
use anyhow::anyhow;

const FULL_SLICE: Range<usize> = 0..usize::MAX;
const CACHE_STATE_FILE_NAME: &str = "cache-sate.json";

/// Capacity encapsulates the maximum number of items a cache can hold.
/// We need to account for number of items as well as the size of each item.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Capacity {
    /// Maximum number of bytes.
    max_num_bytes: usize,
    /// Maximum of number of items.
    max_item_count: usize,
}

/// CacheState is P.O.D.O for serializing/deserializing the cache state.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct CacheState {
    remote_storage_uri: String,
    local_storage_uri: String,
    capacity: Capacity,
    items: Vec<(PathBuf, usize)>,
    num_bytes: usize,
    item_count: usize,
}

/// A struct used as the caching layer for any [`Storage`].
///
/// It has an instance of [`LocalFileStorage`] as local storage and
/// an [`LruCache`] for managing cache eviction.
struct LocalFileCache {
    capacity: Capacity,
    local_storage: LocalFileStorage,
    lru_cache: LruCache<PathBuf, usize>,
    num_bytes: usize,
    item_count: usize,
}

impl LocalFileCache {
    /// Return the cached view of the requested range if available.
    pub async fn get(
        &mut self,
        path: &Path,
        bytes_range: Range<usize>,
    ) -> StorageResult<Option<Bytes>> {
        if self.lru_cache.get(&path.to_path_buf()).is_none() {
            return Ok(None);
        }

        self.local_storage
            .get_slice(path, bytes_range)
            .await
            .map(Some)
    }

    /// Attempt to put the given payload in the cache.
    pub async fn put(&mut self, path: PathBuf, payload: PutPayload) -> StorageResult<()> {
        let payload_length = payload.len().await? as usize;
        if self.exceeds_capacity(payload_length, 0) {
            return Err(StorageErrorKind::InternalError
                .with_error(anyhow!("The payload cannot fit in the cache.")));
        }
        if let Some(item_num_bytes) = self.lru_cache.pop(&path.to_path_buf()) {
            self.num_bytes -= item_num_bytes;
            self.item_count -= 1;
        }
        while self.exceeds_capacity(self.num_bytes + payload_length, self.item_count + 1) {
            if let Some((_, item_num_bytes)) = self.lru_cache.pop_lru() {
                self.num_bytes -= item_num_bytes;
                self.item_count -= 1;
            }
        }

        self.local_storage.put(path.as_path(), payload).await?;

        self.num_bytes += payload_length;
        self.item_count += 1;
        self.lru_cache.put(path, payload_length);
        Ok(())
    }

    /// Attempt to copy the entry from cache if available.
    pub async fn copy_to_file(&mut self, path: &Path, output_path: &Path) -> StorageResult<bool> {
        if self.lru_cache.get(&path.to_path_buf()).is_none() {
            return Ok(false);
        }
        self.local_storage.copy_to_file(path, output_path).await?;
        Ok(true)
    }

    /// Attempt to delete the entry from cache if available.
    pub async fn delete(&mut self, path: &Path) -> StorageResult<()> {
        if self.lru_cache.pop(&path.to_path_buf()).is_some() {
            return self.local_storage.delete(path).await;
        }
        Ok(())
    }

    /// Tell if an item will exceed the capacity once inserted.
    fn exceeds_capacity(&self, num_bytes: usize, item_count: usize) -> bool {
        self.capacity.max_num_bytes <= num_bytes || self.capacity.max_item_count <= item_count
    }

    /// Return a copy of the items in the cache.
    fn get_items(&self) -> Vec<(PathBuf, usize)> {
        let mut cache_items = vec![];
        for (path, size) in self.lru_cache.iter() {
            cache_items.push((path.clone(), *size));
        }
        cache_items
    }
}

/// A storage with a backing [`LocalFileCache`].
pub struct StorageWithLocalFileCache {
    remote_storage: Arc<dyn Storage>,
    cache: Mutex<LocalFileCache>,
    //TODO: solve in-fligh requests will be solved with waiting on channel
    //subscirber
}

impl StorageWithLocalFileCache {
    /// Create an instance of [`StorageWithLocalFileCache`]
    ///
    /// It needs to create both the remote and local Storage to work with.
    /// max_item_count and max_num_bytes are used for the cache [`Capacity`].
    pub fn create(
        remote_uri: &str,
        local_uri: &str,
        max_item_count: usize,
        max_num_bytes: usize,
    ) -> StorageResult<StorageWithLocalFileCache> {
        let remote_storage = StorageUriResolver::default()
            .resolve(remote_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        let local_storage = LocalFileStorage::from_uri(local_uri)?;
        Ok(Self {
            remote_storage,
            cache: Mutex::new(LocalFileCache {
                capacity: Capacity {
                    max_num_bytes,
                    max_item_count,
                },
                local_storage,
                lru_cache: LruCache::unbounded(),
                num_bytes: 0,
                item_count: 0,
            }),
        })
    }

    /// Create an instance of [`StorageWithLocalFileCache`] from a previously persisted state.
    ///
    /// It needs a folder `path` previously used by an instance of [`StorageWithLocalFileCache`].
    /// It assumes `path` is the root where the backing [`LocalFileCache`] will be operating.
    /// Lastly, the file at `{path}/[`CACHE_STATE_FILE_NAME`]` should be present and valid.
    pub fn from_path(path: &Path) -> StorageResult<StorageWithLocalFileCache> {
        let file_path = path.to_path_buf().join(CACHE_STATE_FILE_NAME);
        let json_file = std::fs::File::open(file_path)?;
        let reader = std::io::BufReader::new(json_file);
        let cache_state: CacheState = serde_json::from_reader(reader)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        let remote_storage = StorageUriResolver::default()
            .resolve(&cache_state.remote_storage_uri)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;

        let local_storage = LocalFileStorage::from_uri(&cache_state.local_storage_uri)?;
        let mut lru_cache = LruCache::unbounded();
        for (path, size) in cache_state.items {
            lru_cache.put(path, size);
        }

        Ok(Self {
            remote_storage,
            cache: Mutex::new(LocalFileCache {
                capacity: cache_state.capacity,
                local_storage,
                lru_cache,
                num_bytes: cache_state.num_bytes,
                item_count: cache_state.item_count,
            }),
        })
    }

    /// Extract the cache stage and persiste it on disk.
    async fn save_state(&self) -> StorageResult<()> {
        let cache = self.cache.lock().await;
        let cache_state = CacheState {
            remote_storage_uri: self.remote_storage.uri(),
            local_storage_uri: cache.local_storage.uri(),
            capacity: cache.capacity,
            items: cache.get_items(),
            num_bytes: cache.num_bytes,
            item_count: cache.item_count,
        };

        let file_path = cache.local_storage.get_root().join(CACHE_STATE_FILE_NAME);
        let content: Vec<u8> = serde_json::to_vec(&cache_state)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        atomic_write(&file_path, &content)?;
        Ok(())
    }
}

#[async_trait]
impl Storage for StorageWithLocalFileCache {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.remote_storage.put(path, payload.clone()).await?;
        self.cache
            .lock()
            .await
            .put(path.to_path_buf(), payload)
            .await
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
        self.cache
            .lock()
            .await
            .put(
                path.to_path_buf(),
                PutPayload::LocalFile(output_path.to_path_buf()),
            )
            .await?;
        self.save_state().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        if let Some(bytes) = self.cache.lock().await.get(path, range.clone()).await? {
            return Ok(bytes);
        }

        //download all & cache
        //TODO find ways around copying all bytes in RAM
        let all_bytes = self.remote_storage.get_slice(path, FULL_SLICE).await?;
        let data = Bytes::copy_from_slice(&all_bytes[range.start..range.end]);
        self.cache
            .lock()
            .await
            .put(path.to_path_buf(), PutPayload::InMemory(all_bytes))
            .await?;
        self.save_state().await?;
        Ok(data)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        self.get_slice(path, FULL_SLICE).await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.remote_storage.delete(path).await?;
        self.cache.lock().await.delete(path).await?;
        self.save_state().await
    }

    async fn file_num_bytes(&self, path: &Path) -> crate::StorageResult<u64> {
        self.remote_storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.remote_storage.uri()
    }
}

/// Writes a file in an atomic manner.
//  Copied from tantivy
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
    let mut tempfile = tempfile::Builder::new().tempfile_in(&parent_path)?;
    tempfile.write_all(content)?;
    tempfile.flush()?;
    tempfile.into_temp_path().persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_cannot_put_more_than_capacity() {}

    #[test]
    fn test_oldest_cache_item_is_evicted() {}
}
