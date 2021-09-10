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

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use tempfile::Builder;

use super::ram_cache::RamCache;
use super::{CacheState, DiskCapacity, StorageCache, CACHE_STATE_FILE_NAME};
use crate::{PutPayload, Storage, StorageErrorKind, StorageResult};

/// A struct providing two caching layers backed by a [`Storage`].
/// - An in memory cache: holding chunks(range) of files.
/// - An on disk cache: holding a set of files.
///
/// The Storage is prefered to be an instance of [`LocalFileStorage`].
/// The cache eviction policy in both layers is L.R.U.
pub struct LocalStorageCache {
    pub local_storage: Arc<dyn Storage>,
    pub local_storage_root: PathBuf,
    pub ram_capacity: usize,
    pub ram_cache: RamCache,
    pub disk_capacity: DiskCapacity,
    pub disk_cache: LruCache<PathBuf, usize>,
    pub num_bytes: usize,
    pub num_files: usize,
}

impl LocalStorageCache {
    /// Create new instance of [`LocalStorageCache`]
    pub fn new(
        local_storage: Arc<dyn Storage>,
        local_storage_root: PathBuf,
        ram_capacity_in_bytes: usize,
        max_num_files: usize,
        max_num_bytes: usize,
    ) -> Self {
        Self {
            local_storage,
            local_storage_root,
            ram_capacity: ram_capacity_in_bytes,
            ram_cache: RamCache::new(ram_capacity_in_bytes),
            disk_capacity: DiskCapacity {
                max_num_files,
                max_num_bytes,
            },
            disk_cache: LruCache::unbounded(),
            num_bytes: 0,
            num_files: 0,
        }
    }

    pub fn from_state(
        local_storage: Arc<dyn Storage>,
        local_storage_root: PathBuf,
        ram_capacity: usize,
        disk_capacity: DiskCapacity,
        items: Vec<(PathBuf, usize)>,
    ) -> StorageResult<Self> {
        let num_files = items.len();
        let num_bytes = items.iter().map(|(_, size)| *size).sum();
        if disk_capacity.exceeds_capacity(num_bytes, num_files) {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial cache items should not exceed cache capacity.",
            )));
        }

        let mut disk_cache = LruCache::unbounded();
        for (path, size) in items {
            disk_cache.put(path, size);
        }

        Ok(Self {
            local_storage,
            local_storage_root,
            ram_capacity,
            ram_cache: RamCache::new(ram_capacity),
            disk_capacity,
            disk_cache,
            num_bytes,
            num_files,
        })
    }

    /// Check if an incomming item will exceed the capacity once inserted.
    fn exceeds_capacity(&self, num_bytes: usize, num_files: usize) -> bool {
        self.disk_capacity.exceeds_capacity(num_bytes, num_files)
    }
}

#[async_trait]
impl StorageCache for LocalStorageCache {
    /// Return the disk cached view of the requested file if available.
    async fn get(&mut self, path: &Path) -> StorageResult<Option<Bytes>> {
        if self.disk_cache.get(&path.to_path_buf()).is_none() {
            return Ok(None);
        }

        self.local_storage.get_all(path).await.map(Some)
    }

    /// Attempt to put the given payload (file or raw content) in the disk cache.
    ///
    /// An error is raised if the item cannot fit in the cache.
    async fn put(&mut self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        let payload_length = payload.len().await? as usize;
        if self.exceeds_capacity(payload_length, 0) {
            return Err(StorageErrorKind::InternalError
                .with_error(anyhow!("The payload cannot fit in the cache.")));
        }
        if let Some(item_num_bytes) = self.disk_cache.pop(&path.to_path_buf()) {
            self.num_bytes -= item_num_bytes;
            self.num_files -= 1;
        }
        while self.exceeds_capacity(self.num_bytes + payload_length, self.num_files + 1) {
            if let Some((_, item_num_bytes)) = self.disk_cache.pop_lru() {
                self.num_bytes -= item_num_bytes;
                self.num_files -= 1;
            }
        }

        self.local_storage.put(path, payload).await?;

        self.num_bytes += payload_length;
        self.num_files += 1;
        self.disk_cache.put(path.to_path_buf(), payload_length);
        Ok(())
    }

    /// Return the in-memroy cached view of the requested file chunck(range) if available.
    async fn get_slice(
        &mut self,
        path: &Path,
        bytes_range: Range<usize>,
    ) -> StorageResult<Option<Bytes>> {
        if let Some(bytes) = self.ram_cache.get(path, bytes_range.clone()) {
            return Ok(Some(bytes));
        }

        if self.disk_cache.get(&path.to_path_buf()).is_none() {
            return Ok(None);
        }

        let bytes = self
            .local_storage
            .get_slice(path, bytes_range.clone())
            .await?;
        self.ram_cache.put(path, bytes_range, bytes.clone());

        Ok(Some(bytes))
    }

    /// Attempt to put the given payload (file chunck) in the in-memrory cache.
    async fn put_slice(
        &mut self,
        path: &Path,
        byte_range: Range<usize>,
        bytes: Bytes,
    ) -> StorageResult<()> {
        self.ram_cache.put(path, byte_range, bytes);
        Ok(())
    }

    /// Attempt to copy the entry from the disk cache if available.
    async fn copy_to_file(&mut self, path: &Path, output_path: &Path) -> StorageResult<bool> {
        if self.disk_cache.get(&path.to_path_buf()).is_none() {
            return Ok(false);
        }
        self.local_storage.copy_to_file(path, output_path).await?;
        Ok(true)
    }

    /// Attempt to delete the entry from all caches and storage.
    async fn delete(&mut self, path: &Path) -> StorageResult<bool> {
        self.ram_cache.delete(path);
        if self.disk_cache.pop(&path.to_path_buf()).is_some() {
            self.local_storage.delete(path).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn file_num_bytes(&mut self, path: &Path) -> crate::StorageResult<Option<usize>> {
        Ok(self.disk_cache.get(&path.to_path_buf()).cloned())
    }

    /// Return a copy of the items in the disk cache.
    fn get_items(&self) -> Vec<(PathBuf, usize)> {
        let mut cache_items = vec![];
        for (path, size) in self.disk_cache.iter() {
            cache_items.push((path.clone(), *size));
        }
        cache_items
    }

    /// Persist the state of the entire cache.
    async fn save_state(&self) -> StorageResult<()> {
        let items = self.get_items();
        let cache_state = CacheState {
            local_storage_uri: self.local_storage.uri(),
            ram_capacity: self.ram_capacity,
            disk_capacity: self.disk_capacity,
            items,
        };

        let file_path = self.local_storage_root.join(CACHE_STATE_FILE_NAME);
        let content: Vec<u8> = serde_json::to_vec(&cache_state)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        atomic_write(&file_path, &content)?;

        Ok(())
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

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use super::*;
    use crate::RamStorage;

    #[tokio::test]
    async fn test_correct_instance_from_state() -> anyhow::Result<()> {
        let local_storage = Arc::new(RamStorage::default());
        let local_storage_root = PathBuf::from("/");

        let items = (vec![("foo.split", 8), ("bar.split", 12), ("baz.split", 5)])
            .into_iter()
            .map(|(path, size)| (PathBuf::from(path), size as usize))
            .collect();
        let disk_capacity = DiskCapacity {
            max_num_bytes: 300,
            max_num_files: 5,
        };

        let cache = LocalStorageCache::from_state(
            local_storage,
            local_storage_root,
            100,
            disk_capacity,
            items,
        )?;

        assert_eq!(cache.exceeds_capacity(300, 1), false);
        assert_eq!(cache.exceeds_capacity(301, 1), true);
        assert_eq!(cache.exceeds_capacity(300, 6), true);
        assert_eq!(cache.num_files, 3);
        assert_eq!(cache.num_bytes, 25);

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_instance_from_state() -> anyhow::Result<()> {
        let local_storage = Arc::new(RamStorage::default());
        let local_storage_root = PathBuf::from("/");

        let items = (vec![("foo.split", 8), ("bar.split", 12)])
            .into_iter()
            .map(|(path, size)| (PathBuf::from(path), size as usize))
            .collect();
        let disk_capacity = DiskCapacity {
            max_num_bytes: 15,
            max_num_files: 5,
        };

        let result = LocalStorageCache::from_state(
            local_storage,
            local_storage_root,
            100,
            disk_capacity,
            items,
        );
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_cannot_fit_item_in_cache() -> anyhow::Result<()> {
        let local_storage_root = tempdir()?;
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache =
            LocalStorageCache::new(ram_storage, local_storage_root.into_path(), 5, 5, 5);

        let payload = PutPayload::InMemory(Bytes::from(b"abc".to_vec()));
        cache.put(Path::new("3"), payload).await?;
        let payload = PutPayload::InMemory(Bytes::from(b"abcdef".to_vec()));
        cache.put(Path::new("6"), payload).await.unwrap_err();

        // first entry should still be around
        assert_eq!(cache.get(Path::new("3")).await?.unwrap(), &b"abc"[..]);
        assert_eq!(
            cache.get_slice(Path::new("3"), 0..3).await?.unwrap(),
            &b"abc"[..]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_edge_condition() -> anyhow::Result<()> {
        let local_storage_root = tempdir()?;
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache =
            LocalStorageCache::new(ram_storage, local_storage_root.into_path(), 5, 5, 5);

        {
            let payload = PutPayload::InMemory(Bytes::from(b"abc".to_vec()));
            cache.put(Path::new("3"), payload).await?;
            assert_eq!(cache.get(Path::new("3")).await?.unwrap(), &b"abc"[..]);
            assert_eq!(
                cache.get_slice(Path::new("3"), 0..3).await?.unwrap(),
                &b"abc"[..]
            );
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"de".to_vec()));
            cache.put(Path::new("2"), payload).await?;
            // our first entry should still be here.
            assert_eq!(cache.get(Path::new("3")).await?.unwrap(), &b"abc"[..]);
            assert_eq!(cache.get(Path::new("2")).await?.unwrap(), &b"de"[..]);
        }
        {
            let payload = PutPayload::InMemory(Bytes::from(b"fghij".to_vec()));
            cache.put(Path::new("5"), payload).await?;
            assert_eq!(cache.get(Path::new("5")).await?.unwrap(), &b"fghij"[..]);
            assert_eq!(
                cache.get_slice(Path::new("5"), 1..4).await?.unwrap(),
                &b"ghi"[..]
            );
            // our first two entries should have been removed from the cache
            assert!(cache.get(Path::new("2")).await?.is_none());
            assert!(cache.get(Path::new("3")).await?.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_item_from_cache() -> anyhow::Result<()> {
        let local_storage_root = tempdir()?;
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache =
            LocalStorageCache::new(ram_storage, local_storage_root.into_path(), 5, 5, 5);
        cache
            .put(
                Path::new("1"),
                PutPayload::InMemory(Bytes::from(b"a".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("2"),
                PutPayload::InMemory(Bytes::from(b"bc".to_vec())),
            )
            .await?;
        cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"def".to_vec())),
            )
            .await?;

        assert!(!cache.delete(Path::new("6")).await?);

        assert!(cache.delete(Path::new("3")).await?);

        assert!(cache.get(Path::new("3")).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_item_from_cache() -> anyhow::Result<()> {
        let local_storage_root = tempdir()?;
        let ram_storage = Arc::new(RamStorage::default());
        let mut cache =
            LocalStorageCache::new(ram_storage, local_storage_root.into_path(), 5, 5, 5);
        cache
            .put(
                Path::new("3"),
                PutPayload::InMemory(Bytes::from(b"abc".to_vec())),
            )
            .await?;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("3");

        assert!(
            !cache
                .copy_to_file(Path::new("12"), output_path.as_path())
                .await?
        );

        assert!(
            cache
                .copy_to_file(Path::new("3"), output_path.as_path())
                .await?
        );

        let metadata = tokio::fs::metadata(output_path.as_path()).await?;
        assert_eq!(metadata.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_underlying_ram_cache() -> anyhow::Result<()> {
        let local_storage = Arc::new(RamStorage::default());
        let local_storage_root = PathBuf::from("/");
        let mut cache = LocalStorageCache::new(local_storage, local_storage_root, 5, 5, 5);

        {
            let data = Bytes::from_static(&b"abc"[..]);
            cache.put_slice(Path::new("3"), 0..3, data).await?;
            assert_eq!(
                cache.get_slice(Path::new("3"), 0..3).await?.unwrap(),
                &b"abc"[..]
            );
        }
        {
            let data = Bytes::from_static(&b"de"[..]);
            cache.put_slice(Path::new("2"), 0..2, data).await?;
            // our first entry should still be here.
            assert_eq!(
                cache.get_slice(Path::new("3"), 0..3).await?.unwrap(),
                &b"abc"[..]
            );
            assert_eq!(
                cache.get_slice(Path::new("2"), 0..2).await?.unwrap(),
                &b"de"[..]
            );
        }
        {
            let data = Bytes::from_static(&b"fghij"[..]);
            cache.put_slice(Path::new("5"), 0..5, data).await?;
            assert_eq!(
                cache.get_slice(Path::new("5"), 0..5).await?.unwrap(),
                &b"fghij"[..]
            );
            // our two first entries should have be removed from the cache
            assert!(cache.get_slice(Path::new("2"), 0..2).await?.is_none());
            assert!(cache.get_slice(Path::new("3"), 0..3).await?.is_none());
        }
        {
            // The item insertion will be ignored since it's too large
            let data = Bytes::from_static(&b"klmnop"[..]);
            cache.put_slice(Path::new("6"), 0..6, data).await?;
            assert!(cache.get_slice(Path::new("6"), 0..6).await?.is_none());

            // The previous entry should however be remaining.
            assert_eq!(
                cache.get_slice(Path::new("5"), 0..5).await?.unwrap(),
                &b"fghij"[..]
            );
        }

        Ok(())
    }
}
