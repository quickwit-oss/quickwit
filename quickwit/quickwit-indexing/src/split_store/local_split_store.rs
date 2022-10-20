// Copyright (C) 2022 Quickwit, Inc.
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

// The Local Split Store is a local cache used to improve the performance of indexing nodes.
// Its purpose is simple: when a new split is freshly created, it is usely merged
// very rapidly after.
//
// In order to prevent this merge to force its download, we store it in the
// `LocalSplitStore`. This store is just a cache: a cache miss is acceptable and
// just means that the split will be redownloaded.
//
// The local split store eviction policy however, is rather uncommon.
// On our happy path, a split is stored into the cache, and is then used only once
// to undergo a merge.
//
// For this reason, we simply offer a way to `move splits into the cache`,
// and `move splits out of the cache`. A split is removed from the split store
// after its first access.
//
// Of course a failed merge could require accessing a given split more than once. In that
// case the split will be downloaded again.
//
// The cache size is limited by 3 things:
// - a maximum number of splits as defined in the `SplitStoreQuota`.
// - a maximum number of bytes as defined in the `SplitStoreQuota`.
// - finally, we evict older splits to make sure that the newest split and the oldest
// split only differ by at most `SPLIT_MAX_AGE`.
//
// The point of this final rule invariant is to make sure that the disk space will be
// if the cache is NOT under pressure but some splits are actually useless.
//
// When adding a new split into the cache, if adding the split would break one of the following
// limit, we simply remove split one by one starting by the oldest first, until the split
// can be added.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{fs, io};

use anyhow::Context;
use byte_unit::Byte;
use quickwit_common::split_file;
use quickwit_directories::BundleDirectory;
use quickwit_storage::{StorageErrorKind, StorageResult};
use tantivy::directory::MmapDirectory;
use tantivy::Directory;
use tokio::sync::Mutex;
use tracing::warn;
use ulid::Ulid;

use super::SplitStoreQuota;

/// TODO Make this configurable.
const SPLIT_MAX_AGE: Duration = Duration::from_secs(2 * 24 * 3_600); // 2 days

pub fn get_tantivy_directory_from_split_bundle(
    split_file: &Path,
) -> StorageResult<Box<dyn Directory>> {
    let mmap_directory = MmapDirectory::open(split_file.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Couldn't find parent for {:?}", &split_file),
        )
    })?)?;
    let split_fileslice = mmap_directory.open_read(Path::new(&split_file))?;
    Ok(Box::new(BundleDirectory::open_split(split_fileslice)?))
}

/// Returns the number of bytes held in a given directory.
async fn num_bytes_in_folder(directory_path: &Path) -> io::Result<Byte> {
    let mut total_bytes = 0;
    let mut read_dir = tokio::fs::read_dir(directory_path).await?;
    while let Some(dir_entry) = read_dir.next_entry().await? {
        let metadata = dir_entry.metadata().await?;
        if metadata.is_file() {
            total_bytes += metadata.len();
        } else {
            warn!(
                "Unexpected directory found in split cache. {:?}",
                dir_entry.path()
            );
        }
    }
    Ok(Byte::from_bytes(total_bytes))
}

/// The local split store is a cache for freshly indexed splits.
///
/// In order to save the cost of an extra write, we store them in the form
/// of a directory and the splits are built on the file upon upload.
struct SplitFolder {
    split_id: Ulid,
    num_bytes: Byte,
}

impl SplitFolder {
    /// Creates a new `SplitFolder`.
    ///
    /// There are no specific constraint on `path`.
    pub async fn create(split_id: &str, path: &Path) -> io::Result<Self> {
        let split_id = Ulid::from_str(split_id).map_err(|_err| {
            let error_msg = format!("Split Id should be an `Ulid`. Got `{split_id:?}`.");
            io::Error::new(io::ErrorKind::InvalidInput, error_msg)
        })?;
        let num_bytes = num_bytes_in_folder(path).await?;
        Ok(SplitFolder {
            split_id,
            num_bytes,
        })
    }

    /// Returns the creation time as encoded in the split id ULID.
    fn creation_time(&self) -> SystemTime {
        self.split_id.datetime()
    }
}

fn split_id_from_split_folder(dir_path: &Path) -> io::Result<Option<&str>> {
    let metadata = dir_path.metadata()?;

    if !metadata.is_dir() {
        return Ok(None);
    }

    let split_id = dir_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".split"));

    Ok(split_id)
}

pub struct LocalSplitStore {
    inner: Mutex<InnerLocalSplitStore>,
}

struct InnerLocalSplitStore {
    /// Splits owned by the local split store, which reside in the split_store_folder.
    /// SplitId -> SplitFolder
    split_folders: HashMap<Ulid, SplitFolder>,
    /// Binary heap of split ids.
    /// The binary heap is used for eviction.
    ///
    /// Splits ids are generated using ULID, so that they are sorted
    /// according to their creation date.
    /// We evict the oldest split first. (Note this is not an LRU strategy
    /// because we do not care about the last access time, but we only
    /// consider the creation time.)
    split_ids: BinaryHeap<Reverse<Ulid>>,
    /// The root folder where all data is moved into.
    split_store_folder: PathBuf,
    /// The split store space quota shared among all indexing split stores.
    split_store_space_quota: SplitStoreQuota,
}

impl InnerLocalSplitStore {
    /// Moves a split within the store to an external folder.
    async fn move_out(
        &mut self,
        split_id: Ulid,
        to_folder: &Path,
    ) -> StorageResult<Option<PathBuf>> {
        let split_folder = self.split_folders.remove(&split_id).ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Missing split_id `{split_id}`"))
        })?;
        let from_path = self.split_path(split_id);
        let to_full_path = to_folder.join(from_path.file_name().unwrap());
        tokio::fs::rename(&from_path, &to_full_path).await?;
        self.split_store_space_quota
            .remove_split(split_folder.num_bytes);
        Ok(Some(to_full_path))
    }

    /// Returns the directory filepath of a split in cache.
    fn split_path(&self, split_id: Ulid) -> PathBuf {
        let split_file = split_file(&split_id.to_string());
        self.split_store_folder.join(&split_file)
    }

    /// Remove one split from the cache to make some room.
    ///
    /// # Panics
    /// Panics if there are no remaining splits.
    async fn evict_one_split(&mut self) -> io::Result<()> {
        let split_folder = loop {
            let split_id = self
                .split_ids
                .pop()
                .expect("No remaining split to remove")
                .0;
            if let Some(split_folder) = self.split_folders.remove(&split_id) {
                break split_folder;
            }
        };
        self.split_store_space_quota
            .remove_split(split_folder.num_bytes);
        tokio::fs::remove_dir_all(&self.split_path(split_folder.split_id)).await?;
        Ok(())
    }

    /// Tries to move a `split_folder` file into the cache.
    ///
    /// Move is not an image here. We are litterally moving the directory.
    ///
    /// If the cache capacity does not allow it, this function
    /// just logs a warning and returns Ok(false).
    ///
    /// Ok(true) means the file was effectively accepted.
    async fn move_into_cache(&mut self, split_id_str: &str, split_path: &Path) -> io::Result<bool> {
        let split_folder = SplitFolder::create(split_id_str, split_path).await?;
        let split_id = split_folder.split_id;
        let should_move_split = self.make_room_and_record_split(split_folder).await?;
        if !should_move_split {
            return Ok(false);
        }
        let to_full_path = self.split_path(split_id);
        tokio::fs::rename(split_path, &to_full_path).await?;
        Ok(true)
    }

    /// Removes all splits that have a creation date older than `limit`.
    async fn remove_splits_older_than_limit(&mut self, limit: SystemTime) -> io::Result<()> {
        while let Some(split_id) = self.split_ids.peek() {
            if split_id.0.datetime() >= limit {
                break;
            }
            self.evict_one_split().await?;
        }
        Ok(())
    }

    /// Ensures that there is room to store the split,
    /// return false, if the split should not be added to the cache.
    /// return true and record split , if the split should be moved into the cache.
    async fn make_room_and_record_split(&mut self, split_folder: SplitFolder) -> io::Result<bool> {
        // We don't accept splits that are too large.
        if split_folder.num_bytes > self.split_store_space_quota.max_num_bytes() {
            return Ok(false);
        }

        while !self
            .split_store_space_quota
            .can_fit_split(split_folder.num_bytes)
        {
            self.evict_one_split().await?;
        }

        if let Some(creation_time_limit) = split_folder.creation_time().checked_sub(SPLIT_MAX_AGE) {
            self.remove_splits_older_than_limit(creation_time_limit)
                .await?;
        }

        self.split_store_space_quota
            .add_split(split_folder.num_bytes);
        let split_id = split_folder.split_id;
        self.split_folders.insert(split_id, split_folder);
        self.split_ids.push(Reverse(split_id));
        Ok(true)
    }
}

impl LocalSplitStore {
    pub fn no_caching() -> LocalSplitStore {
        let inner = Mutex::new(InnerLocalSplitStore {
            split_folders: Default::default(),
            split_store_folder: PathBuf::from("no_caching"),
            split_store_space_quota: SplitStoreQuota::no_caching(),
            split_ids: BinaryHeap::default(),
        });
        LocalSplitStore { inner }
    }

    /// Try to open an existing local split store directory.
    ///
    /// If the directory does not exists, it will be created.
    ///
    /// The directory is expected to only contain directory
    /// with a name following the pattern `<ULID.split>`.
    ///
    /// The different pre-existing splits are recorded into
    /// the split store in their creation order. If the split store
    /// quota have been modified, the store will undergo the same
    /// eviction logic.
    pub async fn open(
        split_store_folder: PathBuf,
        space_quota: SplitStoreQuota,
    ) -> anyhow::Result<LocalSplitStore> {
        tokio::fs::create_dir_all(&split_store_folder)
            .await
            .context("Failed to create the split cache directory.")?;

        let mut split_folders: Vec<SplitFolder> = Vec::new();

        for dir_entry_result in fs::read_dir(&split_store_folder)? {
            let dir_entry = dir_entry_result?;
            let dir_path: PathBuf = dir_entry.path();
            let split_id = split_id_from_split_folder(&dir_path)?
                .ok_or_else(|| {
                    let error_msg = format!(
                        "Split folder name should match the format `<split_id>.split`. Got \
                         `{dir_path:?}`."
                    );
                    io::Error::new(io::ErrorKind::InvalidInput, error_msg)
                })?
                .to_string();
            let split_folder = SplitFolder::create(&split_id, &dir_entry.path()).await?;
            split_folders.push(split_folder);
        }

        let mut inner_local_split_store = InnerLocalSplitStore {
            split_store_folder: split_store_folder.clone(),
            split_store_space_quota: space_quota,
            split_folders: HashMap::default(),
            split_ids: BinaryHeap::default(),
        };

        split_folders.sort_by_key(SplitFolder::creation_time);

        // We record all `split_folder`, sorted by `creation_time`.
        for split_folder in split_folders {
            let split_id = split_folder.split_id;
            if !inner_local_split_store
                .make_room_and_record_split(split_folder)
                .await?
            {
                let split_dir = inner_local_split_store.split_path(split_id);
                tokio::fs::remove_dir_all(&split_dir).await?;
            }
        }

        Ok(LocalSplitStore {
            inner: Mutex::new(inner_local_split_store),
        })
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn inspect(&self) -> HashMap<String, Byte> {
        self.inner
            .lock()
            .await
            .split_folders
            .iter()
            .map(|(k, split_folder)| (k.to_string(), split_folder.num_bytes))
            .collect()
    }

    /// Returns a cached split to performs a merge operation.
    ///
    /// For simplicity, this method optimistically assumes that the merge operation will be
    /// successful and removes the split from the cache.
    ///
    /// If the merge operation is a failure and needs to be re-executed, we will
    /// experience a cache miss, and the split will be downloaded from the
    /// storage.
    pub(super) async fn get_cached_split(
        &self,
        split_id: &str,
        output_dir_path: &Path,
    ) -> StorageResult<Option<PathBuf>> {
        let mut split_store_lock = self.inner.lock().await;
        let split_ulid = if let Ok(split_ulid) = Ulid::from_str(split_id) {
            split_ulid
        } else {
            return Ok(None);
        };
        let split_file_res = split_store_lock.move_out(split_ulid, output_dir_path).await;
        match split_file_res {
            Ok(Some(split_path)) => Ok(Some(split_path)),
            Ok(None) => Ok(None),
            Err(storage_err) if storage_err.kind() == StorageErrorKind::DoesNotExist => {
                warn!(split_id = split_id, error = ?storage_err, "Cached split file/folder is missing.");
                split_store_lock.split_folders.remove(&split_ulid);
                Ok(None)
            }
            Err(storage_err) => Err(storage_err),
        }
    }

    /// Tries to move a `split_folder` file into the cache.
    ///
    /// Move is not an image here. We are litterally moving the directory.
    ///
    /// If the cache capacity does not allow it, this function
    /// just logs a warning and returns Ok(false).
    ///
    /// Ok(true) means the file was effectively accepted.
    pub(super) async fn move_into_cache(
        &self,
        split_id: &str,
        split_path: &Path,
    ) -> io::Result<bool> {
        assert!(split_path.is_dir());
        let mut inner = self.inner.lock().await;
        inner.move_into_cache(split_id, split_path).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::io::Write;
    use std::path::Path;

    use byte_unit::Byte;
    use quickwit_directories::BundleDirectory;
    use quickwit_storage::{PutPayload, SplitPayloadBuilder};
    use tantivy::directory::FileSlice;
    use tantivy::Directory;
    use tempfile::tempdir;
    use tokio::fs;
    use ulid::Ulid;

    use crate::split_store::{LocalSplitStore, SplitStoreQuota};

    async fn create_fake_split(
        split_cache_path: &Path,
        split_id: &str,
        len: usize,
    ) -> io::Result<()> {
        let split_path = split_cache_path.join(format!("{split_id}.split"));
        fs::create_dir(&split_path).await?;
        fs::write(split_path.join("splitdata"), &vec![0u8; len]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_local_split_store_load_existing_splits() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let split_id1 = "01GF5449X7DA53TK9F9W2ZJST2";
        let split_id2 = "01GF545472A06WY07SEHGCJF9P";
        create_fake_split(temp_dir.path(), split_id1, 15).await?;
        create_fake_split(temp_dir.path(), split_id2, 13).await?;
        let split_store_space_quota = SplitStoreQuota::default();
        let split_store =
            LocalSplitStore::open(temp_dir.path().to_path_buf(), split_store_space_quota).await?;
        let cache_content = split_store.inspect().await;
        assert_eq!(cache_content.len(), 2);
        assert_eq!(
            cache_content.get(split_id1).cloned(),
            Some(Byte::from_bytes(15))
        );
        assert_eq!(
            cache_content.get(split_id2).cloned(),
            Some(Byte::from_bytes(13))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_with_too_many_files() {
        let dir = tempdir().unwrap();
        create_fake_split(dir.path(), "01GF5215TMV48JT7GZ543BV193", 12)
            .await
            .unwrap(); // 1
        create_fake_split(dir.path(), "01GF520MTTRNCCTQZE264BBYWM", 23)
            .await
            .unwrap(); // 0
        create_fake_split(dir.path(), "01GF521M316V9AEHZWTHN76F2V", 5)
            .await
            .unwrap(); // 3
        create_fake_split(dir.path(), "01GF521CZC1260V8QPA81T46X7", 45)
            .await
            .unwrap(); // 2
        let split_store_space_quota = SplitStoreQuota::new(2, Byte::from_bytes(1_000));
        let local_split_store =
            LocalSplitStore::open(dir.path().to_path_buf(), split_store_space_quota)
                .await
                .unwrap();
        assert_eq!(local_split_store.inspect().await.len(), 2);
    }

    #[tokio::test]
    async fn test_create_with_exceeds_num_bytes() {
        let dir = tempdir().unwrap();
        create_fake_split(dir.path(), "01GF5215TMV48JT7GZ543BV193", 12)
            .await
            .unwrap(); // 1
        create_fake_split(dir.path(), "01GF520MTTRNCCTQZE264BBYWM", 23)
            .await
            .unwrap(); // 0
        create_fake_split(dir.path(), "01GF521M316V9AEHZWTHN76F2V", 5)
            .await
            .unwrap(); // 3
        create_fake_split(dir.path(), "01GF521CZC1260V8QPA81T46X7", 45)
            .await
            .unwrap(); // 2
        let split_store_space_quota = SplitStoreQuota::new(6, Byte::from_bytes(61));
        let local_split_store =
            LocalSplitStore::open(dir.path().to_path_buf(), split_store_space_quota)
                .await
                .unwrap();
        let cache_content = local_split_store.inspect().await;
        assert_eq!(cache_content.len(), 2);
        assert_eq!(cache_content.values().map(Byte::get_bytes).sum::<u64>(), 50);
    }

    #[tokio::test]
    async fn test_remove_splits_out_of_age() {
        let dir = tempdir().unwrap();
        // 2022-10-13T06:12:37.643Z
        create_fake_split(dir.path(), "01GF7ZJBMBMEPMAQSFD09VTST2", 1)
            .await
            .unwrap();
        // 2022-10-12T20:53:23.211Z
        create_fake_split(dir.path(), "01GF6ZJBMBMEPMAQSFD09VTST2", 1)
            .await
            .unwrap();
        // 2022-10-12T02:14:54.347Z
        create_fake_split(dir.path(), "01GF4ZJBMBMEPMAQSFD09VTST2", 1)
            .await
            .unwrap();
        // 2022-10-10T22:17:11.051Z
        create_fake_split(dir.path(), "01GF1ZJBMBMEPMAQSFD09VTST2", 1)
            .await
            .unwrap();
        let split_store_space_quota = SplitStoreQuota::new(6, Byte::from_bytes(100));
        let local_split_store =
            LocalSplitStore::open(dir.path().to_path_buf(), split_store_space_quota)
                .await
                .unwrap();
        let cache_content = local_split_store.inspect().await;
        assert_eq!(cache_content.len(), 3);
        let extra_split = tempdir().unwrap();
        local_split_store
            .move_into_cache("01GFCZJBMBMEPMAQSFD09VTST2", extra_split.path())
            .await
            .unwrap();
        let cache_content = local_split_store.inspect().await;
        assert_eq!(cache_content.len(), 2);
    }

    #[tokio::test]
    async fn test_stream_split_to_bundle_and_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");
        let mut file1 = File::create(&test_filepath1).unwrap();
        file1.write_all(b"ab").unwrap();
        let mut file2 = File::create(&test_filepath2).unwrap();
        file2.write_all(b"def").unwrap();
        let split_streamer =
            SplitPayloadBuilder::get_split_payload(&[test_filepath1, test_filepath2], b"hotcache")
                .unwrap();
        let data = split_streamer.read_all().await.unwrap();
        let bundle_dir = BundleDirectory::open_split(FileSlice::from(data.to_vec())).unwrap();
        let f1_data = bundle_dir.atomic_read(Path::new("f1")).unwrap();
        assert_eq!(&*f1_data, b"ab");
        let f2_data = bundle_dir.atomic_read(Path::new("f2")).unwrap();
        assert_eq!(&f2_data[..], b"def");
    }

    #[tokio::test]
    async fn test_store_and_fetch() {
        let temp_dir_in = tempfile::tempdir().unwrap();
        let split_id = Ulid::default().to_string();
        let split_dir = temp_dir_in.path().join(format!("scratch_{split_id}"));
        tokio::fs::create_dir(&split_dir).await.unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let quota = SplitStoreQuota::default();
        let local_store = LocalSplitStore::open(cache_dir.path().to_path_buf(), quota)
            .await
            .unwrap();
        assert!(split_dir.exists());
        assert!(local_store
            .move_into_cache(&split_id, &split_dir)
            .await
            .unwrap());
        assert!(!split_dir.exists());
        let split_path = local_store
            .get_cached_split(&split_id, temp_dir_in.path())
            .await
            .unwrap()
            .unwrap();
        assert!(split_path.exists());
        assert_eq!(split_path.parent().unwrap(), temp_dir_in.path());
    }
}
