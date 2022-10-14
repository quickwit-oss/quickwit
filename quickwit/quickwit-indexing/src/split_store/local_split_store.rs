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

use std::collections::{HashMap, HashSet};
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use quickwit_common::{ignore_io_error, split_file};
use quickwit_directories::BundleDirectory;
use quickwit_storage::{PutPayload, SplitPayloadBuilder, StorageErrorKind, StorageResult};
use tantivy::directory::MmapDirectory;
use tantivy::Directory;
use tokio::sync::Mutex;
use tracing::error;

use super::SplitStoreSpaceQuota;

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

#[derive(Clone, Debug)]
pub struct SplitFolder {
    path: PathBuf,
}
impl SplitFolder {
    pub fn new(path: PathBuf) -> Self {
        SplitFolder { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl SplitFolder {
    pub fn get_tantivy_directory(&self) -> StorageResult<Box<dyn Directory>> {
        let mmap_directory = MmapDirectory::open(&self.path)?;
        Ok(Box::new(mmap_directory))
    }

    /// Moves the underlying data to a new location.
    async fn move_to(&mut self, new_folder: &Path, split_id: &str) -> StorageResult<()> {
        let new_path = PathBuf::from(split_file(split_id));
        let to_full_path = new_folder.join(new_path);
        tokio::fs::rename(&self.path, &to_full_path).await?;
        self.path = to_full_path.to_path_buf();
        Ok(())
    }

    async fn delete(&self) -> io::Result<()> {
        ignore_io_error!(
            io::ErrorKind::NotFound,
            tokio::fs::remove_dir_all(&self.path).await
        )?;
        Ok(())
    }
}

fn split_id_from_split_folder(dir_entry: &DirEntry) -> Option<String> {
    if !dir_entry.path().is_dir() {
        return None;
    }
    let split_filename = dir_entry.file_name().into_string().ok()?;
    split_filename
        .strip_suffix(".split")
        .map(ToString::to_string)
}

pub struct LocalSplitStore {
    /// Splits owned by the local split store, which reside in the split_store_folder.
    /// SplitId -> (Split Num Bytes, BundledSplitFile)
    split_files: HashMap<String, (usize, SplitFolder)>,
    /// The root folder where all data is moved into.
    split_store_folder: PathBuf,
    /// The split store space quota shared among all indexing split stores.
    split_store_space_quota: Arc<Mutex<SplitStoreSpaceQuota>>,
}

impl LocalSplitStore {
    /// Try to open an existing local split directory.
    ///
    /// All files finishing by .split will be considered to be part of the directory.
    pub async fn open(
        local_storage_root: PathBuf,
        split_store_space_quota: Arc<Mutex<SplitStoreSpaceQuota>>,
    ) -> StorageResult<LocalSplitStore> {
        let mut split_files: HashMap<String, (usize, SplitFolder)> = HashMap::new();
        let mut total_size_in_bytes: usize = 0;
        for dir_entry_result in fs::read_dir(&local_storage_root)? {
            let dir_entry = dir_entry_result?;
            if let Some(split_id) = split_id_from_split_folder(&dir_entry) {
                let split_file = SplitFolder::new(dir_entry.path());
                let paths: Vec<_> = fs::read_dir(dir_entry.path())?
                    .map(|el| el.map(|el| el.path()))
                    .collect::<Result<_, _>>()?;
                // TODO: Do we need the hotcache?
                let split_streamer = SplitPayloadBuilder::get_split_payload(&paths, &[])?;

                let split_num_bytes = split_streamer.len() as usize;
                total_size_in_bytes += split_num_bytes;
                split_files.insert(split_id, (split_num_bytes, split_file));
            }
        }

        split_store_space_quota.lock().await.add_initial_splits(
            local_storage_root.as_path(),
            split_files.len(),
            total_size_in_bytes,
        )?;
        Ok(LocalSplitStore {
            split_store_folder: local_storage_root,
            split_files,
            split_store_space_quota,
        })
    }

    /// Clean the split store.
    /// By only keeping the splits specified and removing other
    /// existing splits in this store.
    pub async fn retain_only(&mut self, split_ids: &[&str]) -> StorageResult<()> {
        let stored_ids_set: HashSet<String> = self.split_files.keys().cloned().collect();
        let to_retain_ids_set: HashSet<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let to_remove_ids_set = &stored_ids_set - &to_retain_ids_set;

        let total_num_splits = to_remove_ids_set.len();
        let mut total_size_splits = 0usize;
        for split_id in to_remove_ids_set {
            total_size_splits += self.remove_split(&split_id).await?;
        }
        self.split_store_space_quota
            .lock()
            .await
            .remove_splits(total_num_splits, total_size_splits);
        Ok(())
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn inspect(&self) -> HashMap<String, usize> {
        self.split_files
            .iter()
            .map(|(k, v)| (k.to_string(), v.0))
            .collect()
    }

    pub async fn remove_split(&mut self, split_id: &str) -> StorageResult<usize> {
        if !self.split_files.contains_key(split_id) {
            return Ok(0);
        }
        if let Some((split_size, split_file)) = self.split_files.remove(split_id) {
            split_file.delete().await?;
            return Ok(split_size);
        }
        Ok(0)
    }

    /// Moves a split into the store.
    pub async fn move_into(
        &self,
        split: &mut SplitFolder,
        new_folder: &Path,
        split_id: &str,
    ) -> StorageResult<()> {
        split.move_to(new_folder, split_id).await?;
        Ok(())
    }

    /// Moves a split within the store to an external folder.
    pub async fn move_out(
        &mut self,
        split_id: &str,
        to_folder: &Path,
    ) -> StorageResult<SplitFolder> {
        let (split_size_in_bytes, mut split_folder) =
            self.split_files.remove(split_id).ok_or_else(|| {
                StorageErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Missing split_id `{}`", split_id))
            })?;
        split_folder.move_to(to_folder, split_id).await?;
        let mut split_store_space_quota_guard = self.split_store_space_quota.lock().await;
        split_store_space_quota_guard.remove_splits(1, split_size_in_bytes);
        Ok(split_folder)
    }

    /// Returns a cached split.
    pub async fn get_cached_split(
        &mut self,
        split_id: &str,
        output_dir_path: &Path,
    ) -> StorageResult<Option<SplitFolder>> {
        if !self.split_files.contains_key(split_id) {
            return Ok(None);
        }
        let split_file_res = self.move_out(split_id, output_dir_path).await;
        match split_file_res {
            Ok(split_file) => {
                self.split_files.remove(split_id);
                Ok(Some(split_file))
            }
            Err(storage_err) if storage_err.kind() == StorageErrorKind::DoesNotExist => {
                error!(split_id = split_id, error = ?storage_err, "Cached split file/folder is missing.");
                self.split_files.remove(split_id);
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
    pub async fn move_into_cache<'a>(
        &'a mut self,
        split_id: &'a str,
        mut split_folder: SplitFolder,
        split_num_bytes: usize,
    ) -> io::Result<bool> {
        assert!(split_folder.path().is_dir());
        let mut split_store_space_quota_guard = self.split_store_space_quota.lock().await;
        if !split_store_space_quota_guard.can_fit_split(split_num_bytes) {
            return Ok(false);
        }

        self.move_into(&mut split_folder, &self.split_store_folder, split_id)
            .await?;

        self.split_files
            .insert(split_id.to_string(), (split_num_bytes, split_folder));
        split_store_space_quota_guard.add_split(split_num_bytes);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use quickwit_storage::PutPayload;
    use tantivy::directory::FileSlice;

    use super::*;

    #[tokio::test]
    async fn test_local_split_store_load_existing_splits() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        tokio::fs::write(&temp_dir.path().join("not-a-split.split"), b"split-content").await?;
        tokio::fs::write(
            &temp_dir.path().join("also-not-a-split.split"),
            b"split-content2",
        )
        .await?;
        tokio::fs::write(&temp_dir.path().join("different-file"), b"split-content").await?;
        tokio::fs::create_dir(&temp_dir.path().join("split1.split")).await?;
        tokio::fs::create_dir(&temp_dir.path().join("split2.split")).await?;
        let split_store_space_quota = Arc::new(Mutex::new(SplitStoreSpaceQuota::default()));
        let split_store =
            LocalSplitStore::open(temp_dir.path().to_path_buf(), split_store_space_quota).await?;
        let cache_content = split_store.inspect();
        assert_eq!(cache_content.len(), 2);
        assert_eq!(cache_content.get("split1").cloned(), Some(28));
        assert_eq!(cache_content.get("split2").cloned(), Some(28));
        assert_eq!(
            split_store
                .split_files
                .values()
                .map(|(size, _)| size)
                .cloned()
                .sum::<usize>(),
            28 * 2
        );
        assert_eq!(split_store.split_files.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_local_split_stores_with_shared_split_store_space_quota() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let split_store_1_path = temp_dir.path().join("store1");
        let split_store_2_path = temp_dir.path().join("store2");

        tokio::fs::create_dir_all(&split_store_1_path.join("split1.split")).await?;
        tokio::fs::create_dir_all(&split_store_1_path.join("split2.split")).await?;
        tokio::fs::create_dir_all(&split_store_2_path).await?;
        let split_store_space_quota = Arc::new(Mutex::new(SplitStoreSpaceQuota::new(4, 120)));
        let mut split_store1 =
            LocalSplitStore::open(split_store_1_path, split_store_space_quota.clone()).await?;
        let mut split_store2 =
            LocalSplitStore::open(split_store_2_path, split_store_space_quota.clone()).await?;

        let task1 = async {
            let split_path = temp_dir.path().join("split3.split");
            tokio::fs::create_dir_all(&split_path).await.unwrap();
            let is_accepted = split_store1
                .move_into_cache("split3", SplitFolder::new(split_path), 28)
                .await
                .unwrap();
            assert!(is_accepted);
        };
        let task2 = async {
            let split_path = temp_dir.path().join("split1.split");
            tokio::fs::create_dir_all(&split_path).await.unwrap();
            let is_accepted = split_store2
                .move_into_cache("split1", SplitFolder::new(split_path), 28)
                .await
                .unwrap();
            assert!(is_accepted);
        };
        tokio::join!(task1, task2);

        // We need this block to drop the `split_store_space_quota_guard`
        // as we cannot make progress while holding it.
        {
            let split_store_space_quota_guard = split_store_space_quota.lock().await;
            assert_eq!(split_store_space_quota_guard.num_splits(), 4);
            assert_eq!(split_store_space_quota_guard.size_in_bytes(), 28 * 4);
        }

        // Check we cannot store anymore items.
        let split_path = temp_dir.path().join("split2.split");
        tokio::fs::create_dir_all(&split_path).await.unwrap();
        let is_not_accepted = !split_store2
            .move_into_cache("split2", SplitFolder::new(split_path), 28)
            .await
            .unwrap();
        assert!(is_not_accepted);

        // Now remove a split and check that the quota has been updated.
        split_store1
            .move_out("split1", temp_dir.path())
            .await
            .unwrap();
        let split_store_space_quota_guard = split_store_space_quota.lock().await;
        assert_eq!(split_store_space_quota_guard.num_splits(), 3);
        assert_eq!(split_store_space_quota_guard.size_in_bytes(), 28 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_split_to_bundle_and_open() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[1, 2, 3],
        )?;

        let data = split_streamer.read_all().await?;

        let bundle_dir = BundleDirectory::open_split(FileSlice::from(data.to_vec()))?;

        let f1_data = bundle_dir.atomic_read(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.atomic_read(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }
}
