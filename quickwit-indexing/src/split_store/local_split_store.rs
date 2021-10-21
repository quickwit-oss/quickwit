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
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::{fs, io};

use quickwit_common::split_file;
use quickwit_storage::{LocalFileStorage, Storage, StorageErrorKind, StorageResult};
use tracing::{error, warn};

use super::IndexingSplitStoreParams;

fn split_id_from_filename(dir_entry: &DirEntry) -> Option<String> {
    if !dir_entry.path().is_file() {
        return None;
    }
    let split_filename = dir_entry.file_name().into_string().ok()?;
    split_filename
        .strip_suffix(".split")
        .map(ToString::to_string)
}

#[derive(Debug, Eq, PartialEq)]
struct SizeInCache {
    pub num_splits: usize,
    pub size_in_bytes: usize,
}

pub struct LocalSplitStore {
    /// The backing local storage.
    local_storage: LocalFileStorage,
    // The list of stored splits.
    stored_splits: HashMap<String, usize>,
    /// The parameters of the cache.
    params: IndexingSplitStoreParams,
}

impl LocalSplitStore {
    /// Try to open an existing local split directory.
    ///
    /// All files finishing by .split will be considered to be part of the directory.
    pub fn open(
        local_storage_root: PathBuf,
        params: IndexingSplitStoreParams,
    ) -> StorageResult<LocalSplitStore> {
        let mut cache_entries = HashMap::new();
        let mut total_size_in_bytes: usize = 0;
        for dir_entry_result in fs::read_dir(&local_storage_root)? {
            let dir_entry = dir_entry_result?;
            if let Some(split_id) = split_id_from_filename(&dir_entry) {
                let split_num_bytes = dir_entry.metadata()?.len() as usize;
                total_size_in_bytes += split_num_bytes;
                cache_entries.insert(split_id, split_num_bytes);
            }
        }

        let local_storage = LocalFileStorage::from(local_storage_root);

        if cache_entries.len() > params.max_num_splits {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial number of files exceeds the maximum number of files allowed.",
            )));
        }

        if total_size_in_bytes > params.max_num_bytes {
            return Err(StorageErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Initial cache size exceeds the maximum size in bytes allowed.",
            )));
        }

        Ok(LocalSplitStore {
            local_storage,
            stored_splits: cache_entries,
            params,
        })
    }

    /// Returns the list of splits in the cache
    pub fn list_splits(&self) -> Vec<String> {
        self.stored_splits.keys().cloned().collect()
    }

    #[cfg(test)]
    pub fn inspect(&self) -> &HashMap<String, usize> {
        &self.stored_splits
    }

    pub async fn remove_split(&mut self, split_id: &str) -> StorageResult<()> {
        if !self.stored_splits.contains_key(split_id) {
            return Ok(());
        }
        let split_path = PathBuf::from(quickwit_common::split_file(split_id));
        self.local_storage.delete(&split_path).await.map_err(|error| {
            error!(file_path = %split_path.display(), error = %error, "Failed to delete split file from local storage.");
            error
        })?;
        self.stored_splits.remove(split_id);
        Ok(())
    }

    pub async fn fetch_split(
        &mut self,
        split_id: &str,
        output_dir_path: &Path,
    ) -> StorageResult<bool> {
        if !self.stored_splits.contains_key(split_id) {
            return Ok(false);
        }
        let split_filename = quickwit_common::split_file(split_id);
        let out_filepath = output_dir_path.join(&split_filename);
        let copy_to_result = self
            .local_storage
            .move_out(Path::new(&split_filename), &out_filepath)
            .await;
        match copy_to_result {
            Ok(_) => {
                self.stored_splits.remove(split_id);
                Ok(true)
            }
            Err(storage_err) if storage_err.kind() == StorageErrorKind::DoesNotExist => {
                error!(split_path = split_filename.as_str(), error = ?storage_err, "Registered file is missing.");
                self.stored_splits.remove(split_id);
                Ok(false)
            }
            Err(storage_err) => Err(storage_err),
        }
    }

    fn size_in_store(&self) -> SizeInCache {
        let size_in_bytes = self.stored_splits.values().cloned().sum::<usize>();
        SizeInCache {
            num_splits: self.stored_splits.len(),
            size_in_bytes,
        }
    }

    /// Tries to move a `split` file into the cache.
    ///
    /// Move is not an image here. We are litterally moving files.
    ///
    /// If the cache capacity does not allow it, this function
    /// just logs a warning and returns Ok(false).
    ///
    /// Ok(true) means the file was effectively accepted.
    pub async fn move_into_cache(
        &mut self,
        split_id: &str,
        split_path: &Path,
        split_num_bytes: usize,
    ) -> io::Result<bool> {
        let size_in_cache = self.size_in_store();

        // Avoid storing in the cache when the maximum number of cached files is reached.
        if size_in_cache.num_splits + 1 > self.params.max_num_splits {
            warn!("Failed to cache file: maximum number of files exceeded.");
            return Ok(false);
        }

        // Ignore storing a file that cannot fit in remaining space in the cache.
        if split_num_bytes + size_in_cache.size_in_bytes > self.params.max_num_bytes {
            warn!("Failed to cache file: maximum size in bytes of cache exceeded.");
            return Ok(false);
        }

        let split_filename = PathBuf::from(split_file(split_id));
        self.local_storage
            .move_into(split_path, &split_filename)
            .await?;

        self.stored_splits
            .insert(split_id.to_string(), split_num_bytes);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_split_store_load_existing_splits() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        tokio::fs::write(&temp_dir.path().join("split1.split"), b"split-content").await?;
        tokio::fs::write(&temp_dir.path().join("split2.split"), b"split-content2").await?;
        tokio::fs::write(&temp_dir.path().join("different-file"), b"split-content").await?;
        tokio::fs::create_dir(&temp_dir.path().join("not-a-split.split")).await?;
        let params = IndexingSplitStoreParams::default();
        let split_store = LocalSplitStore::open(temp_dir.path().to_path_buf(), params)?;
        let cache_content = split_store.inspect();
        assert_eq!(cache_content.len(), 2);
        assert_eq!(cache_content.get("split1").cloned(), Some(13));
        assert_eq!(cache_content.get("split2").cloned(), Some(14));
        assert_eq!(
            split_store.size_in_store(),
            SizeInCache {
                num_splits: 2,
                size_in_bytes: 27
            }
        );
        assert_eq!(
            split_store.list_splits(),
            cache_content.keys().cloned().collect::<Vec<_>>()
        );
        Ok(())
    }
}
