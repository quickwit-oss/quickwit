// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod download_task;
mod split_table;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quickwit_common::split_file;
use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use quickwit_proto::search::ReportSplit;
use quickwit_proto::types::SplitId;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;
use tracing::{error, info, instrument, warn};

use crate::file_descriptor_cache::{FileDescriptorCache, SplitFile};
use crate::split_cache::download_task::spawn_download_task;
use crate::split_cache::split_table::SplitTable;
use crate::{Storage, StorageCache, wrap_storage_with_cache};

/// On disk Cache of splits for searchers.
///
/// The search acts receives reports of splits.
pub struct SearchSplitCache {
    // Directory containing the cached split files.
    // Split ids are universally unique, so we all put them in the same directory.
    root_path: PathBuf,
    // In memory structure, listing the splits we know about regardless
    // of whether they are in cache, being downloaded, or just available for download.
    split_table: Mutex<SplitTable>,
    fd_cache: FileDescriptorCache,
}

impl SearchSplitCache {
    /// Creates a new SplitCache and spawns the task that will continuously search for
    /// download opportunities.
    pub fn with_root_path(
        root_path: PathBuf,
        storage_resolver: crate::StorageResolver,
        limits: SplitCacheLimits,
    ) -> io::Result<Arc<SearchSplitCache>> {
        std::fs::create_dir_all(&root_path)?;
        let mut existing_splits: BTreeMap<SplitId, u64> = Default::default();
        for dir_entry_res in std::fs::read_dir(&root_path)? {
            let dir_entry = dir_entry_res?;
            let path = dir_entry.path();
            let meta = std::fs::metadata(&path)?;
            if meta.is_dir() {
                continue;
            }
            let ext = path.extension().and_then(OsStr::to_str).unwrap_or("");
            match ext {
                "temp" => {
                    // This file is a temporary file that was being downloaded, when Quickwit was
                    // stopped (killed for instance) in a way that prevented
                    // their cleanup. It is important to remove it.
                    if let Err(io_err) = std::fs::remove_file(&path)
                        && io_err.kind() != io::ErrorKind::NotFound
                    {
                        error!(path=?path, "failed to remove temporary file");
                    }
                }
                "split" => {
                    if let Some(split_id) = split_id_from_path(&path) {
                        existing_splits.insert(split_id, meta.len());
                    } else {
                        warn!(path=%path.display(), ".split file with invalid name in split cache directory, ignoring");
                    }
                }
                _ => {
                    warn!(path=%path.display(), "unknown file in split cache directory, ignoring");
                }
            }
        }
        let mut split_table = SplitTable::with_limits_and_existing_splits(limits, existing_splits);

        // In case of a setting change, it could be useful to evict some splits on startup.
        let splits_to_remove_res = split_table.make_room_for_split_if_necessary(u64::MAX);
        if let Ok(splits_to_remove) = splits_to_remove_res {
            info!(
                num_splits = splits_to_remove.len(),
                "Evicting splits from the searcher cache. Has the node configuration changed?"
            );
            delete_evicted_splits(&root_path, &splits_to_remove[..]);
        }
        let fd_cache = FileDescriptorCache::with_fd_cache_capacity(limits.max_file_descriptors);
        let split_cache = Arc::new(SearchSplitCache {
            root_path,
            split_table: Mutex::new(split_table),
            fd_cache,
        });

        spawn_download_task(
            split_cache.clone(),
            storage_resolver,
            limits.num_concurrent_downloads,
        );

        Ok(split_cache)
    }

    /// Remove splits from both the fd cache and the split cache.
    /// This method does NOT update the split table.
    pub(crate) fn evict(&self, splits_to_evict: &[SplitId]) {
        self.fd_cache.evict_split_files(splits_to_evict);
        delete_evicted_splits(&self.root_path, splits_to_evict);
    }

    /// Wraps a storage with our split cache.
    pub fn wrap_storage(self_arc: Arc<Self>, storage: Arc<dyn Storage>) -> Arc<dyn Storage> {
        let cache = Arc::new(SplitCacheBackingStorage {
            split_cache: self_arc,
            storage_root_uri: storage.uri().clone(),
        });
        wrap_storage_with_cache(cache, storage)
    }

    /// Report the split cache about the existence of new splits.
    pub fn report_splits(&self, report_splits: Vec<ReportSplit>) {
        let mut split_table = self.split_table.lock().unwrap();
        for report_split in report_splits {
            // Split ids are opaque: any non-empty string is accepted (no ULID validation).
            let split_id = SplitId::from(report_split.split_id);
            let Ok(storage_uri) = Uri::from_str(&report_split.storage_uri) else {
                error!(storage_uri=%report_split.storage_uri, "received invalid storage uri: ignoring");
                continue;
            };
            split_table.report(split_id, storage_uri);
        }
    }

    // Returns a split guard object. As long as it is not dropped, the
    // split won't be evinced from the cache.
    async fn get_split_file(&self, split_id: SplitId, storage_uri: &Uri) -> Option<SplitFile> {
        // We touch before even checking the fd cache in order to update the file's last access time
        // for the file cache.
        let num_bytes_opt: Option<u64> = self
            .split_table
            .lock()
            .unwrap()
            .touch(split_id.clone(), storage_uri);

        let num_bytes = num_bytes_opt?;
        self.fd_cache
            .get_or_open_split_file(&self.root_path, split_id, num_bytes)
            .await
            .ok()
    }
}

/// Removes the evicted split files from the file system.
/// This function just logs errors, and swallows them.
///
/// At this point, the disk space is already accounted as released,
/// so the error could result in a "disk space leak".
#[instrument]
fn delete_evicted_splits(root_path: &Path, splits_to_delete: &[SplitId]) {
    for split_to_delete in splits_to_delete {
        let split_file_path = root_path.join(split_file(split_to_delete));
        if let Err(_io_err) = std::fs::remove_file(&split_file_path) {
            // This is an pretty critical error. The split size is not tracked anymore at this
            // point.
            error!(path=%split_file_path.display(), "failed to remove split file from cache directory. This is critical as the file is now not taken in account in the cache size limits");
        }
    }
}

fn split_id_from_path(split_path: &Path) -> Option<SplitId> {
    let split_filename = split_path.file_name()?.to_str()?;
    let split_id_str = split_filename.strip_suffix(".split")?;
    // The split id is opaque: we take the whole file stem (a random prefix, if any, is part of
    // the id) rather than parsing it as a ULID.
    Some(SplitId::from(split_id_str))
}

struct SplitCacheBackingStorage {
    split_cache: Arc<SearchSplitCache>,
    storage_root_uri: Uri,
}

impl SplitCacheBackingStorage {
    async fn get_split_file(&self, path: &Path) -> Option<SplitFile> {
        self.split_cache
            .get_split_file(split_id_from_path(path)?, &self.storage_root_uri)
            .await
    }

    fn record_hit_metrics(&self, hit_num_bytes: Option<usize>) {
        let split_metrics = &crate::metrics::SEARCHER_SPLIT_CACHE.cache_metrics;
        if let Some(num_bytes) = hit_num_bytes {
            split_metrics.hits_num_items.inc();
            split_metrics.hits_num_bytes.inc_by(num_bytes as u64);
        } else {
            split_metrics.misses_num_items.inc();
        }
    }
}

#[async_trait]
impl StorageCache for SplitCacheBackingStorage {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let result = match self.get_split_file(path).await {
            Some(file) => file.get_range(byte_range).await.ok(),
            None => None,
        };
        self.record_hit_metrics(result.as_ref().map(|bytes| bytes.len()));
        result
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        byte_range: Range<usize>,
    ) -> Option<Box<dyn AsyncRead + Send + Unpin>> {
        let file = self.get_split_file(path).await;
        self.record_hit_metrics(file.as_ref().map(|_| byte_range.len()));
        Some(Box::new(file?.get_range_stream(byte_range)))
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let result = match self.get_split_file(path).await {
            Some(file) => file.get_all().await.ok(),
            None => None,
        };
        self.record_hit_metrics(result.as_ref().map(|bytes| bytes.len()));
        result
    }

    async fn put(&self, _path: PathBuf, _byte_range: Range<usize>, _bytes: OwnedBytes) {}
    async fn put_all(&self, _path: PathBuf, _bytes: OwnedBytes) {}
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::os::unix::fs::FileExt;

    use bytesize::ByteSize;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{RamStorage, StorageResolver};

    #[tokio::test]
    async fn split_cache_stream_reads_on_demand_across_eviction() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = Path::new("cached.split");
        let file = std::fs::File::create(tempdir.path().join(path)).unwrap();
        let range_end = 2 * 1024 * 1024;
        file.set_len(range_end as u64 + 1).unwrap();
        file.write_all_at(b"a", 7).unwrap();
        let cache = SearchSplitCache::with_root_path(
            tempdir.path().to_owned(),
            StorageResolver::unconfigured(),
            SplitCacheLimits {
                max_num_bytes: ByteSize::mb(64),
                max_num_splits: NonZeroU32::new(8).unwrap(),
                num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
                max_file_descriptors: NonZeroU32::new(8).unwrap(),
            },
        )
        .unwrap();
        let storage =
            SearchSplitCache::wrap_storage(cache.clone(), Arc::new(RamStorage::default()));
        let mut reader = storage.get_slice_stream(path, 7..range_end).await.unwrap();
        assert_eq!(reader.read_u8().await.unwrap(), b'a');
        cache.evict(&[SplitId::from("cached")]);
        // Change the unread tail through the open descriptor to detect eager buffering.
        file.write_all_at(b"bc", range_end as u64 - 1).unwrap();
        let mut remaining_bytes = Vec::new();
        reader.read_to_end(&mut remaining_bytes).await.unwrap();
        assert_eq!(
            (remaining_bytes.len(), remaining_bytes.last()),
            (range_end - 8, Some(&b'b'))
        );
    }
}
