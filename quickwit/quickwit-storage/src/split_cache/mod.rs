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
use tantivy::directory::OwnedBytes;
use tracing::{error, info, instrument, warn};
use ulid::Ulid;

use crate::file_descriptor_cache::{FileDescriptorCache, SplitFile};
use crate::split_cache::download_task::spawn_download_task;
use crate::split_cache::split_table::SplitTable;
use crate::{Storage, StorageCache, wrap_storage_with_cache};

/// On disk Cache of splits for searchers.
///
/// The search acts receives reports of splits.
pub struct SplitCache {
    // Directory containing the cached split files.
    // Split ids are universally unique, so we all put them in the same directory.
    root_path: PathBuf,
    // In memory structure, listing the splits we know about regardless
    // of whether they are in cache, being downloaded, or just available for download.
    split_table: Mutex<SplitTable>,
    fd_cache: FileDescriptorCache,
}

impl SplitCache {
    /// Creates a new SplitCache and spawns the task that will continuously search for
    /// download opportunities.
    pub fn with_root_path(
        root_path: PathBuf,
        storage_resolver: crate::StorageResolver,
        limits: SplitCacheLimits,
    ) -> io::Result<Arc<SplitCache>> {
        std::fs::create_dir_all(&root_path)?;
        let mut existing_splits: BTreeMap<Ulid, u64> = Default::default();
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
                    if let Some(split_ulid) = split_id_from_path(&path) {
                        existing_splits.insert(split_ulid, meta.len());
                    } else {
                        warn!(path=%path.display(), ".split file with invalid ulid in split cache directory, ignoring");
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
        let split_cache = Arc::new(SplitCache {
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
    pub(crate) fn evict(&self, splits_to_evict: &[Ulid]) {
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
            let Ok(split_ulid) = Ulid::from_str(&report_split.split_id) else {
                error!(split_id=%report_split.split_id, "received invalid split ulid: ignoring");
                continue;
            };
            let Ok(storage_uri) = Uri::from_str(&report_split.storage_uri) else {
                error!(storage_uri=%report_split.storage_uri, "received invalid storage uri: ignoring");
                continue;
            };
            split_table.report(split_ulid, storage_uri);
        }
    }

    // Returns a split guard object. As long as it is not dropped, the
    // split won't be evinced from the cache.
    async fn get_split_file(&self, split_id: Ulid, storage_uri: &Uri) -> Option<SplitFile> {
        // We touch before even checking the fd cache in order to update the file's last access time
        // for the file cache.
        let num_bytes_opt: Option<u64> = self
            .split_table
            .lock()
            .unwrap()
            .touch(split_id, storage_uri);

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
fn delete_evicted_splits(root_path: &Path, splits_to_delete: &[Ulid]) {
    for &split_to_delete in splits_to_delete {
        let split_file_path = root_path.join(split_file(split_to_delete));
        if let Err(_io_err) = std::fs::remove_file(&split_file_path) {
            // This is an pretty critical error. The split size is not tracked anymore at this
            // point.
            error!(path=%split_file_path.display(), "failed to remove split file from cache directory. This is critical as the file is now not taken in account in the cache size limits");
        }
    }
}

fn split_id_from_path(split_path: &Path) -> Option<Ulid> {
    let split_filename = split_path.file_name()?.to_str()?;
    let split_id_str = split_filename.strip_suffix(".split")?;
    Ulid::from_str(split_id_str).ok()
}

struct SplitCacheBackingStorage {
    split_cache: Arc<SplitCache>,
    storage_root_uri: Uri,
}

impl SplitCacheBackingStorage {
    async fn get_impl(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_file: SplitFile = self
            .split_cache
            .get_split_file(split_id, &self.storage_root_uri)
            .await?;
        split_file.get_range(byte_range).await.ok()
    }

    async fn get_all_impl(&self, path: &Path) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_file = self
            .split_cache
            .get_split_file(split_id, &self.storage_root_uri)
            .await?;
        split_file.get_all().await.ok()
    }

    fn record_hit_metrics(&self, result_opt: Option<&OwnedBytes>) {
        let split_metrics = &crate::STORAGE_METRICS.searcher_split_cache;
        if let Some(result) = result_opt {
            split_metrics.hits_num_items.inc();
            split_metrics.hits_num_bytes.inc_by(result.len() as u64);
        } else {
            split_metrics.misses_num_items.inc();
        }
    }
}

#[async_trait]
impl StorageCache for SplitCacheBackingStorage {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let result = self.get_impl(path, byte_range).await;
        self.record_hit_metrics(result.as_ref());
        result
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let result = self.get_all_impl(path).await;
        self.record_hit_metrics(result.as_ref());
        result
    }

    async fn put(&self, _path: PathBuf, _byte_range: Range<usize>, _bytes: OwnedBytes) {}
    async fn put_all(&self, _path: PathBuf, _bytes: OwnedBytes) {}
}
