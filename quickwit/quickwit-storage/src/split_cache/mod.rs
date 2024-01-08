// Copyright (C) 2024 Quickwit, Inc.
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

mod download_task;
mod split_table;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use quickwit_proto::search::ReportSplit;
use tantivy::directory::OwnedBytes;
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::split_cache::download_task::{delete_evicted_splits, spawn_download_task};
use crate::split_cache::split_table::{SplitGuard, SplitTable};
use crate::{wrap_storage_with_cache, Storage, StorageCache};

/// On disk Cache of splits for searchers.
///
/// The search acts receives reports of splits.
pub struct SplitCache {
    // Directory containing the cached split files.
    // Split ids are universally unique, so we all put them in the same directory.
    root_path: PathBuf,
    // In memory structure, listing the splits we know about regardless
    // of whether they are in cache, being downloaded, or just available for download.
    split_table: Arc<Mutex<SplitTable>>,
}

impl SplitCache {
    /// Creates a new SplitCache and spawns the task that will continuously search for
    /// download opportunities.
    pub fn with_root_path(
        root_path: PathBuf,
        storage_resolver: crate::StorageResolver,
        limits: SplitCacheLimits,
    ) -> io::Result<SplitCache> {
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
                    if let Err(io_err) = std::fs::remove_file(&path) {
                        if io_err.kind() != io::ErrorKind::NotFound {
                            error!(path=?path, "failed to remove temporary file");
                        }
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
        let splits_to_remove_opt = split_table.make_room_for_split_if_necessary(u64::MAX);
        let root_path_clone = root_path.clone();
        if let Some(splits_to_remove) = splits_to_remove_opt {
            info!(
                num_splits = splits_to_remove.len(),
                "Evicting splits from the searcher cache. Has the node configuration changed?"
            );
            delete_evicted_splits(&root_path_clone, &splits_to_remove[..]);
        }
        let split_table_arc = Arc::new(Mutex::new(split_table));

        spawn_download_task(
            root_path.clone(),
            split_table_arc.clone(),
            storage_resolver,
            limits.num_concurrent_downloads,
        );

        Ok(SplitCache {
            root_path,
            split_table: split_table_arc,
        })
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

    fn cached_split_filepath(&self, split_id: Ulid) -> PathBuf {
        let split_filename = quickwit_common::split_file(split_id);
        self.root_path.join(split_filename)
    }

    // Returns a split guard object. As long as it is not dropped, the
    // split won't be evinced from the cache.
    fn get_split_guard(&self, split_id: Ulid, storage_uri: &Uri) -> Option<SplitFilepath> {
        let split_guard = self
            .split_table
            .lock()
            .unwrap()
            .get_split_guard(split_id, storage_uri)?;
        Some(SplitFilepath {
            _split_guard: split_guard,
            cached_split_file_path: self.cached_split_filepath(split_id),
        })
    }
}

pub struct SplitFilepath {
    _split_guard: SplitGuard,
    cached_split_file_path: PathBuf,
}

impl AsRef<Path> for SplitFilepath {
    fn as_ref(&self) -> &Path {
        &self.cached_split_file_path
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
        let split_guard = self
            .split_cache
            .get_split_guard(split_id, &self.storage_root_uri)?;
        // TODO touch file in cache.
        // We don't use async file io here because it spawn blocks anyway, and it feels dumb to
        // spawn block 3 times in a row.
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(&split_guard).ok()?;
            file.seek(SeekFrom::Start(byte_range.start as u64)).ok()?;
            let mut buf = Vec::with_capacity(byte_range.len());
            file.take(byte_range.len() as u64)
                .read_to_end(&mut buf)
                .ok()?;
            Some(OwnedBytes::new(buf))
        })
        .await
        // TODO Remove file from cache if io error?
        .ok()?
    }

    async fn get_all_impl(&self, path: &Path) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_guard = self
            .split_cache
            .get_split_guard(split_id, &self.storage_root_uri)?;
        // We don't use async file io here because it spawn blocks anyway, and it feels dumb to
        // spawn block 3 times in a row.
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(split_guard).ok()?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).ok()?;
            Some(OwnedBytes::new(buf))
        })
        .await
        .ok()?
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
