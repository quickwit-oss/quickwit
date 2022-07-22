use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use async_trait::async_trait;
use tracing::{error, warn};

use crate::cache::Cache;
use crate::metrics::CacheMetrics;
use crate::OwnedBytes;
use super::file::FileKey;
use super::store::{open_disk_store, FileBackedDirectory, Store};

/// TODO: Move to shared area for memory and disk cache.
const MIN_TIME_SINCE_LAST_ACCESS: Duration = Duration::from_secs(60);

type LruCache = lru::LruCache<FileKey, ()>;

/// TODO: Move to shared area for memory and disk cache.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Capacity {
    Unlimited,
    InBytes(usize),
}

impl Capacity {
    fn exceeds_capacity(&self, num_bytes: usize) -> bool {
        match *self {
            Capacity::Unlimited => false,
            Capacity::InBytes(capacity_in_bytes) => num_bytes > capacity_in_bytes,
        }
    }
}

pub(crate) struct DiskBackedLRUCache {
    store: Store<FileBackedDirectory>,
    lru: Mutex<LruCache>,
    num_items: AtomicUsize,
    num_bytes: AtomicU64,
    capacity: Capacity,
    cache_counters: &'static CacheMetrics,
    retry_files: Mutex<Vec<FileKey>>,
}

impl DiskBackedLRUCache {
    pub(crate) fn open_with_capacity(
        base_path: &Path,
        max_fd: usize,
        capacity: Capacity,
        cache_counters: &'static CacheMetrics,
    ) -> io::Result<Self> {
        let store = open_disk_store(base_path, max_fd)?;
        let mut lru = LruCache::unbounded();

        let mut entries = store.entries().collect::<Vec<_>>();
        entries.sort_by_key(|v| v.last_accessed);

        let mut total_bytes = 0;
        for entry in entries {
            // It's a theoretical possibility that a file with the same
            // key could appear twice as the `store.entries()` iterator is a
            // implementation detail of the directory.
            if lru.put(entry.key, ()).is_none() {
                total_bytes += entry.file_size;
            }
        }

        Ok(Self {
            store,
            num_items: AtomicUsize::new(lru.len()),
            num_bytes: AtomicU64::new(total_bytes),
            lru: Mutex::new(lru),
            capacity,
            cache_counters,
            retry_files: Mutex::new(vec![]),
        })
    }

    pub(crate) fn record_item(&self, num_bytes: u64) {
        self.num_items.fetch_add(1, Ordering::Relaxed);
        self.num_bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.cache_counters.in_cache_count.inc();
        self.cache_counters.in_cache_num_bytes.add(num_bytes as i64);
    }

    pub(crate) fn drop_item(&self, num_bytes: u64) {
        self.num_items.fetch_sub(1, Ordering::Relaxed);
        self.num_bytes.fetch_sub(num_bytes, Ordering::Relaxed);
        self.cache_counters.in_cache_count.dec();
        self.cache_counters.in_cache_num_bytes.sub(num_bytes as i64);
    }

    async fn try_get(
        &self,
        path: &Path,
        byte_range: Range<usize>,
    ) -> anyhow::Result<Option<OwnedBytes>> {
        let path = path.to_path_buf();
        let store = self.store.clone();
        let range = tokio::task::spawn_blocking(move || store.get_range(&path, byte_range)).await??;

        match range {
            None => Ok(None),
            Some(range) => Ok(Some(OwnedBytes::new(range)))
        }
    }

    async fn try_get_all(&self, path: &Path) -> anyhow::Result<Option<OwnedBytes>> {
        let path = path.to_path_buf();
        let store = self.store.clone();
        let range = tokio::task::spawn_blocking(move || store.get_all(&path)).await??;

        match range {
            None => Ok(None),
            Some(range) => Ok(Some(OwnedBytes::new(range)))
        }
    }

    async fn try_put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) -> anyhow::Result<()> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.put_range(&path, byte_range, &bytes))
            .await?
            .map_err(anyhow::Error::from)
    }

    async fn try_put_all(&self, path: PathBuf, bytes: OwnedBytes) -> anyhow::Result<()> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.put_all(&path, &bytes))
            .await?
            .map_err(anyhow::Error::from)
    }

    fn handle_cache_stats(&self, data: &Option<OwnedBytes>) {
        if let Some(buffer) = data {
            self.cache_counters.hits_num_items.inc();
            self.cache_counters.hits_num_bytes.inc_by(buffer.len() as u64);
        } else {
            self.cache_counters.misses_num_items.inc();
        }
    }

    fn calculate_file_to_evict(&self, num_bytes: usize) -> Vec<FileKey> {
        if self.capacity.exceeds_capacity(num_bytes) {
            // The value does not fit in the cache. We simply don't store it.
            warn!(
                capacity_in_bytes = ?self.capacity,
                len = num_bytes,
                "Downloaded a byte slice larger than the cache capacity."
            );
            return vec![];
        }

        let current_num_bytes = self.num_bytes.load(Ordering::Relaxed);

        let mut to_clear = vec![];
        let mut lru_lock = self.lru.lock();
        while self
            .capacity
            .exceeds_capacity(current_num_bytes as usize + num_bytes)
        {
            let maybe_file_metadata = lru_lock
                .peek_lru()
                .and_then(|(k, _)| self.store.get_metadata(*k));

            if let Some(metadata) = maybe_file_metadata {
                let time_delta = super::time_now() - Duration::from_secs(metadata.last_accessed);
                if time_delta < MIN_TIME_SINCE_LAST_ACCESS {
                    // Reset the lru state.
                    reset_lru_state(&mut lru_lock, to_clear);

                    // It is not worth doing an eviction.
                    return vec![];
                }
            }

            if let Some((key, _)) = lru_lock.pop_lru() {
                to_clear.push(key);
            } else {
                reset_lru_state(&mut lru_lock, to_clear);
                error!(
                    "Logical error. Even after removing all of the items in the cache the \
                     capacity is insufficient. This case is guarded against and should never \
                     happen."
                );
                return vec![];
            }
        }

        to_clear
    }

    async fn check_and_evict(&self, num_bytes: usize) -> bool {
        let to_clear = self.calculate_file_to_evict(num_bytes);
        if to_clear.is_empty() {
            return false;
        }

        let store = self.store.clone();

        let to_remove = {
            let mut old_files = self.retry_files.lock().clone();
            old_files.extend(to_clear);
            old_files
        };
        let to_remove_copy = to_remove.clone();

        let result = tokio::task::spawn_blocking(move || {
            let mut failed_files = vec![];
            for key in to_remove {
                if let Err(e) = store.remove_with_key(key) {
                    warn!(
                        error = ?e,
                        "IO Error: Failed to remove file from disk cache. A retry will happen on the next write.",
                    );
                    failed_files.push(key);
                };
            }

            failed_files
        }).await;

        match result {
            Ok(failed_files) => {
                let mut lock = self.retry_files.lock();
                (*lock) = failed_files;
            },
            Err(_) => {
                warn!("Failed to spawn executor task and retrieve result!");

                // Because we have no idea what the result was. We assume
                // everything failed and let it be cleared up on the next write.
                let mut lock = self.retry_files.lock();
                (*lock) = to_remove_copy;
            }
        }

        true
    }
}

#[async_trait]
impl Cache for DiskBackedLRUCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let result = self.try_get(path, byte_range).await;
        let data = convert_and_log_error(result, "get range").flatten();

        self.handle_cache_stats(&data);

        data
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let result = self.try_get_all(path).await;
        let data = convert_and_log_error(result, "get all").flatten();

        self.handle_cache_stats(&data);

        data
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        let num_bytes = bytes.len();

        let can_store = self.check_and_evict(num_bytes).await;

        if !can_store {
            return;
        }

        let result = self.try_put(path, byte_range, bytes).await;
        if convert_and_log_error(result, "put range").is_some() {
            self.record_item(num_bytes as u64);
        };
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        let num_bytes = bytes.len();

        let can_store = self.check_and_evict(num_bytes).await;

        if !can_store {
            return;
        }

        let result = self.try_put_all(path, bytes).await;
        if convert_and_log_error(result, "get all").is_some() {
            self.record_item(num_bytes as u64);
        };
    }
}

/// Resets the lru cache to the original state before popping off the given items.
fn reset_lru_state(existing_cache: &mut LruCache, popped_items: Vec<FileKey>) {
    let mut new_lru = LruCache::unbounded();

    for key in popped_items {
        new_lru.put(key, ());
    }

    while let Some((key, _)) = existing_cache.pop_lru() {
        new_lru.put(key, ());
    }

    *existing_cache = new_lru;
}

fn convert_and_log_error<T>(res: anyhow::Result<T>, op: &str) -> Option<T> {
    match res {
        Ok(data) => {
            Some(data)
        },
        Err(e) => {
            error!(
                error = ?e,
                "IO Error. Failed to access disk backed cache data during the {} operation.",
                op,
            );
            None
        }
    }
}