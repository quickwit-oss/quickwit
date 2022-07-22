use std::io;
use std::path::Path;
use parking_lot::Mutex;

use super::file::{FileEntry, FileKey};
use super::store::{Store, FileBackedDirectory, open_disk_store};


pub struct DiskBackedLRUCache {
    lru: Mutex<lru::LruCache<FileKey, ()>>,
    store: Store<FileBackedDirectory>
}

impl DiskBackedLRUCache {
    pub fn open(base_path: &Path, max_fd: usize) -> io::Result<Self> {
        let store = open_disk_store(base_path, max_fd)?;
        let mut lru = lru::LruCache::unbounded();

        let mut entries = store.entries().collect::<Vec<_>>();
        entries.sort_by_key(|v| v.last_accessed);

        for entry in entries {
            lru.put(entry.key, ());
        }

        Ok(Self {
            lru: Mutex::new(lru),
            store,
        })
    }
}