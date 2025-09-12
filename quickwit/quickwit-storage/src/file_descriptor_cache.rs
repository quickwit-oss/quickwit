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

use std::fs::File;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::OwnedBytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use ulid::Ulid;

use crate::metrics::CacheMetrics;

pub struct FileDescriptorCache {
    fd_cache: Mutex<lru::LruCache<Ulid, SplitFile>>,
    fd_semaphore: Arc<Semaphore>,
    fd_cache_metrics: CacheMetrics,
}

#[derive(Clone)]
pub struct SplitFile(Arc<SplitFileInner>);

struct SplitFileInner {
    num_bytes: u64,
    // Order matters here. We want file to be dropped (closed) before the semaphore.
    file: File,
    _fd_semaphore_guard: OwnedSemaphorePermit,
}

fn get_split_file_path(root_path: &Path, split_id: Ulid) -> PathBuf {
    let split_filename = quickwit_common::split_file(split_id);
    root_path.join(split_filename)
}

impl FileDescriptorCache {
    /// Creates a new file descriptor cache.
    /// `max_fd_limit` is the total number of file descriptors that can be open at the same time.
    /// `fd_cache_capacity` is the number of file descriptors that can be cached. It is required to
    /// be less than `max_fd_limit`.
    ///
    /// # Warning
    ///
    /// The file descriptor cache can be prone to deadlocks.
    /// Currently the risk is only avoided due to the split search concurrency limit.
    ///
    /// When setting the two limit, ensure the max_fd_limit is higher than the split search
    /// concurrency limit and that you have set some margin between the two, and also make sure
    /// the `max_fd_limit` is sufficient to avoid deadlocks.
    ///
    /// TODO It would be good to refactor this to enforce this with a bit of a refactoring.
    /// For instance, client could be forced to declare upfront the number of file descriptors they
    /// will need. In Quickwit however, one task is hitting one split at a time, so the risk is
    /// absent.
    fn new(
        max_fd_limit: NonZeroU32,
        fd_cache_capacity: NonZeroU32,
        fd_cache_metrics: CacheMetrics,
    ) -> FileDescriptorCache {
        assert!(max_fd_limit.get() > fd_cache_capacity.get());
        let fd_cache = Mutex::new(lru::LruCache::new(
            NonZeroUsize::new(fd_cache_capacity.get() as usize).unwrap(),
        ));
        let fd_semaphore = Arc::new(Semaphore::new(max_fd_limit.get() as usize));
        FileDescriptorCache {
            fd_cache,
            fd_semaphore,
            fd_cache_metrics,
        }
    }

    pub fn with_fd_cache_capacity(fd_cache_capacity: NonZeroU32) -> FileDescriptorCache {
        let max_fd_limit = (fd_cache_capacity.get() * 2)
            .clamp(fd_cache_capacity.get() + 100, fd_cache_capacity.get() + 200);
        Self::new(
            NonZeroU32::new(max_fd_limit).unwrap(),
            fd_cache_capacity,
            crate::STORAGE_METRICS.fd_cache_metrics.clone(),
        )
    }

    fn get_split_file(&self, split_id: Ulid) -> Option<SplitFile> {
        self.fd_cache.lock().unwrap().get(&split_id).cloned()
    }

    fn put_split_file(&self, split_id: Ulid, split_file: SplitFile) {
        let mut fd_cache_lock = self.fd_cache.lock().unwrap();
        fd_cache_lock.push(split_id, split_file);
        self.fd_cache_metrics
            .in_cache_count
            .set(fd_cache_lock.len() as i64);
    }

    /// Evicts the given list of split ids from the file descriptor cache.
    /// This method does NOT remove the actual files.
    pub fn evict_split_files(&self, split_ids: &[Ulid]) {
        let mut fd_cache_lock = self.fd_cache.lock().unwrap();
        for split_id in split_ids {
            fd_cache_lock.pop(split_id);
        }
        self.fd_cache_metrics
            .in_cache_count
            .set(fd_cache_lock.len() as i64);
        self.fd_cache_metrics
            .evict_num_items
            .inc_by(split_ids.len() as u64);
    }

    pub async fn get_or_open_split_file(
        &self,
        root_path: &Path,
        split_id: Ulid,
        num_bytes: u64,
    ) -> std::io::Result<SplitFile> {
        if let Some(split_file) = self.get_split_file(split_id) {
            self.fd_cache_metrics.hits_num_items.inc();
            return Ok(split_file);
        } else {
            self.fd_cache_metrics.misses_num_items.inc();
        }
        let split_path = get_split_file_path(root_path, split_id);
        let fd_semaphore_guard = Semaphore::acquire_owned(self.fd_semaphore.clone())
            .await
            .expect("fd_semaphore acquire failed. please report");
        let file: File = tokio::task::spawn_blocking(move || std::fs::File::open(split_path))
            .await
            .map_err(|join_error| {
                io::Error::other(format!("failed to open file: {join_error:?}"))
            })??;
        let split_file = SplitFile(Arc::new(SplitFileInner {
            num_bytes,
            file,
            _fd_semaphore_guard: fd_semaphore_guard,
        }));
        self.put_split_file(split_id, split_file.clone());
        Ok(split_file)
    }
}

impl SplitFile {
    pub async fn get_range(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        use std::os::unix::fs::FileExt;
        let file = self.clone();
        let buf = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::with_capacity(range.len());
            #[allow(clippy::uninit_vec)]
            unsafe {
                buf.set_len(range.len());
            }
            file.0.file.read_exact_at(&mut buf, range.start as u64)?;
            io::Result::Ok(buf)
        })
        .await
        .unwrap()?;
        Ok(OwnedBytes::new(buf))
    }

    pub async fn get_all(&self) -> io::Result<OwnedBytes> {
        self.get_range(0..self.0.num_bytes as usize).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tokio::fs;
    use ulid::Ulid;

    use super::FileDescriptorCache;
    use crate::metrics::CacheMetrics;

    #[tokio::test]
    async fn test_fd_cache_big_cache() {
        let cache_metrics = CacheMetrics::for_component("fdtest");
        let fd_cache = FileDescriptorCache::new(
            NonZeroU32::new(20).unwrap(),
            NonZeroU32::new(10).unwrap(),
            cache_metrics.clone(),
        );
        let tempdir = tempfile::tempdir().unwrap();
        let split_ids: Vec<Ulid> = std::iter::repeat_with(Ulid::new).take(100).collect();
        for &split_id in &split_ids {
            let split_filepath = super::get_split_file_path(tempdir.path(), split_id);
            let content = split_id.to_string();
            assert_eq!(content.len(), 26);
            fs::write(split_filepath, content.as_bytes()).await.unwrap();
        }
        for &split_id in &split_ids[0..10] {
            fd_cache
                .get_or_open_split_file(tempdir.path(), split_id, 26)
                .await
                .unwrap();
        }
        for &split_id in &split_ids[0..10] {
            fd_cache
                .get_or_open_split_file(tempdir.path(), split_id, 26)
                .await
                .unwrap();
        }
        for &split_id in &split_ids[0..10] {
            fd_cache
                .get_or_open_split_file(tempdir.path(), split_id, 26)
                .await
                .unwrap();
        }
        assert_eq!(cache_metrics.in_cache_count.get(), 10);
        assert_eq!(cache_metrics.hits_num_items.get(), 20);
        assert_eq!(cache_metrics.misses_num_items.get(), 10);
    }

    // This mimics Quickwit's workload where the fd cache is much smaller than the number of
    // splits. Each search will read from the same split file, and the cache will help avoid
    // opening the file several times.
    #[tokio::test]
    async fn test_fd_cache_small_cache() {
        let cache_metrics = CacheMetrics::for_component("fdtest2");
        let fd_cache = FileDescriptorCache::new(
            NonZeroU32::new(20).unwrap(),
            NonZeroU32::new(10).unwrap(),
            cache_metrics.clone(),
        );
        let tempdir = tempfile::tempdir().unwrap();
        let split_ids: Vec<Ulid> = std::iter::repeat_with(Ulid::new).take(100).collect();
        for &split_id in &split_ids {
            let split_filepath = super::get_split_file_path(tempdir.path(), split_id);
            let content = split_id.to_string();
            assert_eq!(content.len(), 26);
            fs::write(split_filepath, content.as_bytes()).await.unwrap();
        }
        for &split_id in &split_ids[0..100] {
            for _ in 0..10 {
                fd_cache
                    .get_or_open_split_file(tempdir.path(), split_id, 26)
                    .await
                    .unwrap();
            }
        }
        assert_eq!(cache_metrics.in_cache_count.get(), 10);
        assert_eq!(cache_metrics.hits_num_items.get(), 100 * 9);
        assert_eq!(cache_metrics.misses_num_items.get(), 100);
    }

    #[tokio::test]
    async fn test_split_file() {
        let fd_cache = FileDescriptorCache::with_fd_cache_capacity(NonZeroU32::new(20).unwrap());
        let tempdir = tempfile::tempdir().unwrap();
        let split_id: Ulid = Ulid::new();
        let split_filepath = super::get_split_file_path(tempdir.path(), split_id);
        let content = split_id.to_string();
        assert_eq!(content.len(), 26);
        fs::write(split_filepath, content.as_bytes()).await.unwrap();
        let split_file = fd_cache
            .get_or_open_split_file(tempdir.path(), split_id, 26)
            .await
            .unwrap();
        {
            let bytes = split_file.get_all().await.unwrap();
            assert_eq!(bytes.as_slice(), content.as_bytes());
        }
        {
            let bytes = split_file.get_range(1..3).await.unwrap();
            assert_eq!(bytes.as_slice(), &content.as_bytes()[1..3]);
        }
    }
}
