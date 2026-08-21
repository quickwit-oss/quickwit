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

use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use ahash::HashMap;
use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tantivy::directory::OwnedBytes;
use tokio::io::AsyncRead;
use tokio::sync::OnceCell;

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, Storage, StorageGetSlice, StorageResult};

/// The AsyncDebouncer debounces inflight Futures, so that concurrent async request to the same data
/// source can be deduplicated.
///
/// Concurrent consumers receive clones of the initialized value. Since most futures return a
/// `Result<V, Error>`, the clone requirement also applies to the error.
pub struct AsyncDebouncer<K, V: Clone> {
    cache: Mutex<HashMap<K, Weak<OnceCell<V>>>>,
    /// Number of inserts performed since the last full garbage-collection scan.
    ///
    /// Used to amortize the cost of reclaiming entries left behind by cancelled futures (see
    /// `get_or_create`).
    inserts_since_cleanup: AtomicUsize,
}

impl<K, V: Clone> Default for AsyncDebouncer<K, V> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            inserts_since_cleanup: AtomicUsize::new(0),
        }
    }
}

/// Number of inserts between two full garbage-collection scans of the cache.
///
/// Cancelled `get_or_create` futures leave a stale (non-upgradeable) entry behind. A stale entry
/// is harmless on its own — the next lookup of the same key fails to upgrade and overwrites it —
/// so we only need a periodic sweep to reclaim entries whose key is never accessed again.
const CLEANUP_INTERVAL: usize = 1_024;

impl<K: Hash + Eq, V: Clone + Send + Sync> AsyncDebouncer<K, V> {
    /// Returns the number of entries in the debouncing cache.
    ///
    /// This is always greater than the number of inflight futures, and smaller
    /// than that number + CLEANUP_INTERVAL + 1.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Returns the inflight future for `key`, deduplicating concurrent calls: if a future for
    /// `key` is already inflight, all callers await that same operation; otherwise
    /// `build_future` is invoked to create one.
    ///
    /// The lookup and insertion happen synchronously while the cache lock is held, but the future
    /// builder is invoked later, outside the lock. [`OnceCell`] ensures only one caller initializes
    /// a key. If that caller is cancelled, another waiter can take over initialization.
    fn get_or_create<'a, T, F>(
        &'a self,
        key: K,
        build_future: T,
    ) -> impl Future<Output = V> + Send + 'a
    where
        T: FnOnce() -> F + Send + 'a,
        F: Future<Output = V> + Send + 'a,
    {
        let mut debouncing_cache_guard = self.cache.lock().unwrap();

        let cell = if let Some(cell) = debouncing_cache_guard.get(&key).and_then(Weak::upgrade) {
            cell
        } else {
            let cell = Arc::new(OnceCell::new());
            debouncing_cache_guard.insert(key, Arc::downgrade(&cell));

            // Amortized garbage collection. Running a full scan on every call would make each
            // call O(n); instead we scan once every `CLEANUP_INTERVAL` inserts.
            let num_inserts = self.inserts_since_cleanup.fetch_add(1, Ordering::Relaxed);
            if num_inserts >= CLEANUP_INTERVAL {
                self.inserts_since_cleanup.store(0, Ordering::Relaxed);
                debouncing_cache_guard.retain(|_, weak_cell| weak_cell.upgrade().is_some());
            }
            cell
        };
        drop(debouncing_cache_guard);

        async move { cell.get_or_init(build_future).await.clone() }
    }
}

type DebouncerKey = (PathBuf, Range<usize>);

/// Just to keep in mind there is a race condition on debouncing, when combined with delete
///
/// All on the same key
/// start get R1
/// start delete R2
/// end delete R2
/// start get R3
/// end get R1
/// end get R3
///
/// ==> R3 would return the cached result, although the resource has been deleted.
pub(crate) struct DebouncedStorage<T> {
    storage: T,
    slice_debouncer: AsyncDebouncer<DebouncerKey, StorageResult<OwnedBytes>>,
}

impl<T> fmt::Debug for DebouncedStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DebouncedStorage").finish()
    }
}

impl<T: StorageGetSlice> DebouncedStorage<T> {
    pub(crate) fn new(storage: T) -> Self {
        Self {
            storage,
            slice_debouncer: AsyncDebouncer::default(),
        }
    }
}

#[async_trait]
impl<T: StorageGetSlice> Storage for DebouncedStorage<T> {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        self.storage.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_slice_unboxed(path, range).await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // Getting a stream bypasses the debouncer
        self.storage.get_slice_stream(path, range).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.storage.bulk_delete(paths).await
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.storage.list(prefix)
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let key = (path.to_owned(), 0..usize::MAX);
        self.slice_debouncer
            .get_or_create(key.clone(), || self.storage.get_all(&key.0))
            .await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }
}

impl<T: StorageGetSlice> StorageGetSlice for DebouncedStorage<T> {
    async fn get_slice_unboxed(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        let key = (path.to_owned(), range);
        self.slice_debouncer
            .get_or_create(key.clone(), || {
                self.storage.get_slice_unboxed(&key.0, key.1)
            })
            .await
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Range;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::fs::{self, File};
    use tokio::io::AsyncWriteExt;
    use tokio::task;

    use super::*;

    #[test]
    fn test_sync_and_send() {
        fn is_sync<T: Sync>() {}
        fn is_send<T: Send>() {}
        is_sync::<AsyncDebouncer<String, Result<String, String>>>();
        is_send::<AsyncDebouncer<String, Result<String, String>>>();
    }

    #[derive(Hash, Clone, Debug, Eq, PartialEq)]
    pub struct SliceAddress {
        pub path: PathBuf,
        pub byte_range: Range<usize>,
    }

    async fn get_test_file(temp_dir: &TempDir) -> Arc<PathBuf> {
        let test_filepath1 = Arc::new(temp_dir.path().join("f1"));

        let mut file1 = File::create(test_filepath1.as_ref()).await.unwrap();
        file1.write_all("nice cache dude".as_bytes()).await.unwrap();
        test_filepath1
    }

    #[tokio::test]
    async fn test_async_slice_cache() {
        // test data

        let temp_dir = tempfile::tempdir().unwrap();
        let test_filepath1 = get_test_file(&temp_dir).await;

        let cache: AsyncDebouncer<SliceAddress, Result<String, String>> = AsyncDebouncer::default();

        let addr1 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..20,
        };

        static COUNT: AtomicU32 = AtomicU32::new(0);

        // Load via closure
        let _val = cache
            .get_or_create(addr1.clone(), || {
                let test_filepath1 = test_filepath1.clone();
                async move {
                    COUNT.fetch_add(1, Ordering::SeqCst);
                    let contents = fs::read_to_string(test_filepath1.as_ref().clone())
                        .await
                        // to string, so that the error is cloneable
                        .map_err(|err| err.to_string())?;

                    Ok(contents)
                }
            })
            .await
            .unwrap();

        // Load via function
        let _val = cache
            .get_or_create(addr1, || {
                load_via_fn(test_filepath1.as_ref().clone(), &COUNT)
            })
            .await
            .unwrap();

        assert_eq!(COUNT.load(Ordering::SeqCst), 2);

        // Load via function, new entry
        let addr2 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..30,
        };

        let _val = cache
            .get_or_create(addr2.to_owned(), || {
                load_via_fn(test_filepath1.as_ref().clone(), &COUNT)
            })
            .await
            .unwrap();

        assert_eq!(COUNT.load(Ordering::SeqCst), 3);

        let load = || load_via_fn(test_filepath1.as_ref().clone(), &COUNT);

        let handles = vec![
            cache.get_or_create(addr2.to_owned(), load),
            cache.get_or_create(addr2.to_owned(), load),
        ];

        futures::future::join_all(handles).await;

        // Count is only increased by one, because of debouncing
        assert_eq!(COUNT.load(Ordering::SeqCst), 4);

        // Quadruple debouncing
        let handles = vec![
            cache.get_or_create(addr2.to_owned(), load),
            cache.get_or_create(addr2.to_owned(), load),
            cache.get_or_create(addr2.to_owned(), load),
            cache.get_or_create(addr2.to_owned(), load),
        ];
        futures::future::join_all(handles).await;

        // Count is only increased by one, because of debouncing
        assert_eq!(COUNT.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_debounce() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_filepath1 = get_test_file(&temp_dir).await;

        let cache: AsyncDebouncer<SliceAddress, Result<String, String>> = AsyncDebouncer::default();

        let addr2 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..20,
        };
        static COUNT: AtomicU32 = AtomicU32::new(0);

        let load = || load_via_fn(test_filepath1.as_ref().clone(), &COUNT);

        let handles = vec![
            cache.get_or_create(addr2.to_owned(), load),
            cache.get_or_create(addr2.to_owned(), load),
        ];

        futures::future::join_all(handles).await;

        // Count is only increased by one, because of debouncing
        assert_eq!(COUNT.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cancellation_future() {
        use tokio::time::timeout;
        let cache: AsyncDebouncer<String, Result<String, String>> = AsyncDebouncer::default();

        let load = || async {
            timeout(Duration::from_millis(10), load_via_fn2())
                .await
                .map_err(|err| err.to_string())
        };

        cache
            .get_or_create("key1".to_owned(), load)
            .await
            .unwrap_err();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let val = cache.get_or_create("key1".to_owned(), load).await;
        assert!(val.is_err());
    }

    async fn load_via_fn2() -> String {
        tokio::time::sleep(Duration::from_millis(500)).await;
        "blub".to_string()
    }

    #[tokio::test]
    async fn test_cancellation_task() {
        let debouncer = Arc::new(AsyncDebouncer::default());
        let load = || async { load_via_fn2().await };

        let debouncer_clone = debouncer.clone();
        let handle =
            task::spawn(
                async move { debouncer_clone.get_or_create("key0".to_owned(), load).await },
            );
        tokio::time::sleep(Duration::from_millis(10)).await;
        // This cancels initialization and leaves a stale weak entry until the next lookup.
        handle.abort();

        tokio::time::sleep(Duration::from_secs(1)).await;
        // The task still hangs unfinished
        assert_eq!(debouncer.len(), 1);

        // The next get clears
        debouncer.get_or_create("key0".to_owned(), load).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        // not cleaned up yet.
        assert_eq!(debouncer.len(), 1);
        for i in 0..2 * CLEANUP_INTERVAL {
            let fut = debouncer.get_or_create(format!("key{}", i), load);
            drop(fut);
            assert!(
                debouncer.len() <= CLEANUP_INTERVAL + 1,
                "{}",
                debouncer.len()
            );
        }
    }

    #[tokio::test]
    async fn test_waiter_takes_over_cancelled_initialization() {
        let debouncer = Arc::new(AsyncDebouncer::default());
        let initialization_started = Arc::new(tokio::sync::Notify::new());

        let (first_constructed_tx, first_constructed_rx) = tokio::sync::oneshot::channel();
        let first_debouncer = debouncer.clone();
        let first_started = initialization_started.clone();
        let first_handle = task::spawn(async move {
            let future = first_debouncer.get_or_create("key".to_string(), || async move {
                first_started.notify_one();
                std::future::pending::<String>().await
            });
            let _ = first_constructed_tx.send(());
            future.await
        });
        first_constructed_rx.await.unwrap();
        initialization_started.notified().await;

        let (waiter_constructed_tx, waiter_constructed_rx) = tokio::sync::oneshot::channel();
        let waiter_debouncer = debouncer.clone();
        let waiter_handle = task::spawn(async move {
            let future = waiter_debouncer
                .get_or_create("key".to_string(), || async { "completed".to_string() });
            let _ = waiter_constructed_tx.send(());
            future.await
        });
        waiter_constructed_rx.await.unwrap();

        first_handle.abort();
        assert!(first_handle.await.unwrap_err().is_cancelled());
        let value = tokio::time::timeout(Duration::from_secs(1), waiter_handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, "completed");
    }

    async fn load_via_fn(path: PathBuf, cnt: &AtomicU32) -> Result<String, String> {
        cnt.fetch_add(1, Ordering::SeqCst);
        let contents = fs::read_to_string(path)
            .await
            .map_err(|err| err.to_string())?;
        // sleep so the requests can be reproducible debounced
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(contents)
    }
}
