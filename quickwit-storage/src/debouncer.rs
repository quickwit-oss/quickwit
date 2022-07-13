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

use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fnv::FnvHashMap;
use futures::future::{BoxFuture, WeakShared};
use futures::{Future, FutureExt};
use tantivy::directory::OwnedBytes;

use crate::{Storage, StorageResult};

/// The AsyncDebouncer debounces inflight Futures, so that concurrent async request to the same data
/// source can be deduplicated.
///
/// Since we pass the Future potentially to multiple consumer, everything needs to be cloneable. The
/// data and the future. This is reflected on the generic type bounds for the value V: Clone.
///
/// Since most Futures return an Result<V, Error>, this also encompasses the error.
pub struct AsyncDebouncer<K, V: Clone> {
    cache: Mutex<FnvHashMap<K, WeakShared<BoxFuture<'static, V>>>>,
}

impl<K, V: Clone> Default for AsyncDebouncer<K, V> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl<K: Hash + Eq + Clone, V: Clone> AsyncDebouncer<K, V> {
    /// Returns the number of inflight futures.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Cleanup
    /// In case there is already an existing Future for the passed key, the constructor is not
    /// used.
    pub fn cleanup(&self) {
        let mut guard = self.cache.lock().unwrap();
        guard.retain(|_, v| v.upgrade().is_some());
    }

    /// Instead of the future directly, a constructor to build the future is passed.
    /// In case there is already an existing Future for the passed key, the constructor is not
    /// used.
    pub async fn get_or_create<T, F>(&self, key: K, build_a_future: T) -> V
    where
        T: FnOnce() -> F,
        F: Future<Output = V> + Send + 'static,
    {
        self.cleanup();

        // explicit scope to drop the lock
        let weak_fut_opt = { self.cache.lock().unwrap().get(&key).cloned() };
        if let Some(weak_future) = weak_fut_opt {
            if let Some(future) = weak_future.upgrade() {
                return future.await;
            }
        }

        let fut = Box::pin(build_a_future()) as BoxFuture<'static, V>;
        let fut = fut.shared();
        self.cache.lock().unwrap().insert(
            key.clone(),
            fut.clone().downgrade().expect(
                "future has been dropped, but that shouldn't happen since it's still in scope",
            ),
        );
        let res = fut.await;

        self.cache.lock().unwrap().remove(&key);

        res
    }
}

type DebouncerKey = (PathBuf, Range<usize>);

pub(crate) struct DebouncedStorage<T> {
    // wrap both in Arc, because the Future is stored in the cache, which has 'static lifetime
    // associated
    underlying: Arc<T>,
    slice_debouncer: Arc<AsyncDebouncer<DebouncerKey, StorageResult<OwnedBytes>>>,
}

impl<T: Storage> DebouncedStorage<T> {
    pub(crate) fn new(underlying: T) -> Self {
        Self {
            underlying: Arc::new(underlying),
            slice_debouncer: Arc::new(AsyncDebouncer::default()),
        }
    }
}

#[async_trait]
impl<T: Storage> Storage for DebouncedStorage<T> {
    async fn check(&self) -> anyhow::Result<()> {
        self.underlying.check().await
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        self.underlying.put(path, payload).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        self.underlying.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let (debouncer, underlying) = (self.slice_debouncer.clone(), self.underlying.clone());
        let key = (path.to_owned(), range);
        debouncer
            .get_or_create(key.clone(), || async move {
                underlying.get_slice(&key.0, key.1).await
            })
            .await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.underlying.delete(path).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let (debouncer, underlying) = (self.slice_debouncer.clone(), self.underlying.clone());
        let key = (path.to_owned(), 0..usize::MAX);
        debouncer
            .get_or_create(
                key.clone(),
                || async move { underlying.get_all(&key.0).await },
            )
            .await
    }

    fn uri(&self) -> String {
        self.underlying.uri()
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.underlying.file_num_bytes(path).await
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Range;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use once_cell::sync::OnceCell;
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

    #[derive(Hash, Debug, Clone, PartialEq, Eq)]
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
        let val = cache
            .get_or_create(addr1.clone(), || {
                let test_filepath1 = test_filepath1.clone();
                async move {
                    COUNT.fetch_add(1, Ordering::SeqCst);
                    let contents = Box::pin(fs::read_to_string(test_filepath1.as_ref().clone()))
                        .await
                        // to string, so that the error is cloneable
                        .map_err(|err| err.to_string())?;

                    Ok(contents)
                }
            })
            .await
            .unwrap();

        println!("{}", val);

        // Load via function
        let val = cache
            .get_or_create(addr1, || {
                load_via_fn(test_filepath1.as_ref().clone(), &COUNT)
            })
            .await
            .unwrap();

        println!("{}", val);

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
            println!("a");
            // tokio::time::sleep(Duration::from_secs(1)).await;
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
        println!("b");
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("c");
        "blub".to_string()
    }

    pub static GLOBAL_DEBOUNCER: once_cell::sync::OnceCell<AsyncDebouncer<String, String>> =
        OnceCell::new();
    pub fn get_global_debouncer() -> &'static AsyncDebouncer<String, String> {
        GLOBAL_DEBOUNCER.get_or_init(AsyncDebouncer::default)
    }

    #[tokio::test]
    async fn test_cancellation_task() {
        let load = || async { load_via_fn2().await };

        let handle = task::spawn(async move {
            get_global_debouncer()
                .get_or_create("key1".to_owned(), load)
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        // This will cause  the Future to be cancelled, so it will not be polled anymore.
        // That also means the remove in the cache is not called, which is awaiting the future
        handle.abort();

        tokio::time::sleep(Duration::from_secs(1)).await;
        // The task still hangs unfinished
        assert_eq!(get_global_debouncer().len(), 1);

        // The next get clears
        get_global_debouncer()
            .get_or_create("key1".to_owned(), load)
            .await;

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(get_global_debouncer().len(), 0);
    }

    async fn load_via_fn(path: PathBuf, cnt: &AtomicU32) -> Result<String, String> {
        cnt.fetch_add(1, Ordering::SeqCst);
        let contents = Box::pin(fs::read_to_string(path))
            .await
            .map_err(|err| err.to_string())?;
        // sleep so the requests can be reproducible debounced
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(contents)
    }
}
