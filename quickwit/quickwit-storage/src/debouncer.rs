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

use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::HashMap;
use futures::future::{BoxFuture, Shared, WeakShared};
use futures::{Future, FutureExt};

/// The AsyncDebouncer debounces inflight Futures, so that concurrent async request to the same data
/// source can be deduplicated.
///
/// Since we pass the Future potentially to multiple consumer, everything needs to be cloneable. The
/// data and the future. This is reflected on the generic type bounds for the value V: Clone.
///
/// Since most Futures return an Result<V, Error>, this also encompasses the error.
pub struct AsyncDebouncer<K, V: Clone> {
    cache: Mutex<HashMap<K, WeakShared<BoxFuture<'static, V>>>>,
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

impl<K: Hash + Eq + Clone, V: Clone> AsyncDebouncer<K, V> {
    /// Returns the number of entries in the debouncing cache.
    ///
    /// This is always greater than the number of inflight futures, and smaller
    /// than that number + CLEANUP_INTERVAL + 1.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Returns the inflight future for `key`, deduplicating concurrent calls: if a future for
    /// `key` is already inflight, all callers await that same shared future; otherwise
    /// `build_a_future_fast` is invoked to create one.
    ///
    /// # Hidden contract
    ///
    /// `build_a_future_fast` is invoked **while the internal cache lock is held**. It must
    /// therefore:
    /// - be cheap — it only *constructs* the future, it must not perform blocking work (the future
    ///   itself is awaited later, outside the lock);
    /// - never re-enter this `AsyncDebouncer` (e.g. call `get_or_create` on the same instance),
    ///   which would deadlock, since the cache lock is a non-reentrant [`std::sync::Mutex`].
    ///
    /// Holding the lock across both the lookup and the insert is deliberate: it makes the
    /// lookup-then-insert atomic, so two concurrent callers for the same key cannot both build
    /// and race to insert. The lock is always released before the future is awaited.
    pub(crate) fn get_or_create<T, F>(
        &self,
        key: K,
        build_a_future_fast: T,
    ) -> Shared<BoxFuture<'static, V>>
    where
        T: FnOnce() -> F,
        F: Future<Output = V> + Send + 'static,
    {
        // `created` distinguishes the caller that actually built the entry (and is therefore
        // responsible for removing it once the future resolves) from callers that merely joined
        // an already-inflight future.
        let mut debouncing_cache_guard = self.cache.lock().unwrap();

        // A stale entry (left behind by a cancelled future) fails to upgrade and is simply
        // overwritten by the insert below.
        if let Some(fut) = debouncing_cache_guard
            .get(&key)
            .and_then(WeakShared::upgrade)
        {
            return fut;
        }

        // Note that we are call build_a_future_fast WHILE holding the cache lock.
        // For this reason, it is crucial to only call this function with a cheap and simple future
        // builder.
        let fut = Box::pin(build_a_future_fast()) as BoxFuture<'static, V>;
        let fut = fut.shared();
        let weak_fut = fut.clone().downgrade().unwrap();
        debouncing_cache_guard.insert(key.clone(), weak_fut);

        // Amortized garbage collection. Running a full scan on every call would make
        // each call O(n); instead we scan once every `CLEANUP_INTERVAL` inserts.
        let num_inserts = self.inserts_since_cleanup.fetch_add(1, Ordering::Relaxed);
        if num_inserts >= CLEANUP_INTERVAL {
            self.inserts_since_cleanup.store(0, Ordering::Relaxed);
            debouncing_cache_guard.retain(|_, weak_future| weak_future.upgrade().is_some());
        }

        fut
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
                    let contents = Box::pin(fs::read_to_string(test_filepath1.as_ref().clone()))
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
        // This will cause  the Future to be cancelled, so it will not be polled anymore.
        // That also means the remove in the cache is not called, which is awaiting the future
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
