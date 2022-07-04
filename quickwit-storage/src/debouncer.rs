use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fnv::FnvHashMap;
use futures::future::{BoxFuture, Shared};
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
    cache: Mutex<FnvHashMap<K, Shared<BoxFuture<'static, V>>>>,
}

impl<K: Hash + Eq + Clone, V: Clone> AsyncDebouncer<K, V> {
    /// Creates a new AsyncCache with the given capacity.
    pub fn new() -> Self {
        AsyncDebouncer {
            cache: Mutex::new(FnvHashMap::default()),
        }
    }

    /// Instead of the future directly, a constructor to build the future is passed.
    /// In case there is already an existing Future for the passed key, the constructor is not
    /// used.
    pub async fn get_or_create<T, F>(&self, key: K, build_a_future: T) -> V
    where
        T: FnOnce() -> F,
        F: Future<Output = V> + Send + 'static,
    {
        // explicit scope to drop the lock
        let fut_opt = { self.cache.lock().unwrap().get(&key).cloned() };
        if let Some(future) = fut_opt {
            return future.await;
        }

        let fut = Box::pin(build_a_future()) as BoxFuture<'static, V>;
        let fut = fut.shared();
        self.cache.lock().unwrap().insert(key.clone(), fut.clone());
        let res = fut.await;

        self.cache.lock().unwrap().remove(&key);

        res
    }
}

pub(crate) struct DebouncedStorage<T> {
    // wrap both in Arc, because we store the Future on the cache, which has 'static lifetime
    // associated
    underlying: Arc<T>,
    slice_debouncer: Arc<AsyncDebouncer<(PathBuf, Range<usize>), StorageResult<OwnedBytes>>>,
}

impl<T: Storage> DebouncedStorage<T> {
    pub(crate) fn new(underlying: T) -> Self {
        Self {
            underlying: Arc::new(underlying),
            slice_debouncer: Arc::new(AsyncDebouncer::new()),
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

    use tempfile::TempDir;
    use tokio::fs::{self, File};
    use tokio::io::AsyncWriteExt;

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

        let cache: AsyncDebouncer<SliceAddress, Result<String, String>> = AsyncDebouncer::new();

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

        let cache: AsyncDebouncer<SliceAddress, Result<String, String>> = AsyncDebouncer::new();

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
