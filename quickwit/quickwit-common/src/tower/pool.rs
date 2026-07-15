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

use std::any::TypeId;
use std::borrow::Borrow;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use futures::{Stream, StreamExt};
use tokio::sync::watch;

use super::Change;

/// A pool of `V` values identified by `K` keys. The pool can be updated manually by calling the
/// `add/remove` methods or by listening to a stream of changes.
pub struct Pool<K, V> {
    pool: Arc<RwLock<HashMap<K, V>>>,
    /// Publishes a notification after each mutation.
    changes_tx: watch::Sender<()>,
    /// Receives pool change notifications.
    pub changes_rx: watch::Receiver<()>,
}

impl<K, V> fmt::Debug for Pool<K, V>
where
    K: 'static,
    V: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pool<{:?}, {:?}>", TypeId::of::<K>(), TypeId::of::<V>())
    }
}

impl<K, V> Clone for Pool<K, V> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            changes_tx: self.changes_tx.clone(),
            changes_rx: self.changes_rx.clone(),
        }
    }
}

impl<K, V> Default for Pool<K, V>
where K: Eq + PartialEq + Hash
{
    fn default() -> Self {
        let (changes_tx, changes_rx) = watch::channel(());
        Self {
            pool: Arc::new(RwLock::new(HashMap::default())),
            changes_tx,
            changes_rx,
        }
    }
}

impl<K, V> Pool<K, V>
where
    K: Eq + PartialEq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Listens for the changes emitted by the stream and updates the pool accordingly.
    pub fn listen_for_changes(
        &self,
        change_stream: impl Stream<Item = Change<K, V>> + Send + 'static,
    ) {
        let pool = self.clone();
        let future = async move {
            change_stream
                .for_each(|change| async {
                    match change {
                        Change::Insert(key, service) => {
                            pool.insert(key, service);
                        }
                        Change::Remove(key) => {
                            pool.remove(&key);
                        }
                    }
                })
                .await;
        };
        tokio::spawn(future);
    }

    fn notify_change(&self) {
        self.changes_tx.send_replace(());
    }

    /// Returns whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .is_empty()
    }

    /// Returns the number of values in the pool.
    pub fn len(&self) -> usize {
        self.pool.read().expect("lock should not be poisoned").len()
    }

    /// Returns all the keys in the pool.
    pub fn keys(&self) -> Vec<K> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Returns all the key-value pairs in the pool.
    pub fn keys_values(&self) -> Vec<(K, V)> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Returns all the values in the pool.
    pub fn values(&self) -> Vec<V> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Returns all the key-value pairs in the pool.
    pub fn pairs(&self) -> Vec<(K, V)> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Returns the value associated with the given key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .contains_key(key)
    }

    /// Returns the value associated with the given key.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .get(key)
            .cloned()
    }

    /// Finds a key in the pool that satisfies the given predicate.
    pub fn find(&self, predicate_fn: impl Fn(&K, &V) -> bool) -> Option<(K, V)> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .iter()
            .find(|(key, value)| predicate_fn(key, value))
            .map(|(key, value)| (key.clone(), value.clone()))
    }

    /// Adds a value to the pool.
    pub fn insert(&self, key: K, service: V) {
        self.pool
            .write()
            .expect("lock should not be poisoned")
            .insert(key, service);
        self.notify_change();
    }

    /// Removes a value from the pool.
    fn remove(&self, key: &K) {
        let removed = self
            .pool
            .write()
            .expect("lock should not be poisoned")
            .remove(key);
        if removed.is_some() {
            self.notify_change();
        }
    }
}

impl<K, V> FromIterator<(K, V)> for Pool<K, V>
where K: Eq + PartialEq + Hash
{
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = (K, V)> {
        let (changes_tx, changes_rx) = watch::channel(());
        Self {
            pool: Arc::new(RwLock::new(HashMap::from_iter(iter))),
            changes_tx,
            changes_rx,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    #[tokio::test]
    async fn test_pool_change_notifications() {
        let pool = Pool::default();
        let mut changes_rx = pool.changes_rx.clone();

        pool.insert(1, 11);
        changes_rx.changed().await.unwrap();
        assert_eq!(pool.get(&1), Some(11));

        pool.insert(1, 12);
        changes_rx.changed().await.unwrap();
        assert_eq!(pool.get(&1), Some(12));

        pool.remove(&2);

        pool.remove(&1);
        changes_rx.changed().await.unwrap();
        assert!(!pool.contains_key(&1));
    }

    #[tokio::test]
    async fn test_pool() {
        let (change_stream_tx, change_stream_rx) = tokio::sync::mpsc::channel(10);
        let change_stream = ReceiverStream::new(change_stream_rx);

        let pool = Pool::default();
        pool.listen_for_changes(change_stream);

        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);

        change_stream_tx.send(Change::Insert(1, 11)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(!pool.is_empty());
        assert_eq!(pool.len(), 1);

        assert!(pool.contains_key(&1));
        assert_eq!(pool.get(&1), Some(11));

        change_stream_tx.send(Change::Insert(2, 21)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.get(&2), Some(21));

        assert_eq!(pool.find(|k, _| *k == 1), Some((1, 11)));

        let mut pairs = pool.pairs();
        pairs.sort();

        assert_eq!(pairs, vec![(1, 11), (2, 21)]);

        change_stream_tx.send(Change::Insert(1, 12)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(pool.get(&1), Some(12));

        change_stream_tx.send(Change::Remove(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(pool.len(), 1);

        change_stream_tx.send(Change::Remove(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(pool.is_empty());
    }
}
