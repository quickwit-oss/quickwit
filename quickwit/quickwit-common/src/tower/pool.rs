// Copyright (C) 2023 Quickwit, Inc.
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

use std::any::TypeId;
use std::borrow::Borrow;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use futures::{Stream, StreamExt};

use super::Change;

/// A pool of `V` values identified by `K` keys. The pool can be updated manually by calling the
/// `add/remove` methods or by listening to a stream of changes.
pub struct Pool<K, V> {
    pool: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> fmt::Debug for Pool<K, V>
where
    K: 'static,
    V: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pool<{:?}, {:?}>", TypeId::of::<K>(), TypeId::of::<V>())
    }
}

impl<K, V> Clone for Pool<K, V> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<K, V> Default for Pool<K, V>
where K: Eq + PartialEq + Hash
{
    fn default() -> Self {
        Self {
            pool: Arc::new(RwLock::new(HashMap::default())),
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
            .iter()
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Returns all the values in the pool.
    pub fn values(&self) -> Vec<V> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .iter()
            .map(|(_, value)| value.clone())
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
    pub fn find(&self, func: impl Fn(&K, &V) -> bool) -> Option<(K, V)> {
        self.pool
            .read()
            .expect("lock should not be poisoned")
            .iter()
            .find(|(key, value)| func(key, value))
            .map(|(key, value)| (key.clone(), value.clone()))
    }

    /// Adds a value to the pool.
    pub fn insert(&self, key: K, service: V) {
        self.pool
            .write()
            .expect("lock should not be poisoned")
            .insert(key, service);
    }

    /// Removes a value from the pool.
    pub fn remove(&self, key: &K) {
        self.pool
            .write()
            .expect("lock should not be poisoned")
            .remove(key);
    }
}

impl<K, V> FromIterator<(K, V)> for Pool<K, V>
where K: Eq + PartialEq + Hash
{
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = (K, V)> {
        Self {
            pool: Arc::new(RwLock::new(HashMap::from_iter(iter))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

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
