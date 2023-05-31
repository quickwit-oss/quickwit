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
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tokio::sync::RwLock;

use super::Change;

/// A pool of `V` values identified by `K` keys. The pool can be updated manually by calling the
/// `add/remove` methods or by listening to a stream of changes.
pub struct Pool<K, V> {
    inner: Arc<RwLock<InnerPool<K, V>>>,
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
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> Default for Pool<K, V> {
    fn default() -> Self {
        let inner = InnerPool {
            map: HashMap::new(),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

struct InnerPool<K, V> {
    map: HashMap<K, V>,
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
                            pool.insert(key, service).await;
                        }
                        Change::Remove(key) => {
                            pool.remove(&key).await;
                        }
                    }
                })
                .await;
        };
        tokio::spawn(future);
    }

    /// Returns whether the pool is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.map.is_empty()
    }

    /// Returns the number of values in the pool.
    pub async fn len(&self) -> usize {
        self.inner.read().await.map.len()
    }

    /// Returns all the key-value pairs in the pool.
    pub async fn all(&self) -> Vec<(K, V)> {
        self.inner
            .read()
            .await
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Returns the value associated with the given key.
    pub async fn get<Q>(&self, key: &Q) -> Option<V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.inner.read().await.map.get(key).cloned()
    }

    /// Finds a key in the pool that satisfies the given predicate.
    pub async fn find(&self, func: impl Fn(&K) -> bool) -> Option<K> {
        self.inner
            .read()
            .await
            .map
            .keys()
            .find(|k| func(k))
            .cloned()
    }

    /// Adds a value to the pool.
    pub async fn insert(&self, key: K, service: V) {
        self.inner.write().await.map.insert(key, service);
    }

    /// Removes a value from the pool.
    pub async fn remove(&self, key: &K) {
        self.inner.write().await.map.remove(key);
    }
}

impl<K, V> FromIterator<(K, V)> for Pool<K, V>
where K: Eq + PartialEq + Hash
{
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = (K, V)> {
        let key_values = HashMap::from_iter(iter);
        let inner = InnerPool { map: key_values };

        Self {
            inner: Arc::new(RwLock::new(inner)),
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
        assert!(pool.is_empty().await);
        assert_eq!(pool.len().await, 0);

        change_stream_tx.send(Change::Insert(1, 11)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!pool.is_empty().await);
        assert_eq!(pool.len().await, 1);
        assert_eq!(pool.get(&1).await, Some(11));

        change_stream_tx.send(Change::Insert(2, 21)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.len().await, 2);
        assert_eq!(pool.get(&2).await, Some(21));

        assert_eq!(pool.find(|k| *k == 1).await, Some(1));

        let mut all_nodes = pool.all().await;
        all_nodes.sort();
        assert_eq!(all_nodes, vec![(1, 11), (2, 21)]);

        change_stream_tx.send(Change::Insert(1, 12)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.get(&1).await, Some(12));

        change_stream_tx.send(Change::Remove(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.len().await, 1);

        change_stream_tx.send(Change::Remove(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(pool.is_empty().await);
        assert_eq!(pool.len().await, 0);
    }
}
