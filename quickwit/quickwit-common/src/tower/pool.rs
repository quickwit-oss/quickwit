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

/// A pool of `S` services identified by `K` keys. The pool can be updated manually by calling the
/// `add/remove` methods or by listening to a stream of changes.
#[derive(Default)]
pub struct Pool<K, S> {
    inner: Arc<RwLock<InnerPool<K, S>>>,
}

impl<K, S> fmt::Debug for Pool<K, S>
where
    K: 'static,
    S: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pool<{:?}, {:?}>", TypeId::of::<K>(), TypeId::of::<S>())
    }
}

impl<K, S> Clone for Pool<K, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Default)]
struct InnerPool<K, S> {
    nodes: HashMap<K, S>,
}

impl<K, S> Pool<K, S>
where
    K: Eq + PartialEq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Listens for the changes emitted by the stream and updates the pool accordingly.
    pub fn listen_for_changes(
        &self,
        change_stream: impl Stream<Item = Change<K, S>> + Send + 'static,
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
        self.inner.read().await.nodes.is_empty()
    }

    /// Returns the number of values in the pool.
    pub async fn num_nodes(&self) -> usize {
        self.inner.read().await.nodes.len()
    }

    /// Returns all the values in the pool.
    pub async fn get_all(&self) -> Vec<S> {
        self.inner.read().await.nodes.values().cloned().collect()
    }

    /// Returns the value associated with the given key.
    pub async fn get<Q>(&self, key: &Q) -> Option<S>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.inner.read().await.nodes.get(key).cloned()
    }

    /// Finds a key in the pool that satisfies the given predicate.
    pub async fn find(&self, func: impl Fn(&K) -> bool) -> Option<K> {
        self.inner
            .read()
            .await
            .nodes
            .keys()
            .find(|k| func(k))
            .cloned()
    }

    /// Adds a value to the pool.
    pub async fn insert(&self, key: K, service: S) {
        self.inner.write().await.nodes.insert(key, service);
    }

    /// Removes a value from the pool.
    pub async fn remove(&self, key: &K) {
        self.inner.write().await.nodes.remove(key);
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
        assert_eq!(pool.num_nodes().await, 0);

        change_stream_tx.send(Change::Insert(1, 11)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!pool.is_empty().await);
        assert_eq!(pool.num_nodes().await, 1);
        assert_eq!(pool.get(&1).await, Some(11));

        change_stream_tx.send(Change::Insert(2, 21)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.num_nodes().await, 2);
        assert_eq!(pool.get(&2).await, Some(21));

        assert_eq!(pool.find(|k| *k == 1).await, Some(1));

        let mut all_nodes = pool.get_all().await;
        all_nodes.sort();
        assert_eq!(all_nodes, vec![11, 21]);

        change_stream_tx.send(Change::Insert(1, 12)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.get(&1).await, Some(12));

        change_stream_tx.send(Change::Remove(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.num_nodes().await, 1);

        change_stream_tx.send(Change::Remove(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(pool.is_empty().await);
        assert_eq!(pool.num_nodes().await, 0);
    }
}
