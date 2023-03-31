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
use std::sync::{Arc, Weak};

use futures::{Stream, StreamExt};
use tokio::sync::RwLock;

#[derive(Debug)]
pub enum Change<K, S> {
    Add(K, S),
    Remove(K),
}

/// A pool of services with some routing capabilities.
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

struct InnerPool<K, S> {
    nodes: HashMap<K, S>,
}

impl<K, S> Pool<K, S>
where
    K: Eq + PartialEq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        let inner = InnerPool {
            nodes: HashMap::new(),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn from_change_stream(
        change_stream: impl Stream<Item = Change<K, S>> + Send + 'static,
    ) -> Self {
        let pool = Self::new();
        tokio::spawn({
            let pool = pool.clone();
            async move {
                change_stream
                    .for_each(|change| async {
                        match change {
                            Change::Add(key, service) => {
                                pool.add_node(key, service).await;
                            }
                            Change::Remove(key) => {
                                pool.remove_node(&key).await;
                            }
                        }
                    })
                    .await;
            }
        });
        pool
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.nodes.is_empty()
    }

    pub async fn num_nodes(&self) -> usize {
        self.inner.read().await.nodes.len()
    }

    pub async fn all_nodes(&self) -> Vec<S> {
        self.inner.read().await.nodes.values().cloned().collect()
    }

    // TODO: pick one at random or round robin or according to load.
    pub async fn any_node(&self) -> Option<S> {
        self.inner.read().await.nodes.values().cloned().next()
    }

    pub async fn get_node<Q>(&self, key: &Q) -> Option<S>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.inner.read().await.nodes.get(key).cloned()
    }

    pub async fn find_node(&self, func: impl Fn(&K) -> bool) -> Option<K> {
        self.inner
            .read()
            .await
            .nodes
            .keys()
            .find(|k| func(k))
            .cloned()
    }

    pub async fn add_node(&self, key: K, service: S) {
        self.inner.write().await.nodes.insert(key, service);
    }

    pub async fn remove_node(&self, key: &K) {
        self.inner.write().await.nodes.remove(key);
    }

    pub fn weak(&self) -> WeakPool<K, S> {
        WeakPool {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

pub struct WeakPool<K, S> {
    inner: Weak<RwLock<InnerPool<K, S>>>,
}

impl<K, S> WeakPool<K, S>
where
    K: Eq + PartialEq + Hash + Clone + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    pub fn upgrade(&self) -> Option<Pool<K, S>> {
        self.inner.upgrade().map(|inner| Pool { inner })
    }
}

// pub enum RoutingError {
//     ServiceNotFound,
//     ServiceUnavailable,
// }

// impl<K, S, R> Pool<K, S>
// where
//     K: Eq + PartialEq + Hash,
//     S: Service<R> + Clone,
// {
//     pub async fn route_any(&self, request: R) -> Result<S::Future, RoutingError> {
//         todo!()
//     }

//     pub async fn route_one(&self, key: &K, request: R) -> Result<S::Future, RoutingError> {
//         todo!()
//     }

//     pub async fn route_one_of(&self, keys: impl Iterator<Item = &K>, request: R) ->
// Result<S::Future, RoutingError> {         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let (change_stream_tx, change_stream_rx) = tokio::sync::mpsc::channel(10);
        let change_stream = ReceiverStream::new(change_stream_rx);
        let pool = Pool::from_change_stream(change_stream);
        assert!(pool.is_empty().await);

        change_stream_tx.send(Change::Add(1, 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.get_node(&1).await, Some(1));

        change_stream_tx.send(Change::Add(1, 2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(pool.get_node(&1).await, Some(2));

        change_stream_tx.send(Change::Remove(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(pool.is_empty().await);
    }
}
