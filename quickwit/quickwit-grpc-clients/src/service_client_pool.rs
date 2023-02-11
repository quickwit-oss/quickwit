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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_cluster::ClusterMember;
use quickwit_config::service::QuickwitService;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error};

/// A service client is a gRPC client that sends requests
/// to a [`QuickwitService`] running on a node .
/// It is used by the [`ServiceClientPool`] that manages
/// a list of clients.
#[async_trait]
pub trait ServiceClient: Clone + Send + Sync + 'static {
    /// Returns the [`QuickwitService`] of the client.
    fn service() -> QuickwitService;
    /// Builds a client from a [`SocketAddr`].
    async fn build_client(addr: SocketAddr) -> anyhow::Result<Self>;
    /// Returns the gRPC address of the client.
    fn grpc_addr(&self) -> SocketAddr;
}

/// Service client pool.
/// Clients are stored in a [`HashMap<SocketAddress, ServiceClient>`].
/// When the pool is created with `create_and_update_members`, the pool
/// is dynamically updated with cluster members changes.
/// The pool is used for searcher and indexer clients.
#[derive(Clone)]
pub struct ServiceClientPool<T: ServiceClient> {
    clients: Arc<RwLock<HashMap<SocketAddr, T>>>,
}

impl<T: ServiceClient> Default for ServiceClientPool<T> {
    fn default() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<T: ServiceClient> ServiceClientPool<T> {
    /// Creates a [`ServiceClientPool`] from a clients map.
    pub fn new(clients: HashMap<SocketAddr, T>) -> Self {
        Self {
            clients: Arc::new(RwLock::from(clients)),
        }
    }

    /// Returns a copy of the entire clients hashmap.
    pub fn all(&self) -> HashMap<SocketAddr, T> {
        self.clients
            .read()
            .expect("Client pool lock is poisoned.")
            .clone()
    }

    /// Returns the client with the given `grpc_address`.
    pub fn get(&self, grpc_address: SocketAddr) -> Option<T> {
        self.clients
            .read()
            .expect("Client pool lock is poisoned.")
            .get(&grpc_address)
            .cloned()
    }

    /// Sets the pool hashmap to the given clients hashmap.
    async fn set(&self, clients: HashMap<SocketAddr, T>) {
        *self.clients.write().unwrap() = clients;
    }

    /// Creates a [`ServiceClientPool`] from watched cluster members.
    /// When the pool is created, the thread that monitors cluster members
    /// is started at the same time.
    pub async fn create_and_update_members(
        mut members_watch_channel: WatchStream<Vec<ClusterMember>>,
    ) -> anyhow::Result<Self> {
        let pool = ServiceClientPool::default();
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            while let Some(new_members) = members_watch_channel.next().await {
                let mut new_clients = pool_clone.all();
                update_client_map::<T>(&new_members, &mut new_clients).await;
                pool_clone.set(new_clients).await;
            }
            Result::<(), anyhow::Error>::Ok(())
        });
        Ok(pool)
    }

    /// Creates a [`ServiceClientPool`] from a static list of socket addresses.
    pub async fn for_addrs(grpc_addrs: &[SocketAddr]) -> anyhow::Result<Self> {
        let mut clients_map = HashMap::default();
        for &grpc_addr in grpc_addrs {
            let client = T::build_client(grpc_addr).await?;
            clients_map.insert(grpc_addr, client);
        }
        Ok(Self::new(clients_map))
    }

    /// Creates a [`ServiceClientPool`] from a static list of service clients.
    pub fn for_clients_list(clients: Vec<T>) -> Self {
        let mut clients_map = HashMap::default();
        for client in clients {
            clients_map.insert(client.grpc_addr(), client);
        }
        Self::new(clients_map)
    }
}

async fn update_client_map<T: ServiceClient>(
    cluster_members: &[ClusterMember],
    new_clients: &mut HashMap<SocketAddr, T>,
) {
    let filtered_cluster_members = cluster_members
        .iter()
        .filter(|member| member.enabled_services.contains(&T::service()))
        .collect_vec();
    let members_grpc_addrs: HashSet<SocketAddr> = filtered_cluster_members
        .iter()
        .map(|member| member.grpc_advertise_addr)
        .collect();
    // Create a list of addresses to be removed.
    let addresses_to_remove: Vec<SocketAddr> = new_clients
        .keys()
        .filter(|socket_addr| !members_grpc_addrs.contains(*socket_addr))
        .cloned()
        .collect();

    // Remove clients from the client pool.
    for grpc_address in addresses_to_remove {
        let removed = new_clients.remove(&grpc_address).is_some();
        if removed {
            debug!(grpc_address=?grpc_address, "Remove a client that is connecting to the node that has been downed or left the cluster.");
        }
    }

    // Add clients to the client pool.
    for cluster_member in filtered_cluster_members {
        if let Entry::Vacant(_entry) = new_clients.entry(cluster_member.grpc_advertise_addr) {
            match T::build_client(cluster_member.grpc_advertise_addr).await {
                Ok(client) => {
                    debug!(grpc_address=?cluster_member.grpc_advertise_addr, "Add a new client that is connecting to the node that has been joined the cluster.");
                    new_clients.insert(cluster_member.grpc_advertise_addr, client);
                }
                Err(err) => {
                    error!(grpc_address=?cluster_member.grpc_advertise_addr, err=?err, "Failed to create client.")
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::time::Duration;

    use async_trait::async_trait;
    use itertools::Itertools;
    use quickwit_cluster::ClusterMember;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::search_service_client::SearchServiceClient;
    use quickwit_proto::tonic::transport::{Channel, Endpoint, Uri};
    use tokio::sync::watch;
    use tokio_stream::wrappers::WatchStream;

    use super::{ServiceClient, ServiceClientPool};

    #[async_trait]
    impl ServiceClient for SearchServiceClient<Channel> {
        async fn build_client(addr: SocketAddr) -> anyhow::Result<Self> {
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.to_string().as_str())
                .path_and_query("/")
                .build()
                .unwrap();
            let channel = Endpoint::from(uri).connect_lazy();
            Ok(SearchServiceClient::new(channel))
        }

        fn service() -> quickwit_config::service::QuickwitService {
            QuickwitService::Searcher
        }

        fn grpc_addr(&self) -> SocketAddr {
            todo!()
        }
    }

    #[tokio::test]
    async fn test_client_pool_updates() {
        let metastore_grpc_addr: SocketAddr = ([127, 0, 0, 1], 10).into();
        let searcher_1_grpc_addr: SocketAddr = ([127, 0, 0, 1], 11).into();
        let searcher_2_grpc_addr: SocketAddr = ([127, 0, 0, 1], 12).into();
        let metastore_service_member = ClusterMember::new(
            "0".to_string(),
            0,
            HashSet::from([QuickwitService::Metastore]),
            metastore_grpc_addr,
            metastore_grpc_addr,
            Vec::new(),
        );
        let searcher_1_member = ClusterMember::new(
            "2".to_string(),
            0,
            HashSet::from([QuickwitService::Searcher]),
            searcher_1_grpc_addr,
            searcher_1_grpc_addr,
            Vec::new(),
        );
        let searcher_2_member = ClusterMember::new(
            "2".to_string(),
            0,
            HashSet::from([QuickwitService::Searcher]),
            searcher_2_grpc_addr,
            searcher_2_grpc_addr,
            Vec::new(),
        );
        let (members_tx, members_rx) = watch::channel::<Vec<ClusterMember>>(vec![
            metastore_service_member,
            searcher_1_member,
            searcher_2_member,
        ]);
        let watched_members = WatchStream::new(members_rx);
        let client_pool: ServiceClientPool<SearchServiceClient<Channel>> =
            ServiceClientPool::create_and_update_members(watched_members)
                .await
                .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        let clients = client_pool.all();
        let addrs: Vec<SocketAddr> = clients.into_keys().sorted().collect();
        let mut expected_addrs = vec![searcher_1_grpc_addr, searcher_2_grpc_addr];
        expected_addrs.sort();
        assert_eq!(addrs, expected_addrs);

        members_tx.send(Vec::new()).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(client_pool.all().is_empty());
    }
}
