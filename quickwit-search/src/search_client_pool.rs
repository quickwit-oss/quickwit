// Copyright (C) 2021 Quickwit, Inc.
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

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use http::Uri;
use quickwit_cluster::{Cluster, QuickwitService};
use quickwit_proto::tonic;
use tokio_stream::StreamExt;
use tonic::transport::Endpoint;
use tracing::*;

use crate::rendezvous_hasher::sort_by_rendez_vous_hash;
use crate::SearchServiceClient;

/// Create a SearchServiceClient with SocketAddr as an argument.
/// It will try to reconnect to the node automatically.
async fn create_search_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<SearchServiceClient> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri).connect_lazy();
    let client = SearchServiceClient::from_grpc_client(
        quickwit_proto::search_service_client::SearchServiceClient::new(channel),
        grpc_addr,
    );
    Ok(client)
}

/// Job.
/// The unit in which distributed search is performed.
///
/// The `split_id` is used to define an affinity between a leaf nodes and a job.
/// The `cost` is used to spread the work evenly amongst nodes.
pub trait Job {
    /// SplitId of the split that is targetted.
    fn split_id(&self) -> &str;
    /// Estimation of the load associated with running a given job.
    ///
    /// A list of job will be assigned to leaf nodes in a way that spread
    /// the sum of cost evenly.
    fn cost(&self) -> u32;
}

/// Search client pool implementation.
#[derive(Clone, Default)]
pub struct SearchClientPool {
    /// Search clients.
    /// A hash map with gRPC's SocketAddr as the key and SearchServiceClient as the value.
    /// It is not the cluster listen address.
    clients: Arc<RwLock<HashMap<SocketAddr, SearchServiceClient>>>,
}

/// Update the client pool given a new list of members.
async fn update_client_map(
    members_grpc_addresses: &[SocketAddr],
    new_clients: &mut HashMap<SocketAddr, crate::SearchServiceClient>,
) {
    // Create a list of addresses to be removed.
    let members_addresses = members_grpc_addresses.iter().collect::<HashSet<_>>();
    let addresses_to_remove: Vec<SocketAddr> = new_clients
        .keys()
        .filter(|socket_addr| !members_addresses.contains(*socket_addr))
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
    for grpc_address in members_grpc_addresses {
        if let Entry::Vacant(_entry) = new_clients.entry(*grpc_address) {
            match create_search_service_client(*grpc_address).await {
                Ok(client) => {
                    debug!(grpc_address=?grpc_address, "Add a new client that is connecting to the node that has been joined the cluster.");
                    new_clients.insert(*grpc_address, client);
                }
                Err(err) => {
                    error!(grpc_address=?grpc_address, err=?err, "Failed to create search client.")
                }
            };
        }
    }
}

impl SearchClientPool {
    /// Creates a search client pool for a static list of addresses.
    pub async fn for_addrs(grpc_addrs: &[SocketAddr]) -> anyhow::Result<SearchClientPool> {
        let mut clients_map = HashMap::default();
        for &grpc_addr in grpc_addrs {
            let search_service_client = create_search_service_client(grpc_addr).await?;
            clients_map.insert(grpc_addr, search_service_client);
        }
        Ok(SearchClientPool {
            clients: Arc::new(RwLock::from(clients_map)),
        })
    }

    async fn update_members(&self, member_grpc_addrs: &[SocketAddr]) {
        let mut new_clients = self.clients();
        update_client_map(member_grpc_addrs, &mut new_clients).await;
        *self.clients.write().unwrap() = new_clients;
    }

    /// Returns a copy of the entire member map.
    pub fn clients(&self) -> HashMap<SocketAddr, SearchServiceClient> {
        self.clients
            .read()
            .expect("Client pool lock is poisoned.")
            .clone()
    }

    #[cfg(test)]
    pub async fn from_mocks(
        mock_services: Vec<Arc<dyn crate::SearchService>>,
    ) -> anyhow::Result<Self> {
        let mut mock_clients = HashMap::new();
        for (mock_ord, mock_service) in mock_services.into_iter().enumerate() {
            let grpc_addr: SocketAddr =
                format!("127.0.0.1:{}", 10000 + mock_ord as u16 * 10).parse()?;
            let mock_client = SearchServiceClient::from_service(mock_service, grpc_addr);
            mock_clients.insert(grpc_addr, mock_client);
        }

        Ok(SearchClientPool {
            clients: Arc::new(RwLock::new(mock_clients)),
        })
    }

    /// Create a search client pool given a cluster.
    /// When a client pool is created, the thread that monitors cluster members
    /// will be started at the same time.
    pub async fn create_and_keep_updated(cluster: Arc<Cluster>) -> anyhow::Result<Self> {
        let search_client_pool = SearchClientPool::default();
        let members_grpc_addresses = cluster
            .members_grpc_addresses_for_service(QuickwitService::Searcher)
            .await?;
        search_client_pool
            .update_members(&members_grpc_addresses)
            .await;

        // Prepare to start a thread that will monitor cluster members.
        let search_clients_pool_clone = search_client_pool.clone();
        let mut members_watch_channel = cluster.member_change_watcher();

        // Start to monitor the cluster members.
        tokio::spawn(async move {
            while (members_watch_channel.next().await).is_some() {
                let members_grpc_addresses = cluster
                    .members_grpc_addresses_for_service(QuickwitService::Searcher)
                    .await?;
                search_clients_pool_clone
                    .update_members(&members_grpc_addresses)
                    .await;
            }
            Result::<(), anyhow::Error>::Ok(())
        });

        Ok(search_client_pool)
    }
}

fn job_order_key<J: Job>(job: &J) -> (Reverse<u32>, &str) {
    (Reverse(job.cost()), job.split_id())
}

/// Node is a utility struct used to represent a rendez-vous hashing node.
/// It's used to track the load and the computed hash for a given key
#[derive(Debug, Clone)]
struct Node {
    pub peer_grpc_addr: SocketAddr,
    pub load: u64,
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_grpc_addr.hash(state);
    }
}

impl SearchClientPool {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SocketAddr, Vec<Job>)
    ///
    /// When exclude_addresses filters all clients it is ignored.
    pub fn assign_jobs<J: Job>(
        &self,
        mut jobs: Vec<J>,
        exclude_addresses: &HashSet<SocketAddr>,
    ) -> anyhow::Result<Vec<(SearchServiceClient, Vec<J>)>> {
        let mut splits_groups: HashMap<SocketAddr, Vec<J>> = HashMap::new();

        // Distribute using rendez-vous hashing
        let mut nodes: Vec<Node> = Vec::new();
        let mut socket_to_client: HashMap<SocketAddr, SearchServiceClient> = Default::default();

        {
            // TODO optimize the case where there are few jobs and many clients.
            let clients = self.clients();

            // when exclude_addresses excludes all adresses we discard it
            let empty_set = HashSet::default();
            let exclude_addresses_if_not_saturated = if exclude_addresses.len() == clients.len() {
                &empty_set
            } else {
                exclude_addresses
            };

            for (grpc_addr, client) in clients
                .into_iter()
                .filter(|(grpc_addr, _)| !exclude_addresses_if_not_saturated.contains(grpc_addr))
            {
                nodes.push(Node {
                    peer_grpc_addr: grpc_addr,
                    load: 0,
                });
                socket_to_client.insert(grpc_addr, client);
            }
        }

        // Sort job
        jobs.sort_by(|left, right| {
            // sort_by_key does not work here unfortunately
            job_order_key(left).cmp(&job_order_key(right))
        });

        for job in jobs {
            sort_by_rendez_vous_hash(&mut nodes, job.split_id());
            // choose one of the the first two nodes based on least loaded
            let chosen_node_index: usize = if nodes.len() >= 2 {
                if nodes[0].load > nodes[1].load {
                    1
                } else {
                    0
                }
            } else {
                0
            };

            // update node load for next round
            nodes[chosen_node_index].load += job.cost() as u64;

            let chosen_leaf_grpc_addr: SocketAddr = nodes[chosen_node_index].peer_grpc_addr;
            splits_groups
                .entry(chosen_leaf_grpc_addr)
                .or_insert_with(Vec::new)
                .push(job);
        }

        let mut client_to_jobs = Vec::new();
        for (socket_addr, jobs) in splits_groups {
            // Removing the client in order to ensure a 1:1 cardinality on grpc_addr and clients
            if let Some(client) = socket_to_client.remove(&socket_addr) {
                client_to_jobs.push((client, jobs));
            } else {
                error!("Client is missing. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            }
        }

        Ok(client_to_jobs)
    }

    /// Assigns one job to a client.
    pub fn assign_job<J: Job>(
        &self,
        job: J,
        excluded_addresses: &HashSet<SocketAddr>,
    ) -> anyhow::Result<SearchServiceClient> {
        self.assign_jobs(vec![job], excluded_addresses)?
            .into_iter()
            .next()
            .map(|(client, _jobs)| client)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "`assign_jobs` with {} excluded addresses failed to return at least one \
                     client.",
                    excluded_addresses.len()
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use chitchat::transport::{ChannelTransport, Transport};
    use itertools::Itertools;
    use quickwit_cluster::{create_cluster_for_test, grpc_addr_from_listen_addr_for_test, Cluster};

    use super::create_search_service_client;
    use crate::root::SearchJob;
    use crate::SearchClientPool;

    async fn create_cluster_simple_for_test(
        transport: &dyn Transport,
    ) -> anyhow::Result<Arc<Cluster>> {
        let cluster = create_cluster_for_test(Vec::new(), &["searcher"], transport).await?;
        Ok(Arc::new(cluster))
    }

    #[tokio::test]
    async fn test_search_client_pool_single_node() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_simple_for_test(&transport).await?;
        let client_pool = SearchClientPool::create_and_keep_updated(cluster.clone()).await?;
        let clients = client_pool.clients();
        let addrs: Vec<SocketAddr> = clients.into_keys().collect();
        let expected_addrs = vec![grpc_addr_from_listen_addr_for_test(cluster.listen_addr)];
        assert_eq!(addrs, expected_addrs);
        Ok(())
    }

    #[tokio::test]
    async fn test_search_client_pool_multiple_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_simple_for_test(&transport).await?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test(vec![node_1], &["searcher"], &transport).await?;

        cluster1
            .wait_for_members(|members| members.len() == 2, Duration::from_secs(5))
            .await?;

        let client_pool = SearchClientPool::create_and_keep_updated(cluster1.clone()).await?;
        let clients = client_pool.clients();

        let addrs: Vec<SocketAddr> = clients.into_keys().sorted().collect();
        let mut expected_addrs = vec![
            grpc_addr_from_listen_addr_for_test(cluster1.listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster2.listen_addr),
        ];
        expected_addrs.sort();
        assert_eq!(addrs, expected_addrs);
        Ok(())
    }

    #[tokio::test]
    async fn test_search_client_pool_single_node_assign_jobs() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_simple_for_test(&transport).await?;
        let client_pool = SearchClientPool::create_and_keep_updated(cluster.clone()).await?;
        let jobs = vec![
            SearchJob::for_test("split1", 1),
            SearchJob::for_test("split2", 2),
            SearchJob::for_test("split3", 3),
            SearchJob::for_test("split4", 4),
        ];

        let assigned_jobs = client_pool.assign_jobs(jobs, &HashSet::default())?;
        let expected_assigned_jobs = vec![(
            create_search_service_client(grpc_addr_from_listen_addr_for_test(cluster.listen_addr))
                .await?,
            vec![
                SearchJob::for_test("split4", 4),
                SearchJob::for_test("split3", 3),
                SearchJob::for_test("split2", 2),
                SearchJob::for_test("split1", 1),
            ],
        )];
        assert_eq!(
            assigned_jobs.get(0).unwrap().1,
            expected_assigned_jobs.get(0).unwrap().1
        );
        Ok(())
    }
}
