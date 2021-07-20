/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::*;

use quickwit_cluster::cluster::Cluster;

use crate::client::{create_search_service_client, WrappedSearchServiceClient};
use crate::client_pool::{ClientPool, Job};
use crate::rendezvous_hasher::{sort_by_rendez_vous_hash, Node};
use crate::swim_addr_to_grpc_addr;

/// Search client pool implementation.
#[derive(Clone)]
pub struct SearchClientPool {
    /// Search clients.
    /// A hash map with gRPC's SocketAddr as the key and SearchServiceClient as the value.
    /// It is not the cluster listen address.
    pub clients: Arc<RwLock<HashMap<SocketAddr, WrappedSearchServiceClient>>>,
}

impl SearchClientPool {
    /// Create a search client pool given a cluster.
    /// When a client pool is created, the thread that monitors cluster members
    /// will be started at the same time.
    pub async fn new(cluster: Arc<dyn Cluster>) -> anyhow::Result<Self> {
        let clients = HashMap::new();

        // Create search client pool.
        let client_pool = SearchClientPool {
            clients: Arc::new(RwLock::new(clients)),
        };

        // Prepare to start a thread that will monitor cluster members.
        let thread_clients = Arc::clone(&client_pool.clients);
        let mut members_watch_channel = cluster.member_change_watcher();

        // Start to monitor the cluster members.
        tokio::spawn(async move {
            while let Some(members) = members_watch_channel.next().await {
                let mut clients = thread_clients.write().await;

                // Create a list of addresses to be removed.
                let members_addresses: HashSet<SocketAddr> = members
                    .iter()
                    .map(|member| swim_addr_to_grpc_addr(member.listen_addr))
                    .collect();
                let addrs_to_remove: Vec<SocketAddr> = clients
                    .keys()
                    .filter(|socket_addr| !members_addresses.contains(*socket_addr))
                    .cloned()
                    .collect();

                // Remove clients from the client pool.
                for grpc_addr in addrs_to_remove {
                    let removed = clients.remove(&grpc_addr).is_some();
                    if removed {
                        debug!(grpc_addr=?grpc_addr, "Remove a client that is connecting to the node that has been downed or left the cluster.");
                    }
                }

                // Add clients to the client pool.
                for member in members {
                    let grpc_addr = swim_addr_to_grpc_addr(member.listen_addr);
                    if let Entry::Vacant(_entry) = clients.entry(grpc_addr) {
                        match create_search_service_client(grpc_addr).await {
                            Ok(client) => {
                                debug!(grpc_addr=?grpc_addr, "Add a new client that is connecting to the node that has been joined the cluster.");
                                clients.insert(grpc_addr, client);
                            }
                            Err(err) => {
                                error!(grpc_addr=?grpc_addr, err=?err, "Failed to create search client.")
                            }
                        };
                    }
                }
            }
        });

        Ok(client_pool)
    }
}

#[async_trait]
impl ClientPool for SearchClientPool {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SocketAddr, Vec<Job>)
    async fn assign_jobs(
        &self,
        mut jobs: Vec<Job>,
    ) -> anyhow::Result<Vec<(WrappedSearchServiceClient, Vec<Job>)>> {
        let mut splits_groups: HashMap<SocketAddr, Vec<Job>> = HashMap::new();

        // Distribute using rendez-vous hashing
        let mut nodes: Vec<Node> = Vec::new();
        let mut socket_to_client: HashMap<SocketAddr, WrappedSearchServiceClient> =
            Default::default();

        {
            // restricting the lock guard lifetime.

            // TODO optimize the case where there are few jobs and many clients.
            let clients = self.clients.read().await;

            for (grpc_addr, client) in clients.iter() {
                let node = Node::new(*grpc_addr, 0);
                nodes.push(node);
                socket_to_client.insert(*grpc_addr, client.clone());
            }
        }

        // Sort job
        jobs.sort_by(|left, right| {
            let cost_ord = right.cost.cmp(&left.cost);
            if cost_ord != Ordering::Equal {
                return cost_ord;
            }
            left.split.cmp(&right.split)
        });

        for job in jobs {
            sort_by_rendez_vous_hash(&mut nodes, &job.split);
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
            nodes[chosen_node_index].load += job.cost as u64;

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
                error!("Missing client. This should never happen! Please report");
            }
        }
        Ok(client_to_jobs)
    }
}
