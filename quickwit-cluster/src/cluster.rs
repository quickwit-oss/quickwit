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

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chitchat::server::ChitchatServer;
use chitchat::{FailureDetectorConfig, NodeId, SerializableClusterState};
use itertools::Itertools;
use quickwit_common::net::get_socket_addr;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::error::ClusterResult;
use crate::QuickwitService;

const GRPC_ADDRESS_KEY: &str = "grpc_address";
const AVAILABLE_SERVICES_KEY: &str = "available_services";

/// A member information.
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    /// An ID that makes a member unique.
    pub node_unique_id: String,
    /// timestamp (ms) when node starts.
    pub generation: u64,
    /// advertised UdpServerSocket
    pub gossip_public_address: SocketAddr,
    /// If true, it means self.
    pub is_self: bool,
}

impl Member {
    pub fn new(node_unique_id: String, generation: u64, gossip_public_address: SocketAddr) -> Self {
        Self {
            node_unique_id,
            gossip_public_address,
            generation,
            is_self: true,
        }
    }

    pub fn internal_id(&self) -> String {
        format!("{}/{}", self.node_unique_id, self.generation)
    }
}

impl TryFrom<NodeId> for Member {
    type Error = anyhow::Error;

    fn try_from(node_id: NodeId) -> Result<Self, Self::Error> {
        let (node_unique_id_str, generation_str) = node_id.id.split_once('/').ok_or_else(|| {
            anyhow::anyhow!(
                "Could not create a Member instance from NodeId `{}`",
                node_id.id
            )
        })?;

        let gossip_public_address: SocketAddr = node_id.gossip_public_address.parse()?;
        Ok(Self {
            node_unique_id: node_unique_id_str.to_string(),
            generation: generation_str.parse()?,
            gossip_public_address,
            is_self: false,
        })
    }
}

impl From<Member> for NodeId {
    fn from(member: Member) -> Self {
        Self::new(
            member.internal_id(),
            member.gossip_public_address.to_string(),
        )
    }
}

/// This is an implementation of a cluster using Chitchat.
pub struct Cluster {
    pub node_id: String,
    /// A socket address that represents itself.
    pub listen_addr: SocketAddr,

    /// The actual cluster that implements Chitchat.
    chitchat_server: ChitchatServer,

    /// A receiver(channel) for exchanging members in a cluster.
    members: watch::Receiver<Vec<Member>>,

    /// A stop flag of cluster monitoring task.
    /// Once the cluster is created, a task to monitor cluster events will be started.
    /// Nodes do not need to be monitored for events once they are detached from the cluster.
    /// You need to update this value to get out of the task loop.
    stop: Arc<AtomicBool>,
}

impl Cluster {
    /// Create a cluster given a host key and a listen address.
    /// When a cluster is created, the thread that monitors cluster events
    /// will be started at the same time.
    pub fn new(
        me: Member,
        services: &HashSet<QuickwitService>,
        listen_addr: SocketAddr,
        cluster_id: String,
        grpc_addr: SocketAddr,
        seed_nodes: &[String],
        failure_detector_config: FailureDetectorConfig,
    ) -> ClusterResult<Self> {
        info!(member=?me, listen_addr=?listen_addr, "Create new cluster.");
        let chitchat_server = ChitchatServer::spawn(
            NodeId::from(me.clone()),
            seed_nodes,
            listen_addr.to_string(),
            cluster_id,
            vec![
                (GRPC_ADDRESS_KEY, grpc_addr.to_string()),
                (
                    AVAILABLE_SERVICES_KEY,
                    services.iter().map(|service| service.as_str()).join(","),
                ),
            ],
            failure_detector_config,
        );
        let chitchat = chitchat_server.chitchat();

        let (members_sender, members_receiver) = watch::channel(Vec::new());

        // Create cluster.
        let cluster = Cluster {
            node_id: me.internal_id(),
            listen_addr,
            chitchat_server,
            members: members_receiver,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Add itself as the initial member of the cluster.
        let initial_members: Vec<Member> = vec![me.clone()];
        if members_sender.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        // Prepare to start a task that will monitor cluster events.
        let task_stop = cluster.stop.clone();
        tokio::task::spawn(async move {
            let mut node_change_receiver = chitchat.lock().await.live_nodes_watcher();

            while let Some(members_set) = node_change_receiver.next().await {
                let mut members = members_set
                    .into_iter()
                    .map(Member::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                members.push(me.clone());

                if task_stop.load(Ordering::Relaxed) {
                    debug!("receive a stop signal");
                    break;
                }

                if members_sender.send(members).is_err() {
                    // Somehow the cluster has been dropped.
                    error!("Failed to send a member list.");
                    break;
                }
            }
            Result::<(), anyhow::Error>::Ok(())
        });

        Ok(cluster)
    }

    /// Return [WatchStream] for monitoring change of `members`
    pub fn member_change_watcher(&self) -> WatchStream<Vec<Member>> {
        WatchStream::new(self.members.clone())
    }

    /// Return `members` list
    pub fn members(&self) -> Vec<Member> {
        self.members.borrow().clone()
    }

    /// Returns the gRPC addresses of the members providing the specified service.
    /// Returns the addresses of all the members if no service is specified.
    pub async fn members_grpc_addresses_by_service(
        &self,
        members: &[Member],
        service_opt: Option<QuickwitService>,
    ) -> anyhow::Result<Vec<SocketAddr>> {
        let chitchat = self.chitchat_server.chitchat();
        let chitchat_guard = chitchat.lock().await;
        let mut grpc_addresses = vec![];
        for member in members {
            let node_state = chitchat_guard
                .node_state(&NodeId::from(member.clone()))
                .unwrap();

            let available_services = node_state
                .get(AVAILABLE_SERVICES_KEY)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Could not find `{}` key on node `{}` state.",
                        AVAILABLE_SERVICES_KEY,
                        member.internal_id()
                    )
                })
                .map(|services_str| {
                    services_str
                        .split(',')
                        .map(QuickwitService::try_from)
                        .collect::<Result<Vec<_>, _>>()
                })??;

            let grpc_address = node_state
                .get(GRPC_ADDRESS_KEY)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Could not find `{}` key on node `{}` state.",
                        GRPC_ADDRESS_KEY,
                        member.internal_id()
                    )
                })
                .map(|addr_str| get_socket_addr(&addr_str))??;

            if let Some(service) = &service_opt {
                if available_services.contains(service) {
                    grpc_addresses.push(grpc_address);
                }
                continue;
            }
            grpc_addresses.push(grpc_address);
        }

        Ok(grpc_addresses)
    }

    /// Set a key-value pair on the cluster node's state.
    pub async fn set_key_value<K: ToString, V: ToString>(&self, key: K, value: V) {
        let chitchat = self.chitchat_server.chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().set(key, value);
    }

    /// Leave the cluster.
    pub async fn leave(&self) {
        info!(self_addr = ?self.listen_addr, "Leaving the cluster.");
        // TODO: implements leave/join on Chitchat
        // https://github.com/quickwit-oss/chitchat/issues/30
        self.stop.store(true, Ordering::Relaxed);
    }

    pub async fn state(&self) -> ClusterState {
        let chitchat = self.chitchat_server.chitchat();
        let chitchat_guard = chitchat.lock().await;

        let cluster_state = chitchat_guard.cluster_state();
        let live_nodes = chitchat_guard.live_nodes().cloned().collect::<Vec<_>>();
        let dead_nodes = chitchat_guard.dead_nodes().cloned().collect::<Vec<_>>();
        ClusterState {
            state: SerializableClusterState::from(cluster_state),
            live_nodes,
            dead_nodes,
        }
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.listen_addr, "Shutting down the cluster.");
        let result = self.chitchat_server.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.listen_addr, error = ?error, "Error while shuting down.");
        }

        self.stop.store(true, Ordering::Relaxed);
    }

    /// Convenience method for testing that waits for the predicate to hold true for the cluster's
    /// members.
    pub async fn wait_for_members<F>(
        self: &Cluster,
        mut predicate: F,
        timeout_after: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&Vec<Member>) -> bool,
    {
        timeout(
            timeout_after,
            self.member_change_watcher()
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterState {
    state: SerializableClusterState,
    live_nodes: Vec<NodeId>,
    dead_nodes: Vec<NodeId>,
}

/// Compute the gRPC port from the chitchat listen address for tests.
pub fn grpc_addr_from_listen_addr_for_test(listen_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(listen_addr.ip(), listen_addr.port() + 1)
}

pub fn create_cluster_for_test_with_id(
    peer_uuid: String,
    cluster_id: String,
    seeds: &[String],
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<Cluster> {
    let listen_addr = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        quickwit_common::net::find_available_port()?,
    );
    let failure_detector_config = create_failure_detector_config_for_test();
    let cluster = Cluster::new(
        Member::new(peer_uuid, 1, listen_addr),
        services,
        listen_addr,
        cluster_id,
        grpc_addr_from_listen_addr_for_test(listen_addr),
        seeds,
        failure_detector_config,
    )?;
    Ok(cluster)
}

/// Creates a failure detector config for tests.
fn create_failure_detector_config_for_test() -> FailureDetectorConfig {
    FailureDetectorConfig {
        phi_threshold: 6.0,
        initial_interval: Duration::from_millis(400),
        ..Default::default()
    }
}

/// Creates a local cluster listening on a random port.
pub fn create_cluster_for_test(seeds: &[String], services: &[&str]) -> anyhow::Result<Cluster> {
    let peer_uuid = Uuid::new_v4().to_string();
    let services = services
        .iter()
        .map(|service_str| QuickwitService::try_from(*service_str))
        .collect::<Result<HashSet<_>, _>>()?;
    let cluster =
        create_cluster_for_test_with_id(peer_uuid, "test-cluster".to_string(), seeds, &services)?;
    Ok(cluster)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use itertools::Itertools;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node() -> anyhow::Result<()> {
        let cluster = create_cluster_for_test(&[], &[])?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .collect();
        let expected_members = vec![cluster.listen_addr];
        assert_eq!(members, expected_members);

        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_available_service() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test(&[], &["searcher"])?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test(&[node_1.clone()], &["searcher", "indexer"])?;
        let cluster3 = create_cluster_for_test(&[node_1], &["indexer"])?;

        let mut expected_indexers = vec![
            grpc_addr_from_listen_addr_for_test(cluster2.listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster3.listen_addr),
        ];
        expected_indexers.sort();
        let mut expected_searchers = vec![
            grpc_addr_from_listen_addr_for_test(cluster1.listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster2.listen_addr),
        ];
        expected_searchers.sort();

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members = cluster1.members();

        let all = cluster1
            .members_grpc_addresses_by_service(&members, None)
            .await?;
        assert_eq!(all.len(), 3);

        let mut indexers = cluster1
            .members_grpc_addresses_by_service(&members, Some(QuickwitService::Indexer))
            .await?;
        indexers.sort();
        assert_eq!(indexers.len(), 2);
        assert_eq!(indexers, expected_indexers);

        let mut searchers = cluster1
            .members_grpc_addresses_by_service(&members, Some(QuickwitService::Searcher))
            .await?;
        searchers.sort();
        assert_eq!(searchers.len(), 2);
        assert_eq!(searchers, expected_searchers);

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test(&[], &[])?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test(&[node_1.clone()], &[])?;
        let cluster3 = create_cluster_for_test(&[node_1], &[])?;

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        cluster2.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        cluster3.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_id_isolation() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let cluster1a = create_cluster_for_test_with_id(
            "node_1a".to_string(),
            "cluster1".to_string(),
            &[],
            &HashSet::default(),
        )?;
        let cluster2a = create_cluster_for_test_with_id(
            "node_2a".to_string(),
            "cluster2".to_string(),
            &[cluster1a.listen_addr.to_string()],
            &HashSet::default(),
        )?;

        let cluster1b = create_cluster_for_test_with_id(
            "node_1b".to_string(),
            "cluster1".to_string(),
            &[
                cluster1a.listen_addr.to_string(),
                cluster2a.listen_addr.to_string(),
            ],
            &HashSet::default(),
        )?;
        let cluster2b = create_cluster_for_test_with_id(
            "node_2b".to_string(),
            "cluster2".to_string(),
            &[
                cluster1a.listen_addr.to_string(),
                cluster2a.listen_addr.to_string(),
            ],
            &HashSet::default(),
        )?;

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1a, &cluster2a, &cluster1b, &cluster2b] {
            cluster
                .wait_for_members(|members| members.len() == 2, wait_secs)
                .await
                .unwrap();
        }

        let members_a: Vec<SocketAddr> = cluster1a
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members_a = vec![cluster1a.listen_addr, cluster1b.listen_addr];
        expected_members_a.sort();
        assert_eq!(members_a, expected_members_a);

        let members_b: Vec<SocketAddr> = cluster2a
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members_b = vec![cluster2a.listen_addr, cluster2b.listen_addr];
        expected_members_b.sort();
        assert_eq!(members_b, expected_members_b);

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_issue_1018() -> anyhow::Result<()> {
        let cluster_id = "unified-cluster";
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id(
            "node1".to_string(),
            cluster_id.to_string(),
            &[],
            &HashSet::default(),
        )?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            "node2".to_string(),
            cluster_id.to_string(),
            &[node_1.clone()],
            &HashSet::default(),
        )?;

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 2, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![cluster1.listen_addr, cluster2.listen_addr];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        cluster2.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let grpc_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            quickwit_common::net::find_available_port()?,
        );
        let cluster2 = Cluster::new(
            Member::new("newid".to_string(), 1, cluster2_listen_addr),
            &HashSet::default(),
            cluster2_listen_addr,
            cluster_id.to_string(),
            grpc_addr,
            &[node_1],
            create_failure_detector_config_for_test(),
        )?;

        for _ in 0..4_000 {
            if cluster1.members().len() > 2 {
                panic!("too many members");
            }
            sleep(Duration::from_millis(1)).await;
        }

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_3_nodes_issue_1018() -> anyhow::Result<()> {
        let cluster_id = "three-nodes-cluster";
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id(
            "node1".to_string(),
            cluster_id.to_string(),
            &[],
            &HashSet::default(),
        )?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            "node2".to_string(),
            cluster_id.to_string(),
            &[node_1.clone()],
            &HashSet::default(),
        )?;
        let node_2 = cluster2.listen_addr.to_string();
        let cluster3 = create_cluster_for_test_with_id(
            "node3".to_string(),
            cluster_id.to_string(),
            &[node_2],
            &HashSet::default(),
        )?;

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        let cluster3_listen_addr = cluster3.listen_addr;
        cluster2.shutdown().await;
        cluster3.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let grpc_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            quickwit_common::net::find_available_port()?,
        );
        let cluster2 = Cluster::new(
            Member::new("newid".to_string(), 1, cluster2_listen_addr),
            &HashSet::default(),
            cluster2_listen_addr,
            cluster_id.to_string(),
            grpc_addr,
            &[node_1],
            create_failure_detector_config_for_test(),
        )?;
        let node_2 = cluster2.listen_addr.to_string();

        let cluster3 = Cluster::new(
            Member::new("newid2".to_string(), 1, cluster3_listen_addr),
            &HashSet::default(),
            cluster3_listen_addr,
            cluster_id.to_string(),
            grpc_addr,
            &[node_2],
            create_failure_detector_config_for_test(),
        )?;

        sleep(Duration::from_secs(10)).await;
        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node2"));
        assert!(!cluster3
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node2"));

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "node3"));

        Ok(())
    }
}
