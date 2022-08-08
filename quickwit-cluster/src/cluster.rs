// Copyright (C) 2022 Quickwit, Inc.
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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::Transport;
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, ClusterStateSnapshot, FailureDetectorConfig,
    NodeId,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::error::{ClusterError, ClusterResult};
use crate::QuickwitService;

const GRPC_ADDRESS_KEY: &str = "grpc_address";
const GOSSIP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(1)
};
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
    /// Available services.
    pub available_services: HashSet<QuickwitService>,
    /// gRPC address
    pub grpc_address: SocketAddr,
}

impl Member {
    pub fn new(
        node_unique_id: String,
        generation: u64,
        gossip_public_address: SocketAddr,
        available_services: HashSet<QuickwitService>,
        grpc_address: SocketAddr,
    ) -> Self {
        Self {
            node_unique_id,
            gossip_public_address,
            generation,
            is_self: true,
            available_services,
            grpc_address,
        }
    }

    pub fn internal_id(&self) -> String {
        format!("{}/{}", self.node_unique_id, self.generation)
    }
}

impl From<Member> for NodeId {
    fn from(member: Member) -> Self {
        Self::new(member.internal_id(), member.gossip_public_address)
    }
}

// Build a cluster member from [`ClusterStateSnapshot`].
// TODO: instead of passing a [`ClusterStateSnapshot`], passing a `NodeState` would be nicer but
// the struct is not yet public.
fn build_member_from_chichat<'a>(
    node_id: &'a NodeId,
    cluster_state_snapshot: &'a ClusterStateSnapshot,
) -> anyhow::Result<Member> {
    let (node_unique_id_str, generation_str) = node_id.id.split_once('/').ok_or_else(|| {
        anyhow::anyhow!(
            "Could not create a Member instance from NodeId `{}`",
            node_id.id
        )
    })?;
    let node_state = cluster_state_snapshot
        .node_states
        .get(&node_id.id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find node id `{}` in ChitChat state.",
                node_unique_id_str,
            )
        })?;
    let available_services = node_state
        .get(AVAILABLE_SERVICES_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find `{}` key on node `{}` state.",
                AVAILABLE_SERVICES_KEY,
                node_unique_id_str
            )
        })
        .map(|available_services_val| {
            parse_available_services_val(available_services_val, node_unique_id_str)
        })??;
    let grpc_address = node_state
        .get(GRPC_ADDRESS_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find `{}` key on node `{}` state.",
                GRPC_ADDRESS_KEY,
                node_unique_id_str
            )
        })
        .map(|addr_str| addr_str.parse::<SocketAddr>())??;
    let generation = generation_str.parse()?;
    Ok(Member::new(
        node_unique_id_str.to_string(),
        generation,
        node_id.gossip_public_address,
        available_services,
        grpc_address,
    ))
}

/// This is an implementation of a cluster using Chitchat.
pub struct Cluster {
    /// ID of the cluster joined.
    pub cluster_id: String,
    /// Node ID.
    pub node_id: String,
    /// A socket address that represents itself.
    pub gossip_listen_addr: SocketAddr,

    /// A handle to command Chitchat.
    /// If it is dropped, the chitchat server will stop.
    chitchat_handle: ChitchatHandle,

    /// A receiver (channel) for exchanging information about the nodes in the cluster.
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
    #[allow(clippy::too_many_arguments)] // TODO refactor me #1480
    pub async fn join(
        me: Member,
        gossip_listen_addr: SocketAddr,
        cluster_id: String,
        peer_seed_addrs: Vec<String>,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
    ) -> ClusterResult<Self> {
        info!(
            cluster_id = %cluster_id,
            node_id = %me.node_unique_id,
            grpc_public_addr = %me.grpc_address,
            available_services = ?me.available_services,
            gossip_listen_addr = %gossip_listen_addr,
            gossip_public_addr = %me.gossip_public_address,
            peer_seed_addrs = %peer_seed_addrs.join(", "),
            "Joining cluster."
        );
        let chitchat_config = ChitchatConfig {
            node_id: NodeId::from(me.clone()),
            cluster_id: cluster_id.clone(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr: gossip_listen_addr,
            seed_nodes: peer_seed_addrs,
            failure_detector_config,
        };
        let chitchat_handle = spawn_chitchat(
            chitchat_config,
            vec![
                (GRPC_ADDRESS_KEY.to_string(), me.grpc_address.to_string()),
                (
                    AVAILABLE_SERVICES_KEY.to_string(),
                    me.available_services
                        .iter()
                        .map(|service| service.as_str())
                        .join(","),
                ),
            ],
            transport,
        )
        .await
        .map_err(|cause| ClusterError::UDPPortBindingError {
            listen_addr: gossip_listen_addr,
            cause: cause.to_string(),
        })?;
        let chitchat = chitchat_handle.chitchat();

        let (members_sender, members_receiver) = watch::channel(Vec::new());

        // Create cluster.
        let cluster = Cluster {
            cluster_id: cluster_id.clone(),
            node_id: me.internal_id(),
            gossip_listen_addr,
            chitchat_handle,
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
                // TODO: would be nicer to just get the node states instead of getting a snapshot.
                // For that, we need to make chitchat [`NodeState`] public.
                let state_snapshot = chitchat.lock().await.state_snapshot();
                let mut members = members_set
                    .into_iter()
                    .map(|node_id| build_member_from_chichat(&node_id, &state_snapshot))
                    .filter_map(|member_res| {
                        // Just log an error for members that cannot be built.
                        if let Err(error) = &member_res {
                            error!(
                                "Ignore cluster member, an error occured when building it: {:?}",
                                error
                            );
                        }
                        member_res.ok()
                    })
                    .collect_vec();
                members.push(me.clone());

                if task_stop.load(Ordering::Relaxed) {
                    debug!("Received a stop signal. Stopping.");
                    break;
                }

                if members_sender.send(members).is_err() {
                    // Somehow the cluster has been dropped.
                    error!("Failed to send members list. Stopping.");
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
    pub fn members_grpc_addresses_for_service(&self, service: &QuickwitService) -> Vec<SocketAddr> {
        self.members
            .borrow()
            .iter()
            .filter(|member| member.available_services.contains(service))
            .map(|member| member.grpc_address)
            .collect_vec()
    }

    /// Set a key-value pair on the cluster node's state.
    pub async fn set_key_value<K: ToString, V: ToString>(&self, key: K, value: V) {
        let chitchat = self.chitchat_handle.chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().set(key, value);
    }

    pub async fn state(&self) -> ClusterState {
        let chitchat = self.chitchat_handle.chitchat();
        let chitchat_guard = chitchat.lock().await;

        let state = chitchat_guard.state_snapshot();
        let live_nodes = chitchat_guard.live_nodes().cloned().collect::<Vec<_>>();
        let dead_nodes = chitchat_guard.dead_nodes().cloned().collect::<Vec<_>>();
        ClusterState {
            state,
            live_nodes,
            dead_nodes,
        }
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.gossip_listen_addr, "Shutting down the cluster.");
        let result = self.chitchat_handle.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.gossip_listen_addr, error = ?error, "Error while shuting down.");
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

fn parse_available_services_val(
    available_services_val: &str,
    node_unique_id_str: &str,
) -> anyhow::Result<HashSet<QuickwitService>> {
    let mut available_services = HashSet::new();
    // Case where a node was started without any services.
    if available_services_val.is_empty() {
        return Ok(available_services);
    }
    let available_services_str = available_services_val.split(',');
    for service_str in available_services_str {
        if let Ok(service) = QuickwitService::try_from(service_str) {
            available_services.insert(service);
        } else {
            anyhow::bail!(
                "Unknown service found `{}` on node `{}` state.",
                service_str,
                node_unique_id_str,
            );
        }
    }
    Ok(available_services)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterState {
    state: ClusterStateSnapshot,
    live_nodes: Vec<NodeId>,
    dead_nodes: Vec<NodeId>,
}

/// Compute the gRPC port from the chitchat listen address for tests.
pub fn grpc_addr_from_listen_addr_for_test(listen_addr: SocketAddr) -> SocketAddr {
    let grpc_port = listen_addr.port() + 1u16;
    (listen_addr.ip(), grpc_port).into()
}

pub async fn create_cluster_for_test_with_id(
    node_id: u16,
    cluster_id: String,
    seeds: Vec<String>,
    services: &HashSet<QuickwitService>,
    transport: &dyn Transport,
) -> anyhow::Result<Cluster> {
    let listen_addr: SocketAddr = ([127, 0, 0, 1], node_id).into();
    let node_id = format!("node_{node_id}");
    let failure_detector_config = create_failure_detector_config_for_test();
    let cluster = Cluster::join(
        Member::new(
            node_id,
            1,
            listen_addr,
            services.clone(),
            grpc_addr_from_listen_addr_for_test(listen_addr),
        ),
        listen_addr,
        cluster_id,
        seeds,
        failure_detector_config,
        transport,
    )
    .await?;
    Ok(cluster)
}

/// Creates a failure detector config for tests.
fn create_failure_detector_config_for_test() -> FailureDetectorConfig {
    FailureDetectorConfig {
        phi_threshold: 6.0,
        initial_interval: GOSSIP_INTERVAL,
        ..Default::default()
    }
}

/// Creates a local cluster listening on a random port.
pub async fn create_cluster_for_test(
    seeds: Vec<String>,
    services: &[&str],
    transport: &dyn Transport,
) -> anyhow::Result<Cluster> {
    static NODE_AUTO_INCREMENT: AtomicU16 = AtomicU16::new(1u16);
    let node_id = NODE_AUTO_INCREMENT.fetch_add(1, Ordering::Relaxed);
    let services = services
        .iter()
        .map(|service_str| QuickwitService::try_from(*service_str))
        .collect::<Result<HashSet<_>, _>>()?;
    let cluster = create_cluster_for_test_with_id(
        node_id,
        "test-cluster".to_string(),
        seeds,
        &services,
        transport,
    )
    .await?;
    Ok(cluster)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport).await?;
        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .collect();
        let expected_members = vec![cluster.gossip_listen_addr];
        assert_eq!(members, expected_members);
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_available_searcher() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["searcher"], &transport).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["searcher"], &transport).await?;
        let cluster3 = create_cluster_for_test(vec![node_1], &["indexer"], &transport).await?;

        let mut expected_searchers = vec![
            grpc_addr_from_listen_addr_for_test(cluster1.gossip_listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster2.gossip_listen_addr),
        ];
        expected_searchers.sort();

        let wait_secs = Duration::from_secs(30);
        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }

        let mut searchers = cluster1.members_grpc_addresses_for_service(&QuickwitService::Searcher);
        searchers.sort();
        assert_eq!(searchers, expected_searchers);
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_available_indexer() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 =
            create_cluster_for_test(Vec::new(), &["searcher", "indexer"], &transport).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["indexer"], &transport).await?;
        let cluster3 =
            create_cluster_for_test(vec![node_1], &["indexer", "searcher"], &transport).await?;

        let mut expected_indexers = vec![
            grpc_addr_from_listen_addr_for_test(cluster1.gossip_listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster2.gossip_listen_addr),
            grpc_addr_from_listen_addr_for_test(cluster3.gossip_listen_addr),
        ];
        expected_indexers.sort();

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }

        let mut indexers = cluster1.members_grpc_addresses_for_service(&QuickwitService::Indexer);
        indexers.sort();
        assert_eq!(indexers, expected_indexers);

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &[], &transport).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test(vec![node_1.clone()], &[], &transport).await?;
        let cluster3 = create_cluster_for_test(vec![node_1], &[], &transport).await?;

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
            cluster1.gossip_listen_addr,
            cluster2.gossip_listen_addr,
            cluster3.gossip_listen_addr,
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
        let transport = ChannelTransport::default();

        let cluster1a = create_cluster_for_test_with_id(
            11u16,
            "cluster1".to_string(),
            Vec::new(),
            &HashSet::default(),
            &transport,
        )
        .await?;
        let cluster2a = create_cluster_for_test_with_id(
            21u16,
            "cluster2".to_string(),
            vec![cluster1a.gossip_listen_addr.to_string()],
            &HashSet::default(),
            &transport,
        )
        .await?;
        let cluster1b = create_cluster_for_test_with_id(
            12,
            "cluster1".to_string(),
            vec![
                cluster1a.gossip_listen_addr.to_string(),
                cluster2a.gossip_listen_addr.to_string(),
            ],
            &HashSet::default(),
            &transport,
        )
        .await?;
        let cluster2b = create_cluster_for_test_with_id(
            22,
            "cluster2".to_string(),
            vec![
                cluster1a.gossip_listen_addr.to_string(),
                cluster2a.gossip_listen_addr.to_string(),
            ],
            &HashSet::default(),
            &transport,
        )
        .await?;

        let wait_secs = Duration::from_secs(10);

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
        let mut expected_members_a =
            vec![cluster1a.gossip_listen_addr, cluster1b.gossip_listen_addr];
        expected_members_a.sort();
        assert_eq!(members_a, expected_members_a);

        let members_b: Vec<SocketAddr> = cluster2a
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members_b =
            vec![cluster2a.gossip_listen_addr, cluster2b.gossip_listen_addr];
        expected_members_b.sort();
        assert_eq!(members_b, expected_members_b);

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_issue_1018() -> anyhow::Result<()> {
        let cluster_id = "unified-cluster";
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test_with_id(
            1u16,
            cluster_id.to_string(),
            Vec::new(),
            &HashSet::default(),
            &transport,
        )
        .await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            2u16,
            cluster_id.to_string(),
            vec![node_1.clone()],
            &HashSet::default(),
            &transport,
        )
        .await?;

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
        let mut expected_members = vec![cluster1.gossip_listen_addr, cluster2.gossip_listen_addr];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.gossip_listen_addr;
        cluster2.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        let grpc_port = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr: SocketAddr = ([127, 0, 0, 1], grpc_port).into();
        let cluster2 = Cluster::join(
            Member::new(
                "new_id".to_string(),
                1,
                cluster2_listen_addr,
                HashSet::default(),
                grpc_addr,
            ),
            cluster2_listen_addr,
            cluster_id.to_string(),
            vec![node_1],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;

        for cluster in [cluster1, cluster2] {
            cluster
                .wait_for_members(
                    |members| {
                        assert!(members.len() <= 2);
                        for member in members {
                            assert!(["node_1", "new_id"].contains(&member.node_unique_id.as_str()))
                        }
                        members.len() == 2
                    },
                    wait_secs,
                )
                .await
                .unwrap();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_3_nodes_issue_1018() -> anyhow::Result<()> {
        let cluster_id = "three-nodes-cluster";
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test_with_id(
            1u16,
            cluster_id.to_string(),
            Vec::new(),
            &HashSet::default(),
            &transport,
        )
        .await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            2u16,
            cluster_id.to_string(),
            vec![node_1.clone()],
            &HashSet::default(),
            &transport,
        )
        .await?;
        let node_2 = cluster2.gossip_listen_addr.to_string();
        let cluster3 = create_cluster_for_test_with_id(
            3u16,
            cluster_id.to_string(),
            vec![node_2],
            &HashSet::default(),
            &transport,
        )
        .await?;

        let wait_secs = Duration::from_secs(4);

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
            cluster1.gossip_listen_addr,
            cluster2.gossip_listen_addr,
            cluster3.gossip_listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.gossip_listen_addr;
        let cluster3_listen_addr = cluster3.gossip_listen_addr;
        cluster2.shutdown().await;
        cluster3.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        let grpc_port = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr: SocketAddr = ([127, 0, 0, 1], grpc_port).into();
        let cluster2 = Cluster::join(
            Member::new(
                "new_node_2".to_string(),
                1,
                cluster2_listen_addr,
                HashSet::default(),
                grpc_addr,
            ),
            cluster2_listen_addr,
            cluster_id.to_string(),
            vec![node_1],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;
        let node_2 = cluster2.gossip_listen_addr.to_string();

        let cluster3 = Cluster::join(
            Member::new(
                "new_node_3".to_string(),
                1,
                cluster3_listen_addr,
                HashSet::default(),
                grpc_addr,
            ),
            cluster3_listen_addr,
            cluster_id.to_string(),
            vec![node_2],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }

        let expected_member_ids: HashSet<String> = ["new_node_3", "node_1", "new_node_2"]
            .iter()
            .map(ToString::to_string)
            .collect();

        for cluster in [cluster1, cluster2, cluster3] {
            let member_ids: HashSet<String> = cluster
                .members()
                .iter()
                .map(|member| member.node_unique_id.clone())
                .collect();
            assert_eq!(&member_ids, &expected_member_ids);
        }
        Ok(())
    }
}
