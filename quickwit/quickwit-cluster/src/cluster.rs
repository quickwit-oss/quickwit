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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chitchat::transport::{ChannelTransport, Transport};
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, ClusterStateSnapshot, FailureDetectorConfig,
    NodeId, NodeState,
};
use itertools::Itertools;
use quickwit_proto::indexing_api::IndexingTask;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::error::{ClusterError, ClusterResult};
use crate::member::{
    build_cluster_members, ClusterMember, ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY,
    INDEXING_TASK_PREFIX, INDEXING_TASK_SEPARATOR,
};
use crate::QuickwitService;

const GOSSIP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(25)
} else {
    Duration::from_secs(1)
};

const MARKED_FOR_DELETION_GRACE_PERIOD: usize = if cfg!(any(test, feature = "testsuite")) {
    100 // ~ HEARTBEAT * 100 = 2.5 seconds.
} else {
    5_000 // ~ HEARTBEAT * 5_000 ~ 4 hours.
};

// Health key and values used to store node's health in chitchat state.
const HEALTH_KEY: &str = "health";
const HEALTH_VALUE_READY: &str = "READY";
const HEALTH_VALUE_NOT_READY: &str = "NOT_READY";

/// This is an implementation of a cluster using Chitchat.
pub struct Cluster {
    /// ID of the cluster joined.
    pub cluster_id: String,
    /// Node ID.
    pub node_id: NodeId,
    /// A socket address that represents itself.
    pub gossip_listen_addr: SocketAddr,

    /// A handle to command Chitchat.
    /// If it is dropped, the chitchat server will stop.
    chitchat_handle: ChitchatHandle,

    /// A watcher that receives ready members changes from chitchat.
    members_rx: watch::Receiver<Vec<ClusterMember>>,

    /// A stop flag of cluster monitoring task.
    /// Once the cluster is created, a task to monitor cluster events will be started.
    /// Nodes do not need to be monitored for events once they are detached from the cluster.
    /// You need to update this value to get out of the task loop.
    stop: Arc<AtomicBool>,
}

fn is_ready_predicate(node_state: &NodeState) -> bool {
    node_state
        .get(HEALTH_KEY)
        .map(|health_value| health_value == HEALTH_VALUE_READY)
        .unwrap_or(false)
}

impl Cluster {
    /// Create a cluster given a host key and a listen address.
    /// When a cluster is created, the thread that monitors cluster events
    /// will be started at the same time.
    pub async fn join(
        self_node: ClusterMember,
        gossip_listen_addr: SocketAddr,
        cluster_id: String,
        peer_seed_addrs: Vec<String>,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
    ) -> ClusterResult<Self> {
        info!(
            cluster_id = %cluster_id,
            node_id = %self_node.node_id,
            enabled_services = ?self_node.enabled_services,
            gossip_listen_addr = %gossip_listen_addr,
            gossip_advertise_addr = %self_node.gossip_advertise_addr,
            grpc_advertise_addr = %self_node.grpc_advertise_addr,
            peer_seed_addrs = %peer_seed_addrs.join(", "),
            "Joining cluster."
        );
        let self_node_id = NodeId::from(self_node.clone());
        let chitchat_config = ChitchatConfig {
            cluster_id: cluster_id.clone(),
            node_id: self_node_id.clone(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr: gossip_listen_addr,
            seed_nodes: peer_seed_addrs,
            failure_detector_config,
            is_ready_predicate: Some(Box::new(is_ready_predicate)),
            marked_for_deletion_grace_period: MARKED_FOR_DELETION_GRACE_PERIOD,
        };
        let chitchat_handle = spawn_chitchat(
            chitchat_config,
            vec![
                (
                    GRPC_ADVERTISE_ADDR_KEY.to_string(),
                    self_node.grpc_advertise_addr.to_string(),
                ),
                (
                    ENABLED_SERVICES_KEY.to_string(),
                    self_node
                        .enabled_services
                        .iter()
                        .map(|service| service.as_str())
                        .join(","),
                ),
                (HEALTH_KEY.to_string(), HEALTH_VALUE_NOT_READY.to_string()),
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
            node_id: self_node_id.clone(),
            gossip_listen_addr,
            chitchat_handle,
            members_rx: members_receiver,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Add itself as the initial member of the cluster.
        let initial_members: Vec<ClusterMember> = vec![self_node.clone()];
        if members_sender.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        // Prepare to start a task that will monitor cluster events.
        let task_stop = cluster.stop.clone();
        tokio::task::spawn(async move {
            let mut node_change_receiver = chitchat.lock().await.ready_nodes_watcher();

            while let Some(mut members_set) = node_change_receiver.next().await {
                let cluster_state_snapshot = chitchat.lock().await.state_snapshot();
                // `members_set` does not contain `self`.
                members_set.insert(self_node_id.clone());
                let cluster_members = build_cluster_members(members_set, &cluster_state_snapshot);
                if task_stop.load(Ordering::Relaxed) {
                    debug!("Received a stop signal. Stopping.");
                    break;
                }

                if members_sender.send(cluster_members).is_err() {
                    // Somehow the cluster has been dropped.
                    error!("Failed to send members list. Stopping.");
                    break;
                }
            }
            Result::<(), anyhow::Error>::Ok(())
        });

        Ok(cluster)
    }

    /// Returns a [`WatchStream`] for monitoring change of ready `members`.
    /// Note that it does not monitor changes of properties of members, this means
    /// that a [`ClusterMember`] has no guarantee to have its properties up-to-date.
    /// To get the latest properties of a [`ClusterMember`], use
    /// `Self::ready_members_from_chitchat_state`.
    pub fn ready_member_change_watcher(&self) -> WatchStream<Vec<ClusterMember>> {
        WatchStream::new(self.members_rx.clone())
    }

    /// Returns the last [`ClusterMember`] sent by chitchat.
    /// Note that a [`ClusterMember`] has no guarantee to have its properties up-to-date.
    /// To get the latest properties of a [`ClusterMember`], use
    /// `Self::ready_members_from_chitchat_state`.
    pub async fn ready_members(&self) -> Vec<ClusterMember> {
        self.members_rx.borrow().clone()
    }

    /// Returns ready [`ClusterMember`]s built directly from the current chitchat state.
    /// This guarantees to have members with up-to-date properties.
    pub async fn ready_members_from_chitchat_state(&self) -> Vec<ClusterMember> {
        let cluster_snapshot = self.snapshot().await;
        let mut ready_nodes = cluster_snapshot.ready_nodes.clone();
        let is_self_node_ready = cluster_snapshot
            .chitchat_state_snapshot
            .node_states
            .get(&cluster_snapshot.self_node_id.id)
            .map(is_ready_predicate)
            .unwrap_or(false);
        if is_self_node_ready {
            ready_nodes.insert(cluster_snapshot.self_node_id.clone());
        }
        build_cluster_members(ready_nodes, &cluster_snapshot.chitchat_state_snapshot)
    }

    // Returns the gRPC addresses of the members providing the specified service. Used for testing.
    #[cfg(test)]
    pub fn members_grpc_advertise_addr_for_service(
        &self,
        service: &QuickwitService,
    ) -> Vec<SocketAddr> {
        self.members_rx
            .borrow()
            .iter()
            .filter(|member| member.enabled_services.contains(service))
            .map(|member| member.grpc_advertise_addr)
            .collect_vec()
    }

    /// Set a key-value pair on the cluster node's state.
    pub async fn set_key_value<K: ToString, V: ToString>(&self, key: K, value: V) {
        let chitchat = self.chitchat_handle.chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        chitchat_guard.self_node_state().set(key, value);
    }

    pub async fn snapshot(&self) -> ClusterSnapshot {
        let chitchat = self.chitchat_handle.chitchat();
        let chitchat_guard = chitchat.lock().await;

        let chitchat_state_snapshot = chitchat_guard.state_snapshot();
        let ready_nodes = chitchat_guard
            .ready_nodes()
            .cloned()
            .collect::<HashSet<_>>();
        let dead_nodes = chitchat_guard.dead_nodes().cloned().collect::<HashSet<_>>();
        let live_nodes = chitchat_guard.live_nodes().cloned().collect::<HashSet<_>>();
        ClusterSnapshot {
            cluster_id: self.cluster_id.clone(),
            self_node_id: self.node_id.clone(),
            chitchat_state_snapshot,
            ready_nodes,
            live_nodes,
            dead_nodes,
        }
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.gossip_listen_addr, "Shutting down chitchat.");
        let result = self.chitchat_handle.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.gossip_listen_addr, error = ?error, "Error while shutting down chitchat.");
        }

        self.stop.store(true, Ordering::Relaxed);
    }

    /// Waiting for the predicate to hold true for the cluster's members.
    pub async fn wait_for_members<F>(
        self: &Cluster,
        mut predicate: F,
        timeout_after: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&[ClusterMember]) -> bool,
    {
        timeout(
            timeout_after,
            self.ready_member_change_watcher()
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await
        .map_err(|_| anyhow!("Waiting for members deadline has elapsed."))?;
        Ok(())
    }

    /// Set self readiness value.
    pub async fn set_self_node_ready(&self, ready: bool) {
        let health_value = if ready {
            HEALTH_VALUE_READY
        } else {
            HEALTH_VALUE_NOT_READY
        };
        self.set_key_value(HEALTH_KEY, health_value).await
    }

    /// Returns true if self is ready.
    pub async fn is_self_node_ready(&self) -> bool {
        let chitchat = self.chitchat_handle.chitchat();
        let mut chitchat_mutex = chitchat.lock().await;
        let node_state = chitchat_mutex.self_node_state();
        is_ready_predicate(node_state)
    }

    /// Updates indexing tasks in chitchat state.
    /// Tasks are grouped by (index_id, source_id), each group is stored in a key as follows:
    /// - key: `{INDEXING_TASK_PREFIX}{INDEXING_TASK_SEPARATOR}{index_id}{INDEXING_TASK_SEPARATOR}{source_id}`
    /// - value: Number of indexing tasks in the group.
    /// Keys present in chitchat state but not in the given `indexing_tasks` are marked for
    /// deletion.
    pub async fn update_self_node_indexing_tasks(
        &self,
        indexing_tasks: &[IndexingTask],
    ) -> anyhow::Result<()> {
        let chitchat = self.chitchat_handle.chitchat();
        let mut chitchat_guard = chitchat.lock().await;
        let mut current_indexing_tasks_keys: HashSet<_> = chitchat_guard
            .self_node_state()
            .iter_key_values(|key, _| key.starts_with(INDEXING_TASK_PREFIX))
            .map(|(key, _)| key.to_string())
            .collect();
        for (indexing_task, indexing_tasks_group) in
            indexing_tasks.iter().group_by(|&task| task).into_iter()
        {
            let key = format!(
                "{INDEXING_TASK_PREFIX}{INDEXING_TASK_SEPARATOR}{}{INDEXING_TASK_SEPARATOR}{}",
                indexing_task.index_id, indexing_task.source_id
            );
            current_indexing_tasks_keys.remove(&key);
            chitchat_guard
                .self_node_state()
                .set(key, indexing_tasks_group.count().to_string());
        }
        for obsolete_task_key in current_indexing_tasks_keys {
            chitchat_guard
                .self_node_state()
                .mark_for_deletion(&obsolete_task_key);
        }
        Ok(())
    }
}

// Not used within the code, used for documentation.
#[derive(Debug, utoipa::ToSchema)]
pub struct NodeIdSchema {
    #[schema(example = "node-1")]
    /// The unique identifier of the node in the cluster.
    pub id: String,

    #[schema(example = "127.0.0.1:8000", value_type = String)]
    /// The SocketAddr other peers should use to communicate.
    pub gossip_public_address: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug, utoipa::ToSchema)]
pub struct ClusterSnapshot {
    #[schema(example = "qw-cluster-1")]
    /// The ID of the cluster that the node is a part of.
    pub cluster_id: String,

    #[schema(value_type = NodeIdSchema)]
    /// The unique ID of the current node.
    pub self_node_id: NodeId,

    #[schema(
        value_type = Object,
        example = json!({
            "key_values": {
                "grpc_advertise_addr": "127.0.0.1:8080",
                "enabled_services": "searcher",
            },
            "max_version": 5,
        })
    )]
    /// A snapshot of the current cluster state.
    pub chitchat_state_snapshot: ClusterStateSnapshot,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of node IDs that are ready to handle operations.
    pub ready_nodes: HashSet<NodeId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of cluster node IDs that are alive.
    pub live_nodes: HashSet<NodeId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of node IDs flagged as dead or faulty.
    pub dead_nodes: HashSet<NodeId>,
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
    enabled_services: &HashSet<QuickwitService>,
    transport: &dyn Transport,
    self_node_is_ready: bool,
) -> anyhow::Result<Cluster> {
    let gossip_advertise_addr: SocketAddr = ([127, 0, 0, 1], node_id).into();
    let node_id = format!("node_{node_id}");
    let failure_detector_config = create_failure_detector_config_for_test();
    let cluster = Cluster::join(
        ClusterMember::new(
            node_id,
            1,
            enabled_services.clone(),
            gossip_advertise_addr,
            grpc_addr_from_listen_addr_for_test(gossip_advertise_addr),
            Vec::new(),
        ),
        gossip_advertise_addr,
        cluster_id,
        seeds,
        failure_detector_config,
        transport,
    )
    .await?;
    cluster.set_self_node_ready(self_node_is_ready).await;
    Ok(cluster)
}

/// Creates a failure detector config for tests.
fn create_failure_detector_config_for_test() -> FailureDetectorConfig {
    FailureDetectorConfig {
        phi_threshold: 3.0,
        initial_interval: GOSSIP_INTERVAL,
        ..Default::default()
    }
}

/// Creates a local cluster listening on a random port.
pub async fn create_cluster_for_test(
    seeds: Vec<String>,
    enabled_services: &[&str],
    transport: &dyn Transport,
    self_node_is_ready: bool,
) -> anyhow::Result<Cluster> {
    static NODE_AUTO_INCREMENT: AtomicU16 = AtomicU16::new(1u16);
    let node_id = NODE_AUTO_INCREMENT.fetch_add(1, Ordering::Relaxed);
    let enabled_services = enabled_services
        .iter()
        .map(|service_str| QuickwitService::from_str(service_str))
        .collect::<Result<HashSet<_>, _>>()?;
    let cluster = create_cluster_for_test_with_id(
        node_id,
        "test-cluster".to_string(),
        seeds,
        &enabled_services,
        transport,
        self_node_is_ready,
    )
    .await?;
    Ok(cluster)
}

/// Creates a fake cluster for the CLI indexing commands.
pub async fn create_fake_cluster_for_cli() -> anyhow::Result<Cluster> {
    create_cluster_for_test(Vec::new(), &[], &ChannelTransport::default(), false).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use itertools::Itertools;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_proto::indexing_api::IndexingTask;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node_readiness() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false).await?;
        let members: Vec<SocketAddr> = cluster
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
            .collect();
        let expected_members = vec![cluster.gossip_listen_addr];
        assert_eq!(members, expected_members);
        assert!(!cluster.is_self_node_ready().await);
        assert!(cluster.ready_members_from_chitchat_state().await.is_empty());
        let cluster_snapshot = cluster.snapshot().await;
        let self_node_state_not_ready = cluster_snapshot
            .chitchat_state_snapshot
            .node_states
            .get(&cluster.node_id.id)
            .unwrap();
        assert_eq!(
            self_node_state_not_ready.get(HEALTH_KEY).unwrap(),
            HEALTH_VALUE_NOT_READY
        );
        cluster.set_self_node_ready(true).await;
        assert!(cluster.is_self_node_ready().await);
        assert_eq!(cluster.ready_members_from_chitchat_state().await.len(), 1);
        let cluster_state = cluster.snapshot().await;
        let self_node_state_ready = cluster_state
            .chitchat_state_snapshot
            .node_states
            .get(&cluster.node_id.id)
            .unwrap();
        assert_eq!(
            self_node_state_ready.get(HEALTH_KEY).unwrap(),
            HEALTH_VALUE_READY
        );
        assert!(!cluster.ready_members().await.is_empty(),);
        cluster.set_self_node_ready(false).await;
        assert!(!cluster.is_self_node_ready().await);
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_members_built_from_chitchat_state() {
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let cluster2 = create_cluster_for_test(
            vec![cluster1.gossip_listen_addr.to_string()],
            &["indexer", "metastore"],
            &transport,
            true,
        )
        .await
        .unwrap();
        let indexing_task = IndexingTask {
            index_id: "index-1".to_string(),
            source_id: "source-1".to_string(),
        };
        cluster2
            .set_key_value(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:1001")
            .await;
        cluster2
            .update_self_node_indexing_tasks(&[indexing_task.clone(), indexing_task.clone()])
            .await
            .unwrap();
        cluster1
            .wait_for_members(|members| members.len() == 2, Duration::from_secs(30))
            .await
            .unwrap();
        let members = cluster1.ready_members_from_chitchat_state().await;
        let member_node_1 = members
            .iter()
            .find(|member| member.chitchat_id() == cluster1.node_id.id)
            .unwrap();
        let member_node_2 = members
            .iter()
            .find(|member| member.chitchat_id() == cluster2.node_id.id)
            .unwrap();
        assert_eq!(
            member_node_1.enabled_services,
            HashSet::from_iter([QuickwitService::Indexer])
        );
        assert!(member_node_1.indexing_tasks.is_empty());
        assert_eq!(
            member_node_2.grpc_advertise_addr,
            ([127, 0, 0, 1], 1001).into()
        );
        assert_eq!(
            member_node_2.enabled_services,
            HashSet::from_iter([QuickwitService::Indexer, QuickwitService::Metastore].into_iter())
        );
        assert_eq!(
            member_node_2.indexing_tasks,
            vec![indexing_task.clone(), indexing_task.clone()]
        );
    }

    #[tokio::test]
    async fn test_chitchat_state_set_high_number_of_tasks() {
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let cluster2 = Arc::new(
            create_cluster_for_test(
                vec![cluster1.gossip_listen_addr.to_string()],
                &["indexer", "metastore"],
                &transport,
                true,
            )
            .await
            .unwrap(),
        );
        let cluster3 = Arc::new(
            create_cluster_for_test(
                vec![cluster1.gossip_listen_addr.to_string()],
                &["indexer", "metastore"],
                &transport,
                true,
            )
            .await
            .unwrap(),
        );
        let mut random_generator = rand::thread_rng();
        let indexing_tasks = (0..1_000)
            .map(|_| {
                let index_id = random_generator.gen_range(0..=10_000);
                let source_id = random_generator.gen_range(0..=100);
                IndexingTask {
                    index_id: format!("index-{index_id}"),
                    source_id: format!("source-{source_id}"),
                }
            })
            .collect_vec();
        cluster1
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await
            .unwrap();
        for cluster in [&cluster2, &cluster3] {
            let cluster_clone = cluster.clone();
            let indexing_tasks_clone = indexing_tasks.clone();
            wait_until_predicate(
                move || {
                    test_indexing_tasks_in_given_node(
                        cluster_clone.clone(),
                        cluster1.node_id.gossip_public_address,
                        indexing_tasks_clone.clone(),
                    )
                },
                Duration::from_secs(4),
                Duration::from_millis(500),
            )
            .await
            .unwrap();
        }

        // Mark tasks for deletion.
        cluster1.update_self_node_indexing_tasks(&[]).await.unwrap();
        for cluster in [&cluster2, &cluster3] {
            let cluster_clone = cluster.clone();
            wait_until_predicate(
                move || {
                    test_indexing_tasks_in_given_node(
                        cluster_clone.clone(),
                        cluster1.node_id.gossip_public_address,
                        Vec::new(),
                    )
                },
                Duration::from_secs(4),
                Duration::from_millis(500),
            )
            .await
            .unwrap();
        }

        // Re-add tasks.
        cluster1
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await
            .unwrap();
        for cluster in [&cluster2, &cluster3] {
            let cluster_clone = cluster.clone();
            let indexing_tasks_clone = indexing_tasks.clone();
            wait_until_predicate(
                move || {
                    test_indexing_tasks_in_given_node(
                        cluster_clone.clone(),
                        cluster1.node_id.gossip_public_address,
                        indexing_tasks_clone.clone(),
                    )
                },
                Duration::from_secs(4),
                Duration::from_millis(500),
            )
            .await
            .unwrap();
        }
    }

    async fn test_indexing_tasks_in_given_node(
        cluster: Arc<Cluster>,
        gossip_public_address: SocketAddr,
        indexing_tasks: Vec<IndexingTask>,
    ) -> bool {
        let members = cluster.ready_members_from_chitchat_state().await;
        let node_opt = members
            .iter()
            .find(|member| member.gossip_advertise_addr == gossip_public_address);
        let Some(node) = node_opt else {
            return false;
        };
        let node_grouped_tasks: HashMap<IndexingTask, usize> = node
            .indexing_tasks
            .iter()
            .group_by(|task| (*task).clone())
            .into_iter()
            .map(|(key, group)| (key, group.count()))
            .collect();
        let grouped_tasks: HashMap<IndexingTask, usize> = indexing_tasks
            .iter()
            .group_by(|task| (*task).clone())
            .into_iter()
            .map(|(key, group)| (key, group.count()))
            .collect();
        node_grouped_tasks == grouped_tasks
    }

    #[tokio::test]
    async fn test_chitchat_state_with_malformatted_indexing_task_key() {
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        {
            let chitchat_handle = cluster1.chitchat_handle.chitchat();
            let mut chitchat_guard = chitchat_handle.lock().await;
            chitchat_guard.self_node_state().set(
                format!("{INDEXING_TASK_PREFIX}{INDEXING_TASK_SEPARATOR}my_good_index{INDEXING_TASK_SEPARATOR}my_source"),
                "2".to_string(),
            );
            chitchat_guard.self_node_state().set(
                format!("{INDEXING_TASK_PREFIX}{INDEXING_TASK_SEPARATOR}my_bad_index{INDEXING_TASK_SEPARATOR}my_source"),
                "malformatted value".to_string(),
            );
        }
        let member = cluster1.ready_members_from_chitchat_state().await;
        assert_eq!(member[0].indexing_tasks.len(), 2);
    }
    #[tokio::test]
    async fn test_cluster_available_searcher() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["searcher"], &transport, true).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["searcher"], &transport, true).await?;
        let cluster3 =
            create_cluster_for_test(vec![node_1], &["indexer"], &transport, true).await?;
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

        let mut searchers =
            cluster1.members_grpc_advertise_addr_for_service(&QuickwitService::Searcher);
        searchers.sort();
        assert_eq!(searchers, expected_searchers);
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_available_indexer() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 =
            create_cluster_for_test(Vec::new(), &["searcher", "indexer"], &transport, true).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["indexer"], &transport, true).await?;
        let cluster3 =
            create_cluster_for_test(vec![node_1], &["indexer", "searcher"], &transport, true)
                .await?;
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

        let mut indexers =
            cluster1.members_grpc_advertise_addr_for_service(&QuickwitService::Indexer);
        indexers.sort();
        assert_eq!(indexers, expected_indexers);

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &[], &transport, true).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test(vec![node_1.clone()], &[], &transport, true).await?;
        let cluster3 = create_cluster_for_test(vec![node_1], &[], &transport, true).await?;

        let wait_secs = Duration::from_secs(30);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
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
            true,
        )
        .await?;
        let cluster2a = create_cluster_for_test_with_id(
            21u16,
            "cluster2".to_string(),
            vec![cluster1a.gossip_listen_addr.to_string()],
            &HashSet::default(),
            &transport,
            true,
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
            true,
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
            true,
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
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
            .sorted()
            .collect();
        let mut expected_members_a =
            vec![cluster1a.gossip_listen_addr, cluster1b.gossip_listen_addr];
        expected_members_a.sort();
        assert_eq!(members_a, expected_members_a);

        let members_b: Vec<SocketAddr> = cluster2a
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
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
            true,
        )
        .await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            2u16,
            cluster_id.to_string(),
            vec![node_1.clone()],
            &HashSet::default(),
            &transport,
            true,
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
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
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
            ClusterMember::new(
                "new_id".to_string(),
                1,
                HashSet::default(),
                cluster2_listen_addr,
                grpc_addr,
                Vec::new(),
            ),
            cluster2_listen_addr,
            cluster_id.to_string(),
            vec![node_1],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;
        cluster2.set_self_node_ready(true).await;

        for cluster in [cluster1, cluster2] {
            cluster
                .wait_for_members(
                    |members| {
                        assert!(members.len() <= 2);
                        for member in members {
                            assert!(["node_1", "new_id"].contains(&member.node_id.as_str()))
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
            true,
        )
        .await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id(
            2u16,
            cluster_id.to_string(),
            vec![node_1.clone()],
            &HashSet::default(),
            &transport,
            true,
        )
        .await?;
        let node_2 = cluster2.gossip_listen_addr.to_string();
        let cluster3 = create_cluster_for_test_with_id(
            3u16,
            cluster_id.to_string(),
            vec![node_2],
            &HashSet::default(),
            &transport,
            true,
        )
        .await?;

        let wait_secs = Duration::from_secs(5);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .ready_members()
            .await
            .iter()
            .map(|member| member.gossip_advertise_addr)
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
            ClusterMember::new(
                "new_node_2".to_string(),
                1,
                HashSet::default(),
                cluster2_listen_addr,
                grpc_addr,
                Vec::new(),
            ),
            cluster2_listen_addr,
            cluster_id.to_string(),
            vec![node_1],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;
        cluster2.set_self_node_ready(true).await;
        let node_2 = cluster2.gossip_listen_addr.to_string();

        let cluster3 = Cluster::join(
            ClusterMember::new(
                "new_node_3".to_string(),
                1,
                HashSet::default(),
                cluster3_listen_addr,
                grpc_addr,
                Vec::new(),
            ),
            cluster3_listen_addr,
            cluster_id.to_string(),
            vec![node_2],
            create_failure_detector_config_for_test(),
            &transport,
        )
        .await?;
        cluster3.set_self_node_ready(true).await;

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
                .ready_members()
                .await
                .iter()
                .map(|member| member.node_id.clone())
                .collect();
            assert_eq!(&member_ids, &expected_member_ids);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_with_node_becoming_ready() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster1 =
            create_cluster_for_test(Vec::new(), &["searcher", "indexer"], &transport, true).await?;
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["indexer"], &transport, false).await?;
        let wait_secs = Duration::from_secs(30);

        // Cluster 2 sees 2 members: himself and node 1.
        cluster2
            .wait_for_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();
        // However cluster 1 sees only 1 member, himself.
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();
        // Cluster2 now becomes ready.
        cluster2.set_self_node_ready(true).await;
        // And cluster1 now sees 2 members.
        cluster1
            .wait_for_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        Ok(())
    }
}
