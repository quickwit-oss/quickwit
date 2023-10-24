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

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chitchat::transport::Transport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, ClusterStateSnapshot,
    FailureDetectorConfig, NodeState,
};
use futures::Stream;
use itertools::Itertools;
use quickwit_proto::indexing::IndexingTask;
use quickwit_proto::types::NodeId;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time::timeout;
use tokio_stream::wrappers::{UnboundedReceiverStream, WatchStream};
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::change::{compute_cluster_change_events, ClusterChange};
use crate::member::{
    build_cluster_member, ClusterMember, NodeStateExt, ENABLED_SERVICES_KEY,
    GRPC_ADVERTISE_ADDR_KEY, INDEXING_TASK_PREFIX, READINESS_KEY, READINESS_VALUE_NOT_READY,
    READINESS_VALUE_READY,
};
use crate::ClusterNode;

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

#[derive(Clone)]
pub struct Cluster {
    cluster_id: String,
    self_chitchat_id: ChitchatId,
    /// Socket address (UDP) the node listens on for receiving gossip messages.
    gossip_listen_addr: SocketAddr,
    inner: Arc<RwLock<InnerCluster>>,
}

impl Debug for Cluster {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Cluster")
            .field("cluster_id", &self.cluster_id)
            .field("self_node_id", &self.self_chitchat_id.node_id)
            .field("gossip_listen_addr", &self.gossip_listen_addr)
            .field(
                "gossip_advertise_addr",
                &self.self_chitchat_id.gossip_advertise_addr,
            )
            .finish()
    }
}

impl Cluster {
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn self_chitchat_id(&self) -> &ChitchatId {
        &self.self_chitchat_id
    }

    pub fn self_node_id(&self) -> &str {
        &self.self_chitchat_id.node_id
    }

    pub fn gossip_listen_addr(&self) -> SocketAddr {
        self.gossip_listen_addr
    }

    pub fn gossip_advertise_addr(&self) -> SocketAddr {
        self.self_chitchat_id.gossip_advertise_addr
    }

    pub async fn join(
        cluster_id: String,
        self_node: ClusterMember,
        gossip_listen_addr: SocketAddr,
        peer_seed_addrs: Vec<String>,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
    ) -> anyhow::Result<Self> {
        info!(
            cluster_id=%cluster_id,
            node_id=%self_node.node_id,
            enabled_services=?self_node.enabled_services,
            gossip_listen_addr=%gossip_listen_addr,
            gossip_advertise_addr=%self_node.gossip_advertise_addr,
            grpc_advertise_addr=%self_node.grpc_advertise_addr,
            peer_seed_addrs=%peer_seed_addrs.join(", "),
            "Joining cluster."
        );
        let chitchat_config = ChitchatConfig {
            cluster_id: cluster_id.clone(),
            chitchat_id: self_node.chitchat_id(),
            listen_addr: gossip_listen_addr,
            seed_nodes: peer_seed_addrs,
            failure_detector_config,
            gossip_interval: GOSSIP_INTERVAL,
            marked_for_deletion_grace_period: MARKED_FOR_DELETION_GRACE_PERIOD,
        };
        let chitchat_handle = spawn_chitchat(
            chitchat_config,
            vec![
                (
                    ENABLED_SERVICES_KEY.to_string(),
                    self_node
                        .enabled_services
                        .iter()
                        .map(|service| service.as_str())
                        .join(","),
                ),
                (
                    GRPC_ADVERTISE_ADDR_KEY.to_string(),
                    self_node.grpc_advertise_addr.to_string(),
                ),
                (
                    READINESS_KEY.to_string(),
                    READINESS_VALUE_NOT_READY.to_string(),
                ),
            ],
            transport,
        )
        .await?;

        let chitchat = chitchat_handle.chitchat();
        let live_nodes_stream = chitchat.lock().await.live_nodes_watcher();
        let (ready_members_tx, ready_members_rx) = watch::channel(Vec::new());

        spawn_ready_members_task(cluster_id.clone(), live_nodes_stream, ready_members_tx);
        let inner = InnerCluster {
            cluster_id: cluster_id.clone(),
            self_chitchat_id: self_node.chitchat_id(),
            chitchat_handle,
            live_nodes: BTreeMap::new(),
            change_stream_subscribers: Vec::new(),
            ready_members_rx,
        };
        let cluster = Cluster {
            cluster_id,
            self_chitchat_id: self_node.chitchat_id(),
            gossip_listen_addr,
            inner: Arc::new(RwLock::new(inner)),
        };
        spawn_ready_nodes_change_stream_task(cluster.clone()).await;
        Ok(cluster)
    }

    /// Deprecated: this is going away soon.
    pub async fn ready_members(&self) -> Vec<ClusterMember> {
        self.inner.read().await.ready_members_rx.borrow().clone()
    }

    /// Deprecated: this is going away soon.
    pub async fn ready_members_watcher(&self) -> WatchStream<Vec<ClusterMember>> {
        WatchStream::new(self.inner.read().await.ready_members_rx.clone())
    }

    /// Returns a stream of changes affecting the set of ready nodes in the cluster.
    pub async fn ready_nodes_change_stream(&self) -> impl Stream<Item = ClusterChange> {
        // The subscriber channel must be unbounded because we do no want to block when sending the
        // events.
        let (change_stream_tx, change_stream_rx) = mpsc::unbounded_channel();
        let mut inner = self.inner.write().await;
        for node in inner.live_nodes.values() {
            if node.is_ready() {
                change_stream_tx
                    .send(ClusterChange::Add(node.clone()))
                    .expect("The receiver end of the channel should be open.");
            }
        }
        inner.change_stream_subscribers.push(change_stream_tx);
        UnboundedReceiverStream::new(change_stream_rx)
    }

    /// Returns whether the self node is ready.
    pub async fn is_self_node_ready(&self) -> bool {
        self.chitchat()
            .await
            .lock()
            .await
            .node_state(&self.self_chitchat_id)
            .expect("The self node should always be present in the set of live nodes.")
            .is_ready()
    }

    /// Sets the self node's readiness.
    pub async fn set_self_node_readiness(&self, readiness: bool) {
        let readiness_value = if readiness {
            READINESS_VALUE_READY
        } else {
            READINESS_VALUE_NOT_READY
        };
        self.set_self_key_value(READINESS_KEY, readiness_value)
            .await
    }

    /// Sets a key-value pair on the cluster node's state.
    pub async fn set_self_key_value<K: Into<String>, V: Into<String>>(&self, key: K, value: V) {
        self.chitchat()
            .await
            .lock()
            .await
            .self_node_state()
            .set(key, value);
    }

    /// Waits until the predicate holds true for the set of ready members.
    pub async fn wait_for_ready_members<F>(
        &self,
        mut predicate: F,
        timeout_after: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&[ClusterMember]) -> bool,
    {
        timeout(
            timeout_after,
            self.ready_members_watcher()
                .await
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await
        .context("deadline has passed before predicate held true")?;
        Ok(())
    }

    /// Returns a snapshot of the cluster state, including the underlying Chitchat state.
    pub async fn snapshot(&self) -> ClusterSnapshot {
        let chitchat = self.chitchat().await;
        let chitchat_guard = chitchat.lock().await;
        let chitchat_state_snapshot = chitchat_guard.state_snapshot();
        let mut ready_nodes = HashSet::new();
        let mut live_nodes = HashSet::new();

        for chitchat_id in chitchat_guard.live_nodes().cloned() {
            let node_state = chitchat_guard.node_state(&chitchat_id).expect(
                "The node should always be present in the cluster state because we hold the \
                 Chitchat mutex.",
            );
            if node_state.is_ready() {
                ready_nodes.insert(chitchat_id);
            } else {
                live_nodes.insert(chitchat_id);
            }
        }
        let dead_nodes = chitchat_guard.dead_nodes().cloned().collect::<HashSet<_>>();

        ClusterSnapshot {
            cluster_id: self.cluster_id.clone(),
            self_node_id: self.self_chitchat_id.node_id.clone(),
            ready_nodes,
            live_nodes,
            dead_nodes,
            chitchat_state_snapshot,
        }
    }

    /// Leaves the cluster.
    pub async fn shutdown(self) {
        info!(
            cluster_id=%self.cluster_id,
            node_id=%self.self_chitchat_id.node_id,
            "Leaving the cluster."
        );
        self.set_self_node_readiness(false).await;
        tokio::time::sleep(GOSSIP_INTERVAL * 2).await;
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
        let chitchat = self.chitchat().await;
        let mut chitchat_guard = chitchat.lock().await;
        let mut current_indexing_tasks_keys: HashSet<_> = chitchat_guard
            .self_node_state()
            .key_values(|key, _| key.starts_with(INDEXING_TASK_PREFIX))
            .map(|(key, _)| key.to_string())
            .collect();
        for (indexing_task, indexing_tasks_group) in
            indexing_tasks.iter().group_by(|&task| task).into_iter()
        {
            let key = format!("{INDEXING_TASK_PREFIX}:{}", indexing_task.to_string());
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

    async fn chitchat(&self) -> Arc<Mutex<Chitchat>> {
        self.inner.read().await.chitchat_handle.chitchat()
    }
}

/// Deprecated: this is going away soon.
fn spawn_ready_members_task(
    cluster_id: String,
    mut live_nodes_stream: WatchStream<BTreeMap<ChitchatId, NodeState>>,
    ready_members_tx: watch::Sender<Vec<ClusterMember>>,
) {
    let fut = async move {
        while let Some(new_live_nodes) = live_nodes_stream.next().await {
            let mut new_ready_members = Vec::with_capacity(new_live_nodes.len());

            for (chitchat_id, node_state) in new_live_nodes {
                let member = match build_cluster_member(chitchat_id, &node_state) {
                    Ok(member) => member,
                    Err(error) => {
                        warn!(
                            cluster_id=%cluster_id,
                            error=?error,
                            "Failed to build cluster member from Chitchat node state."
                        );
                        continue;
                    }
                };
                if member.is_ready {
                    new_ready_members.push(member);
                }
            }
            if *ready_members_tx.borrow() != new_ready_members
                && ready_members_tx.send(new_ready_members).is_err()
            {
                break;
            }
        }
    };
    tokio::spawn(fut);
}

async fn spawn_ready_nodes_change_stream_task(cluster: Cluster) {
    let cluster_guard = cluster.inner.read().await;
    let cluster_id = cluster_guard.cluster_id.clone();
    let self_chitchat_id = cluster_guard.self_chitchat_id.clone();
    let chitchat = cluster_guard.chitchat_handle.chitchat();
    let weak_cluster = Arc::downgrade(&cluster.inner);
    drop(cluster_guard);
    drop(cluster);

    let mut previous_live_node_states = BTreeMap::new();
    let mut live_nodes_watcher = chitchat.lock().await.live_nodes_watcher();

    let future = async move {
        while let Some(new_live_node_states) = live_nodes_watcher.next().await {
            let Some(cluster) = weak_cluster.upgrade() else {
                break;
            };
            let mut cluster_guard = cluster.write().await;
            let previous_live_nodes = &mut cluster_guard.live_nodes;

            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                previous_live_nodes,
                &previous_live_node_states,
                &new_live_node_states,
            )
            .await;
            if !events.is_empty() {
                cluster_guard
                    .change_stream_subscribers
                    .retain(|change_stream_tx| {
                        events
                            .iter()
                            .all(|event| change_stream_tx.send(event.clone()).is_ok())
                    });
            }
            previous_live_node_states = new_live_node_states;
        }
    };
    tokio::spawn(future);
}

struct InnerCluster {
    cluster_id: String,
    self_chitchat_id: ChitchatId,
    chitchat_handle: ChitchatHandle,
    live_nodes: BTreeMap<NodeId, ClusterNode>,
    change_stream_subscribers: Vec<mpsc::UnboundedSender<ClusterChange>>,
    ready_members_rx: watch::Receiver<Vec<ClusterMember>>,
}

// Not used within the code, used for documentation.
#[derive(Debug, utoipa::ToSchema)]
pub struct NodeIdSchema {
    #[schema(example = "node-1")]
    /// The unique identifier of the node in the cluster.
    pub node_id: String,

    #[schema(example = "1683736537", value_type = u64)]
    /// A numeric identifier incremented every time the node leaves and rejoins the cluster.
    pub generation_id: u64,

    #[schema(example = "127.0.0.1:8000", value_type = String)]
    /// The socket address peers should use to gossip with the node.
    pub gossip_advertise_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ClusterSnapshot {
    #[schema(example = "qw-cluster-1")]
    /// The ID of the cluster that the node is a part of.
    pub cluster_id: String,

    #[schema(value_type = NodeIdSchema)]
    /// The unique ID of the current node.
    pub self_node_id: String,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of cluster node IDs that are ready to handle requests.
    pub ready_nodes: HashSet<ChitchatId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of cluster node IDs that are alive but not ready.
    pub live_nodes: HashSet<ChitchatId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of cluster node IDs flagged as dead or faulty.
    pub dead_nodes: HashSet<ChitchatId>,

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
    /// A complete snapshot of the Chitchat cluster state.
    pub chitchat_state_snapshot: ClusterStateSnapshot,
}

/// Computes the gRPC port from the listen address for tests.
#[cfg(any(test, feature = "testsuite"))]
pub fn grpc_addr_from_listen_addr_for_test(listen_addr: SocketAddr) -> SocketAddr {
    let grpc_port = listen_addr.port() + 1u16;
    (listen_addr.ip(), grpc_port).into()
}

#[cfg(any(test, feature = "testsuite"))]
pub async fn create_cluster_for_test_with_id(
    node_id: u16,
    cluster_id: String,
    peer_seed_addrs: Vec<String>,
    enabled_services: &HashSet<quickwit_config::service::QuickwitService>,
    transport: &dyn Transport,
    self_node_readiness: bool,
) -> anyhow::Result<Cluster> {
    let gossip_advertise_addr: SocketAddr = ([127, 0, 0, 1], node_id).into();
    let node_id: NodeId = format!("node_{node_id}").into();
    let self_node = ClusterMember::new(
        node_id,
        crate::GenerationId(1),
        self_node_readiness,
        enabled_services.clone(),
        gossip_advertise_addr,
        grpc_addr_from_listen_addr_for_test(gossip_advertise_addr),
        Vec::new(),
    );
    let failure_detector_config = create_failure_detector_config_for_test();
    let cluster = Cluster::join(
        cluster_id,
        self_node,
        gossip_advertise_addr,
        peer_seed_addrs,
        failure_detector_config,
        transport,
    )
    .await?;
    cluster.set_self_node_readiness(self_node_readiness).await;
    Ok(cluster)
}

/// Creates a failure detector config for tests.
#[cfg(any(test, feature = "testsuite"))]
fn create_failure_detector_config_for_test() -> FailureDetectorConfig {
    FailureDetectorConfig {
        phi_threshold: 5.0,
        initial_interval: GOSSIP_INTERVAL,
        ..Default::default()
    }
}

/// Creates a local cluster listening on a random port.
#[cfg(any(test, feature = "testsuite"))]
pub async fn create_cluster_for_test(
    seeds: Vec<String>,
    enabled_services: &[&str],
    transport: &dyn Transport,
    self_node_readiness: bool,
) -> anyhow::Result<Cluster> {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use quickwit_config::service::QuickwitService;

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
        self_node_readiness,
    )
    .await?;
    Ok(cluster)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use itertools::Itertools;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::indexing::IndexingTask;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn test_single_node_cluster_readiness() {
        let transport = ChannelTransport::default();
        let node = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();

        let mut ready_members_watcher = node.ready_members_watcher().await;
        let ready_members = ready_members_watcher.next().await.unwrap();

        assert!(ready_members.is_empty());
        assert!(!node.is_self_node_ready().await);

        let cluster_snapshot = node.snapshot().await;
        assert!(cluster_snapshot.ready_nodes.is_empty());

        let self_node_state = cluster_snapshot
            .chitchat_state_snapshot
            .node_state_snapshots
            .into_iter()
            .find(|node_state_snapshot| node_state_snapshot.chitchat_id == node.self_chitchat_id)
            .map(|node_state_snapshot| node_state_snapshot.node_state)
            .unwrap();
        assert_eq!(
            self_node_state.get(READINESS_KEY).unwrap(),
            READINESS_VALUE_NOT_READY
        );

        node.set_self_node_readiness(true).await;

        let ready_members = ready_members_watcher.next().await.unwrap();
        assert_eq!(ready_members.len(), 1);
        assert!(node.is_self_node_ready().await);

        let cluster_snapshot = node.snapshot().await;
        assert_eq!(cluster_snapshot.ready_nodes.len(), 1);

        let self_node_state = cluster_snapshot
            .chitchat_state_snapshot
            .node_state_snapshots
            .into_iter()
            .find(|node_state_snapshot| node_state_snapshot.chitchat_id == node.self_chitchat_id)
            .map(|node_state_snapshot| node_state_snapshot.node_state)
            .unwrap();
        assert_eq!(
            self_node_state.get(READINESS_KEY).unwrap(),
            READINESS_VALUE_READY
        );

        node.set_self_node_readiness(false).await;

        let ready_members = ready_members_watcher.next().await.unwrap();
        assert!(ready_members.is_empty());
        assert!(!node.is_self_node_ready().await);

        let cluster_snapshot = node.snapshot().await;
        assert!(cluster_snapshot.ready_nodes.is_empty());

        let self_node_state = cluster_snapshot
            .chitchat_state_snapshot
            .node_state_snapshots
            .into_iter()
            .find(|node_state_snapshot| node_state_snapshot.chitchat_id == node.self_chitchat_id)
            .map(|node_state_snapshot| node_state_snapshot.node_state)
            .unwrap();
        assert_eq!(
            self_node_state.get(READINESS_KEY).unwrap(),
            READINESS_VALUE_NOT_READY
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let node_1 = create_cluster_for_test(Vec::new(), &[], &transport, true).await?;
        let node_1_change_stream = node_1.ready_nodes_change_stream().await;

        let peer_seeds = vec![node_1.gossip_listen_addr.to_string()];
        let node_2 = create_cluster_for_test(peer_seeds, &[], &transport, true).await?;

        let peer_seeds = vec![node_2.gossip_listen_addr.to_string()];
        let node_3 = create_cluster_for_test(peer_seeds, &[], &transport, true).await?;

        let wait_secs = Duration::from_secs(30);

        for node in [&node_1, &node_2, &node_3] {
            node.wait_for_ready_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = node_1
            .ready_members()
            .await
            .into_iter()
            .map(|member| member.gossip_advertise_addr)
            .sorted()
            .collect();
        let mut expected_members = vec![
            node_1.gossip_listen_addr,
            node_2.gossip_listen_addr,
            node_3.gossip_listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        node_2.shutdown().await;
        node_1
            .wait_for_ready_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        node_3.shutdown().await;
        node_1
            .wait_for_ready_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        node_1.shutdown().await;

        let cluster_changes: Vec<ClusterChange> = node_1_change_stream.collect().await;
        assert_eq!(cluster_changes.len(), 6);
        assert!(matches!(&cluster_changes[0], ClusterChange::Add(_)));
        assert!(matches!(&cluster_changes[1], ClusterChange::Add(_)));
        assert!(matches!(&cluster_changes[2], ClusterChange::Add(_)));
        assert!(matches!(&cluster_changes[3], ClusterChange::Remove(_)));
        assert!(matches!(&cluster_changes[4], ClusterChange::Remove(_)));
        assert!(matches!(&cluster_changes[5], ClusterChange::Remove(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_node_cluster_readiness() {
        let transport = ChannelTransport::default();
        let node_1 =
            create_cluster_for_test(Vec::new(), &["searcher", "indexer"], &transport, true)
                .await
                .unwrap();

        let peer_seeds = vec![node_1.gossip_listen_addr.to_string()];
        let node_2 = create_cluster_for_test(peer_seeds, &["indexer"], &transport, false)
            .await
            .unwrap();

        let wait_secs = Duration::from_secs(5);

        // Bother cluster 1 and cluster 2 see only one ready member.
        node_1
            .wait_for_ready_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        node_2
            .wait_for_ready_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        // Now, node 2 becomes ready.
        node_2.set_self_node_readiness(true).await;

        // Bother cluster 1 and cluster 2 see only two ready members.
        node_1
            .wait_for_ready_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        node_2
            .wait_for_ready_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();
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
            index_uid: "index-1:11111111111111111111111111".to_string(),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
        };
        cluster2
            .set_self_key_value(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:1001")
            .await;
        cluster2
            .update_self_node_indexing_tasks(&[indexing_task.clone(), indexing_task.clone()])
            .await
            .unwrap();
        cluster1
            .wait_for_ready_members(|members| members.len() == 2, Duration::from_secs(30))
            .await
            .unwrap();
        let members = cluster1.ready_members().await;
        let member_node_1 = members
            .iter()
            .find(|member| member.chitchat_id() == cluster1.self_chitchat_id)
            .unwrap();
        let member_node_2 = members
            .iter()
            .find(|member| member.chitchat_id() == cluster2.self_chitchat_id)
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
        // TODO: increase it back to 1000 when https://github.com/quickwit-oss/chitchat/issues/81 is fixed
        let indexing_tasks = (0..500)
            .map(|_| {
                let index_id = random_generator.gen_range(0..=10_000);
                let source_id = random_generator.gen_range(0..=100);
                IndexingTask {
                    index_uid: format!("index-{index_id}:11111111111111111111111111"),
                    source_id: format!("source-{source_id}"),
                    shard_ids: Vec::new(),
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
                        cluster1.self_chitchat_id.gossip_advertise_addr,
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
                        cluster1.self_chitchat_id.gossip_advertise_addr,
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
                        cluster1.self_chitchat_id.gossip_advertise_addr,
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
        gossip_advertise_addr: SocketAddr,
        indexing_tasks: Vec<IndexingTask>,
    ) -> bool {
        let members = cluster.ready_members().await;
        let node_opt = members
            .iter()
            .find(|member| member.gossip_advertise_addr == gossip_advertise_addr);
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
        let node = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        {
            let chitchat_handle = node.inner.read().await.chitchat_handle.chitchat();
            let mut chitchat_guard = chitchat_handle.lock().await;
            chitchat_guard.self_node_state().set(
                format!(
                    "{INDEXING_TASK_PREFIX}:my_good_index:my_source:11111111111111111111111111"
                ),
                "2".to_string(),
            );
            chitchat_guard.self_node_state().set(
                format!("{INDEXING_TASK_PREFIX}:my_bad_index:my_source:11111111111111111111111111"),
                "malformatted value".to_string(),
            );
        }
        node.wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
            .await
            .unwrap();
        let ready_members = node.ready_members().await;
        assert_eq!(ready_members[0].indexing_tasks.len(), 2);
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
                .wait_for_ready_members(|members| members.len() == 2, wait_secs)
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
}
