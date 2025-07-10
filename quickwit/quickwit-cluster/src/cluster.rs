// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chitchat::transport::Transport;
use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, ClusterStateSnapshot,
    FailureDetectorConfig, KeyChangeEvent, ListenerHandle, NodeState, spawn_chitchat,
};
use itertools::Itertools;
use quickwit_common::tower::ClientGrpcConfig;
use quickwit_proto::indexing::{IndexingPipelineId, IndexingTask, PipelineMetrics};
use quickwit_proto::types::{NodeId, NodeIdRef, PipelineUid, ShardId};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tracing::{info, warn};

use crate::change::{ClusterChange, ClusterChangeStreamFactory, compute_cluster_change_events};
use crate::grpc_gossip::spawn_catchup_callback_task;
use crate::member::{
    ClusterMember, ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY, NodeStateExt,
    PIPELINE_METRICS_PREFIX, READINESS_KEY, READINESS_VALUE_NOT_READY, READINESS_VALUE_READY,
    build_cluster_member,
};
use crate::metrics::spawn_metrics_task;
use crate::{ClusterChangeStream, ClusterNode};

const MARKED_FOR_DELETION_GRACE_PERIOD: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(2_500) // 2.5 secs
} else {
    Duration::from_secs(3_600 * 2) // 2 hours.
};

// An indexing task key is formatted as
// `{INDEXING_TASK_PREFIX}{PIPELINE_ULID}`.
const INDEXING_TASK_PREFIX: &str = "indexer.task:";

#[derive(Clone)]
pub struct Cluster {
    cluster_id: String,
    self_chitchat_id: ChitchatId,
    /// Socket address (UDP) the node listens on for receiving gossip messages.
    pub gossip_listen_addr: SocketAddr,
    // TODO this object contains a tls config. We might want to change it to a
    // ArcSwap<ClientGrpcConfig> or something so that some task can watch for new certificates
    // and update this (hot reloading)
    client_grpc_config: ClientGrpcConfig,
    gossip_interval: Duration,
    inner: Arc<RwLock<InnerCluster>>,
}

impl Debug for Cluster {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("Cluster")
            .field("cluster_id", &self.cluster_id)
            .field("self_node_id", &self.self_chitchat_id.node_id)
            .field("gossip_listen_addr", &self.gossip_listen_addr)
            .field(
                "gossip_advertise_addr",
                &self.self_chitchat_id.gossip_advertise_addr,
            )
            .field("gossip_interval", &self.gossip_interval)
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

    pub fn self_node_id(&self) -> &NodeIdRef {
        NodeIdRef::from_str(&self.self_chitchat_id.node_id)
    }

    pub fn gossip_listen_addr(&self) -> SocketAddr {
        self.gossip_listen_addr
    }

    pub fn gossip_advertise_addr(&self) -> SocketAddr {
        self.self_chitchat_id.gossip_advertise_addr
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn join(
        cluster_id: String,
        self_node: ClusterMember,
        gossip_listen_addr: SocketAddr,
        peer_seed_addrs: Vec<String>,
        gossip_interval: Duration,
        failure_detector_config: FailureDetectorConfig,
        transport: &dyn Transport,
        client_grpc_config: ClientGrpcConfig,
    ) -> anyhow::Result<Self> {
        info!(
            cluster_id=%cluster_id,
            node_id=%self_node.node_id,
            generation_id=self_node.generation_id.as_u64(),
            enabled_services=?self_node.enabled_services,
            gossip_listen_addr=%gossip_listen_addr,
            gossip_advertise_addr=%self_node.gossip_advertise_addr,
            grpc_advertise_addr=%self_node.grpc_advertise_addr,
            peer_seed_addrs=%peer_seed_addrs.join(", "),
            "joining cluster"
        );
        // Set up catchup callback and extra liveness predicate functions.
        let (catchup_callback_tx, catchup_callback_rx) = watch::channel(());
        let catchup_callback = move || {
            let _ = catchup_callback_tx.send(());
        };
        let extra_liveness_predicate = |node_state: &NodeState| {
            [ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY]
                .iter()
                .all(|key| node_state.contains_key(key))
        };
        let chitchat_config = ChitchatConfig {
            cluster_id: cluster_id.clone(),
            chitchat_id: self_node.chitchat_id(),
            listen_addr: gossip_listen_addr,
            seed_nodes: peer_seed_addrs,
            failure_detector_config,
            gossip_interval,
            marked_for_deletion_grace_period: MARKED_FOR_DELETION_GRACE_PERIOD,
            catchup_callback: Some(Box::new(catchup_callback)),
            extra_liveness_predicate: Some(Box::new(extra_liveness_predicate)),
        };
        let chitchat_handle = spawn_chitchat(
            chitchat_config,
            vec![
                (
                    ENABLED_SERVICES_KEY.to_string(),
                    self_node.enabled_services.iter().join(","),
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
        let chitchat_guard = chitchat.lock().await;
        let live_nodes_rx = chitchat_guard.live_nodes_watcher();
        let live_nodes_stream = chitchat_guard.live_nodes_watch_stream();
        let (ready_members_tx, ready_members_rx) = watch::channel(Vec::new());
        spawn_ready_members_task(cluster_id.clone(), live_nodes_stream, ready_members_tx);
        drop(chitchat_guard);

        let weak_chitchat = Arc::downgrade(&chitchat);
        spawn_metrics_task(weak_chitchat.clone(), self_node.chitchat_id());

        spawn_catchup_callback_task(
            cluster_id.clone(),
            self_node.chitchat_id(),
            weak_chitchat,
            live_nodes_rx,
            catchup_callback_rx.clone(),
            client_grpc_config.clone(),
        )
        .await;

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
            gossip_interval,
            inner: Arc::new(RwLock::new(inner)),
            client_grpc_config,
        };
        spawn_change_stream_task(cluster.clone()).await;
        Ok(cluster)
    }

    /// Deprecated: this is going away soon.
    pub async fn ready_members(&self) -> Vec<ClusterMember> {
        self.inner.read().await.ready_members_rx.borrow().clone()
    }

    /// Deprecated: this is going away soon.
    async fn ready_members_watcher(&self) -> WatchStream<Vec<ClusterMember>> {
        WatchStream::new(self.inner.read().await.ready_members_rx.clone())
    }

    pub async fn ready_nodes(&self) -> Vec<ClusterNode> {
        self.inner
            .write()
            .await
            .live_nodes
            .values()
            .filter(|node| node.is_ready())
            .cloned()
            .collect()
    }

    /// Returns a stream of changes affecting the set of ready nodes in the cluster.
    pub fn change_stream(&self) -> ClusterChangeStream {
        let (change_stream, change_stream_tx) = ClusterChangeStream::new_unbounded();
        let inner = self.inner.clone();
        // We spawn a task so the signature of this function is sync.
        let future = async move {
            let mut inner = inner.write().await;
            for node in inner.live_nodes.values() {
                if node.is_ready() {
                    change_stream_tx
                        .send(ClusterChange::Add(node.clone()))
                        .expect("receiver end of the channel should be open");
                }
            }
            inner.change_stream_subscribers.push(change_stream_tx);
        };
        tokio::spawn(future);
        change_stream
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
    pub async fn set_self_key_value(&self, key: impl Display, value: impl Display) {
        self.chitchat()
            .await
            .lock()
            .await
            .self_node_state()
            .set(key, value);
    }

    /// Sets a key-value pair on the cluster node's state.
    pub async fn set_self_key_value_delete_after_ttl(
        &self,
        key: impl ToString,
        value: impl ToString,
    ) {
        let chitchat = self.chitchat().await;
        let mut chitchat_lock = chitchat.lock().await;
        let chitchat_self_node = chitchat_lock.self_node_state();
        let key = key.to_string();
        chitchat_self_node.set_with_ttl(key.clone(), value);
    }

    pub async fn get_self_key_value(&self, key: &str) -> Option<String> {
        self.chitchat()
            .await
            .lock()
            .await
            .self_node_state()
            .get(key)
            .map(|value| value.to_string())
    }

    pub async fn remove_self_key(&self, key: &str) {
        self.chitchat()
            .await
            .lock()
            .await
            .self_node_state()
            .delete(key)
    }

    pub async fn subscribe(
        &self,
        key_prefix: &str,
        callback: impl Fn(KeyChangeEvent) + Send + Sync + 'static,
    ) -> ListenerHandle {
        self.chitchat()
            .await
            .lock()
            .await
            .subscribe_event(key_prefix, callback)
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

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn leave(&self) {
        info!(
            cluster_id=%self.cluster_id,
            node_id=%self.self_chitchat_id.node_id,
            "leaving the cluster"
        );
        self.set_self_node_readiness(false).await;
        tokio::time::sleep(self.gossip_interval * 2).await;
    }

    pub async fn initiate_shutdown(&self) -> anyhow::Result<()> {
        self.inner.read().await.chitchat_handle.initiate_shutdown()
    }

    /// This exposes in chitchat some metrics about the CPU usage of cooperative pipelines.
    /// The metrics are exposed as follows:
    /// Key:        pipeline_metrics:<index_uid>:<source_id>
    /// Value:      179m,76MB/s
    pub async fn update_self_node_pipeline_metrics(
        &self,
        pipeline_metrics: &HashMap<&IndexingPipelineId, PipelineMetrics>,
    ) {
        let chitchat = self.chitchat().await;
        let mut chitchat_guard = chitchat.lock().await;
        let node_state = chitchat_guard.self_node_state();
        let mut current_metrics_keys: HashSet<String> = node_state
            .iter_prefix(PIPELINE_METRICS_PREFIX)
            .map(|(key, _)| key.to_string())
            .collect();
        for (pipeline_id, metrics) in pipeline_metrics {
            let key = format!("{PIPELINE_METRICS_PREFIX}{pipeline_id}");
            current_metrics_keys.remove(&key);
            node_state.set(key, metrics.to_string());
        }
        for obsolete_task_key in current_metrics_keys {
            node_state.delete(&obsolete_task_key);
        }
    }

    /// Updates indexing tasks in chitchat state.
    /// Tasks are grouped by (index_id, source_id), each group is stored in a key as follows:
    /// - key: `{INDEXING_TASK_PREFIX}{index_id}{INDEXING_TASK_SEPARATOR}{source_id}`
    /// - value: Number of indexing tasks in the group.
    ///
    /// Keys present in chitchat state but not in the given `indexing_tasks` are marked for
    /// deletion.
    pub async fn update_self_node_indexing_tasks(&self, indexing_tasks: &[IndexingTask]) {
        let chitchat = self.chitchat().await;
        let mut chitchat_guard = chitchat.lock().await;
        let node_state = chitchat_guard.self_node_state();
        set_indexing_tasks_in_node_state(indexing_tasks, node_state);
    }

    pub async fn chitchat(&self) -> Arc<Mutex<Chitchat>> {
        self.inner.read().await.chitchat_handle.chitchat()
    }

    pub async fn chitchat_server_termination_watcher(
        &self,
    ) -> impl Future<Output = anyhow::Result<()>> + use<> {
        self.inner
            .read()
            .await
            .chitchat_handle
            .termination_watcher()
    }
}

impl ClusterChangeStreamFactory for Cluster {
    fn create(&self) -> ClusterChangeStream {
        self.change_stream()
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

/// Parses indexing tasks from the chitchat node state.
pub fn parse_indexing_tasks(node_state: &NodeState) -> Vec<IndexingTask> {
    node_state
        .iter_prefix(INDEXING_TASK_PREFIX)
        .map(|(key, versioned_value)| (key, versioned_value.value.as_str()))
        .flat_map(|(key, value)| {
            let indexing_task_opt = chitchat_kv_to_indexing_task(key, value);
            if indexing_task_opt.is_none() {
                warn!(key=%key, value=%value, "failed to parse indexing task from chitchat kv");
            }
            indexing_task_opt
        })
        .collect()
}

/// Writes the given indexing tasks in the given node state.
///
/// If previous indexing tasks were present in the node state but were not in the given tasks, they
/// are marked for deletion.
pub(crate) fn set_indexing_tasks_in_node_state(
    indexing_tasks: &[IndexingTask],
    node_state: &mut NodeState,
) {
    let mut current_indexing_tasks_keys: HashSet<String> = node_state
        .iter_prefix(INDEXING_TASK_PREFIX)
        .map(|(key, _)| key.to_string())
        .collect();
    for indexing_task in indexing_tasks {
        let (key, value) = indexing_task_to_chitchat_kv(indexing_task);
        current_indexing_tasks_keys.remove(&key);
        node_state.set(key, value);
    }
    for obsolete_task_key in current_indexing_tasks_keys {
        node_state.delete(&obsolete_task_key);
    }
}

fn indexing_task_to_chitchat_kv(indexing_task: &IndexingTask) -> (String, String) {
    let IndexingTask {
        index_uid: _,
        source_id,
        shard_ids,
        pipeline_uid: _,
        params_fingerprint: _,
    } = indexing_task;
    let index_uid = indexing_task.index_uid();
    let key = format!("{INDEXING_TASK_PREFIX}{}", indexing_task.pipeline_uid());
    let shard_ids_str = shard_ids.iter().sorted().join(",");
    let fingerprint = indexing_task.params_fingerprint;
    let value = format!("{index_uid}:{source_id}:{fingerprint}:{shard_ids_str}");
    (key, value)
}

fn parse_shard_ids_str(shard_ids_str: &str) -> Vec<ShardId> {
    shard_ids_str
        .split(',')
        .filter(|shard_id_str| !shard_id_str.is_empty())
        .map(ShardId::from)
        .collect()
}

fn chitchat_kv_to_indexing_task(key: &str, value: &str) -> Option<IndexingTask> {
    let pipeline_uid_str = key.strip_prefix(INDEXING_TASK_PREFIX)?;
    let pipeline_uid = PipelineUid::from_str(pipeline_uid_str).ok()?;
    let mut field_iterator = value.rsplitn(4, ':');
    let shards_str = field_iterator.next()?;
    let fingerprint_str = field_iterator.next()?;
    let source_id = field_iterator.next()?;
    let index_uid = field_iterator.next()?;
    let params_fingerprint: u64 = fingerprint_str.parse().ok()?;
    let index_uid = index_uid.parse().ok()?;
    let shard_ids = parse_shard_ids_str(shards_str);
    Some(IndexingTask {
        index_uid: Some(index_uid),
        source_id: source_id.to_string(),
        pipeline_uid: Some(pipeline_uid),
        shard_ids,
        params_fingerprint,
    })
}

async fn spawn_change_stream_task(cluster: Cluster) {
    let cluster_guard = cluster.inner.read().await;
    let cluster_id = cluster_guard.cluster_id.clone();
    let client_grpc_config = cluster.client_grpc_config.clone();
    let self_chitchat_id = cluster_guard.self_chitchat_id.clone();
    let chitchat = cluster_guard.chitchat_handle.chitchat();
    let weak_cluster = Arc::downgrade(&cluster.inner);
    drop(cluster_guard);
    drop(cluster);

    let mut previous_live_node_states = BTreeMap::new();
    let mut live_nodes_watch_stream = chitchat.lock().await.live_nodes_watch_stream();

    let future = async move {
        while let Some(new_live_node_states) = live_nodes_watch_stream.next().await {
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
                &client_grpc_config,
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
    node_id: NodeId,
    gossip_advertise_port: u16,
    cluster_id: String,
    peer_seed_addrs: Vec<String>,
    enabled_services: &HashSet<quickwit_config::service::QuickwitService>,
    transport: &dyn Transport,
    self_node_readiness: bool,
) -> anyhow::Result<Cluster> {
    use quickwit_proto::indexing::PIPELINE_FULL_CAPACITY;
    let gossip_advertise_addr: SocketAddr = ([127, 0, 0, 1], gossip_advertise_port).into();
    let self_node = ClusterMember {
        node_id,
        generation_id: crate::GenerationId(1),
        is_ready: self_node_readiness,
        enabled_services: enabled_services.clone(),
        gossip_advertise_addr,
        grpc_advertise_addr: grpc_addr_from_listen_addr_for_test(gossip_advertise_addr),
        indexing_tasks: Vec::new(),
        indexing_cpu_capacity: PIPELINE_FULL_CAPACITY,
    };
    let failure_detector_config = create_failure_detector_config_for_test();
    let cluster = Cluster::join(
        cluster_id,
        self_node,
        gossip_advertise_addr,
        peer_seed_addrs,
        Duration::from_millis(25),
        failure_detector_config,
        transport,
        Default::default(),
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
        initial_interval: Duration::from_millis(25),
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
    use std::sync::atomic::{AtomicU16, Ordering};

    use quickwit_config::service::QuickwitService;

    static GOSSIP_ADVERTISE_PORT_SEQUENCE: AtomicU16 = AtomicU16::new(1u16);
    let gossip_advertise_port = GOSSIP_ADVERTISE_PORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let node_id: NodeId = format!("node-{gossip_advertise_port}").into();

    let enabled_services = enabled_services
        .iter()
        .map(|service_str| QuickwitService::from_str(service_str))
        .collect::<Result<HashSet<_>, _>>()?;
    let cluster = create_cluster_for_test_with_id(
        node_id,
        gossip_advertise_port,
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
    use quickwit_proto::types::IndexUid;
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
            .node_states
            .into_iter()
            .find(|node_state| node_state.chitchat_id() == &node.self_chitchat_id)
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
            .node_states
            .into_iter()
            .find(|node_state| node_state.chitchat_id() == &node.self_chitchat_id)
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
            .node_states
            .into_iter()
            .find(|node_state| node_state.chitchat_id() == &node.self_chitchat_id)
            .unwrap();
        assert_eq!(
            self_node_state.get(READINESS_KEY).unwrap(),
            READINESS_VALUE_NOT_READY
        );
        node.leave().await;
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        let transport = ChannelTransport::default();
        let node_1 = create_cluster_for_test(Vec::new(), &[], &transport, true).await?;
        let node_1_change_stream = node_1.change_stream();

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

        node_2.leave().await;
        node_1
            .wait_for_ready_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        node_3.leave().await;
        node_1
            .wait_for_ready_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        node_1.leave().await;
        drop(node_1);

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
        let index_uid: IndexUid = IndexUid::for_test("index-1", 1);
        let indexing_task1 = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid.clone()),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        let indexing_task2 = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(2u128)),
            index_uid: Some(index_uid.clone()),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        cluster2
            .set_self_key_value(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:1001")
            .await;
        cluster2
            .update_self_node_indexing_tasks(&[indexing_task1.clone(), indexing_task2.clone()])
            .await;
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
            &member_node_2.indexing_tasks,
            &[indexing_task1, indexing_task2]
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
            .map(|pipeline_id| {
                let index_id = random_generator.gen_range(0..=10_000);
                let source_id = random_generator.gen_range(0..=100);
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(pipeline_id as u128)),
                    index_uid: Some(
                        format!("index-{index_id}:11111111111111111111111111")
                            .parse()
                            .unwrap(),
                    ),
                    source_id: format!("source-{source_id}"),
                    shard_ids: Vec::new(),
                    params_fingerprint: 0,
                }
            })
            .collect_vec();
        cluster1
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await;
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
                Duration::from_secs(5),
                Duration::from_millis(100),
            )
            .await
            .unwrap();
        }

        // Mark tasks for deletion.
        cluster1.update_self_node_indexing_tasks(&[]).await;
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
            .await;
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
            .chunk_by(|task| (*task).clone())
            .into_iter()
            .map(|(key, group)| (key, group.count()))
            .collect();
        let grouped_tasks: HashMap<IndexingTask, usize> = indexing_tasks
            .iter()
            .chunk_by(|task| (*task).clone())
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
                format!("{INDEXING_TASK_PREFIX}01BX5ZZKBKACTAV9WEVGEMMVS0"),
                "my_index:00000000000000000000000000:my_source:41:1,3".to_string(),
            );
            chitchat_guard.self_node_state().set(
                format!("{INDEXING_TASK_PREFIX}01BX5ZZKBKACTAV9WEVGEMMVS1"),
                "my_index-00000000000000000000000000-my_source:53:3,5".to_string(),
            );
        }
        node.wait_for_ready_members(|members| members.len() == 1, Duration::from_secs(5))
            .await
            .unwrap();
        let ready_members = node.ready_members().await;
        assert_eq!(ready_members[0].indexing_tasks.len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_id_isolation() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();

        let cluster1a = create_cluster_for_test_with_id(
            "node-11".into(),
            11,
            "cluster1".to_string(),
            Vec::new(),
            &HashSet::default(),
            &transport,
            true,
        )
        .await?;
        let cluster2a = create_cluster_for_test_with_id(
            "node-21".into(),
            21,
            "cluster2".to_string(),
            vec![cluster1a.gossip_listen_addr.to_string()],
            &HashSet::default(),
            &transport,
            true,
        )
        .await?;
        let cluster1b = create_cluster_for_test_with_id(
            "node-12".into(),
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
            "node-22".into(),
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

    fn test_serialize_indexing_tasks_aux(
        indexing_tasks: &[IndexingTask],
        node_state: &mut NodeState,
    ) {
        set_indexing_tasks_in_node_state(indexing_tasks, node_state);
        let ser_deser_indexing_tasks = parse_indexing_tasks(node_state);
        assert_eq!(indexing_tasks, ser_deser_indexing_tasks);
    }

    #[test]
    fn test_serialize_indexing_tasks() {
        let mut node_state = NodeState::for_test();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        test_serialize_indexing_tasks_aux(&[], &mut node_state);
        test_serialize_indexing_tasks_aux(
            &[IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "my-source1".to_string(),
                shard_ids: vec![ShardId::from(1), ShardId::from(2)],
                params_fingerprint: 0,
            }],
            &mut node_state,
        );
        // change in the set of shards
        test_serialize_indexing_tasks_aux(
            &[IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "my-source1".to_string(),
                shard_ids: vec![ShardId::from(1), ShardId::from(2), ShardId::from(3)],
                params_fingerprint: 0,
            }],
            &mut node_state,
        );
        test_serialize_indexing_tasks_aux(
            &[
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(1u128)),
                    index_uid: Some(index_uid.clone()),
                    source_id: "my-source1".to_string(),
                    shard_ids: vec![ShardId::from(1), ShardId::from(2)],
                    params_fingerprint: 0,
                },
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(2u128)),
                    index_uid: Some(index_uid.clone()),
                    source_id: "my-source1".to_string(),
                    shard_ids: vec![ShardId::from(3), ShardId::from(4)],
                    params_fingerprint: 0,
                },
            ],
            &mut node_state,
        );
        // different index.
        test_serialize_indexing_tasks_aux(
            &[
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(1u128)),
                    index_uid: Some(index_uid.clone()),
                    source_id: "my-source1".to_string(),
                    shard_ids: vec![ShardId::from(1), ShardId::from(2)],
                    params_fingerprint: 0,
                },
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(2u128)),
                    index_uid: Some(IndexUid::for_test("test-index2", 0)),
                    source_id: "my-source1".to_string(),
                    shard_ids: vec![ShardId::from(3), ShardId::from(4)],
                    params_fingerprint: 0,
                },
            ],
            &mut node_state,
        );
        // same index, different source.
        test_serialize_indexing_tasks_aux(
            &[
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(1u128)),
                    index_uid: Some(index_uid.clone()),
                    source_id: "my-source1".to_string(),
                    shard_ids: vec![ShardId::from(1), ShardId::from(2)],
                    params_fingerprint: 0,
                },
                IndexingTask {
                    pipeline_uid: Some(PipelineUid::for_test(2u128)),
                    index_uid: Some(index_uid.clone()),
                    source_id: "my-source2".to_string(),
                    shard_ids: vec![ShardId::from(3), ShardId::from(4)],
                    params_fingerprint: 0,
                },
            ],
            &mut node_state,
        );
    }

    #[test]
    fn test_parse_shard_ids_str() {
        assert!(parse_shard_ids_str("").is_empty());
        assert!(parse_shard_ids_str(",").is_empty());
        assert_eq!(
            parse_shard_ids_str("00000000000000000012,"),
            [ShardId::from(12)]
        );
        assert_eq!(
            parse_shard_ids_str("00000000000000000012,00000000000000000023,"),
            [ShardId::from(12), ShardId::from(23)]
        );
    }

    #[test]
    fn test_parse_chitchat_kv() {
        assert!(
            chitchat_kv_to_indexing_task("invalidulid", "my_index:uid:my_source:42:1,3").is_none()
        );
        let task = super::chitchat_kv_to_indexing_task(
            "indexer.task:01BX5ZZKBKACTAV9WEVGEMMVS0",
            "my_index:00000000000000000000000000:my_source:42:00000000000000000001,\
             00000000000000000003",
        )
        .unwrap();
        assert_eq!(task.params_fingerprint, 42);
        assert_eq!(
            task.pipeline_uid(),
            PipelineUid::from_str("01BX5ZZKBKACTAV9WEVGEMMVS0").unwrap()
        );
        assert_eq!(
            &task.index_uid().to_string(),
            "my_index:00000000000000000000000000"
        );
        assert_eq!(&task.source_id, "my_source");
        assert_eq!(&task.shard_ids, &[ShardId::from(1), ShardId::from(3)]);
    }
}
