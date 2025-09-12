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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::pin::Pin;
use std::task::{Context, Poll};

use chitchat::{ChitchatId, NodeState};
use futures::Stream;
use pin_project::pin_project;
use quickwit_common::sorted_iter::{KeyDiff, SortedByKeyIterator};
use quickwit_common::tower::{ClientGrpcConfig, make_channel, warmup_channel};
use quickwit_proto::types::NodeId;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::ClusterNode;
use crate::member::NodeStateExt;

/// Describes a change in the cluster.
#[derive(Debug, Clone)]
pub enum ClusterChange {
    Add(ClusterNode),
    Update(ClusterNode),
    Remove(ClusterNode),
}

/// A stream of cluster change events.
#[pin_project]
pub struct ClusterChangeStream(#[pin] UnboundedReceiverStream<ClusterChange>);

impl ClusterChangeStream {
    pub fn new_unbounded() -> (Self, mpsc::UnboundedSender<ClusterChange>) {
        let (change_stream_tx, change_stream_rx) = mpsc::unbounded_channel();
        (
            Self(UnboundedReceiverStream::new(change_stream_rx)),
            change_stream_tx,
        )
    }
}

impl Stream for ClusterChangeStream {
    type Item = ClusterChange;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }
}

/// A factory for creating cluster change streams.
pub trait ClusterChangeStreamFactory: Clone + Send + 'static {
    fn create(&self) -> ClusterChangeStream;
}

/// Compares the digests of the previous and new set of lives nodes, identifies the changes that
/// occurred in the cluster, and emits the corresponding events, focusing on ready nodes only.
pub(crate) async fn compute_cluster_change_events(
    cluster_id: &str,
    self_chitchat_id: &ChitchatId,
    previous_nodes: &mut BTreeMap<NodeId, ClusterNode>,
    previous_node_states: &BTreeMap<ChitchatId, NodeState>,
    new_node_states: &BTreeMap<ChitchatId, NodeState>,
    client_grpc_config: &ClientGrpcConfig,
) -> Vec<ClusterChange> {
    let mut cluster_events = Vec::new();

    for key_diff in previous_node_states
        .iter()
        .diff_by_key(new_node_states.iter())
    {
        match key_diff {
            // The node has joined the cluster.
            KeyDiff::Added(chitchat_id, node_state) => {
                let node_events = compute_cluster_change_events_on_added(
                    cluster_id,
                    self_chitchat_id,
                    chitchat_id,
                    node_state,
                    previous_nodes,
                    client_grpc_config.clone(),
                )
                .await;

                cluster_events.extend(node_events);
            }
            // The node's state has changed.
            KeyDiff::Unchanged(chitchat_id, previous_node_state, new_node_state)
                if previous_node_state.max_version() != new_node_state.max_version() =>
            {
                let node_event_opt = compute_cluster_change_events_on_updated(
                    cluster_id,
                    self_chitchat_id,
                    chitchat_id,
                    new_node_state,
                    previous_nodes,
                )
                .await;

                if let Some(node_event) = node_event_opt {
                    cluster_events.push(node_event);
                }
            }
            // The node's state has not changed.
            KeyDiff::Unchanged(_chitchat_id, _previous_max_version, _new_max_version) => {}
            // The node has left the cluster, i.e. it is considered dead by the failure detector.
            KeyDiff::Removed(chitchat_id, _node_state) => {
                let node_event_opt =
                    compute_cluster_change_events_on_removed(chitchat_id, previous_nodes);

                if let Some(node_event) = node_event_opt {
                    cluster_events.push(node_event);
                }
            }
        };
    }
    cluster_events
}

async fn compute_cluster_change_events_on_added(
    cluster_id: &str,
    self_chitchat_id: &ChitchatId,
    new_chitchat_id: &ChitchatId,
    new_node_state: &NodeState,
    previous_nodes: &mut BTreeMap<NodeId, ClusterNode>,
    client_grpc_config: ClientGrpcConfig,
) -> Vec<ClusterChange> {
    let is_self_node = self_chitchat_id == new_chitchat_id;
    let new_node_id: NodeId = new_chitchat_id.node_id.clone().into();
    let maybe_previous_node_entry = previous_nodes.entry(new_node_id);

    let mut events = Vec::new();
    let mut verb = "joined";

    if let Entry::Occupied(previous_node_entry) = maybe_previous_node_entry {
        let previous_node_ref = previous_node_entry.get();

        if previous_node_ref.chitchat_id().generation_id > new_chitchat_id.generation_id {
            warn!(
                node_id=%new_chitchat_id.node_id,
                generation_id=%new_chitchat_id.generation_id,
                "ignoring node `{}` rejoining the cluster with a lower generation ID",
                new_chitchat_id.node_id
            );
            return events;
        }
        let previous_node = previous_node_entry.remove();
        verb = "rejoined";

        if previous_node.is_ready() {
            events.push(ClusterChange::Remove(previous_node));
        }
    }
    let Some(new_node) = try_new_node(
        cluster_id,
        new_chitchat_id,
        new_node_state,
        is_self_node,
        &client_grpc_config,
    )
    .await
    else {
        return events;
    };
    info!(
        node_id=%new_chitchat_id.node_id,
        generation_id=%new_chitchat_id.generation_id,
        "node `{}` has {verb} the cluster",
        new_chitchat_id.node_id,
    );
    let new_node_id: NodeId = new_node.node_id().into();
    previous_nodes.insert(new_node_id, new_node.clone());

    if new_node.is_ready() {
        info!(
            node_id=%new_chitchat_id.node_id,
            generation_id=%new_chitchat_id.generation_id,
            "node `{}` has transitioned to ready state",
            new_chitchat_id.node_id
        );
        warmup_channel(new_node.channel()).await;
        events.push(ClusterChange::Add(new_node));
    }
    events
}

async fn compute_cluster_change_events_on_updated(
    cluster_id: &str,
    self_chitchat_id: &ChitchatId,
    updated_chitchat_id: &ChitchatId,
    updated_node_state: &NodeState,
    previous_nodes: &mut BTreeMap<NodeId, ClusterNode>,
) -> Option<ClusterChange> {
    let previous_node = previous_nodes.get(&updated_chitchat_id.node_id)?.clone();

    if previous_node.chitchat_id().generation_id > updated_chitchat_id.generation_id {
        warn!(
            node_id=%updated_chitchat_id.node_id,
            generation_id=%updated_chitchat_id.generation_id,
            "ignoring node `{}` update with a lower generation ID",
            updated_chitchat_id.node_id
        );
        return None;
    }
    let previous_channel = previous_node.channel();
    let is_self_node = self_chitchat_id == updated_chitchat_id;
    let updated_node = try_new_node_with_channel(
        cluster_id,
        updated_chitchat_id,
        updated_node_state,
        previous_channel,
        is_self_node,
    )?;
    let updated_node_id: NodeId = updated_node.chitchat_id().node_id.clone().into();
    previous_nodes.insert(updated_node_id, updated_node.clone());

    if !previous_node.is_ready() && updated_node.is_ready() {
        warmup_channel(updated_node.channel()).await;

        info!(
            node_id=%updated_chitchat_id.node_id,
            generation_id=%updated_chitchat_id.generation_id,
            "node `{}` has transitioned to ready state",
            updated_chitchat_id.node_id
        );
        Some(ClusterChange::Add(updated_node))
    } else if previous_node.is_ready() && !updated_node.is_ready() {
        info!(
            node_id=%updated_chitchat_id.node_id,
            generation_id=%updated_chitchat_id.generation_id,
            "node `{}` has transitioned out of ready state",
            updated_chitchat_id.node_id
        );
        Some(ClusterChange::Remove(updated_node))
    } else if previous_node.is_ready() && updated_node.is_ready() {
        Some(ClusterChange::Update(updated_node))
    } else {
        None
    }
}

fn compute_cluster_change_events_on_removed(
    removed_chitchat_id: &ChitchatId,
    previous_nodes: &mut BTreeMap<NodeId, ClusterNode>,
) -> Option<ClusterChange> {
    let removed_node_id: NodeId = removed_chitchat_id.node_id.clone().into();

    if let Entry::Occupied(previous_node_entry) = previous_nodes.entry(removed_node_id) {
        let previous_node_ref = previous_node_entry.get();

        if previous_node_ref.chitchat_id().generation_id == removed_chitchat_id.generation_id {
            info!(
                node_id=%removed_chitchat_id.node_id,
                generation_id=%removed_chitchat_id.generation_id,
                "node `{}` has left the cluster",
                removed_chitchat_id.node_id
            );
            let previous_node = previous_node_entry.remove();

            if previous_node.is_ready() {
                return Some(ClusterChange::Remove(previous_node));
            }
        }
    };
    None
}

fn try_new_node_with_channel(
    cluster_id: &str,
    chitchat_id: &ChitchatId,
    node_state: &NodeState,
    channel: Channel,
    is_self_node: bool,
) -> Option<ClusterNode> {
    match ClusterNode::try_new(chitchat_id.clone(), node_state, channel, is_self_node) {
        Ok(node) => Some(node),
        Err(error) => {
            warn!(
                cluster_id=%cluster_id,
                node_id=%chitchat_id.node_id,
                error=%error,
                "failed to create cluster node from Chitchat node state"
            );
            None
        }
    }
}

async fn try_new_node(
    cluster_id: &str,
    chitchat_id: &ChitchatId,
    node_state: &NodeState,
    is_self_node: bool,
    grpc_config: &ClientGrpcConfig,
) -> Option<ClusterNode> {
    match node_state.grpc_advertise_addr() {
        Ok(socket_addr) => {
            let channel = make_channel(socket_addr, grpc_config.clone()).await;
            try_new_node_with_channel(cluster_id, chitchat_id, node_state, channel, is_self_node)
        }
        Err(error) => {
            warn!(
                cluster_id=%cluster_id,
                node_id=%chitchat_id.node_id,
                error=%error,
                "failed to read or parse gRPC advertise address"
            );
            None
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
pub mod for_test {
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc;

    use super::*;

    #[derive(Clone, Default)]
    pub struct ClusterChangeStreamFactoryForTest {
        inner: Arc<Mutex<Option<mpsc::UnboundedSender<ClusterChange>>>>,
    }

    impl ClusterChangeStreamFactoryForTest {
        pub fn change_stream_tx(&self) -> mpsc::UnboundedSender<ClusterChange> {
            self.inner.lock().unwrap().take().unwrap()
        }
    }

    impl ClusterChangeStreamFactory for ClusterChangeStreamFactoryForTest {
        fn create(&self) -> ClusterChangeStream {
            let (change_stream, change_stream_tx) = ClusterChangeStream::new_unbounded();
            *self.inner.lock().unwrap() = Some(change_stream_tx);
            change_stream
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use itertools::Itertools;
    use quickwit_config::service::QuickwitService;
    use tonic::transport::Channel;

    use super::*;
    use crate::member::{
        ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY, READINESS_VALUE_NOT_READY,
        READINESS_VALUE_READY,
    };

    pub(crate) struct NodeStateBuilder {
        enabled_services: HashSet<QuickwitService>,
        grpc_advertise_addr: SocketAddr,
        readiness: bool,
        key_values: Vec<(String, String)>,
    }

    impl Default for NodeStateBuilder {
        fn default() -> Self {
            Self {
                enabled_services: QuickwitService::supported_services(),
                grpc_advertise_addr: "127.0.0.1:7281".parse().unwrap(),
                readiness: false,
                key_values: Vec::new(),
            }
        }
    }

    impl NodeStateBuilder {
        pub(crate) fn with_grpc_advertise_addr(mut self, grpc_advertise_addr: SocketAddr) -> Self {
            self.grpc_advertise_addr = grpc_advertise_addr;
            self
        }

        pub(crate) fn with_readiness(mut self, readiness: bool) -> Self {
            self.readiness = readiness;
            self
        }

        pub(crate) fn with_key_value(mut self, key: &str, value: &str) -> Self {
            self.key_values.push((key.to_string(), value.to_string()));
            self
        }

        pub(crate) fn build(self) -> NodeState {
            let mut node_state = NodeState::for_test();

            node_state.set(
                ENABLED_SERVICES_KEY,
                self.enabled_services
                    .iter()
                    .map(|service| service.as_str())
                    .join(","),
            );
            node_state.set(
                GRPC_ADVERTISE_ADDR_KEY,
                self.grpc_advertise_addr.to_string(),
            );
            node_state.set(
                READINESS_KEY,
                if self.readiness {
                    READINESS_VALUE_READY
                } else {
                    READINESS_VALUE_NOT_READY
                },
            );
            for (key, value) in self.key_values {
                node_state.set(key, value);
            }
            node_state
        }
    }

    #[tokio::test]
    async fn test_compute_cluster_change_events_on_added() {
        let cluster_id = "test-cluster".to_string();
        let self_port = 1234;
        let self_chitchat_id = ChitchatId::for_local_test(self_port);
        {
            // New node joins the cluster with an invalid gRPC advertise address.
            let port = 1235;
            let new_chitchat_id = ChitchatId::for_local_test(port);
            let mut new_node_state = NodeStateBuilder::default().build();
            new_node_state.set(GRPC_ADVERTISE_ADDR_KEY, "bogus-grpc-advertise-addr");
            let mut previous_nodes = BTreeMap::new();

            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &new_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;
            assert!(events.is_empty());
            assert!(previous_nodes.is_empty());
        }
        {
            // New node joins the cluster but is not ready.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let new_chitchat_id = ChitchatId::for_local_test(port);
            let new_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(false)
                .build();
            let mut previous_nodes = BTreeMap::new();

            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &new_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;
            assert!(events.is_empty());

            let node = previous_nodes.get(&new_chitchat_id.node_id).unwrap();

            assert_eq!(node.chitchat_id(), &new_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(!node.is_self_node());
            assert!(!node.is_ready());
        }
        {
            // New node joins the cluster and is ready.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let new_chitchat_id = ChitchatId::for_local_test(port);
            let new_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let mut previous_nodes = BTreeMap::new();

            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &new_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;

            let ClusterChange::Add(node) = &events[0] else {
                panic!("expected `ClusterChange::Add` event, got `{:?}`", events[0]);
            };
            assert_eq!(node.chitchat_id(), &new_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(!node.is_self_node());
            assert!(node.is_ready());
            assert_eq!(previous_nodes.get(&new_chitchat_id.node_id).unwrap(), node);

            // Node rejoins with same node ID but newer generation ID.
            let mut rejoined_chitchat_id = ChitchatId::for_local_test(port);
            rejoined_chitchat_id.generation_id += 1;

            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &rejoined_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;
            assert_eq!(events.len(), 2);

            let ClusterChange::Remove(removed_node) = &events[0] else {
                panic!(
                    "expected `ClusterChange::Remove` event, got `{:?}`",
                    events[0]
                );
            };
            assert_eq!(removed_node.chitchat_id(), &new_chitchat_id);

            let ClusterChange::Add(rejoined_node) = &events[1] else {
                panic!("expected `ClusterChange::Add` event, got `{:?}`", events[1]);
            };
            assert_eq!(rejoined_node.chitchat_id(), &rejoined_chitchat_id);
            assert_eq!(
                previous_nodes.get(&rejoined_chitchat_id.node_id).unwrap(),
                rejoined_node
            );

            // Node comes back from the dead with an older generation ID.
            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &new_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;
            assert!(events.is_empty());
            assert_eq!(
                previous_nodes.get(&rejoined_chitchat_id.node_id).unwrap(),
                rejoined_node
            );
        }
        {
            // Self node joins the cluster and is ready.
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], self_port + 1).into();
            let new_chitchat_id = self_chitchat_id.clone();
            let new_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let mut previous_nodes = BTreeMap::new();

            let events = compute_cluster_change_events_on_added(
                &cluster_id,
                &self_chitchat_id,
                &new_chitchat_id,
                &new_node_state,
                &mut previous_nodes,
                Default::default(),
            )
            .await;
            assert_eq!(events.len(), 1);

            let ClusterChange::Add(node) = &events[0] else {
                panic!("expected `ClusterChange::Add` event, got `{:?}`", events[0]);
            };
            assert_eq!(node.chitchat_id(), &new_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(node.is_self_node());
            assert!(node.is_ready());
            assert_eq!(previous_nodes.get(&new_chitchat_id.node_id).unwrap(), node);
        }
    }

    #[tokio::test]
    async fn test_compute_cluster_change_events_on_updated() {
        let cluster_id = "test-cluster".to_string();
        let self_port = 1234;
        let self_chitchat_id = ChitchatId::for_local_test(self_port);
        {
            // Node becomes ready.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let updated_chitchat_id = ChitchatId::for_local_test(port);
            let updated_node_id: NodeId = updated_chitchat_id.node_id.clone().into();
            let previous_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(false)
                .build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                updated_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(updated_node_id, previous_node)]);

            let updated_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .with_key_value("my-key", "my-value")
                .build();
            let event = compute_cluster_change_events_on_updated(
                &cluster_id,
                &self_chitchat_id,
                &updated_chitchat_id,
                &updated_node_state,
                &mut previous_nodes,
            )
            .await
            .unwrap();
            let ClusterChange::Add(node) = event else {
                panic!("expected `ClusterChange::Add` event, got `{event:?}`");
            };
            assert_eq!(node.chitchat_id(), &updated_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(node.is_ready());
            assert!(!node.is_self_node());
            assert_eq!(
                previous_nodes.get(&updated_chitchat_id.node_id).unwrap(),
                &node
            );
        }
        {
            // Node changes.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let updated_chitchat_id = ChitchatId::for_local_test(port);
            let updated_node_id: NodeId = updated_chitchat_id.node_id.clone().into();
            let previous_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                updated_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(updated_node_id, previous_node)]);

            let updated_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .with_key_value("my-key", "my-value")
                .build();
            let event = compute_cluster_change_events_on_updated(
                &cluster_id,
                &self_chitchat_id,
                &updated_chitchat_id,
                &updated_node_state,
                &mut previous_nodes,
            )
            .await
            .unwrap();

            let ClusterChange::Update(node) = event else {
                panic!("expected `ClusterChange::Remove` event, got `{event:?}`");
            };
            assert_eq!(node.chitchat_id(), &updated_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(!node.is_self_node());
            assert!(node.is_ready());
            assert_eq!(
                previous_nodes.get(&updated_chitchat_id.node_id).unwrap(),
                &node
            );
        }
        {
            // Node is no longer ready.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let updated_chitchat_id = ChitchatId::for_local_test(port);
            let updated_node_id: NodeId = updated_chitchat_id.node_id.clone().into();
            let previous_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                updated_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(updated_node_id, previous_node)]);

            let updated_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(false)
                .with_key_value("my-key", "my-value")
                .build();
            let event = compute_cluster_change_events_on_updated(
                &cluster_id,
                &self_chitchat_id,
                &updated_chitchat_id,
                &updated_node_state,
                &mut previous_nodes,
            )
            .await
            .unwrap();
            let ClusterChange::Remove(node) = event else {
                panic!("expected `ClusterChange::Remove` event, got `{event:?}`");
            };
            assert_eq!(node.chitchat_id(), &updated_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(!node.is_self_node());
            assert!(!node.is_ready());
            assert_eq!(
                previous_nodes.get(&updated_chitchat_id.node_id).unwrap(),
                &node
            );
        }
        {
            // Ignore node update with a lower generation ID.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let updated_chitchat_id = ChitchatId::for_local_test(port);
            let updated_node_id: NodeId = updated_chitchat_id.node_id.clone().into();
            let mut previous_chitchat_id = updated_chitchat_id.clone();
            previous_chitchat_id.generation_id += 1;
            let previous_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                previous_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes =
                BTreeMap::from_iter([(updated_node_id, previous_node.clone())]);

            let updated_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(false)
                .with_key_value("my-key", "my-value")
                .build();
            let event_opt = compute_cluster_change_events_on_updated(
                &cluster_id,
                &self_chitchat_id,
                &updated_chitchat_id,
                &updated_node_state,
                &mut previous_nodes,
            )
            .await;
            assert!(event_opt.is_none());

            assert_eq!(
                previous_nodes.get(&updated_chitchat_id.node_id).unwrap(),
                &previous_node
            );
        }
    }

    #[tokio::test]
    async fn test_compute_cluster_change_events_on_removed() {
        {
            // Node leaves the cluster but it's missing from the previous live nodes.
            let port = 1235;
            let removed_chitchat_id = ChitchatId::for_local_test(port);
            let mut previous_nodes = BTreeMap::default();

            let event_opt =
                compute_cluster_change_events_on_removed(&removed_chitchat_id, &mut previous_nodes);
            assert!(event_opt.is_none());
        }
        {
            // Node leaves the cluster in not ready state.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let removed_chitchat_id = ChitchatId::for_local_test(port);
            let removed_node_id: NodeId = removed_chitchat_id.node_id.clone().into();
            let previous_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(false)
                .build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                removed_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(removed_node_id, previous_node)]);

            let event_opt =
                compute_cluster_change_events_on_removed(&removed_chitchat_id, &mut previous_nodes);
            assert!(event_opt.is_none());
            assert!(!previous_nodes.contains_key(&removed_chitchat_id.node_id));
        }
        {
            // Node leaves the cluster in ready state.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let removed_chitchat_id = ChitchatId::for_local_test(port);
            let removed_node_id: NodeId = removed_chitchat_id.node_id.clone().into();
            let removed_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let removed_node = ClusterNode::try_new(
                removed_chitchat_id.clone(),
                &removed_node_state,
                channel,
                false,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(removed_node_id.clone(), removed_node)]);

            let event =
                compute_cluster_change_events_on_removed(&removed_chitchat_id, &mut previous_nodes)
                    .unwrap();

            let ClusterChange::Remove(node) = event else {
                panic!("expected `ClusterChange::Remove` event, got `{event:?}`");
            };
            assert_eq!(node.chitchat_id(), &removed_chitchat_id);
            assert_eq!(node.grpc_advertise_addr(), grpc_advertise_addr);
            assert!(!node.is_self_node());
            assert!(node.is_ready());
            assert!(!previous_nodes.contains_key(&removed_chitchat_id.node_id));
        }
        {
            // Node leaves the cluster in ready state but in the meantime it has rejoined the
            // cluster with a newer generation ID.
            let port = 1235;
            let grpc_advertise_addr: SocketAddr = ([127, 0, 0, 1], port + 1).into();
            let removed_chitchat_id = ChitchatId::for_local_test(port);

            let mut rejoined_chitchat_id = removed_chitchat_id.clone();
            rejoined_chitchat_id.generation_id += 1;
            let rejoined_node_id: NodeId = rejoined_chitchat_id.node_id.clone().into();
            let rejoined_node_state = NodeStateBuilder::default()
                .with_grpc_advertise_addr(grpc_advertise_addr)
                .with_readiness(true)
                .build();
            let channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let rejoined_node = ClusterNode::try_new(
                rejoined_chitchat_id.clone(),
                &rejoined_node_state,
                channel,
                false,
            )
            .unwrap();
            let mut previous_nodes =
                BTreeMap::from_iter([(rejoined_node_id.clone(), rejoined_node.clone())]);

            let event_opt =
                compute_cluster_change_events_on_removed(&removed_chitchat_id, &mut previous_nodes);
            assert!(event_opt.is_none());
            assert_eq!(
                previous_nodes.get(&rejoined_node_id).unwrap(),
                &rejoined_node
            );
        }
    }

    #[tokio::test]
    async fn test_compute_cluster_change_events() {
        let cluster_id = "test-cluster".to_string();
        let self_port = 1234;
        let self_chitchat_id = ChitchatId::for_local_test(self_port);
        let self_node_id: NodeId = self_chitchat_id.node_id.clone().into();
        {
            let mut previous_nodes = BTreeMap::default();
            let previous_node_states = BTreeMap::default();
            let new_node_states = BTreeMap::default();
            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &previous_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert!(events.is_empty());
        }
        {
            // Node remained unchanged.
            let previous_node_state = NodeStateBuilder::default().with_readiness(true).build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                self_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(self_node_id.clone(), previous_node)]);
            let previous_node_states =
                BTreeMap::from_iter([(self_chitchat_id.clone(), previous_node_state)]);

            let new_node_state = NodeStateBuilder::default().with_readiness(true).build();
            let new_node_states = BTreeMap::from_iter([(self_chitchat_id.clone(), new_node_state)]);

            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &previous_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert!(events.is_empty());
        }
        {
            // Node joins the cluster.
            let mut previous_nodes = BTreeMap::default();
            let previous_node_states = BTreeMap::default();
            let new_chitchat_id = ChitchatId::for_local_test(self_port + 1);
            let new_node_state = NodeStateBuilder::default().with_readiness(true).build();
            let new_node_states = BTreeMap::from_iter([(new_chitchat_id, new_node_state)]);
            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &previous_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert_eq!(events.len(), 1);

            let ClusterChange::Add(_node) = events[0].clone() else {
                panic!("expected `ClusterChange::Add` event, got `{:?}`", events[0]);
            };

            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &new_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert_eq!(events.len(), 0);
        }
        {
            // Node changes.
            let previous_node_state = NodeStateBuilder::default().with_readiness(true).build();
            let previous_channel = Channel::from_static("http://127.0.0.1:12345/").connect_lazy();
            let is_self_node = true;
            let previous_node = ClusterNode::try_new(
                self_chitchat_id.clone(),
                &previous_node_state,
                previous_channel,
                is_self_node,
            )
            .unwrap();
            let mut previous_nodes = BTreeMap::from_iter([(self_node_id, previous_node)]);
            let previous_node_states =
                BTreeMap::from_iter([(self_chitchat_id.clone(), previous_node_state)]);

            let new_node_state = NodeStateBuilder::default()
                .with_readiness(true)
                .with_key_value("my-key", "my-value")
                .build();
            let new_node_states = BTreeMap::from_iter([(self_chitchat_id.clone(), new_node_state)]);

            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &previous_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert_eq!(events.len(), 1);

            let ClusterChange::Update(_node) = events[0].clone() else {
                panic!(
                    "Expected `ClusterChange::Update` event, got `{:?}`",
                    events[0]
                );
            };

            // Node leaves the cluster.
            let new_node_states = BTreeMap::default();
            let events = compute_cluster_change_events(
                &cluster_id,
                &self_chitchat_id,
                &mut previous_nodes,
                &previous_node_states,
                &new_node_states,
                &Default::default(),
            )
            .await;
            assert_eq!(events.len(), 1);

            let ClusterChange::Remove(_node) = events[0].clone() else {
                panic!(
                    "Expected `ClusterChange::Remove` event, got `{:?}`",
                    events[0]
                );
            };
        }
    }
}
