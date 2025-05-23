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

use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use chitchat::{ChitchatId, NodeState};
use quickwit_config::service::QuickwitService;
use quickwit_proto::indexing::{CpuCapacity, IndexingTask};
use quickwit_proto::types::NodeIdRef;
use tonic::transport::Channel;

use crate::member::build_cluster_member;

#[derive(Clone)]
pub struct ClusterNode {
    inner: Arc<InnerNode>,
}

impl ClusterNode {
    /// Attempts to create a new `ClusterNode` from a Chitchat `NodeState`.
    pub(crate) fn try_new(
        chitchat_id: ChitchatId,
        node_state: &NodeState,
        channel: Channel,
        is_self_node: bool,
    ) -> anyhow::Result<Self> {
        let member = build_cluster_member(chitchat_id.clone(), node_state)?;
        let inner = InnerNode {
            chitchat_id,
            channel,
            enabled_services: member.enabled_services,
            grpc_advertise_addr: member.grpc_advertise_addr,
            indexing_tasks: member.indexing_tasks,
            indexing_capacity: member.indexing_cpu_capacity,
            is_ready: member.is_ready,
            is_self_node,
        };
        let node = ClusterNode {
            inner: Arc::new(inner),
        };
        Ok(node)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn for_test(
        node_id: &str,
        port: u16,
        is_self_node: bool,
        enabled_services: &[&str],
        indexing_tasks: &[IndexingTask],
    ) -> Self {
        use quickwit_common::tower::{ClientGrpcConfig, make_channel};

        use crate::cluster::set_indexing_tasks_in_node_state;
        use crate::member::{ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY};

        let gossip_advertise_addr = ([127, 0, 0, 1], port).into();
        let grpc_advertise_addr = ([127, 0, 0, 1], port + 1).into();
        let chitchat_id = ChitchatId::new(node_id.to_string(), 0, gossip_advertise_addr);
        let channel = make_channel(grpc_advertise_addr, ClientGrpcConfig::default()).await;
        let mut node_state = NodeState::for_test();
        node_state.set(ENABLED_SERVICES_KEY, enabled_services.join(","));
        node_state.set(GRPC_ADVERTISE_ADDR_KEY, grpc_advertise_addr.to_string());
        set_indexing_tasks_in_node_state(indexing_tasks, &mut node_state);
        Self::try_new(chitchat_id, &node_state, channel, is_self_node).unwrap()
    }

    pub fn chitchat_id(&self) -> &ChitchatId {
        &self.inner.chitchat_id
    }

    pub fn node_id(&self) -> &NodeIdRef {
        NodeIdRef::from_str(&self.inner.chitchat_id.node_id)
    }

    pub fn channel(&self) -> Channel {
        self.inner.channel.clone()
    }

    pub fn enabled_services(&self) -> &HashSet<QuickwitService> {
        &self.inner.enabled_services
    }

    pub fn is_indexer(&self) -> bool {
        self.inner
            .enabled_services
            .contains(&QuickwitService::Indexer)
    }

    pub fn is_ingester(&self) -> bool {
        self.inner
            .enabled_services
            .contains(&QuickwitService::Indexer)
    }

    pub fn is_searcher(&self) -> bool {
        self.inner
            .enabled_services
            .contains(&QuickwitService::Searcher)
    }

    pub fn grpc_advertise_addr(&self) -> SocketAddr {
        self.inner.grpc_advertise_addr
    }

    pub fn indexing_tasks(&self) -> &[IndexingTask] {
        &self.inner.indexing_tasks
    }

    pub fn indexing_capacity(&self) -> CpuCapacity {
        self.inner.indexing_capacity
    }

    pub fn is_ready(&self) -> bool {
        self.inner.is_ready
    }

    pub fn is_self_node(&self) -> bool {
        self.inner.is_self_node
    }
}

impl Debug for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("node_id", &self.inner.chitchat_id.node_id)
            .field("enabled_services", &self.inner.enabled_services)
            .field("is_ready", &self.inner.is_ready)
            .finish()
    }
}

#[cfg(test)]
impl PartialEq for ClusterNode {
    fn eq(&self, other: &Self) -> bool {
        self.inner.chitchat_id == other.inner.chitchat_id
            && self.inner.enabled_services == other.inner.enabled_services
            && self.inner.grpc_advertise_addr == other.inner.grpc_advertise_addr
            && self.inner.indexing_tasks == other.inner.indexing_tasks
            && self.inner.is_ready == other.inner.is_ready
            && self.inner.is_self_node == other.inner.is_self_node
    }
}

struct InnerNode {
    chitchat_id: ChitchatId,
    channel: Channel,
    enabled_services: HashSet<QuickwitService>,
    grpc_advertise_addr: SocketAddr,
    indexing_tasks: Vec<IndexingTask>,
    indexing_capacity: CpuCapacity,
    is_ready: bool,
    is_self_node: bool,
}
