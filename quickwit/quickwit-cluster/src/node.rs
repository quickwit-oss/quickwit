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

use std::fmt::Debug;
use std::sync::Arc;

use chitchat::{ChitchatId, NodeState};
use quickwit_config::service::QuickwitService;
#[cfg(any(test, feature = "testsuite"))]
use quickwit_proto::indexing::IndexingTask;
#[cfg(any(test, feature = "testsuite"))]
use quickwit_proto::ingest::ingester::IngesterStatus;
use tonic::transport::Channel;

use crate::member::{ClusterMember, build_cluster_member};

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
        let member = build_cluster_member(chitchat_id, node_state)?;
        let inner = InnerNode {
            member,
            channel,
            is_self_node,
        };
        Ok(ClusterNode {
            inner: Arc::new(inner),
        })
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn for_test(
        node_id: &str,
        port: u16,
        is_self_node: bool,
        enabled_services: &[&str],
        indexing_tasks: &[IndexingTask],
        ingester_status: IngesterStatus,
    ) -> Self {
        use quickwit_common::shared_consts::INGESTER_STATUS_KEY;

        use crate::change::for_test::channel_factory_for_test;
        use crate::cluster::set_indexing_tasks_in_node_state;
        use crate::member::{ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY};

        let gossip_advertise_addr = ([127, 0, 0, 1], port).into();
        let grpc_advertise_addr = ([127, 0, 0, 1], port + 1).into();
        let chitchat_id = ChitchatId::new(node_id, 0, gossip_advertise_addr);
        let channel = channel_factory_for_test()
            .make_channel(grpc_advertise_addr)
            .await;
        let mut node_state = NodeState::for_test();
        node_state.set(ENABLED_SERVICES_KEY, enabled_services.join(","));
        node_state.set(GRPC_ADVERTISE_ADDR_KEY, grpc_advertise_addr.to_string());
        node_state.set(INGESTER_STATUS_KEY, ingester_status.as_json_str_name());
        set_indexing_tasks_in_node_state(indexing_tasks, &mut node_state);
        Self::try_new(chitchat_id, &node_state, channel, is_self_node).unwrap()
    }

    pub fn channel(&self) -> Channel {
        self.inner.channel.clone()
    }

    pub fn member(&self) -> &ClusterMember {
        &self.inner.member
    }

    pub fn is_service_enabled(&self, service: QuickwitService) -> bool {
        self.enabled_services.contains(&service)
    }

    pub fn is_indexer(&self) -> bool {
        self.is_service_enabled(QuickwitService::Indexer)
    }

    pub fn is_ingester(&self) -> bool {
        self.is_service_enabled(QuickwitService::Indexer)
    }

    pub fn is_searcher(&self) -> bool {
        self.is_service_enabled(QuickwitService::Searcher)
    }

    pub fn is_self_node(&self) -> bool {
        self.inner.is_self_node
    }

    pub fn availability_zone(&self) -> Option<&str> {
        self.inner.member.availability_zone.as_deref()
    }

    pub fn enable_standalone_compactors(&self) -> bool {
        self.inner.member.enable_standalone_compactors
    }
}

impl std::ops::Deref for ClusterNode {
    type Target = ClusterMember;

    fn deref(&self) -> &ClusterMember {
        self.member()
    }
}

impl Debug for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("node_id", &self.inner.member.node_id)
            .field("enabled_services", &self.inner.member.enabled_services)
            .field("is_ready", &self.inner.member.is_ready)
            .finish()
    }
}

#[cfg(test)]
impl PartialEq for ClusterNode {
    fn eq(&self, other: &Self) -> bool {
        self.inner.member == other.inner.member
            && self.inner.is_self_node == other.inner.is_self_node
    }
}

struct InnerNode {
    member: ClusterMember,
    channel: Channel,
    is_self_node: bool,
}
