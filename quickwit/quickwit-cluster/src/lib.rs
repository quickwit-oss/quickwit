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

#![deny(clippy::disallowed_methods)]

mod change;
mod cluster;
mod member;
mod node;

#[cfg(any(test, feature = "testsuite"))]
pub use chitchat::transport::ChannelTransport;
use chitchat::transport::UdpTransport;
use chitchat::FailureDetectorConfig;
use quickwit_config::service::QuickwitService;
use quickwit_config::NodeConfig;
use quickwit_proto::types::NodeId;
use time::OffsetDateTime;

pub use crate::change::ClusterChange;
#[cfg(any(test, feature = "testsuite"))]
pub use crate::cluster::{create_cluster_for_test, grpc_addr_from_listen_addr_for_test};
pub use crate::cluster::{Cluster, ClusterSnapshot, NodeIdSchema};
pub use crate::member::ClusterMember;
pub use crate::node::ClusterNode;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GenerationId(u64);

impl GenerationId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc().unix_timestamp_nanos() as u64)
    }
}

impl From<u64> for GenerationId {
    fn from(generation_id: u64) -> Self {
        Self(generation_id)
    }
}

pub async fn start_cluster_service(node_config: &NodeConfig) -> anyhow::Result<Cluster> {
    let cluster_id = node_config.cluster_id.clone();
    let gossip_listen_addr = node_config.gossip_listen_addr;
    let peer_seed_addrs = node_config.peer_seed_addrs().await?;
    let indexing_tasks = Vec::new();

    let node_id: NodeId = node_config.node_id.clone().into();
    let generation_id = GenerationId::now();
    let is_ready = false;
    let self_node = ClusterMember::new(
        node_id,
        generation_id,
        is_ready,
        node_config.enabled_services.clone(),
        node_config.gossip_advertise_addr,
        node_config.grpc_advertise_addr,
        indexing_tasks,
    );
    let cluster = Cluster::join(
        cluster_id,
        self_node,
        gossip_listen_addr,
        peer_seed_addrs,
        FailureDetectorConfig::default(),
        &UdpTransport,
    )
    .await?;
    Ok(cluster)
}
