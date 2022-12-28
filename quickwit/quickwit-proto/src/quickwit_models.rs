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

use chitchat::NodeId;
use quickwit_common::service::QuickwitService;

use crate::quickwit_indexing_api::IndexingTask;

/// Cluster member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMember {
    /// A unique node ID across the cluster.
    /// The Chitchat node ID is the concatenation of the node ID and the start timestamp:
    /// `{node_id}/{start_timestamp}`.
    pub node_id: String,
    /// The start timestamp (seconds) of the node.
    pub start_timestamp: u64,
    /// Enabled services, i.e. services configured to run on the node. Depending on the node and
    /// service health, each service may or may not be available/running.
    pub enabled_services: HashSet<QuickwitService>,
    /// Gossip advertise address, i.e. the address that other nodes should use to gossip with the
    /// node.
    pub gossip_advertise_addr: SocketAddr,
    /// gRPC advertise address, i.e. the address that other nodes should use to communicate with
    /// the node via gRPC.
    pub grpc_advertise_addr: SocketAddr,
    /// Running indexing tasks.
    /// Empty if the ndoe is not an indexer.
    pub running_indexing_tasks: Vec<IndexingTask>,
}

impl ClusterMember {
    pub fn new(
        node_id: String,
        start_timestamp: u64,
        enabled_services: HashSet<QuickwitService>,
        gossip_advertise_addr: SocketAddr,
        grpc_advertise_addr: SocketAddr,
        running_indexing_tasks: Vec<IndexingTask>,
    ) -> Self {
        Self {
            node_id,
            start_timestamp,
            enabled_services,
            gossip_advertise_addr,
            grpc_advertise_addr,
            running_indexing_tasks,
        }
    }

    pub fn chitchat_id(&self) -> String {
        format!("{}/{}", self.node_id, self.start_timestamp)
    }
}

impl From<ClusterMember> for NodeId {
    fn from(member: ClusterMember) -> Self {
        Self::new(member.chitchat_id(), member.gossip_advertise_addr)
    }
}
