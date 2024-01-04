// Copyright (C) 2024 Quickwit, Inc.
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

use anyhow::Context;
use chitchat::{ChitchatId, NodeState};
use quickwit_proto::indexing::{CpuCapacity, IndexingTask};
use quickwit_proto::types::NodeId;
use tracing::{error, warn};

use crate::cluster::parse_indexing_tasks;
use crate::{GenerationId, QuickwitService};

// Keys used to store member's data in chitchat state.
pub(crate) const GRPC_ADVERTISE_ADDR_KEY: &str = "grpc_advertise_addr";
pub(crate) const ENABLED_SERVICES_KEY: &str = "enabled_services";
pub(crate) const PIPELINE_METRICS_PREFIX: &str = "pipeline_metrics:";

// Readiness key and values used to store node's readiness in Chitchat state.
pub(crate) const READINESS_KEY: &str = "readiness";
pub(crate) const READINESS_VALUE_READY: &str = "READY";
pub(crate) const READINESS_VALUE_NOT_READY: &str = "NOT_READY";

pub const INDEXING_CPU_CAPACITY_KEY: &str = "indexing_cpu_capacity";

pub(crate) trait NodeStateExt {
    fn grpc_advertise_addr(&self) -> anyhow::Result<SocketAddr>;

    fn is_ready(&self) -> bool;
}

impl NodeStateExt for NodeState {
    fn grpc_advertise_addr(&self) -> anyhow::Result<SocketAddr> {
        self.get(GRPC_ADVERTISE_ADDR_KEY)
            .with_context(|| {
                format!("could not find key `{GRPC_ADVERTISE_ADDR_KEY}` in Chitchat node state")
            })
            .map(|grpc_advertise_addr_value| {
                grpc_advertise_addr_value.parse().with_context(|| {
                    format!("failed to parse gRPC advertise address `{grpc_advertise_addr_value}`")
                })
            })?
    }

    fn is_ready(&self) -> bool {
        self.get(READINESS_KEY)
            .map(|health_value| health_value == READINESS_VALUE_READY)
            .unwrap_or(false)
    }
}

/// Cluster member.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterMember {
    /// A unique node ID across the cluster.
    /// The Chitchat node ID is the concatenation of the node ID and the start timestamp:
    /// `{node_id}/{start_timestamp}`.
    pub node_id: NodeId,
    /// The start timestamp (seconds) of the node.
    pub generation_id: GenerationId,
    /// Enabled services, i.e. services configured to run on the node. Depending on the node and
    /// service health, each service may or may not be available/running.
    pub enabled_services: HashSet<QuickwitService>,
    /// Gossip advertise address, i.e. the address that other nodes should use to gossip with the
    /// node.
    pub gossip_advertise_addr: SocketAddr,
    /// gRPC advertise address, i.e. the address that other nodes should use to communicate with
    /// the node via gRPC.
    pub grpc_advertise_addr: SocketAddr,
    /// Running indexing plan.
    /// None if the node is not an indexer or the indexer has not yet started some indexing
    /// pipelines.
    pub indexing_tasks: Vec<IndexingTask>,
    /// Indexing cpu capacity of the node expressed in milli cpu.
    pub indexing_cpu_capacity: CpuCapacity,
    pub is_ready: bool,
}

impl ClusterMember {
    pub fn chitchat_id(&self) -> ChitchatId {
        ChitchatId::new(
            self.node_id.clone().into(),
            self.generation_id.as_u64(),
            self.gossip_advertise_addr,
        )
    }
}

impl From<ClusterMember> for ChitchatId {
    fn from(member: ClusterMember) -> Self {
        member.chitchat_id()
    }
}

fn parse_indexing_cpu_capacity(node_state: &NodeState) -> CpuCapacity {
    let Some(indexing_capacity_str) = node_state.get(INDEXING_CPU_CAPACITY_KEY) else {
        return CpuCapacity::zero();
    };
    if let Ok(indexing_capacity) = CpuCapacity::from_str(indexing_capacity_str) {
        indexing_capacity
    } else {
        error!(indexing_capacity=?indexing_capacity_str, "received an unparseable indexing capacity from node");
        CpuCapacity::zero()
    }
}

// Builds a cluster member from a [`NodeState`].
pub(crate) fn build_cluster_member(
    chitchat_id: ChitchatId,
    node_state: &NodeState,
) -> anyhow::Result<ClusterMember> {
    let is_ready = node_state.is_ready();
    let enabled_services = node_state
        .get(ENABLED_SERVICES_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "could not find `{}` key in node `{}` state",
                ENABLED_SERVICES_KEY,
                chitchat_id.node_id
            )
        })
        .map(|enabled_services_str| {
            parse_enabled_services_str(enabled_services_str, &chitchat_id.node_id)
        })?;
    let grpc_advertise_addr = node_state.grpc_advertise_addr()?;
    let indexing_tasks = parse_indexing_tasks(node_state);
    let indexing_cpu_capacity = parse_indexing_cpu_capacity(node_state);
    let member = ClusterMember {
        node_id: chitchat_id.node_id.into(),
        generation_id: chitchat_id.generation_id.into(),
        is_ready,
        enabled_services,
        gossip_advertise_addr: chitchat_id.gossip_advertise_addr,
        grpc_advertise_addr,
        indexing_tasks,
        indexing_cpu_capacity,
    };
    Ok(member)
}

fn parse_enabled_services_str(
    enabled_services_str: &str,
    node_id: &str,
) -> HashSet<QuickwitService> {
    let enabled_services: HashSet<QuickwitService> = enabled_services_str
        .split(',')
        .filter(|service_str| !service_str.is_empty())
        .filter_map(|service_str| match service_str.parse() {
            Ok(service) => Some(service),
            Err(_) => {
                warn!(
                    node_id=%node_id,
                    service=%service_str,
                    "Found unknown service enabled on node."
                );
                None
            }
        })
        .collect();
    if enabled_services.is_empty() {
        warn!(
            node_id=%node_id,
            "Node has no enabled services."
        )
    }
    enabled_services
}
