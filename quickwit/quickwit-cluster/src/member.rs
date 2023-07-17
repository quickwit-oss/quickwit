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

use anyhow::{anyhow, Context};
use chitchat::{ChitchatId, NodeState};
use itertools::Itertools;
use quickwit_proto::indexing::IndexingTask;
use tracing::warn;

use crate::{GenerationId, NodeRole};

// Keys used to store member's data in chitchat state.
pub(crate) const GRPC_ADVERTISE_ADDR_KEY: &str = "grpc_advertise_addr";
pub(crate) const ASSIGNED_ROLES_KEY: &str = "assigned_roles";
// An indexing task key is formatted as
// `{INDEXING_TASK_PREFIX}{INDEXING_TASK_SEPARATOR}{index_id}{INDEXING_TASK_SEPARATOR}{source_id}`.
pub(crate) const INDEXING_TASK_PREFIX: &str = "indexing_task";
pub(crate) const INDEXING_TASK_SEPARATOR: char = ':';

// Readiness key and values used to store node's readiness in Chitchat state.
pub(crate) const READINESS_KEY: &str = "readiness";
pub(crate) const READINESS_VALUE_READY: &str = "READY";
pub(crate) const READINESS_VALUE_NOT_READY: &str = "NOT_READY";

pub(crate) trait NodeStateExt {
    fn grpc_advertise_addr(&self) -> anyhow::Result<SocketAddr>;

    fn is_ready(&self) -> bool;
}

impl NodeStateExt for NodeState {
    fn grpc_advertise_addr(&self) -> anyhow::Result<SocketAddr> {
        self.get(GRPC_ADVERTISE_ADDR_KEY)
            .with_context(|| {
                format!("Could not find key `{GRPC_ADVERTISE_ADDR_KEY}` in Chitchat node state.")
            })
            .map(|grpc_advertise_addr_value| {
                grpc_advertise_addr_value.parse().with_context(|| {
                    format!("Failed to parse gRPC advertise address `{grpc_advertise_addr_value}`.")
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
    pub node_id: String,
    /// The start timestamp (seconds) of the node.
    pub generation_id: GenerationId,
    /// Roles assigned to the node that determine which services are running
    /// on it. Depending on the node roles and services health, some services may or may not be
    /// available/running.
    pub assigned_roles: HashSet<NodeRole>,
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
    pub is_ready: bool,
}

impl ClusterMember {
    pub fn new(
        node_id: String,
        generation_id: GenerationId,
        is_ready: bool,
        assigned_roles: HashSet<NodeRole>,
        gossip_advertise_addr: SocketAddr,
        grpc_advertise_addr: SocketAddr,
        indexing_tasks: Vec<IndexingTask>,
    ) -> Self {
        Self {
            node_id,
            generation_id,
            is_ready,
            assigned_roles: assigned_roles,
            gossip_advertise_addr,
            grpc_advertise_addr,
            indexing_tasks,
        }
    }

    pub fn chitchat_id(&self) -> ChitchatId {
        ChitchatId::new(
            self.node_id.clone(),
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

// Builds a cluster member from a [`NodeState`].
pub(crate) fn build_cluster_member(
    chitchat_id: ChitchatId,
    node_state: &NodeState,
) -> anyhow::Result<ClusterMember> {
    let is_ready = node_state.is_ready();
    let assigned_roles = node_state
        .get(ASSIGNED_ROLES_KEY)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Could not find `{}` key in node `{}` state.",
                ASSIGNED_ROLES_KEY,
                chitchat_id.node_id
            )
        })
        .map(|assigned_roles_str| {
            parse_assigned_roles_str(assigned_roles_str, &chitchat_id.node_id)
        })?;
    let grpc_advertise_addr = node_state.grpc_advertise_addr()?;
    let indexing_tasks = parse_indexing_tasks(node_state, &chitchat_id.node_id);
    let member = ClusterMember::new(
        chitchat_id.node_id,
        chitchat_id.generation_id.into(),
        is_ready,
        assigned_roles,
        chitchat_id.gossip_advertise_addr,
        grpc_advertise_addr,
        indexing_tasks,
    );
    Ok(member)
}

// Parses indexing task key into the IndexingTask.
fn parse_indexing_task_key(key: &str) -> anyhow::Result<IndexingTask> {
    let (_prefix, reminder) = key.split_once(INDEXING_TASK_SEPARATOR).ok_or_else(|| {
        anyhow!(
            "Indexing task must contain the delimiter character `:`: `{}`",
            key
        )
    })?;
    IndexingTask::try_from(reminder)
}

/// Parses indexing tasks serialized in keys formatted as
/// `INDEXING_TASK_PREFIX:index_id:index_incarnation:source_id`. Malformed keys and values are
/// ignored, just warnings are emitted.
pub(crate) fn parse_indexing_tasks(node_state: &NodeState, node_id: &str) -> Vec<IndexingTask> {
    node_state
        .key_values(|key, _| key.starts_with(INDEXING_TASK_PREFIX))
        .map(|(key, versioned_value)| {
            let indexing_task = parse_indexing_task_key(key)?;
            let num_tasks: usize = versioned_value.value.parse()?;
            Ok((0..num_tasks).map(move |_| indexing_task.clone()))
        })
        .flatten_ok()
        .filter_map(
            |indexing_task_parsing_result: anyhow::Result<IndexingTask>| {
                match indexing_task_parsing_result {
                    Ok(indexing_task) => Some(indexing_task),
                    Err(error) => {
                        warn!(
                            node_id=%node_id,
                            error=%error,
                            "Malformated indexing task key and value on node."
                        );
                        None
                    }
                }
            },
        )
        .collect()
}

fn parse_assigned_roles_str(assigned_roles_str: &str, node_id: &str) -> HashSet<NodeRole> {
    let assigned_roles: HashSet<NodeRole> = assigned_roles_str
        .split(',')
        .filter(|role_str| !role_str.is_empty())
        .filter_map(|role_str| match role_str.parse() {
            Ok(role) => Some(role),
            Err(_) => {
                warn!(
                    node_id=%node_id,
                    role=%role_str,
                    "Unknown node role."
                );
                None
            }
        })
        .collect();
    if assigned_roles.is_empty() {
        warn!(
            node_id=%node_id,
            "Node has no assigned role."
        )
    }
    assigned_roles
}
