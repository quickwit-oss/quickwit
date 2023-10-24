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
use quickwit_proto::types::NodeId;
use tracing::warn;

use crate::{GenerationId, QuickwitService};

// Keys used to store member's data in chitchat state.
pub(crate) const GRPC_ADVERTISE_ADDR_KEY: &str = "grpc_advertise_addr";
pub(crate) const ENABLED_SERVICES_KEY: &str = "enabled_services";
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
    pub is_ready: bool,
}

impl ClusterMember {
    pub fn new(
        node_id: NodeId,
        generation_id: GenerationId,
        is_ready: bool,
        enabled_services: HashSet<QuickwitService>,
        gossip_advertise_addr: SocketAddr,
        grpc_advertise_addr: SocketAddr,
        indexing_tasks: Vec<IndexingTask>,
    ) -> Self {
        Self {
            node_id,
            generation_id,
            is_ready,
            enabled_services,
            gossip_advertise_addr,
            grpc_advertise_addr,
            indexing_tasks,
        }
    }

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
    let indexing_tasks = parse_indexing_tasks(node_state, &chitchat_id.node_id);
    let member = ClusterMember::new(
        chitchat_id.node_id.into(),
        chitchat_id.generation_id.into(),
        is_ready,
        enabled_services,
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
            "indexing task must contain the delimiter character `:`: `{}`",
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
