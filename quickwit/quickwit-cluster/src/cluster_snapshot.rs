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

use std::collections::BTreeSet;

use chitchat::{ChitchatId, ClusterStateSnapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ClusterSnapshot {
    #[schema(example = "qw-cluster-1")]
    /// The ID of the cluster that the node is a part of.
    pub cluster_id: String,

    #[schema(value_type = NodeIdSchema)]
    /// The unique ID of the current node.
    pub self_node_id: ChitchatId,

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
    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of cluster node IDs that are ready to handle requests.
    pub ready_nodes: BTreeSet<ChitchatId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of node IDs that are alive but not considered ready yet.
    pub live_nodes: BTreeSet<ChitchatId>,

    #[schema(value_type  = Vec<NodeIdSchema>)]
    /// The set of node IDs flagged as dead or faulty.
    pub dead_nodes: BTreeSet<ChitchatId>,

    /// A snapshot of the current cluster state.
    pub chitchat_state_snapshot: ClusterStateSnapshot,
}
