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

use std::fmt::Debug;
use std::sync::Arc;

use chitchat::ChitchatId;

#[derive(Clone)]
pub struct ClusterNode {
    inner: Arc<InnerNode>,
}

impl ClusterNode {
    //     pub fn try_from_node_state(
    //         chitchat_id: ChitchatId,
    //         node_state: &ClusterNodeState,
    //         channel: Channel,
    //         is_self_node: bool,
    //     ) -> anyhow::Result<Self> {
    //         let is_ready = node_state
    //             .get(READINESS_KEY)
    //             .map(|value| value == READINESS_VALUE_READY)
    //             .unwrap_or(false);
    //         // let grpc_advertise_addr = parse_grpc_advertisse_addr(node_state)?;
    //         // let enabled_services = parse_enabled_services(node_state)?;
    //         // let indexing_tasks = NodeStateExtparse_indexing_tasks(node_state)?;
    //         let inner = InnerNode {
    //             chitchat_id,
    //             is_ready,
    //             is_self_node,
    //             enabled_services,
    //             grpc_advertise_addr,
    //             channel,
    //         };
    //         let node = Node {
    //             inner: Arc::new(inner),
    //         };
    //         Ok(node)
    //     }

    pub fn chitchat_id(&self) -> &ChitchatId {
        &self.inner.chitchat_id
    }

    pub fn node_id(&self) -> &str {
        &self.inner.chitchat_id.node_id
    }

    //     pub fn channel(&self) -> Channel {
    //         self.inner.channel.clone()
    //     }

    //     pub fn enabled_services(&self) -> &BTreeSet<QuickwitService> {
    //         &self.inner.enabled_services
    //     }

    //     pub fn indexing_tasks(&self) -> &[IndexingTask] {
    //         &self.inner.indexing_tasks
    //     }

    pub fn is_ready(&self) -> bool {
        self.inner.is_ready
    }

    //     pub fn is_self_node(&self) -> bool {
    //         self.inner.is_self_node
    //     }
}

impl Debug for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("node_id", &self.inner.chitchat_id.node_id)
            .field("is_ready", &self.inner.is_ready)
            // .field("is_self_node", &self.inner.is_self_node)
            // .field("enabled_services", &self.inner.enabled_services)
            .finish()
    }
}

struct InnerNode {
    chitchat_id: ChitchatId,
    is_ready: bool,
    // is_self_node: bool,
    // grpc_advertise_addr: SocketAddr,
    // channel: Channel,
    // enabled_services: BTreeSet<QuickwitService>,
    // indexing_tasks: Vec<IndexingTask>,
}
