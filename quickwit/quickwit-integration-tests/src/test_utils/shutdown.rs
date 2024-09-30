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

use std::collections::{HashMap, HashSet};

use quickwit_actors::ActorExitStatus;
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_config::service::QuickwitService;
use quickwit_proto::types::NodeId;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::task::JoinHandle;

type NodeJoinHandle = JoinHandle<Result<HashMap<String, ActorExitStatus>, anyhow::Error>>;

pub(crate) struct NodeShutdownHandle {
    sender: Sender<()>,
    receiver: Receiver<()>,
    pub node_services: HashSet<QuickwitService>,
    pub node_id: NodeId,
    join_handle_opt: Option<NodeJoinHandle>,
}

impl NodeShutdownHandle {
    pub(crate) fn new(node_id: NodeId, node_services: HashSet<QuickwitService>) -> Self {
        let (sender, receiver) = watch::channel(());
        Self {
            sender,
            receiver,
            node_id,
            node_services,
            join_handle_opt: None,
        }
    }

    pub(crate) fn shutdown_signal(&self) -> BoxFutureInfaillible<()> {
        let receiver = self.receiver.clone();
        Box::pin(async move {
            receiver.clone().changed().await.unwrap();
        })
    }

    pub(crate) fn set_node_join_handle(&mut self, join_handle: NodeJoinHandle) {
        self.join_handle_opt = Some(join_handle);
    }

    /// Initiate node shutdown and wait for it to complete

    pub(crate) async fn shutdown(
        self,
    ) -> anyhow::Result<HashMap<std::string::String, ActorExitStatus>> {
        self.sender.send(()).unwrap();
        self.join_handle_opt
            .expect("node join handle was not set before shutdown")
            .await
            .unwrap()
    }
}
