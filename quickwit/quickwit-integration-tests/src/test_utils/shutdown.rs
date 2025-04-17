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
