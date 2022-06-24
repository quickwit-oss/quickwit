// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::Universe;
use quickwit_config::NodeConfig;
use quickwit_metastore::{quickwit_metastore_uri_resolver, Metastore};
use quickwit_proto::nodelet_service_server::{self as grpc, NodeletServiceServer};
use quickwit_proto::{tonic, StopNodeletRequest, StopNodeletResponse};
use tokio::sync::RwLock;
use tonic::transport::Server;

mod error;
mod grpc_service;

pub use error::NodeletResult;
use tonic::transport::server::Router;

enum Components {
    Indexer,
    Searcher,
}

#[async_trait]
pub trait Component: Send + Sync + 'static {
    async fn stop(&self) -> anyhow::Result<()>;
}

// struct DummyComponent;

// #[async_trait]
// impl Component for DummyComponent {
//     async fn stop(&self) -> anyhow::Result<()> {
//         Ok(())
//     }
// }

#[derive(Clone)]
pub struct Nodelet {
    pub(crate) inner: Arc<RwLock<NodeletImpl>>,
}

impl Nodelet {
    pub async fn spawn(
        node_config: NodeConfig,
        components: Vec<Box<dyn Component>>,
    ) -> anyhow::Result<()> {
        let grpc_listen_addr = node_config.grpc_listen_addr().await?;
        let rest_listen_addr = node_config.rest_listen_addr().await?;

        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&node_config.metastore_uri())
            .await?;

        let universe = Universe::new();

        let nodelet_impl = NodeletImpl {
            node_config,
            metastore,
            components,
            universe,
        };
        let nodelet = Self {
            inner: Arc::new(RwLock::new(nodelet_impl)),
        };
        let mut server = Server::builder();
        let mut router = server.add_service(NodeletServiceServer::new(nodelet.clone()));
        for component in &components {
            // router.add_service(component.service_server())
        }
        let grpc_server = router.serve(grpc_listen_addr);
        tokio::try_join!(grpc_server)?;
        Ok(())
    }
}

pub struct NodeletImpl {
    node_config: NodeConfig,
    metastore: Arc<dyn Metastore>,
    components: Vec<Box<dyn Component>>,
    universe: Universe,
}

impl NodeletImpl {
    async fn stop_nodelet(
        &mut self,
        request: StopNodeletRequest,
    ) -> NodeletResult<StopNodeletResponse> {
        // for component in &self.components {
        //     component.stop().await?;
        // }
        Ok(StopNodeletResponse {})
    }
}