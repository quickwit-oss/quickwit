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

use std::net::SocketAddr;
use std::sync::Arc;

use quickwit_actors::Mailbox;
use quickwit_index_management::{GrpcIndexManagementServiceAdapter, IndexManagementService};
use quickwit_cluster::QuickwitService;
use quickwit_proto::search_service_server::SearchServiceServer;
use quickwit_proto::index_management_service_server::IndexManagementServiceServer;
use quickwit_proto::tonic;
use quickwit_search::SearchService;
use tonic::transport::Server;
use tracing::*;

use crate::search_api::GrpcSearchAdapter;

/// Starts gRPC service given a gRPC address and a search service and cluster service.
pub(crate) async fn start_grpc_server(
    grpc_listen_addr: SocketAddr,
    search_service_opt: Option<Arc<dyn SearchService>>,
    index_management_service_opt: Option<Mailbox<IndexManagementService>>,
) -> anyhow::Result<()> {
    info!(grpc_listen_addr = ?grpc_listen_addr, "Starting gRPC server.");

    let mut server = Server::builder();

    // We only mount the gRPC service if the searcher is enabled on this node.
    let search_grpc_service = if let Some(search_service) = search_service_opt {
        let search_service_adpater = GrpcSearchAdapter::from(search_service);
        Some(SearchServiceServer::new(search_service_adpater))
    } else {
        None
    };

    let index_management_grpc_service = if let Some(index_management_service) = index_management_service_opt {
        let index_management_service_adapter = GrpcIndexManagementServiceAdapter::from(index_management_service);
        Some(IndexManagementServiceServer::new(index_management_service_adapter))
    } else {
        None
    };

    let server_router = server
        .add_optional_service(search_grpc_service)
        .add_optional_service(index_management_grpc_service);
    server_router.serve(grpc_listen_addr).await?;

    Ok(())
}
