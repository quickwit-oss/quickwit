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

use quickwit_config::service::QuickwitService;
use quickwit_metastore::GrpcMetastoreAdapter;
use quickwit_proto::metastore_api::metastore_api_service_server::MetastoreApiServiceServer;
use quickwit_proto::search_service_server::SearchServiceServer;
use quickwit_proto::tonic;
use tonic::transport::Server;
use tracing::*;

use crate::search_api::GrpcSearchAdapter;
use crate::QuickwitServices;

/// Starts gRPC services given a gRPC address.
pub(crate) async fn start_grpc_server(
    grpc_listen_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(grpc_listen_addr = ?grpc_listen_addr, "Starting gRPC server.");

    let mut server = Server::builder();

    // We only mount the gRPC search service if `QuickwitService::Searcher` is enabled on this node.
    let search_grpc_service = if quickwit_services
        .services
        .contains(&QuickwitService::Searcher)
    {
        let search_service = quickwit_services.search_service.clone();
        let grpc_search_service = GrpcSearchAdapter::from(search_service);
        Some(SearchServiceServer::new(grpc_search_service))
    } else {
        None
    };

    // We only mount the gRPC metastore service if `QuickwitService::Metastore` is enabled on this
    // node.
    let metastore_grpc = if quickwit_services
        .services
        .contains(&QuickwitService::Metastore)
    {
        let metastore = quickwit_services.metastore.clone();
        let grpc_metastore = GrpcMetastoreAdapter::from(metastore);
        Some(MetastoreApiServiceServer::new(grpc_metastore))
    } else {
        None
    };

    let server_router = server
        .add_optional_service(search_grpc_service)
        .add_optional_service(metastore_grpc);
    server_router.serve(grpc_listen_addr).await?;

    Ok(())
}
