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

use std::net::SocketAddr;

use quickwit_proto::cluster_service_server::ClusterServiceServer;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsServiceServer;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceServiceServer;
use quickwit_proto::search_service_server::SearchServiceServer;
use quickwit_proto::tonic;
use tonic::transport::Server;
use tracing::*;

use crate::cluster_api::GrpcClusterAdapter;
use crate::search_api::GrpcSearchAdapter;
use crate::QuickwitServices;

/// Start gRPC service given a gRPC address and a search service and cluster service.
pub(crate) async fn start_grpc_server(
    grpc_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(grpc_addr=?grpc_addr, "Start gRPC service.");

    let mut server = Server::builder();

    let grpc_cluster_service = GrpcClusterAdapter::from(quickwit_services.cluster_service.clone());
    let mut server_router = server.add_service(ClusterServiceServer::new(grpc_cluster_service));

    if let Some(search_service) = quickwit_services.search_service.clone() {
        let grpc_search_service = GrpcSearchAdapter::from(search_service);
        server_router = server_router.add_service(SearchServiceServer::new(grpc_search_service));
    }
    if let Some(oltp_service) = &quickwit_services.oltp_service {
        server_router = server_router.add_service(LogsServiceServer::new(oltp_service.clone()));
        server_router = server_router.add_service(TraceServiceServer::new(oltp_service.clone()));
    }

    server_router.serve(grpc_addr).await?;
    Ok(())
}
