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
use quickwit_proto::search_service_server::SearchServiceServer;
use quickwit_proto::tonic;
use tonic::transport::Server;
use tracing::*;

use crate::cluster_api::GrpcClusterAdapter;
use crate::search_api::GrpcSearchAdapter;

/// Start gRPC service given a gRPC address and a search service and cluster service.
pub async fn start_grpc_service(
    grpc_addr: SocketAddr,
    search_service: GrpcSearchAdapter,
    cluster_service: GrpcClusterAdapter,
) -> anyhow::Result<()> {
    info!(grpc_addr=?grpc_addr, "Start gRPC service.");
    Server::builder()
        .add_service(ClusterServiceServer::new(cluster_service))
        .add_service(SearchServiceServer::new(search_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}
