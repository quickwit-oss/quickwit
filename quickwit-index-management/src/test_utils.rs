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

use std::sync::Arc;

use quickwit_actors::Universe;
use quickwit_common::uri::Uri as QuickiwtUri;
use quickwit_metastore::Metastore;
use quickwit_proto::index_management_service_server::IndexManagementServiceServer;
use quickwit_proto::tonic;
use quickwit_storage::StorageUriResolver;
use tonic::transport::Server;

use crate::grpc_adapter::GrpcIndexManagementServiceAdapter;
use crate::{IndexManagementClient, IndexManagementService};

// Creates an [`IndexManagementClient`] and use a gRPC server with the adapter so
// that it sends requests to the [`IndexManagementService`] and thus to the
// given metastore.
pub async fn create_index_management_client_for_test(
    mock_metastore: Arc<dyn Metastore>,
    universe: &Universe,
) -> anyhow::Result<IndexManagementClient> {
    let (client, server) = tokio::io::duplex(1024);
    let storage_resolver = StorageUriResolver::for_test();
    let default_index_root_uri = QuickiwtUri::new("ram:///test".to_string());
    let index_management_service =
        IndexManagementService::new(mock_metastore, storage_resolver, default_index_root_uri);
    let (service_mailbox, _) = universe.spawn_actor(index_management_service).spawn();
    let grpc_adapter = GrpcIndexManagementServiceAdapter::new(service_mailbox);
    tokio::spawn(async move {
        Server::builder()
            .add_service(IndexManagementServiceServer::new(grpc_adapter))
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });
    let client = IndexManagementClient::from_duplex_stream(client).await?;
    Ok(client)
}
