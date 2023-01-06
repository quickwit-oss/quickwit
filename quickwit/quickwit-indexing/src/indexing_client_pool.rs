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

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::indexing_client::{create_indexing_service_client, IndexingServiceClient};

/// Simplistic indexing client pool that leaks as dead clients are not removed.
/// If a client for a given gRPC address is not in the pool, it will be created
/// on the fly. The pool is not updated with cluster member changes.
// TODO(fmassot): dynamically update the pool with cluster member changes.
pub struct IndexingClientPool {
    clients: HashMap<SocketAddr, IndexingServiceClient>,
}

impl IndexingClientPool {
    pub fn new(clients_list: Vec<IndexingServiceClient>) -> Self {
        let clients_map = clients_list
            .into_iter()
            .map(|client| (client.grpc_addr(), client))
            .collect();
        Self {
            clients: clients_map,
        }
    }

    pub async fn get_or_create(
        &self,
        grpc_addr: SocketAddr,
    ) -> anyhow::Result<IndexingServiceClient> {
        if let Some(client) = self.clients.get(&grpc_addr) {
            return Ok(client.clone());
        }
        create_indexing_service_client(grpc_addr).await
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_grpc_clients::create_channel_from_duplex_stream;
    use quickwit_proto::indexing_api::indexing_service_server::IndexingServiceServer;
    use quickwit_proto::indexing_api::ApplyIndexingPlanRequest;
    use quickwit_proto::tonic::transport::Server;

    use super::IndexingServiceClient;
    use crate::grpc_adapter::GrpcIndexingAdapter;
    use crate::IndexingService;

    #[tokio::test]
    async fn test_indexing_service_client_local() {
        let grpc_addr = ([127, 0, 0, 1], 1).into();
        let universe = Universe::new();
        let (mailbox, inbox) = universe.create_test_mailbox::<IndexingService>();
        let mut client = IndexingServiceClient::from_service(mailbox, grpc_addr);
        client
            .apply_indexing_plan(ApplyIndexingPlanRequest {
                indexing_tasks: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(inbox.drain_for_test().len(), 1);
    }

    #[tokio::test]
    async fn test_indexing_service_client_grpc() {
        let universe = Universe::new();
        let (mailbox, inbox) = universe.create_test_mailbox::<IndexingService>();
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            Server::builder()
                .add_service(IndexingServiceServer::new(GrpcIndexingAdapter::from(
                    mailbox,
                )))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        let channel = create_channel_from_duplex_stream(client).await.unwrap();
        let grpc_addr = ([127, 0, 0, 1], 1).into();
        let grpc_client =
            quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient::new(
                channel,
            );
        let mut client = IndexingServiceClient::from_grpc_client(grpc_client, grpc_addr);
        client
            .apply_indexing_plan(ApplyIndexingPlanRequest {
                indexing_tasks: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(inbox.drain_for_test().len(), 1);
    }
}
