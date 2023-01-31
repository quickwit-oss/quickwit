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

use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use quickwit_actors::Mailbox;
use quickwit_config::service::QuickwitService;
use quickwit_grpc_clients::service_client_pool::ServiceClient;
use quickwit_proto::indexing_api::ApplyIndexingPlanRequest;
use quickwit_proto::tonic::transport::{Channel, Endpoint, Uri};

use crate::IndexingService;

#[derive(Clone)]
enum IndexingServiceClientImpl {
    Grpc(quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient<Channel>),
    Local(Mailbox<IndexingService>),
}

#[derive(Clone)]
pub struct IndexingServiceClient {
    client_impl: IndexingServiceClientImpl,
    grpc_addr: SocketAddr,
}

impl fmt::Debug for IndexingServiceClient {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match &self.client_impl {
            IndexingServiceClientImpl::Local(_service) => {
                write!(formatter, "Local(grpc_addr={})", self.grpc_addr)
            }
            IndexingServiceClientImpl::Grpc(_grpc_client) => {
                write!(formatter, "Grpc(grpc_addr={})", self.grpc_addr)
            }
        }
    }
}

#[async_trait]
impl ServiceClient for IndexingServiceClient {
    fn service() -> QuickwitService {
        QuickwitService::Indexer
    }

    async fn build_client(grpc_addr: SocketAddr) -> anyhow::Result<Self> {
        create_indexing_service_client(grpc_addr).await
    }

    fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }
}

impl IndexingServiceClient {
    pub fn from_grpc_client(
        client: quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient<
            Channel,
        >,
        grpc_addr: SocketAddr,
    ) -> Self {
        Self {
            client_impl: IndexingServiceClientImpl::Grpc(client),
            grpc_addr,
        }
    }

    pub fn from_service(service: Mailbox<IndexingService>, grpc_addr: SocketAddr) -> Self {
        Self {
            client_impl: IndexingServiceClientImpl::Local(service),
            grpc_addr,
        }
    }

    pub async fn apply_indexing_plan(
        &mut self,
        indexing_plan_request: ApplyIndexingPlanRequest,
    ) -> anyhow::Result<()> {
        match &mut self.client_impl {
            IndexingServiceClientImpl::Local(service) => {
                service.send_message(indexing_plan_request).await?;
                Ok(())
            }
            IndexingServiceClientImpl::Grpc(client) => {
                let _ = client.apply_indexing_plan(indexing_plan_request).await?;
                Ok(())
            }
        }
    }

    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }
}

pub async fn create_indexing_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<IndexingServiceClient> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    let channel = Endpoint::from(uri).connect_lazy();
    let client = IndexingServiceClient::from_grpc_client(
        quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient::new(channel),
        grpc_addr,
    );
    Ok(client)
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
