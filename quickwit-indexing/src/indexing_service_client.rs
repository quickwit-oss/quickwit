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
use quickwit_actors::Mailbox;
use quickwit_proto::tonic;
use tonic::transport::Channel;
use tonic::Request;

use crate::IndexingServiceError;
use crate::actors::IndexingService;



/// Impl is an enumeration that meant to manage Quickwit's index service client types.
#[derive(Clone)]
enum IndexingServiceClientImpl {
    Local(Mailbox<IndexingService>),
    Grpc(quickwit_proto::quicklet_service_client::QuickletServiceClient<Channel>),
}

/// Parse tonic error and returns `IndexingServiceError`.
pub fn parse_grpc_error(grpc_error: &tonic::Status) -> IndexingServiceError {
    // TODO: parse correctly IndexingServiceError message.
    IndexingServiceError::InvalidParams(anyhow::anyhow!(grpc_error.message().to_string()))
}

/// Index service client.
/// It contains the client implementation and the gRPC address of the node to which the client
/// connects.
#[derive(Clone)]
pub struct IndexingServiceClient {
    client_impl: IndexingServiceClientImpl,
    grpc_addr: SocketAddr,
}

impl fmt::Debug for IndexingServiceClient {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match &self.client_impl {
            IndexingServiceClientImpl::Local(_service) => {
                write!(formatter, "Local({:?})", self.grpc_addr)
            }
            IndexingServiceClientImpl::Grpc(_grpc_client) => {
                write!(formatter, "Grpc({:?})", self.grpc_addr)
            }
        }
    }
}

impl IndexingServiceClient {
    /// Create a search service client instance given a gRPC client and gRPC address.
    pub fn from_grpc_client(
        client: quickwit_proto::quicklet_service_client::QuickletServiceClient<Channel>,
        grpc_addr: SocketAddr,
    ) -> Self {
        Self {
            client_impl: IndexingServiceClientImpl::Grpc(client),
            grpc_addr,
        }
    }

    /// Create a search service client instance given a search service and gRPC address.
    pub fn from_service(service: Mailbox<IndexingService>, grpc_addr: SocketAddr) -> Self {
        IndexingServiceClient {
            client_impl: IndexingServiceClientImpl::Local(service),
            grpc_addr,
        }
    }

    /// Return the grpc_addr the underlying client connects to.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Applies physical indexing plan.
    pub async fn apply_physical_indexing_plan(
        &mut self,
        physical_plan_req: quickwit_proto::PhysicalIndexingPlanRequest,
    ) -> Result<(), IndexingServiceError> {
        match &mut self.client_impl {
            IndexingServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(physical_plan_req);
                let _ = grpc_client
                    .apply_physical_indexing_plan(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(())
            }
            IndexingServiceClientImpl::Local(service) => service
                .ask_for_res(physical_plan_req)
                .await
                // TODO: return the correct error.
                .map_err(|error| IndexingServiceError::InvalidParams(anyhow::anyhow!(error))),
        }
    }
}
