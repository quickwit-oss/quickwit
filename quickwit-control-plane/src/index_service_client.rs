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
use std::sync::Arc;

use quickwit_actors::Mailbox;
use quickwit_core::IndexServiceError;
use quickwit_metastore::IndexMetadata;
use quickwit_proto::tonic;
use tonic::transport::Channel;
use tonic::Request;
use tracing::*;

use crate::actors::IndexService;

//
// IndexServiceClient used by the IndexingService to stage/publish/replace splits.
// Note: Having a local and gRPC iimplementations is nice but a bit heavy. Should we keep that?
//

/// Impl is an enumeration that meant to manage Quickwit's index service client types.
#[derive(Clone)]
enum IndexServiceClientImpl {
    Local(Mailbox<IndexService>),
    Grpc(quickwit_proto::index_service_client::IndexServiceClient<Channel>),
}

/// Parse tonic error and returns `IndexServiceError`.
pub fn parse_grpc_error(grpc_error: &tonic::Status) -> IndexServiceError {
    // TODO: parse correctly IndexServiceError message.
    IndexServiceError::InternalError(grpc_error.message().to_string())
}

/// Index service client.
/// It contains the client implementation and the gRPC address of the node to which the client
/// connects.
#[derive(Clone)]
pub struct IndexServiceClient {
    client_impl: IndexServiceClientImpl,
    grpc_addr: SocketAddr,
}

impl fmt::Debug for IndexServiceClient {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match &self.client_impl {
            IndexServiceClientImpl::Local(_service) => {
                write!(formatter, "Local({:?})", self.grpc_addr)
            }
            IndexServiceClientImpl::Grpc(_grpc_client) => {
                write!(formatter, "Grpc({:?})", self.grpc_addr)
            }
        }
    }
}

impl IndexServiceClient {
    /// Create an index service client instance given a gRPC client and gRPC address.
    pub fn from_grpc_client(
        client: quickwit_proto::index_service_client::IndexServiceClient<Channel>,
        grpc_addr: SocketAddr,
    ) -> Self {
        Self {
            client_impl: IndexServiceClientImpl::Grpc(client),
            grpc_addr,
        }
    }

    /// Create an index service client instance given a index service mailbox and gRPC address.
    pub fn from_service(service: Mailbox<IndexService>, grpc_addr: SocketAddr) -> Self {
        IndexServiceClient {
            client_impl: IndexServiceClientImpl::Local(service),
            grpc_addr,
        }
    }

    /// Return the grpc_addr the underlying client connects to.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Perform get index metadata.
    pub async fn get_index_metadata(
        &mut self,
        get_index_metadata_req: quickwit_proto::GetIndexMetadataRequest,
    ) -> Result<IndexMetadata, IndexServiceError> {
        match &mut self.client_impl {
            IndexServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(get_index_metadata_req);
                let tonic_response = grpc_client
                    .get_index_metadata(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                let inner_response = tonic_response.into_inner();
                // TODO: remove unwrap.
                let index_metadata =
                    serde_json::from_str(&inner_response.index_metadata_serialized_json).unwrap();
                Ok(index_metadata)
            }
            IndexServiceClientImpl::Local(service) => service
                .ask_for_res(get_index_metadata_req)
                .await
                .map_err(|error| IndexServiceError::InternalError(error.to_string())),
        }
    }

    pub async fn get_all_splits(
        &mut self,
        get_splits_req: quickwit_proto::GetSplitsMetadatasRequest,
    ) -> Result<Vec<IndexMetadata>, IndexServiceError> {
        todo!()
    }

    pub async fn stage_split(
        &mut self,
        state_split_req: quickwit_proto::StageSplitRequest,
    ) -> Result<Vec<IndexMetadata>, IndexServiceError> {
        todo!()
    }

    pub async fn publish_split(
        &mut self,
        state_split_req: quickwit_proto::PublishSplitRequest,
    ) -> Result<(), IndexServiceError> {
        todo!()
    }

    pub async fn replace_splits(
        &mut self,
        state_split_req: quickwit_proto::ReplaceSplitsRequest,
    ) -> Result<(), IndexServiceError> {
        todo!()
    }
}
