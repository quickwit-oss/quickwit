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

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use http::Uri;
use opentelemetry::global;
use opentelemetry::propagation::Injector;
use quickwit_proto::LeafSearchStreamResponse;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::parse_grpc_error;
use crate::SearchService;

struct MetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for MetadataMap<'a> {
    /// Sets a key and value in the MetadataMap.  Does nothing if the key or value are not valid
    /// inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(metadata_key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(metadata_value) = tonic::metadata::MetadataValue::from_str(&value) {
                self.0.insert(metadata_key, metadata_value);
            }
        }
    }
}

/// Impl is an enumeration that meant to manage Quickwit's search service client types.
#[derive(Clone)]
enum SearchServiceClientImpl {
    Local(Arc<dyn SearchService>),
    Grpc(quickwit_proto::search_service_client::SearchServiceClient<Channel>),
}

/// A search service client.
/// It contains the client implementation and the gRPC address of the node to which the client
/// connects.
#[derive(Clone)]
pub struct SearchServiceClient {
    client_impl: SearchServiceClientImpl,
    grpc_addr: SocketAddr,
}

impl fmt::Debug for SearchServiceClient {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match &self.client_impl {
            SearchServiceClientImpl::Local(_service) => {
                write!(formatter, "Local({:?})", self.grpc_addr)
            }
            SearchServiceClientImpl::Grpc(_grpc_client) => {
                write!(formatter, "Grpc({:?})", self.grpc_addr)
            }
        }
    }
}

impl SearchServiceClient {
    /// Create a search service client instance given a gRPC client and gRPC address.
    pub fn from_grpc_client(
        client: quickwit_proto::search_service_client::SearchServiceClient<Channel>,
        grpc_addr: SocketAddr,
    ) -> Self {
        SearchServiceClient {
            client_impl: SearchServiceClientImpl::Grpc(client),
            grpc_addr,
        }
    }

    /// Create a search service client instance given a search service and gRPC address.
    pub fn from_service(service: Arc<dyn SearchService>, grpc_addr: SocketAddr) -> Self {
        SearchServiceClient {
            client_impl: SearchServiceClientImpl::Local(service),
            grpc_addr,
        }
    }

    /// Return the grpc_addr the underlying client connects to.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Perform root search.
    pub async fn root_search(
        &mut self,
        request: quickwit_proto::SearchRequest,
    ) -> crate::Result<quickwit_proto::SearchResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(request);
                let tonic_response = grpc_client
                    .root_search(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.root_search(request).await,
        }
    }

    /// Perform leaf search.
    pub async fn leaf_search(
        &mut self,
        request: quickwit_proto::LeafSearchRequest,
    ) -> crate::Result<quickwit_proto::LeafSearchResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let mut tonic_request = Request::new(request);
                global::get_text_map_propagator(|propagator| {
                    propagator.inject_context(
                        &tracing::Span::current().context(),
                        &mut MetadataMap(tonic_request.metadata_mut()),
                    )
                });
                let tonic_response = grpc_client
                    .leaf_search(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.leaf_search(request).await,
        }
    }

    /// Perform leaf stream.
    pub async fn leaf_search_stream(
        &mut self,
        request: quickwit_proto::LeafSearchStreamRequest,
    ) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let mut grpc_client_clone = grpc_client.clone();
                let span = info_span!(
                    "client:leaf_search_stream",
                    grpc_addr=?self.grpc_addr()
                );
                let mut tonic_request = Request::new(request);
                global::get_text_map_propagator(|propagator| {
                    propagator.inject_context(
                        &tracing::Span::current().context(),
                        &mut MetadataMap(tonic_request.metadata_mut()),
                    )
                });
                let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
                tokio::spawn(
                    async move {
                        let tonic_result = grpc_client_clone
                            .leaf_search_stream(tonic_request)
                            .await
                            .map_err(|tonic_error| parse_grpc_error(&tonic_error));
                        // If the grpc client fails, send the error in the channel and stop.
                        if let Err(error) = tonic_result {
                            // It is ok to ignore error sending error.
                            let _ = result_sender.send(Err(error));
                            return;
                        }
                        let mut results_stream = tonic_result
                            .unwrap()
                            .into_inner()
                            .map_err(|tonic_error| parse_grpc_error(&tonic_error));
                        while let Some(search_result) = results_stream.next().await {
                            let send_result = result_sender.send(search_result);
                            // If we get a sending error, stop consuming the stream.
                            if send_result.is_err() {
                                break;
                            }
                        }
                    }
                    .instrument(span),
                );
                UnboundedReceiverStream::new(result_receiver)
            }
            SearchServiceClientImpl::Local(service) => {
                let stream_result = service.leaf_search_stream(request).await;
                stream_result.unwrap_or_else(|error| {
                    let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
                    // Receiver cannot be closed here, ignore error.
                    let _ = result_sender.send(Err(error));
                    UnboundedReceiverStream::new(result_receiver)
                })
            }
        }
    }

    /// Perform fetch docs.
    pub async fn fetch_docs(
        &mut self,
        request: quickwit_proto::FetchDocsRequest,
    ) -> crate::Result<quickwit_proto::FetchDocsResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let mut tonic_request = Request::new(request);
                global::get_text_map_propagator(|propagator| {
                    propagator.inject_context(
                        &tracing::Span::current().context(),
                        &mut MetadataMap(tonic_request.metadata_mut()),
                    )
                });
                let tonic_response = grpc_client
                    .fetch_docs(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.fetch_docs(request).await,
        }
    }
}

/// Create a SearchServiceClient with SocketAddr as an argument.
/// It will try to reconnect to the node automatically.
pub async fn create_search_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<SearchServiceClient> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri).connect_lazy();
    let client = SearchServiceClient::from_grpc_client(
        quickwit_proto::search_service_client::SearchServiceClient::new(channel),
        grpc_addr,
    );
    Ok(client)
}
