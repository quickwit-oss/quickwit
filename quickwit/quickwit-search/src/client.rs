// Copyright (C) 2023 Quickwit, Inc.
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
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use http::Uri;
use quickwit_proto::search::{
    GetKvRequest, LeafSearchStreamResponse, PutKvRequest, ReportSplitsRequest,
};
use quickwit_proto::tonic::codegen::InterceptedService;
use quickwit_proto::tonic::transport::{Channel, Endpoint};
use quickwit_proto::tonic::Request;
use quickwit_proto::{tonic, SpanContextInterceptor};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower::timeout::Timeout;
use tracing::*;

use crate::error::parse_grpc_error;
use crate::SearchService;

/// Impl is an enumeration that meant to manage Quickwit's search service client types.
#[derive(Clone)]
enum SearchServiceClientImpl {
    Local(Arc<dyn SearchService>),
    Grpc(
        quickwit_proto::search::search_service_client::SearchServiceClient<
            InterceptedService<Timeout<Channel>, SpanContextInterceptor>,
        >,
    ),
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
        client: quickwit_proto::search::search_service_client::SearchServiceClient<
            InterceptedService<Timeout<Channel>, SpanContextInterceptor>,
        >,
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

    /// Returns whether the underlying client is local or remote.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn is_local(&self) -> bool {
        matches!(self.client_impl, SearchServiceClientImpl::Local(_))
    }

    /// Perform root search.
    pub async fn root_search(
        &mut self,
        request: quickwit_proto::search::SearchRequest,
    ) -> crate::Result<quickwit_proto::search::SearchResponse> {
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
        request: quickwit_proto::search::LeafSearchRequest,
    ) -> crate::Result<quickwit_proto::search::LeafSearchResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(request);
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
        request: quickwit_proto::search::LeafSearchStreamRequest,
    ) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let mut grpc_client_clone = grpc_client.clone();
                let span = info_span!(
                    "client:leaf_search_stream",
                    grpc_addr=?self.grpc_addr()
                );
                let tonic_request = Request::new(request);
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
        request: quickwit_proto::search::FetchDocsRequest,
    ) -> crate::Result<quickwit_proto::search::FetchDocsResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(request);
                let tonic_response = grpc_client
                    .fetch_docs(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.fetch_docs(request).await,
        }
    }

    /// Perform leaf list terms.
    pub async fn leaf_list_terms(
        &mut self,
        request: quickwit_proto::search::LeafListTermsRequest,
    ) -> crate::Result<quickwit_proto::search::LeafListTermsResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(request);
                let tonic_response = grpc_client
                    .leaf_list_terms(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.leaf_list_terms(request).await,
        }
    }

    /// Gets the value associated to a key stored locally in the targetted node.
    /// This call is not "distributed".
    /// If the key is not present on the targetted search `None` is simply returned.
    pub async fn get_kv(&mut self, get_kv_req: GetKvRequest) -> crate::Result<Option<Vec<u8>>> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Local(service) => {
                let search_after_context_opt = service.get_kv(get_kv_req).await;
                Ok(search_after_context_opt)
            }
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let grpc_resp: tonic::Response<quickwit_proto::search::GetKvResponse> = grpc_client
                    .get_kv(get_kv_req)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                let get_search_after_context_resp = grpc_resp.into_inner();
                Ok(get_search_after_context_resp.payload)
            }
        }
    }

    /// Gets the value associated to a key stored locally in the targetted node.
    /// This call is not "distributed". It is up to the client to put the K,V pair
    /// on several nodes.
    pub async fn put_kv(&mut self, put_kv_req: PutKvRequest) -> crate::Result<()> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Local(service) => {
                service.put_kv(put_kv_req).await;
            }
            SearchServiceClientImpl::Grpc(grpc_client) => {
                grpc_client
                    .put_kv(put_kv_req)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
            }
        }
        Ok(())
    }

    pub async fn report_splits(&mut self, report_splits_request: ReportSplitsRequest) {
        match &mut self.client_impl {
            SearchServiceClientImpl::Local(service) => {
                service.report_splits(report_splits_request).await;
            }
            SearchServiceClientImpl::Grpc(search_client) => {
                search_client.report_splits(report_splits_request).await;
            }
        }
    }
}

/// Creates a [`SearchServiceClient`] from a socket address.
/// The underlying channel connects lazily and is set up to time out after 5 seconds. It reconnects
/// automatically should the connection be dropped.
pub fn create_search_client_from_grpc_addr(grpc_addr: SocketAddr) -> SearchServiceClient {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()
        .expect("The URI should be well-formed.");
    let channel = Endpoint::from(uri).connect_lazy();
    let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
    let client =
        quickwit_proto::search::search_service_client::SearchServiceClient::with_interceptor(
            timeout_channel,
            SpanContextInterceptor,
        );
    SearchServiceClient::from_grpc_client(client, grpc_addr)
}

/// Creates a [`SearchServiceClient`] from a pre-established connection (channel).
pub fn create_search_client_from_channel(
    grpc_addr: SocketAddr,
    channel: Timeout<Channel>,
) -> SearchServiceClient {
    let client =
        quickwit_proto::search::search_service_client::SearchServiceClient::with_interceptor(
            channel,
            SpanContextInterceptor,
        );
    SearchServiceClient::from_grpc_client(client, grpc_addr)
}
