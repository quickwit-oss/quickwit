// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use http::Uri;
use quickwit_proto::search::{GetKvRequest, PutKvRequest, ReportSplitsRequest};
use quickwit_proto::tonic::Request;
use quickwit_proto::tonic::codegen::InterceptedService;
use quickwit_proto::tonic::transport::{Channel, Endpoint};
use quickwit_proto::{SpanContextInterceptor, tonic};
use tower::timeout::Timeout;
use tracing::warn;

use crate::SearchService;
use crate::error::parse_grpc_error;

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
            SearchServiceClientImpl::Grpc(grpc_client) => grpc_client
                .root_search(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
            SearchServiceClientImpl::Local(service) => service.root_search(request).await,
        }
    }

    /// Perform leaf search.
    pub async fn leaf_search(
        &mut self,
        request: quickwit_proto::search::LeafSearchRequest,
    ) -> crate::Result<quickwit_proto::search::LeafSearchResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => grpc_client
                .leaf_search(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
            SearchServiceClientImpl::Local(service) => service.leaf_search(request).await,
        }
    }

    /// Perform leaf search.
    pub async fn leaf_list_fields(
        &mut self,
        request: quickwit_proto::search::LeafListFieldsRequest,
    ) -> crate::Result<quickwit_proto::search::ListFieldsResponse> {
        match &mut self.client_impl {
            SearchServiceClientImpl::Grpc(grpc_client) => {
                let tonic_request = Request::new(request);
                let tonic_response = grpc_client
                    .leaf_list_fields(tonic_request)
                    .await
                    .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
                Ok(tonic_response.into_inner())
            }
            SearchServiceClientImpl::Local(service) => service.leaf_list_fields(request).await,
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

    /// Gets the value associated to a key stored locally in the targeted node.
    /// This call is not "distributed".
    /// If the key is not present on the targeted search `None` is simply returned.
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

    /// Gets the value associated to a key stored locally in the targeted node.
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

    /// Indexers call report_splits to inform searchers node about the presence of a split, which
    /// would then be considered as a candidate for the searcher split cache.
    pub async fn report_splits(&mut self, report_splits_request: ReportSplitsRequest) {
        match &mut self.client_impl {
            SearchServiceClientImpl::Local(service) => {
                let _ = service.report_splits(report_splits_request).await;
            }
            SearchServiceClientImpl::Grpc(search_client) => {
                // Ignoring any error.
                if search_client
                    .report_splits(report_splits_request)
                    .await
                    .is_err()
                {
                    warn!(
                        "Failed to report splits. This is not critical as this message is only \
                         used to identify caching opportunities."
                    );
                }
            }
        }
    }
}

/// Creates a [`SearchServiceClient`] from a socket address.
/// The underlying channel connects lazily and is set up to time out after 5 seconds. It reconnects
/// automatically should the connection be dropped.
pub fn create_search_client_from_grpc_addr(
    grpc_addr: SocketAddr,
    max_message_size: ByteSize,
) -> SearchServiceClient {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()
        .expect("The URI should be well-formed.");
    let channel = Endpoint::from(uri).connect_lazy();
    let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
    create_search_client_from_channel(grpc_addr, timeout_channel, max_message_size)
}

/// Creates a [`SearchServiceClient`] from a pre-established connection (channel).
pub fn create_search_client_from_channel(
    grpc_addr: SocketAddr,
    channel: Timeout<Channel>,
    max_message_size: ByteSize,
) -> SearchServiceClient {
    let client =
        quickwit_proto::search::search_service_client::SearchServiceClient::with_interceptor(
            channel,
            SpanContextInterceptor,
        )
        .max_decoding_message_size(max_message_size.0 as usize)
        .max_encoding_message_size(max_message_size.0 as usize);
    SearchServiceClient::from_grpc_client(client, grpc_addr)
}
