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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytesize::ByteSize;
use futures::future::{BoxFuture, FutureExt, Shared};
use http::Uri;
use quickwit_common::tracing_utils::SpanContextInterceptor;
use quickwit_proto::search::{GetKvRequest, PutKvRequest, ReportSplitsRequest};
use quickwit_proto::tonic;
use quickwit_proto::tonic::Request;
use quickwit_proto::tonic::codegen::InterceptedService;
use quickwit_proto::tonic::transport::{Channel, Endpoint};
use tokio::time::Instant;
use tower::timeout::Timeout;
use tracing::warn;

use crate::SearchService;
use crate::error::parse_grpc_error;

const LOAD_CACHE_TTL: Duration = Duration::from_secs(1);

type SharedLoadFut = Shared<BoxFuture<'static, crate::Result<usize>>>;

enum LoadCacheState {
    Empty,
    Fresh { value: usize, fetched_at: Instant },
    InFlight { fut: SharedLoadFut, token: Arc<()> },
}

struct LoadCache {
    state: Mutex<LoadCacheState>,
}

impl LoadCache {
    fn new() -> Self {
        LoadCache {
            state: Mutex::new(LoadCacheState::Empty),
        }
    }

    /// Returns the cached load value, or fetches it if the cache is empty or expired.
    ///
    /// Concurrent callers are collapsed into a single in-flight request: all callers
    /// that arrive while a fetch is in-progress share its result (including errors).
    /// Errors are not cached — the next call after an error triggers a fresh fetch.
    async fn get(
        self: Arc<Self>,
        fetch: impl FnOnce() -> BoxFuture<'static, crate::Result<usize>>,
    ) -> crate::Result<usize> {
        let fut_and_token = {
            let mut state = self.state.lock().unwrap();
            let fut_opt = match &*state {
                LoadCacheState::Fresh { value, fetched_at }
                    if fetched_at.elapsed() < LOAD_CACHE_TTL =>
                {
                    return Ok(*value);
                }
                LoadCacheState::InFlight { fut, token } => Some((fut.clone(), token.clone())),
                _ => None,
            };
            if let Some(pair) = fut_opt {
                pair
            } else {
                let shared = fetch().shared();
                let token = Arc::new(());
                *state = LoadCacheState::InFlight {
                    fut: shared.clone(),
                    token: token.clone(),
                };
                (shared, token)
            }
        };
        let (fut, token) = fut_and_token;

        let result = fut.await;

        {
            let mut state = self.state.lock().unwrap();
            if let LoadCacheState::InFlight {
                token: current_token,
                ..
            } = &*state
                && Arc::ptr_eq(current_token, &token)
            {
                *state = match &result {
                    Ok(value) => LoadCacheState::Fresh {
                        value: *value,
                        fetched_at: Instant::now(),
                    },
                    Err(_) => LoadCacheState::Empty,
                };
            }
        }

        result
    }
}

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
    load_cache: Arc<LoadCache>,
    /// In test/testsuite builds, overrides the load returned by `get_load()` for local clients,
    /// so that tests using mock services don't need to set up `get_load` expectations.
    #[cfg(any(test, feature = "testsuite"))]
    test_load: Option<usize>,
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
            load_cache: Arc::new(LoadCache::new()),
            #[cfg(any(test, feature = "testsuite"))]
            test_load: None,
        }
    }

    /// Create a search service client instance given a search service and gRPC address.
    pub fn from_service(service: Arc<dyn SearchService>, grpc_addr: SocketAddr) -> Self {
        SearchServiceClient {
            client_impl: SearchServiceClientImpl::Local(service),
            grpc_addr,
            load_cache: Arc::new(LoadCache::new()),
            #[cfg(any(test, feature = "testsuite"))]
            test_load: None,
        }
    }

    /// Sets the load to return from `get_load()` for this client in test/testsuite builds.
    ///
    /// This short-circuits the call to the underlying service so that mock services
    /// do not need to set up `get_load` expectations in tests unrelated to load-aware placement.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn with_test_load(mut self, load: usize) -> Self {
        self.test_load = Some(load);
        self
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

    /// Returns the current load of the targeted node, expressed as the sum of job costs
    /// across all queued and active tasks in its SearchPermitProvider.
    ///
    /// Results are cached for [`LOAD_CACHE_TTL`]. Concurrent callers while a fetch is
    /// in-flight share a single request via [`LoadCache`].
    pub async fn get_load(&mut self) -> crate::Result<usize> {
        // In test/testsuite builds, short-circuit for local clients so that mock services
        // do not need a `get_load` expectation in tests unrelated to load-aware placement.
        #[cfg(any(test, feature = "testsuite"))]
        if let SearchServiceClientImpl::Local(_) = &self.client_impl {
            return Ok(self.test_load.unwrap_or(0));
        }
        let client_impl = self.client_impl.clone();
        Arc::clone(&self.load_cache)
            .get(|| Box::pin(fetch_load_uncached(client_impl)))
            .await
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

async fn fetch_load_uncached(mut client_impl: SearchServiceClientImpl) -> crate::Result<usize> {
    match &mut client_impl {
        SearchServiceClientImpl::Local(service) => Ok(service.get_load().await),
        SearchServiceClientImpl::Grpc(grpc_client) => {
            match grpc_client
                .get_load(quickwit_proto::search::GetLoadRequest {})
                .await
            {
                Ok(response) => Ok(response.into_inner().load_job_cost as usize),
                // Older searcher nodes do not implement `get_load`. To
                // preserve a smooth upgrade path, treat them as unloaded.
                Err(tonic_error) if tonic_error.code() == tonic::Code::Unimplemented => Ok(0),
                Err(tonic_error) => Err(parse_grpc_error(&tonic_error)),
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::time;

    use super::*;
    use crate::SearchError;

    #[tokio::test]
    async fn test_load_cache_hit() {
        let counter = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(LoadCache::new());
        let c = counter.clone();
        let result1 = Arc::clone(&cache)
            .get(|| {
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(42_usize)
                })
            })
            .await
            .unwrap();
        let c = counter.clone();
        let result2 = Arc::clone(&cache)
            .get(|| {
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(99_usize)
                })
            })
            .await
            .unwrap();
        assert_eq!(result1, 42);
        assert_eq!(result2, 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_load_cache_expiry() {
        let cache = Arc::new(LoadCache::new());
        Arc::clone(&cache)
            .get(|| Box::pin(async { Ok(1_usize) }))
            .await
            .unwrap();
        time::advance(LOAD_CACHE_TTL + Duration::from_millis(1)).await;
        let result = Arc::clone(&cache)
            .get(|| Box::pin(async { Ok(2_usize) }))
            .await
            .unwrap();
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_load_cache_single_flight() {
        let counter = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(LoadCache::new());
        let barrier = Arc::new(tokio::sync::Barrier::new(10));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let counter = counter.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    Arc::clone(&cache)
                        .get(|| {
                            let counter = counter.clone();
                            Box::pin(async move {
                                counter.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                Ok(7_usize)
                            })
                        })
                        .await
                })
            })
            .collect();
        let results = futures::future::join_all(handles).await;
        for r in &results {
            assert_eq!(*r.as_ref().unwrap().as_ref().unwrap(), 7);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_load_cache_error_not_cached() {
        let cache = Arc::new(LoadCache::new());
        let result1 = Arc::clone(&cache)
            .get(|| {
                Box::pin(async { Err(SearchError::Internal("oops".to_string())) })
            })
            .await;
        assert!(result1.is_err());
        let result2 = Arc::clone(&cache)
            .get(|| Box::pin(async { Ok(7_usize) }))
            .await;
        assert_eq!(result2.unwrap(), 7);
    }
}
