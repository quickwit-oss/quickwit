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

//! BatchingSearchService: transparent SearchService wrapper that batches root_search calls.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, GetKvRequest, LeafListFieldsRequest, LeafListTermsRequest,
    LeafListTermsResponse, LeafSearchRequest, LeafSearchResponse, ListFieldsRequest,
    ListFieldsResponse, ListTermsRequest, ListTermsResponse, PutKvRequest, ReportSplitsRequest,
    ReportSplitsResponse, ScrollRequest, SearchPlanResponse, SearchRequest, SearchResponse,
};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use super::combine::batch_grouping_key;
use super::dispatcher::{BatchEntry, batch_dispatcher};
use super::normalize::normalize_request;
use crate::SearchError;
use crate::service::SearchService;

/// Transparent [`SearchService`] wrapper that batches concurrent `root_search` calls.
pub struct BatchingSearchService {
    inner: Arc<dyn SearchService>,
    batch_tx: mpsc::UnboundedSender<BatchEntry>,
}

impl BatchingSearchService {
    /// Creates a new batching wrapper around the given search service.
    ///
    /// Spawns a background task that collects `root_search` calls and
    /// dispatches them in batches after `window` elapses.
    pub fn new(inner: Arc<dyn SearchService>, window: Duration) -> Arc<Self> {
        let (batch_tx, batch_rx) = mpsc::unbounded_channel();
        let service = Arc::new(Self {
            inner: inner.clone(),
            batch_tx,
        });
        tokio::spawn(batch_dispatcher(inner, batch_rx, window));
        service
    }
}

impl std::fmt::Debug for BatchingSearchService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchingSearchService").finish()
    }
}

#[async_trait]
impl SearchService for BatchingSearchService {
    async fn root_search(&self, request: SearchRequest) -> crate::Result<SearchResponse> {
        if !request.enable_request_batching {
            return self.inner.root_search(request).await;
        }
        let normalized = normalize_request(request);
        let batch_key = batch_grouping_key(&normalized);
        let span = tracing::debug_span!("batch_route");
        async move {
            let (result_tx, result_rx) = oneshot::channel();
            let entry = BatchEntry {
                request: normalized,
                result_tx,
                batch_key,
                span: tracing::Span::current(),
            };
            match self.batch_tx.send(entry) {
                Ok(()) => result_rx.await.map_err(|_| {
                    SearchError::Internal("batching result channel dropped".to_string())
                })?,
                Err(tokio::sync::mpsc::error::SendError(entry)) => {
                    tracing::error!("batching channel closed, falling back to unbatched search");
                    self.inner.root_search(entry.request).await
                }
            }
        }
        .instrument(span)
        .await
    }

    // all other methods delegate directly to the inner service
    async fn leaf_search(&self, request: LeafSearchRequest) -> crate::Result<LeafSearchResponse> {
        self.inner.leaf_search(request).await
    }
    async fn fetch_docs(&self, request: FetchDocsRequest) -> crate::Result<FetchDocsResponse> {
        self.inner.fetch_docs(request).await
    }
    async fn root_list_terms(&self, request: ListTermsRequest) -> crate::Result<ListTermsResponse> {
        self.inner.root_list_terms(request).await
    }
    async fn leaf_list_terms(
        &self,
        request: LeafListTermsRequest,
    ) -> crate::Result<LeafListTermsResponse> {
        self.inner.leaf_list_terms(request).await
    }
    async fn scroll(&self, request: ScrollRequest) -> crate::Result<SearchResponse> {
        self.inner.scroll(request).await
    }
    async fn put_kv(&self, request: PutKvRequest) {
        self.inner.put_kv(request).await
    }
    async fn get_kv(&self, request: GetKvRequest) -> Option<Vec<u8>> {
        self.inner.get_kv(request).await
    }
    async fn report_splits(&self, request: ReportSplitsRequest) -> ReportSplitsResponse {
        self.inner.report_splits(request).await
    }
    async fn root_list_fields(
        &self,
        request: ListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse> {
        self.inner.root_list_fields(request).await
    }
    async fn leaf_list_fields(
        &self,
        request: LeafListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse> {
        self.inner.leaf_list_fields(request).await
    }
    async fn search_plan(&self, request: SearchRequest) -> crate::Result<SearchPlanResponse> {
        self.inner.search_plan(request).await
    }
}
