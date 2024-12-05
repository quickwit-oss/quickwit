// Copyright (C) 2024 Quickwit, Inc.
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

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_config::SearcherConfig;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, GetKvRequest, Hit, LeafListFieldsRequest,
    LeafListTermsRequest, LeafListTermsResponse, LeafSearchRequest, LeafSearchResponse,
    LeafSearchStreamRequest, LeafSearchStreamResponse, ListFieldsRequest, ListFieldsResponse,
    ListTermsRequest, ListTermsResponse, PutKvRequest, ReportSplitsRequest, ReportSplitsResponse,
    ScrollRequest, SearchPlanResponse, SearchRequest, SearchResponse, SearchStreamRequest,
    SnippetRequest,
};
use quickwit_storage::{
    MemorySizedCache, QuickwitCache, SplitCache, StorageCache, StorageResolver,
};
use tantivy::aggregation::AggregationLimitsGuard;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::leaf::multi_leaf_search;
use crate::leaf_cache::LeafSearchCache;
use crate::list_fields::{leaf_list_fields, root_list_fields};
use crate::list_fields_cache::ListFieldsCache;
use crate::list_terms::{leaf_list_terms, root_list_terms};
use crate::root::fetch_docs_phase;
use crate::scroll_context::{MiniKV, ScrollContext, ScrollKeyAndStartOffset};
use crate::search_permit_provider::SearchPermitProvider;
use crate::search_stream::{leaf_search_stream, root_search_stream};
use crate::{fetch_docs, root_search, search_plan, ClusterClient, SearchError};

#[derive(Clone)]
/// The search service implementation.
pub struct SearchServiceImpl {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    cluster_client: ClusterClient,
    searcher_context: Arc<SearcherContext>,
    search_after_cache: MiniKV,
}

/// Trait representing a search service.
///
/// It mirrors the gRPC service `SearchService`, but with a more concrete
/// error type that can be converted into an API Error.
/// The REST API relies directly on the `SearchService`.
/// Also, it is mockable.
#[mockall::automock]
#[async_trait]
pub trait SearchService: 'static + Send + Sync {
    /// Root search API.
    /// This RPC identifies the set of splits on which the query should run on,
    /// and dispatches the multiple calls to `LeafSearch`.
    ///
    /// It is also in charge of merging back the responses.
    async fn root_search(&self, request: SearchRequest) -> crate::Result<SearchResponse>;

    /// Performs a leaf search on a given set of splits.
    ///
    /// It is like a regular search except that:
    /// - the node should perform the search locally instead of dispatching
    /// it to other nodes.
    /// - it should be applied on the given subset of splits
    /// - hit content is not fetched, and we instead return a so-called `PartialHit`.
    async fn leaf_search(&self, request: LeafSearchRequest) -> crate::Result<LeafSearchResponse>;

    /// Fetches the documents contents from the document store.
    /// This methods takes `PartialHit`s and returns `Hit`s.
    async fn fetch_docs(&self, request: FetchDocsRequest) -> crate::Result<FetchDocsResponse>;

    /// Performs a root search returning a receiver for streaming
    async fn root_search_stream(
        &self,
        request: SearchStreamRequest,
    ) -> crate::Result<Pin<Box<dyn futures::Stream<Item = crate::Result<Bytes>> + Send>>>;

    /// Performs a leaf search on a given set of splits and returns a stream.
    async fn leaf_search_stream(
        &self,
        request: LeafSearchStreamRequest,
    ) -> crate::Result<UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>>>;

    /// Root search API.
    /// This RPC identifies the set of splits on which the query should run on,
    /// and dispatches the multiple calls to `LeafSearch`.
    ///
    /// It is also in charge of merging back the responses.
    async fn root_list_terms(&self, request: ListTermsRequest) -> crate::Result<ListTermsResponse>;

    /// Performs a leaf search on a given set of splits.
    ///
    /// It is like a regular search except that:
    /// - the node should perform the search locally instead of dispatching
    /// it to other nodes.
    /// - it should be applied on the given subset of splits
    /// - hit content is not fetched, and we instead return a so-called `PartialHit`.
    async fn leaf_list_terms(
        &self,
        request: LeafListTermsRequest,
    ) -> crate::Result<LeafListTermsResponse>;

    /// Performs a scroll request.
    async fn scroll(&self, scroll_request: ScrollRequest) -> crate::Result<SearchResponse>;

    /// Stores a Key value in the local cache.
    /// This operation is not distributed. The distribution logic lives in
    /// the `ClusterClient`.
    async fn put_kv(&self, put_kv: PutKvRequest);

    /// Gets the payload associated to a key in the local cache.
    /// See also `put_kv(..)`.
    async fn get_kv(&self, get_kv: GetKvRequest) -> Option<Vec<u8>>;

    /// Indexers call report_splits to inform searchers node about the presence of a split, which
    /// would then be considered as a candidate for the searcher split cache.
    async fn report_splits(&self, report_splits: ReportSplitsRequest) -> ReportSplitsResponse;

    /// Return the list of fields for a given or multiple indices.
    async fn root_list_fields(
        &self,
        list_fields: ListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse>;

    /// Return the list of fields for one index.
    async fn leaf_list_fields(
        &self,
        list_fields: LeafListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse>;

    /// Describe how a search would be processed.
    async fn search_plan(&self, request: SearchRequest) -> crate::Result<SearchPlanResponse>;
}

impl SearchServiceImpl {
    /// Creates a new search service.
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        cluster_client: ClusterClient,
        searcher_context: Arc<SearcherContext>,
    ) -> Self {
        SearchServiceImpl {
            metastore,
            storage_resolver,
            cluster_client,
            searcher_context,
            search_after_cache: MiniKV::default(),
        }
    }
}

pub fn deserialize_doc_mapper(doc_mapper_str: &str) -> crate::Result<Arc<DocMapper>> {
    let doc_mapper = serde_json::from_str::<Arc<DocMapper>>(doc_mapper_str).map_err(|err| {
        SearchError::Internal(format!("failed to deserialize doc mapper: `{err}`"))
    })?;
    Ok(doc_mapper)
}

#[async_trait]
impl SearchService for SearchServiceImpl {
    async fn root_search(&self, search_request: SearchRequest) -> crate::Result<SearchResponse> {
        let search_result = root_search(
            &self.searcher_context,
            search_request,
            self.metastore.clone(),
            &self.cluster_client,
        )
        .await?;
        Ok(search_result)
    }

    async fn leaf_search(
        &self,
        leaf_search_request: LeafSearchRequest,
    ) -> crate::Result<LeafSearchResponse> {
        // Check leaf_search_request existence before tracing with `instrument` call.
        if leaf_search_request.search_request.is_none() {
            return Err(SearchError::Internal("no search request".to_string()));
        }

        let leaf_search_response = multi_leaf_search(
            self.searcher_context.clone(),
            leaf_search_request,
            &self.storage_resolver,
        )
        .await?;

        Ok(leaf_search_response)
    }

    async fn fetch_docs(
        &self,
        fetch_docs_request: FetchDocsRequest,
    ) -> crate::Result<FetchDocsResponse> {
        let index_uri = Uri::from_str(&fetch_docs_request.index_uri)?;
        let storage = self.storage_resolver.resolve(&index_uri).await?;
        let snippet_request_opt: Option<&SnippetRequest> =
            fetch_docs_request.snippet_request.as_ref();
        let doc_mapper = deserialize_doc_mapper(&fetch_docs_request.doc_mapper)?;
        let fetch_docs_response = fetch_docs(
            self.searcher_context.clone(),
            fetch_docs_request.partial_hits,
            storage,
            &fetch_docs_request.split_offsets,
            doc_mapper,
            snippet_request_opt,
        )
        .await?;

        Ok(fetch_docs_response)
    }

    async fn root_search_stream(
        &self,
        stream_request: SearchStreamRequest,
    ) -> crate::Result<Pin<Box<dyn futures::Stream<Item = crate::Result<Bytes>> + Send>>> {
        let data = root_search_stream(
            stream_request,
            self.metastore.clone(),
            self.cluster_client.clone(),
        )
        .await?;
        Ok(Box::pin(data))
    }

    async fn leaf_search_stream(
        &self,
        leaf_stream_request: LeafSearchStreamRequest,
    ) -> crate::Result<UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>>> {
        let stream_request = leaf_stream_request
            .request
            .ok_or_else(|| SearchError::Internal("no search request".to_string()))?;
        let index_uri = Uri::from_str(&leaf_stream_request.index_uri)?;
        let storage = self.storage_resolver.resolve(&index_uri).await?;
        let doc_mapper = deserialize_doc_mapper(&leaf_stream_request.doc_mapper)?;
        let leaf_receiver = leaf_search_stream(
            self.searcher_context.clone(),
            stream_request,
            storage,
            leaf_stream_request.split_offsets,
            doc_mapper,
        )
        .await;
        Ok(leaf_receiver)
    }

    async fn root_list_terms(
        &self,
        list_terms_request: ListTermsRequest,
    ) -> crate::Result<ListTermsResponse> {
        let search_result = root_list_terms(
            &list_terms_request,
            self.metastore.clone(),
            &self.cluster_client,
        )
        .await?;

        Ok(search_result)
    }

    async fn leaf_list_terms(
        &self,
        leaf_search_request: LeafListTermsRequest,
    ) -> crate::Result<LeafListTermsResponse> {
        let search_request = leaf_search_request
            .list_terms_request
            .ok_or_else(|| SearchError::Internal("no search request".to_string()))?;
        let index_uri = Uri::from_str(&leaf_search_request.index_uri)?;
        let storage = self.storage_resolver.resolve(&index_uri).await?;
        let split_ids = leaf_search_request.split_offsets;

        let leaf_search_response = leaf_list_terms(
            self.searcher_context.clone(),
            &search_request,
            storage.clone(),
            &split_ids[..],
        )
        .await?;

        Ok(leaf_search_response)
    }

    async fn scroll(&self, scroll_request: ScrollRequest) -> crate::Result<SearchResponse> {
        scroll(scroll_request, &self.cluster_client, &self.searcher_context).await
    }

    async fn put_kv(&self, put_request: PutKvRequest) {
        let ttl = Duration::from_secs(put_request.ttl_secs as u64);
        self.search_after_cache
            .put(put_request.key, put_request.payload, ttl)
            .await;
    }

    async fn get_kv(&self, get_request: GetKvRequest) -> Option<Vec<u8>> {
        let payload: Vec<u8> = self.search_after_cache.get(&get_request.key).await?;
        Some(payload)
    }

    async fn report_splits(&self, report_splits: ReportSplitsRequest) -> ReportSplitsResponse {
        if let Some(split_cache) = self.searcher_context.split_cache_opt.as_ref() {
            split_cache.report_splits(report_splits.report_splits);
        }
        ReportSplitsResponse {}
    }

    async fn root_list_fields(
        &self,
        list_fields_req: ListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse> {
        root_list_fields(
            list_fields_req,
            &self.cluster_client,
            self.metastore.clone(),
        )
        .await
    }

    async fn leaf_list_fields(
        &self,
        list_fields_req: LeafListFieldsRequest,
    ) -> crate::Result<ListFieldsResponse> {
        let index_uri = Uri::from_str(&list_fields_req.index_uri)?;
        let storage = self.storage_resolver.resolve(&index_uri).await?;
        let index_id = list_fields_req.index_id;
        let split_ids = list_fields_req.split_offsets;
        leaf_list_fields(
            index_id,
            storage,
            &self.searcher_context,
            &split_ids[..],
            &list_fields_req.fields,
        )
        .await
    }

    async fn search_plan(
        &self,
        search_request: SearchRequest,
    ) -> crate::Result<SearchPlanResponse> {
        let search_plan = search_plan(search_request, self.metastore.clone()).await?;
        Ok(search_plan)
    }
}

pub(crate) async fn scroll(
    scroll_request: ScrollRequest,
    cluster_client: &ClusterClient,
    searcher_context: &SearcherContext,
) -> crate::Result<SearchResponse> {
    let start = Instant::now();
    let current_scroll = ScrollKeyAndStartOffset::from_str(&scroll_request.scroll_id)
        .map_err(|msg| SearchError::InvalidArgument(msg.to_string()))?;
    let start_doc = current_scroll.start_offset;
    let scroll_key: [u8; 16] = current_scroll.scroll_key();
    let payload = cluster_client.get_kv(&scroll_key[..]).await;
    let payload =
        payload.ok_or_else(|| SearchError::Internal("scroll key not found".to_string()))?;

    let mut scroll_context = ScrollContext::load(&payload)
        .map_err(|_| SearchError::Internal("corrupted Scroll context".to_string()))?;

    let end_doc: u64 = start_doc + scroll_context.max_hits_per_page;

    let mut partial_hits = Vec::new();
    let mut scroll_context_modified = false;

    let cached_results = scroll_context.get_cached_partial_hits(start_doc..end_doc);
    partial_hits.extend_from_slice(cached_results);
    if (partial_hits.len() as u64) < current_scroll.max_hits_per_page as u64 {
        let search_after = partial_hits
            .last()
            .cloned()
            .unwrap_or_else(|| current_scroll.search_after.clone());
        let cursor = start_doc + partial_hits.len() as u64;
        scroll_context
            .load_batch_starting_at(cursor, search_after, cluster_client, searcher_context)
            .await?;
        partial_hits.extend_from_slice(scroll_context.get_cached_partial_hits(cursor..end_doc));
        scroll_context_modified = true;
    }

    // Fetch the actual documents.
    let hits: Vec<Hit> = fetch_docs_phase(
        &scroll_context.indexes_metas_for_leaf_search,
        &partial_hits[..],
        &scroll_context.split_metadatas[..],
        &scroll_context.search_request,
        cluster_client,
    )
    .await?;

    let next_scroll_id = current_scroll.next_page(
        hits.len() as u64,
        partial_hits.last().cloned().unwrap_or_default(),
    );

    if let Some(scroll_ttl_secs) = scroll_request.scroll_ttl_secs {
        if scroll_context_modified {
            scroll_context.clear_cache_if_unneeded();
            let payload = scroll_context.serialize();
            let scroll_ttl = Duration::from_secs(scroll_ttl_secs as u64);
            cluster_client
                .put_kv(&scroll_key, &payload, scroll_ttl)
                .await;
        }
    }

    Ok(SearchResponse {
        hits,
        num_hits: scroll_context.total_num_hits,
        elapsed_time_micros: start.elapsed().as_micros() as u64,
        scroll_id: Some(next_scroll_id.to_string()),
        errors: Vec::new(),
        aggregation: None,
        failed_splits: scroll_context.failed_splits,
        num_successful_splits: scroll_context.num_successful_splits,
    })
}
/// [`SearcherContext`] provides a common set of variables
/// shared by a searcher instance (which instantiates a
/// [`SearchServiceImpl`]).
pub struct SearcherContext {
    /// Searcher config.
    pub searcher_config: SearcherConfig,
    /// Fast fields cache.
    pub fast_fields_cache: Arc<dyn StorageCache>,
    /// Counting semaphore to limit concurrent leaf search split requests.
    pub search_permit_provider: SearchPermitProvider,
    /// Split footer cache.
    pub split_footer_cache: MemorySizedCache<String>,
    /// Counting semaphore to limit concurrent split stream requests.
    pub split_stream_semaphore: Semaphore,
    /// Recent sub-query cache.
    pub leaf_search_cache: LeafSearchCache,
    /// Search split cache. `None` if no split cache is configured.
    pub split_cache_opt: Option<Arc<SplitCache>>,
    /// List fields cache. Caches the list fields response for a given split.
    pub list_fields_cache: ListFieldsCache,
    /// The aggregation limits are passed to limit the memory usage.
    pub aggregation_limit: AggregationLimitsGuard,
}

impl std::fmt::Debug for SearcherContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SearcherContext")
            .field("searcher_config", &self.searcher_config)
            .field("split_stream_semaphore", &self.split_stream_semaphore)
            .finish()
    }
}

impl SearcherContext {
    #[cfg(test)]
    pub fn for_test() -> SearcherContext {
        let searcher_config = SearcherConfig::default();
        SearcherContext::new(searcher_config, None)
    }

    /// Creates a new searcher context, given a searcher config, and an optional `SplitCache`.
    pub fn new(searcher_config: SearcherConfig, split_cache_opt: Option<Arc<SplitCache>>) -> Self {
        let capacity_in_bytes = searcher_config.split_footer_cache_capacity.as_u64() as usize;
        let global_split_footer_cache = MemorySizedCache::with_capacity_in_bytes(
            capacity_in_bytes,
            &quickwit_storage::STORAGE_METRICS.split_footer_cache,
        );
        let leaf_search_split_semaphore = SearchPermitProvider::new(
            searcher_config.max_num_concurrent_split_searches,
            searcher_config.warmup_memory_budget,
        );
        let split_stream_semaphore =
            Semaphore::new(searcher_config.max_num_concurrent_split_streams);
        let fast_field_cache_capacity = searcher_config.fast_field_cache_capacity.as_u64() as usize;
        let storage_long_term_cache = Arc::new(QuickwitCache::new(fast_field_cache_capacity));
        let leaf_search_cache =
            LeafSearchCache::new(searcher_config.partial_request_cache_capacity.as_u64() as usize);
        let list_fields_cache =
            ListFieldsCache::new(searcher_config.partial_request_cache_capacity.as_u64() as usize);
        let aggregation_limit = AggregationLimitsGuard::new(
            Some(searcher_config.aggregation_memory_limit.as_u64()),
            Some(searcher_config.aggregation_bucket_limit),
        );

        Self {
            searcher_config,
            fast_fields_cache: storage_long_term_cache,
            search_permit_provider: leaf_search_split_semaphore,
            split_footer_cache: global_split_footer_cache,
            split_stream_semaphore,
            leaf_search_cache,
            list_fields_cache,
            split_cache_opt,
            aggregation_limit,
        }
    }

    /// Returns the shared instance to track the aggregation memory usage.
    pub fn get_aggregation_limits(&self) -> AggregationLimitsGuard {
        self.aggregation_limit.clone()
    }
}
