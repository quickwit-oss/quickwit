/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_metastore::Metastore;
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, LeafSearchRequest, LeafSearchResult, SearchRequest,
    SearchResult,
};
use quickwit_proto::{LeafSearchStreamRequest, LeafSearchStreamResult, SearchStreamRequest};
use quickwit_storage::StorageUriResolver;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info;

use crate::fetch_docs;
use crate::leaf_search;
use crate::root_search;
use crate::search_stream::{leaf_search_stream, root_search_stream};
use crate::SearchClientPool;
use crate::SearchError;

#[derive(Clone)]
/// The search service implementation.
pub struct SearchServiceImpl {
    metastore_router: HashMap<String, Arc<dyn Metastore>>,
    storage_resolver: StorageUriResolver,
    client_pool: Arc<SearchClientPool>,
}

/// Trait representing a search service.
///
/// It mirrors the grpc service SearchService, but with a more concrete
/// error type that can be converted into API Error.
/// The rest API relies directly on the SearchService.
/// Also, it is mockable.
#[mockall::automock]
#[async_trait]
pub trait SearchService: 'static + Send + Sync {
    /// Root search API.
    /// This RPC identifies the set of splits on which the query should run on,
    /// and dispatch the several calls to `LeafSearch`.
    ///
    /// It is also in charge of merging back the results.
    async fn root_search(&self, request: SearchRequest) -> Result<SearchResult, SearchError>;

    /// Perform a leaf search on a given set of splits.
    ///
    /// It is like a regular search except that:
    /// - the node should perform the search locally instead of dispatching
    /// it to other nodes.
    /// - it should be applied on the given subset of splits
    /// - Hit content is not fetched, and we instead return so called `PartialHit`.
    async fn leaf_search(
        &self,
        _request: LeafSearchRequest,
    ) -> Result<LeafSearchResult, SearchError>;

    /// Fetches the documents contents from the document store.
    /// This methods takes `PartialHit`s and returns `Hit`s.
    async fn fetch_docs(&self, _request: FetchDocsRequest) -> Result<FetchDocsResult, SearchError>;

    /// Perfomrs a root search returning a receiver for streaming
    async fn root_search_stream(
        &self,
        _request: SearchStreamRequest,
    ) -> Result<Vec<Bytes>, SearchError>;

    /// Perform a leaf search on a given set of splits and return a stream.
    async fn leaf_search_stream(
        &self,
        _request: LeafSearchStreamRequest,
    ) -> crate::Result<UnboundedReceiverStream<Result<LeafSearchStreamResult, tonic::Status>>>;
}

impl SearchServiceImpl {
    /// Create search service
    pub fn new(
        metastore_router: HashMap<String, Arc<dyn Metastore>>,
        storage_resolver: StorageUriResolver,
        client_pool: Arc<SearchClientPool>,
    ) -> Self {
        SearchServiceImpl {
            metastore_router,
            storage_resolver,
            client_pool,
        }
    }
}

#[async_trait]
impl SearchService for SearchServiceImpl {
    async fn root_search(
        &self,
        search_request: SearchRequest,
    ) -> Result<SearchResult, SearchError> {
        let metastore = self
            .metastore_router
            .get(&search_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: search_request.index_id.clone(),
            })?;

        let search_result =
            root_search(&search_request, metastore.as_ref(), &self.client_pool).await?;

        Ok(search_result)
    }

    async fn leaf_search(
        &self,
        leaf_search_request: LeafSearchRequest,
    ) -> Result<LeafSearchResult, SearchError> {
        let search_request = leaf_search_request
            .search_request
            .ok_or_else(|| SearchError::InternalError("No search request.".to_string()))?;
        info!(index=?search_request.index_id, splits=?leaf_search_request.split_ids, "leaf_search");
        let metastore = self
            .metastore_router
            .get(&search_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: search_request.index_id.clone(),
            })?;
        let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
        //let split_meta_data = metastore
        //.list_split_ids(
        //&search_request.index_id,
        //quickwit_metastore::SplitState::Published,
        //None,
        //&leaf_search_request.split_ids,
        //)
        //.await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let split_ids = leaf_search_request.split_ids;
        let index_config = index_metadata.index_config;

        let leaf_search_result = leaf_search(
            index_config,
            &search_request,
            &split_ids[..],
            storage.clone(),
        )
        .await?;

        Ok(leaf_search_result)
    }

    async fn fetch_docs(
        &self,
        fetch_docs_request: FetchDocsRequest,
    ) -> Result<FetchDocsResult, SearchError> {
        let index_id = fetch_docs_request.index_id;
        let metastore = self
            .metastore_router
            .get(&index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: index_id.clone(),
            })?;
        let index_metadata = metastore.index_metadata(&index_id).await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;

        let fetch_docs_result =
            fetch_docs(fetch_docs_request.partial_hits, storage.clone()).await?;

        Ok(fetch_docs_result)
    }

    async fn root_search_stream(
        &self,
        stream_request: SearchStreamRequest,
    ) -> Result<Vec<Bytes>, SearchError> {
        let metastore = self
            .metastore_router
            .get(&stream_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: stream_request.index_id.clone(),
            })?;
        let data =
            root_search_stream(&stream_request, metastore.as_ref(), &self.client_pool).await?;
        Ok(data)
    }

    async fn leaf_search_stream(
        &self,
        leaf_stream_request: LeafSearchStreamRequest,
    ) -> crate::Result<UnboundedReceiverStream<Result<LeafSearchStreamResult, tonic::Status>>> {
        let stream_request = leaf_stream_request
            .request
            .ok_or_else(|| SearchError::InternalError("No search request.".to_string()))?;
        info!(index=?stream_request.index_id, splits=?leaf_stream_request.split_ids, "leaf_search");
        let metastore = self
            .metastore_router
            .get(&stream_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: stream_request.index_id.clone(),
            })?;
        let index_metadata = metastore.index_metadata(&stream_request.index_id).await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let split_ids = leaf_stream_request.split_ids;
        let index_config = index_metadata.index_config;
        let leaf_receiver =
            leaf_search_stream(index_config, &stream_request, split_ids, storage.clone()).await;
        Ok(leaf_receiver)
    }
}
