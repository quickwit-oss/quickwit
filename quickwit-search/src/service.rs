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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use bytes::Bytes;
use quickwit_metastore::Metastore;
use quickwit_proto::{ExportRequest, LeafExportRequest, LeafExportResult};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, LeafSearchRequest, LeafSearchResult, SearchRequest,
    SearchResult,
};
use quickwit_storage::StorageUriResolver;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::export::root_export;
use crate::export::{leaf_export, FastFieldCollectorBuilder};
use crate::fetch_docs;
use crate::leaf_search;
use crate::make_collector;
use crate::root_search;
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
    async fn root_export(&self, _request: ExportRequest) -> Result<Bytes, SearchError>;

    /// Perform a leaf search on a given set of splits and return a stream.
    async fn leaf_export(
        &self,
        _request: LeafExportRequest,
    ) -> crate::Result<ReceiverStream<Result<LeafExportResult, tonic::Status>>>;
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
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let split_ids = leaf_search_request.split_ids;
        let index_config = index_metadata.index_config;
        let query = index_config.query(&search_request)?;
        let collector = make_collector(index_config.as_ref(), &search_request);

        let leaf_search_result =
            leaf_search(query.as_ref(), collector, &split_ids[..], storage.clone()).await?;

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

    async fn root_export(&self, export_request: ExportRequest) -> Result<Bytes, SearchError> {
        let metastore = self
            .metastore_router
            .get(&export_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: export_request.index_id.clone(),
            })?;
        let data = root_export(&export_request, metastore.as_ref(), &self.client_pool).await?;
        Ok(Bytes::from(data))
    }

    async fn leaf_export(
        &self,
        leaf_export_request: LeafExportRequest,
    ) -> crate::Result<ReceiverStream<Result<LeafExportResult, tonic::Status>>> {
        let export_request = leaf_export_request
            .export_request
            .ok_or_else(|| SearchError::InternalError("No search request.".to_string()))?;
        info!(index=?export_request.index_id, splits=?leaf_export_request.split_ids, "leaf_search");
        let metastore = self
            .metastore_router
            .get(&export_request.index_id)
            .cloned()
            .ok_or_else(|| SearchError::IndexDoesNotExist {
                index_id: export_request.index_id.clone(),
            })?;
        let index_metadata = metastore.index_metadata(&export_request.index_id).await?;
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
        let split_ids = leaf_export_request.split_ids;
        let index_config = index_metadata.index_config;
        let search_request = SearchRequest::from(export_request.clone());
        let query = index_config.query(&search_request)?;
        // TODO: works only on i64, this needs to handle common tantivy types.
        let fast_field_to_export = export_request.fast_field.clone();
        let schema = index_config.schema();
        let fast_field = schema
            .get_field(&fast_field_to_export)
            .ok_or_else(|| SearchError::InvalidQuery("Fast field does not exist.".to_owned()))?;
        let fast_field_type = schema.get_field_entry(fast_field).field_type();
        let fast_field_collector_builder = FastFieldCollectorBuilder::new(
            fast_field_type.value_type(),
            export_request.fast_field.clone(),
            index_config.timestamp_field_name(),
            index_config.timestamp_field(),
            export_request.start_timestamp,
            export_request.end_timestamp,
        )?;
        let leaf_export_stream = leaf_export(
            query,
            fast_field_collector_builder,
            split_ids,
            storage.clone(),
            export_request.output_format.into(),
        )
        .await;
        Ok(leaf_export_stream)
    }
}
