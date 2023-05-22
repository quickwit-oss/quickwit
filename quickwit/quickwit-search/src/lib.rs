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

//! This projects implements quickwit's search API.
#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]

mod client;
mod cluster_client;
mod collector;
mod error;
mod fetch_docs;
mod filters;
mod find_trace_ids_collector;
mod leaf;
mod leaf_cache;
mod retry;
mod root;
mod search_job_placer;
mod search_response_rest;
mod search_stream;
mod service;
mod thread_pool;

mod metrics;

#[cfg(test)]
mod tests;

pub use collector::QuickwitAggregations;
use metrics::SEARCH_METRICS;
use quickwit_common::tower::Pool;
use quickwit_doc_mapper::DocMapper;
use quickwit_query::query_ast::QueryAst;
use root::{finalize_aggregation, validate_request};
use service::SearcherContext;
use tantivy::schema::NamedFieldDocument;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
pub use find_trace_ids_collector::FindTraceIdsCollector;
use itertools::Itertools;
use quickwit_config::{build_doc_mapper, SearcherConfig};
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_metastore::{ListSplitsQuery, Metastore, SplitMetadata, SplitState};
use quickwit_proto::{
    Hit, IndexUid, PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets,
};
use quickwit_storage::StorageResolver;
use tantivy::DocAddress;

pub use crate::client::{
    create_search_client_from_channel, create_search_client_from_grpc_addr, SearchServiceClient,
};
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{parse_grpc_error, SearchError};
use crate::fetch_docs::fetch_docs;
use crate::leaf::{leaf_list_terms, leaf_search};
pub use crate::root::{jobs_to_leaf_request, root_list_terms, root_search, SearchJob};
pub use crate::search_job_placer::{Job, SearchJobPlacer};
pub use crate::search_response_rest::SearchResponseRest;
pub use crate::search_stream::root_search_stream;
pub use crate::service::{MockSearchService, SearchService, SearchServiceImpl};
use crate::thread_pool::run_cpu_intensive;

/// A pool of searcher clients identified by their gRPC socket address.
pub type SearcherPool = Pool<SocketAddr, SearchServiceClient>;

/// GlobalDocAddress serves as a hit address.
#[derive(Clone, Eq, Debug, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct GlobalDocAddress {
    pub split: String,
    pub doc_addr: DocAddress,
}

impl GlobalDocAddress {
    fn from_partial_hit(partial_hit: &PartialHit) -> Self {
        Self {
            split: partial_hit.split_id.to_string(),
            doc_addr: DocAddress {
                segment_ord: partial_hit.segment_ord,
                doc_id: partial_hit.doc_id,
            },
        }
    }
}

fn extract_split_and_footer_offsets(split_metadata: &SplitMetadata) -> SplitIdAndFooterOffsets {
    SplitIdAndFooterOffsets {
        split_id: split_metadata.split_id.clone(),
        split_footer_start: split_metadata.footer_offsets.start,
        split_footer_end: split_metadata.footer_offsets.end,
        timestamp_start: split_metadata
            .time_range
            .as_ref()
            .map(|time_range| *time_range.start()),
        timestamp_end: split_metadata
            .time_range
            .as_ref()
            .map(|time_range| *time_range.end()),
    }
}

/// Extract the list of relevant splits for a given search request.
async fn list_relevant_splits(
    // TODO: switch search request to index_uid and remove this.
    index_uid: IndexUid,
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
) -> crate::Result<Vec<SplitMetadata>> {
    let mut query = ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published);

    if let Some(start_ts) = search_request.start_timestamp {
        query = query.with_time_range_start_gte(start_ts);
    }

    if let Some(end_ts) = search_request.end_timestamp {
        query = query.with_time_range_end_lt(end_ts);
    }

    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast).map_err(|_| {
        SearchError::InternalError(format!(
            "Failed to deserialize query_ast: `{}`",
            search_request.query_ast
        ))
    })?;
    if let Some(tags_filter) = extract_tags_from_query(query_ast) {
        query = query.with_tags_filter(tags_filter);
    }

    let split_metas = metastore.list_splits(query).await?;
    Ok(split_metas
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>())
}

/// Converts a Tantivy `NamedFieldDocument` into a json string using the
/// schema defined by the DocMapper.
///
/// We perform this conversion at leaf level only to avoid having
/// another intermediate json format between the leaves and the root.
fn convert_document_to_json_string(
    named_field_doc: NamedFieldDocument,
    doc_mapper: &dyn DocMapper,
) -> anyhow::Result<String> {
    let NamedFieldDocument(named_field_doc_map) = named_field_doc;
    let doc_json_map = doc_mapper.doc_to_json(named_field_doc_map)?;
    let content_json =
        serde_json::to_string(&doc_json_map).expect("Json serialization should never fail.");
    Ok(content_json)
}

/// Performs a search on the current node.
/// See also `[distributed_search]`.
pub async fn single_node_search(
    mut search_request: SearchRequest,
    metastore: &dyn Metastore,
    storage_resolver: StorageResolver,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config = index_metadata.into_index_config();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::InternalError(format!("Failed to build doc mapper. Cause: {err}"))
        })?;

    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast)?;
    let query_ast_resolved: QueryAst =
        query_ast.parse_user_query(doc_mapper.default_search_fields())?;
    search_request.query_ast = serde_json::to_string(&query_ast_resolved)?;

    let index_storage = storage_resolver.resolve(&index_config.index_uri).await?;
    let metas = list_relevant_splits(index_uid, &search_request, metastore).await?;
    let split_metadata: Vec<SplitIdAndFooterOffsets> =
        metas.iter().map(extract_split_and_footer_offsets).collect();
    validate_request(&*doc_mapper, &search_request)?;

    // Verifying that the query is valid.
    doc_mapper
        .query(doc_mapper.schema(), &query_ast_resolved, true)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;

    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default()));

    let leaf_search_response = leaf_search(
        searcher_context.clone(),
        &search_request,
        index_storage.clone(),
        &split_metadata[..],
        doc_mapper.clone(),
    )
    .await
    .context("Failed to perform leaf search.")?;

    let search_request_opt = if !search_request.snippet_fields.is_empty() {
        Some(&search_request)
    } else {
        None
    };

    let fetch_docs_response = fetch_docs(
        searcher_context.clone(),
        leaf_search_response.partial_hits,
        index_storage,
        &split_metadata,
        doc_mapper,
        search_request_opt,
    )
    .await
    .context("Failed to perform fetch docs.")?;
    let hits: Vec<Hit> = fetch_docs_response
        .hits
        .into_iter()
        .map(|leaf_hit| Hit {
            json: leaf_hit.leaf_json,
            partial_hit: leaf_hit.partial_hit,
            snippet: leaf_hit.leaf_snippet_json,
        })
        .collect();
    let elapsed = start_instant.elapsed();

    let aggregations: Option<QuickwitAggregations> = search_request
        .aggregation_request
        .as_ref()
        .map(|agg| serde_json::from_str(agg))
        .transpose()?;

    let aggregation = finalize_aggregation(
        leaf_search_response.intermediate_aggregation_result,
        aggregations,
        &searcher_context,
    )?;
    Ok(SearchResponse {
        aggregation,
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: leaf_search_response
            .failed_splits
            .iter()
            .map(|error| format!("{error:?}"))
            .collect_vec(),
    })
}

/// Starts a search node, aka a `searcher`.
pub async fn start_searcher_service(
    searcher_config: SearcherConfig,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageResolver,
    search_job_placer: SearchJobPlacer,
) -> anyhow::Result<Arc<dyn SearchService>> {
    let cluster_client = ClusterClient::new(search_job_placer.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_resolver,
        cluster_client,
        search_job_placer,
        searcher_config,
    ));
    Ok(search_service)
}

/// Creates a tantivy Term from a &str.
#[cfg(any(test, feature = "testsuite"))]
#[macro_export]
macro_rules! encode_term_for_test {
    ($field:expr, $value:expr) => {
        ::tantivy::schema::Term::from_field_text(
            ::tantivy::schema::Field::from_field_id($field),
            $value,
        )
        .serialized_term()
        .to_vec()
    };
    ($value:expr) => {
        encode_term_for_test!(0, $value)
    };
}

/// Creates a `SearcherPool` for tests from an iterator of socket addresses and mock search
/// services.
#[cfg(any(test, feature = "testsuite"))]
pub fn searcher_pool_for_test(
    iter: impl IntoIterator<Item = (&'static str, MockSearchService)>,
) -> SearcherPool {
    SearcherPool::from_iter(
        iter.into_iter()
            .map(|(grpc_addr_str, mock_search_service)| {
                let grpc_addr: SocketAddr = grpc_addr_str
                    .parse()
                    .expect("The gRPC address should be valid socket address.");
                let client =
                    SearchServiceClient::from_service(Arc::new(mock_search_service), grpc_addr);
                (grpc_addr, client)
            }),
    )
}
