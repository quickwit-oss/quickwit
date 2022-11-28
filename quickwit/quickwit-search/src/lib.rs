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
mod leaf;
mod rendezvous_hasher;
mod retry;
mod root;
mod search_client_pool;
mod search_response_rest;
mod search_stream;
mod service;
mod thread_pool;

mod metrics;
#[cfg(test)]
mod tests;

use metrics::SEARCH_METRICS;
use quickwit_doc_mapper::DocMapper;
use root::validate_request;
use service::SearcherContext;
use tantivy::schema::NamedFieldDocument;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::cmp::Reverse;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use quickwit_config::{build_doc_mapper, QuickwitConfig, SearcherConfig};
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_metastore::{ListSplitsQuery, Metastore, SplitMetadata, SplitState};
use quickwit_proto::{Hit, PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets};
use quickwit_storage::StorageUriResolver;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::DocAddress;

pub use crate::client::SearchServiceClient;
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{parse_grpc_error, SearchError};
use crate::fetch_docs::fetch_docs;
use crate::leaf::leaf_search;
pub use crate::root::{jobs_to_leaf_request, root_search, SearchJob};
pub use crate::search_client_pool::{create_search_service_client, SearchClientPool};
pub use crate::search_response_rest::SearchResponseRest;
pub use crate::search_stream::root_search_stream;
pub use crate::service::{MockSearchService, SearchService, SearchServiceImpl};
use crate::thread_pool::run_cpu_intensive;

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

fn partial_hit_sorting_key(partial_hit: &PartialHit) -> (Reverse<u64>, GlobalDocAddress) {
    (
        Reverse(partial_hit.sorting_field_value),
        GlobalDocAddress::from_partial_hit(partial_hit),
    )
}

fn extract_split_and_footer_offsets(split_metadata: &SplitMetadata) -> SplitIdAndFooterOffsets {
    SplitIdAndFooterOffsets {
        split_id: split_metadata.split_id.clone(),
        split_footer_start: split_metadata.footer_offsets.start as u64,
        split_footer_end: split_metadata.footer_offsets.end as u64,
    }
}

/// Extract the list of relevant splits for a given search request.
async fn list_relevant_splits(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
) -> crate::Result<Vec<SplitMetadata>> {
    let mut query = ListSplitsQuery::for_index(&search_request.index_id)
        .with_split_state(SplitState::Published);

    if let Some(start_ts) = search_request.start_timestamp {
        query = query.with_time_range_start_gte(start_ts);
    }

    if let Some(end_ts) = search_request.end_timestamp {
        query = query.with_time_range_end_lt(end_ts);
    }

    if let Some(tags_filter) = extract_tags_from_query(&search_request.query)? {
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
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    storage_resolver: StorageUriResolver,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let index_config = metastore
        .index_metadata(&search_request.index_id)
        .await?
        .into_index_config();

    // This should never happen.
    //
    // The IndexConfig object has an option because it is used both
    // as the index config the user supplies. After validation however,
    // it should contain an `index_uri`.
    //
    // TODO see if it can be improved.
    let index_storage = storage_resolver.resolve(&index_config.index_uri)?;
    let metas = list_relevant_splits(search_request, metastore).await?;
    let split_metadata: Vec<SplitIdAndFooterOffsets> =
        metas.iter().map(extract_split_and_footer_offsets).collect();
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::InternalError(format!("Failed to build doc mapper. Cause: {}", err))
        })?;

    validate_request(search_request)?;

    // Validates the query by effectively building it against the current schema.
    doc_mapper.query(doc_mapper.schema(), search_request)?;
    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default()));
    let leaf_search_response = leaf_search(
        searcher_context.clone(),
        search_request,
        index_storage.clone(),
        &split_metadata[..],
        doc_mapper.clone(),
    )
    .await
    .context("Failed to perform leaf search.")?;

    let search_request_opt = if !search_request.snippet_fields.is_empty() {
        Some(search_request)
    } else {
        None
    };
    let schema = doc_mapper.schema();

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
    let aggregation = if let Some(intermediate_aggregation_result) =
        leaf_search_response.intermediate_aggregation_result
    {
        let res: IntermediateAggregationResults =
            serde_json::from_str(&intermediate_aggregation_result)?;
        let req: Aggregations = serde_json::from_str(search_request.aggregation_request())?;
        let res: AggregationResults = res.into_final_bucket_result(req, &schema)?;
        Some(serde_json::to_string(&res)?)
    } else {
        None
    };
    Ok(SearchResponse {
        aggregation,
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: leaf_search_response
            .failed_splits
            .iter()
            .map(|error| format!("{:?}", error))
            .collect_vec(),
    })
}

/// Starts a search node, aka a `searcher`.
pub async fn start_searcher_service(
    quickwit_config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
    search_client_pool: SearchClientPool,
) -> anyhow::Result<Arc<dyn SearchService>> {
    let cluster_client = ClusterClient::new(search_client_pool.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_uri_resolver,
        cluster_client,
        search_client_pool,
        quickwit_config.searcher_config.clone(),
    ));
    Ok(search_service)
}
