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
use root::validate_request;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use quickwit_cluster::Cluster;
use quickwit_config::{build_doc_mapper, QuickwitConfig, SEARCHER_CONFIG_INSTANCE};
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{Metastore, SplitMetadata, SplitState};
use quickwit_proto::{PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets};
use quickwit_storage::StorageUriResolver;
use serde_json::Value as JsonValue;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::DocAddress;

pub use crate::client::SearchServiceClient;
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{parse_grpc_error, SearchError};
use crate::fetch_docs::fetch_docs;
use crate::leaf::leaf_search;
pub use crate::root::root_search;
pub use crate::search_client_pool::SearchClientPool;
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

fn extract_time_range(
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
) -> Option<Range<i64>> {
    match (start_timestamp_opt, end_timestamp_opt) {
        (Some(start_timestamp), Some(end_timestamp)) => Some(Range {
            start: start_timestamp,
            end: end_timestamp,
        }),
        (_, Some(end_timestamp)) => Some(Range {
            start: i64::MIN,
            end: end_timestamp,
        }),
        (Some(start_timestamp), _) => Some(Range {
            start: start_timestamp,
            end: i64::MAX,
        }),
        _ => None,
    }
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
    let time_range_opt =
        extract_time_range(search_request.start_timestamp, search_request.end_timestamp);
    let tags_filter = extract_tags_from_query(&search_request.query)?;
    let split_metas = metastore
        .list_splits(
            &search_request.index_id,
            SplitState::Published,
            time_range_opt,
            tags_filter,
        )
        .await?;
    Ok(split_metas
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>())
}

/// Converts a `LeafHit` into a `Hit`.
///
/// Splits may have been created with different DocMappers.
/// For this reason, leaves are returning a document that is -as much
/// as possible-, `DocMapper` agnostic.
///
/// As a result, all documents will have the actual same schema,
/// hence facilitating the implementation on the consumer side.
///
/// For instance, if the cardinality of a field changed from single-valued
/// to multivalued, we do want the documents emitted from old splits to
/// also serialize the fields values as a JsonArray.
///
/// The `convert_leaf_hit` is critical and needs to be tested against
/// allowed DocMapper changes.
fn convert_leaf_hit(
    leaf_hit: quickwit_proto::LeafHit,
    doc_mapper: &dyn DocMapper,
) -> crate::Result<quickwit_proto::Hit> {
    let hit_json: BTreeMap<String, Vec<JsonValue>> = serde_json::from_str(&leaf_hit.leaf_json)
        .map_err(|_| SearchError::InternalError("Invalid leaf json.".to_string()))?;
    let doc = doc_mapper.doc_to_json(hit_json)?;
    let json = serde_json::to_string(&doc).expect("Json serialization should never fail.");
    Ok(quickwit_proto::Hit {
        json,
        partial_hit: leaf_hit.partial_hit,
    })
}

/// Performs a search on the current node.
/// See also `[distributed_search]`.
pub async fn single_node_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    storage_resolver: StorageUriResolver,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_storage = storage_resolver.resolve(&index_metadata.index_uri)?;
    let metas = list_relevant_splits(search_request, metastore).await?;
    let split_metadata: Vec<SplitIdAndFooterOffsets> =
        metas.iter().map(extract_split_and_footer_offsets).collect();
    let doc_mapper = build_doc_mapper(
        &index_metadata.doc_mapping,
        &index_metadata.search_settings,
        &index_metadata.indexing_settings,
    )
    .map_err(|err| {
        SearchError::InternalError(format!("Failed to build doc mapper. Cause: {}", err))
    })?;

    validate_request(search_request)?;

    // Validates the query by effectively building it against the current schema.
    doc_mapper.query(doc_mapper.schema(), search_request)?;

    let leaf_search_response = leaf_search(
        search_request,
        index_storage.clone(),
        &split_metadata[..],
        doc_mapper.clone(),
    )
    .await
    .context("Failed to perform leaf search.")?;
    let fetch_docs_response = fetch_docs(
        leaf_search_response.partial_hits,
        index_storage,
        &split_metadata,
    )
    .await
    .context("Failed to perform fetch docs.")?;
    let hits: Vec<quickwit_proto::Hit> = fetch_docs_response
        .hits
        .into_iter()
        .map(|leaf_hit| crate::convert_leaf_hit(leaf_hit, &*doc_mapper))
        .collect::<crate::Result<_>>()?;
    let elapsed = start_instant.elapsed();
    let aggregation = if let Some(intermediate_aggregation_result) =
        leaf_search_response.intermediate_aggregation_result
    {
        let res: IntermediateAggregationResults =
            serde_json::from_str(&intermediate_aggregation_result)?;
        let req: Aggregations = serde_json::from_str(search_request.aggregation_request())?;
        let res: AggregationResults = res.into_final_bucket_result(req)?;
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
    cluster: Arc<Cluster>,
) -> anyhow::Result<Arc<dyn SearchService>> {
    SEARCHER_CONFIG_INSTANCE
        .set(quickwit_config.searcher_config.clone())
        .expect("could not set searcher config in global once cell");
    let client_pool = SearchClientPool::create_and_keep_updated(
        &cluster.members(),
        cluster.member_change_watcher(),
    )
    .await?;
    let cluster_client = ClusterClient::new(client_pool.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_uri_resolver,
        cluster_client,
        client_pool,
    ));
    Ok(search_service)
}
