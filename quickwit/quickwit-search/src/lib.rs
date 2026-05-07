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

//! This projects implements quickwit's search API.
#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]

mod client;
mod cluster_client;
mod collector;
mod error;
mod fetch_docs;
mod find_trace_ids_collector;

mod invoker;
/// Leaf search operations.
pub mod leaf;
mod leaf_cache;
mod list_fields;
mod list_fields_cache;
mod list_terms;
mod metrics_trackers;
mod retry;
mod root;
mod scroll_context;
mod search_job_placer;
mod search_response_rest;
mod service;
pub(crate) mod top_k_collector;

mod metrics;
mod search_permit_provider;

#[cfg(test)]
mod tests;

pub use collector::QuickwitAggregations;
use metrics::SEARCH_METRICS;
use quickwit_common::thread_pool::ThreadPool;
use quickwit_common::tower::Pool;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, ListSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use tantivy::schema::NamedFieldDocument;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, LazyLock};

pub use find_trace_ids_collector::{FindTraceIdsCollector, Span};
use quickwit_config::SearcherConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_metastore::{
    IndexMetadata, ListIndexesMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState,
};
use quickwit_proto::search::{
    LeafResourceStats, PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets,
    SplitResourceStats,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::StorageResolver;
pub use service::SearcherContext;
use tantivy::DocAddress;

pub use crate::client::{
    SearchServiceClient, create_search_client_from_channel, create_search_client_from_grpc_addr,
};
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{SearchError, parse_grpc_error};
use crate::fetch_docs::fetch_docs;
pub use crate::invoker::LambdaLeafSearchInvoker;
pub use crate::root::{
    IndexMetasForLeafSearch, SearchJob, ensure_all_indexes_found, jobs_to_leaf_request,
    root_search, search_plan,
};
pub use crate::search_job_placer::{Job, SearchJobPlacer};
pub use crate::search_response_rest::{
    AggregationResults, SearchPlanResponseRest, SearchResponseRest,
};
pub use crate::service::{MockSearchService, SearchService, SearchServiceImpl};

/// A pool of searcher clients identified by their gRPC socket address.
pub type SearcherPool = Pool<SocketAddr, SearchServiceClient>;

fn search_thread_pool() -> &'static ThreadPool {
    static SEARCH_THREAD_POOL: LazyLock<ThreadPool> =
        LazyLock::new(|| ThreadPool::new("search", None));
    &SEARCH_THREAD_POOL
}

/// GlobalDocAddress serves as a hit address.
#[derive(Clone, Eq, Debug, PartialEq, Hash, Ord, PartialOrd)]
pub struct GlobalDocAddress {
    /// Split containing the document
    pub split: String,
    /// Document address inside the split
    pub doc_addr: DocAddress,
}

/// An error happened converting a string to a GLobalDocAddress
#[derive(Debug, Clone, Copy)]
pub struct GlobalDocAddressParseError;

impl GlobalDocAddress {
    /// Extract a GlobalDocAddress from a PartialHit
    pub fn from_partial_hit(partial_hit: &PartialHit) -> Self {
        Self {
            split: partial_hit.split_id.to_string(),
            doc_addr: DocAddress {
                segment_ord: partial_hit.segment_ord,
                doc_id: partial_hit.doc_id,
            },
        }
    }
}

impl std::fmt::Display for GlobalDocAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.split)?;
        write!(
            f,
            ":{:08x}:{:08x}",
            self.doc_addr.segment_ord, self.doc_addr.doc_id
        )
    }
}

impl std::str::FromStr for GlobalDocAddress {
    type Err = GlobalDocAddressParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut s_iter = s.splitn(3, ':');
        let split = s_iter.next().ok_or(GlobalDocAddressParseError)?.to_string();
        let segment = s_iter.next().ok_or(GlobalDocAddressParseError)?;
        let doc_id = s_iter.next().ok_or(GlobalDocAddressParseError)?;

        let segment_ord =
            u32::from_str_radix(segment, 16).map_err(|_| GlobalDocAddressParseError)?;
        let doc_id = u32::from_str_radix(doc_id, 16).map_err(|_| GlobalDocAddressParseError)?;

        Ok(GlobalDocAddress {
            split,
            doc_addr: DocAddress {
                segment_ord,
                doc_id,
            },
        })
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
        num_docs: split_metadata.num_docs as u64,
    }
}

/// Get all splits of given index ids
pub async fn list_all_splits(
    index_uids: Vec<IndexUid>,
    metastore: &mut MetastoreServiceClient,
) -> crate::Result<Vec<SplitMetadata>> {
    list_relevant_splits(index_uids, None, None, None, metastore).await
}

/// Extract the list of relevant splits for a given request.
pub async fn list_relevant_splits(
    index_uids: Vec<IndexUid>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    tags_filter_opt: Option<TagFilterAst>,
    metastore: &mut MetastoreServiceClient,
) -> crate::Result<Vec<SplitMetadata>> {
    let Some(mut query) = ListSplitsQuery::try_from_index_uids(index_uids) else {
        return Ok(Vec::new());
    };
    query = query.with_split_state(SplitState::Published);

    if let Some(start_ts) = start_timestamp {
        query = query.with_time_range_start_gte(start_ts);
    }
    if let Some(end_ts) = end_timestamp {
        query = query.with_time_range_end_lt(end_ts);
    }
    if let Some(tags_filter) = tags_filter_opt {
        query = query.with_tags_filter(tags_filter);
    }
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits_metadata: Vec<SplitMetadata> = metastore
        .list_splits(list_splits_request)
        .await?
        .collect_splits_metadata()
        .await?;
    Ok(splits_metadata)
}

/// Resolve index patterns and returns IndexMetadata for found indices.
/// Patterns follow the elastic search patterns.
pub async fn resolve_index_patterns(
    index_id_patterns: &[String],
    metastore: &mut MetastoreServiceClient,
) -> crate::Result<Vec<IndexMetadata>> {
    let list_indexes_metadata_request = if index_id_patterns.is_empty() {
        ListIndexesMetadataRequest::all()
    } else {
        ListIndexesMetadataRequest {
            index_id_patterns: index_id_patterns.to_vec(),
        }
    };

    // Get the index ids from the request
    let indexes_metadata = metastore
        .list_indexes_metadata(list_indexes_metadata_request)
        .await?
        .deserialize_indexes_metadata()
        .await?;
    ensure_all_indexes_found(&indexes_metadata, index_id_patterns)?;
    Ok(indexes_metadata)
}

/// Converts a Tantivy `NamedFieldDocument` into a json string using the
/// schema defined by the DocMapper.
///
/// We perform this conversion at leaf level only to avoid having
/// another intermediate json format between the leaves and the root.
fn convert_document_to_json_string(
    named_field_doc: NamedFieldDocument,
    doc_mapper: &DocMapper,
) -> anyhow::Result<String> {
    let NamedFieldDocument(named_field_doc_map) = named_field_doc;
    let doc_json_map = doc_mapper.doc_to_json(named_field_doc_map)?;
    let content_json =
        serde_json::to_string(&doc_json_map).expect("Json serialization should never fail.");
    Ok(content_json)
}

/// Starts a search node, aka a `searcher`.
pub async fn start_searcher_service(
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    search_job_placer: SearchJobPlacer,
    searcher_context: Arc<SearcherContext>,
) -> anyhow::Result<Arc<dyn SearchService>> {
    let cluster_client = ClusterClient::new(search_job_placer);
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_resolver,
        cluster_client,
        searcher_context,
    ));
    Ok(search_service)
}

/// Performs a search on the current node.
/// See also `[distributed_search]`.
pub async fn single_node_search(
    search_request: SearchRequest,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> crate::Result<SearchResponse> {
    let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 7280u16);
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let cluster_client = ClusterClient::new(search_job_placer);
    let searcher_config = SearcherConfig::default();
    let searcher_context = Arc::new(SearcherContext::new_without_invoker(searcher_config, None));
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore.clone(),
        storage_resolver,
        cluster_client.clone(),
        searcher_context.clone(),
    ));
    let search_service_client =
        SearchServiceClient::from_service(search_service.clone(), socket_addr);
    searcher_pool.insert(socket_addr, search_service_client);
    root_search(
        &searcher_context,
        search_request,
        metastore,
        &cluster_client,
    )
    .await
}

/// Creates a tantivy Term from a &str.
#[cfg(any(test, feature = "testsuite"))]
#[macro_export]
macro_rules! encode_term_for_test {
    ($field:expr, $value:expr) => {{
        #[allow(deprecated)]
        {
            ::tantivy::schema::Term::from_field_text(
                ::tantivy::schema::Field::from_field_id($field),
                $value,
            )
            .serialized_term()
            .to_vec()
        }
    }};
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

/// Sum of the per-phase microsecond fields used to rank `worst_split`.
/// Intentionally excludes `wait_for_search_permit_microsecs`.
pub(crate) fn split_phase_sum_microsecs(stats: &SplitResourceStats) -> u64 {
    stats.warmup_microsecs + stats.wait_for_cpu_pool_microsecs + stats.cpu_search_microsecs
}

/// Field-wise sum of two `SplitResourceStats` (every field is extensive).
pub(crate) fn add_split_stats(acc: &mut SplitResourceStats, other: &SplitResourceStats) {
    acc.split_num_docs += other.split_num_docs;
    acc.input_memory_bytes += other.input_memory_bytes;
    acc.downloaded_bytes += other.downloaded_bytes;
    acc.downloaded_req += other.downloaded_req;
    acc.matched_doc += other.matched_doc;
    acc.wait_for_search_permit_microsecs += other.wait_for_search_permit_microsecs;
    acc.warmup_microsecs += other.warmup_microsecs;
    acc.wait_for_cpu_pool_microsecs += other.wait_for_cpu_pool_microsecs;
    acc.cpu_search_microsecs += other.cpu_search_microsecs;
}

/// Merge another `LeafResourceStats` into `acc`.
///
/// Phase 2 plumbing: every numeric field is summed except `lambda_bottleneck` and
/// `wall_time_microsecs` which take the maximum (these are leaf-level scalars and
/// will be set authoritatively in later phases). `worst_split` is selected by
/// `split_phase_sum_microsecs`; `sum_split_resource` is field-wise summed.
pub(crate) fn add_leaf_stats(acc: &mut LeafResourceStats, other: &LeafResourceStats) {
    acc.partial_result_cache_num_splits += other.partial_result_cache_num_splits;
    acc.partial_result_cache_num_docs += other.partial_result_cache_num_docs;
    acc.lambda_num_splits += other.lambda_num_splits;
    acc.lambda_num_docs += other.lambda_num_docs;
    acc.lambda_success_num_splits += other.lambda_success_num_splits;
    acc.lambda_success_num_docs += other.lambda_success_num_docs;
    acc.lambda_bottleneck = acc.lambda_bottleneck.max(other.lambda_bottleneck);
    acc.num_localexec_splits += other.num_localexec_splits;
    acc.num_localexec_num_docs += other.num_localexec_num_docs;
    acc.wall_time_microsecs = acc.wall_time_microsecs.max(other.wall_time_microsecs);
    if let Some(other_split) = &other.sum_split_resource {
        let acc_split = acc
            .sum_split_resource
            .get_or_insert_with(SplitResourceStats::default);
        add_split_stats(acc_split, other_split);
    }
    acc.worst_split = [acc.worst_split, other.worst_split]
        .into_iter()
        .flatten()
        .max_by_key(split_phase_sum_microsecs);
}

/// Merge an iterator of `Option<LeafResourceStats>` into a single `Option<LeafResourceStats>`.
///
/// `None` entries are skipped. The accumulator is materialized lazily on the first
/// non-`None` entry so a fully-empty iterator still returns `None`.
pub(crate) fn merge_leaf_stats_it<'a>(
    stats_it: impl IntoIterator<Item = &'a Option<LeafResourceStats>>,
) -> Option<LeafResourceStats> {
    let mut acc: Option<LeafResourceStats> = None;
    for new_stats in stats_it {
        let Some(new_stats) = new_stats else {
            continue;
        };
        let acc = acc.get_or_insert_with(LeafResourceStats::default);
        add_leaf_stats(acc, new_stats);
    }
    acc
}

#[cfg(test)]
mod stats_merge_tests {
    use super::*;

    fn split_stats(num_docs: u64, warmup: u64, search: u64) -> SplitResourceStats {
        SplitResourceStats {
            split_num_docs: num_docs,
            warmup_microsecs: warmup,
            cpu_search_microsecs: search,
            ..Default::default()
        }
    }

    fn leaf_stats_one_split(split: SplitResourceStats) -> LeafResourceStats {
        LeafResourceStats {
            num_localexec_splits: 1,
            num_localexec_num_docs: split.split_num_docs,
            sum_split_resource: Some(split),
            worst_split: Some(split),
            ..Default::default()
        }
    }

    #[test]
    fn test_add_split_stats_sums_every_field() {
        let mut acc = split_stats(100, 50, 200);
        let other = split_stats(200, 80, 300);
        add_split_stats(&mut acc, &other);
        assert_eq!(acc.split_num_docs, 300);
        assert_eq!(acc.warmup_microsecs, 130);
        assert_eq!(acc.cpu_search_microsecs, 500);
    }

    #[test]
    fn test_add_leaf_stats_sums_extensives_and_picks_worst_split() {
        let split_a = split_stats(100, 50, 200);
        let split_b = split_stats(200, 80, 300);

        let mut acc = leaf_stats_one_split(split_a);
        let other = leaf_stats_one_split(split_b);
        add_leaf_stats(&mut acc, &other);

        assert_eq!(acc.num_localexec_splits, 2);
        assert_eq!(acc.num_localexec_num_docs, 300);

        let summed = acc.sum_split_resource.unwrap();
        assert_eq!(summed.split_num_docs, 300);
        assert_eq!(summed.warmup_microsecs, 130);
        assert_eq!(summed.cpu_search_microsecs, 500);

        // worst_split is the one with the largest phase-sum (split_b: 80 + 300 = 380).
        let worst = acc.worst_split.unwrap();
        assert_eq!(worst.split_num_docs, 200);
    }

    #[test]
    fn test_merge_leaf_stats_it() {
        let merged = merge_leaf_stats_it(Vec::<&Option<LeafResourceStats>>::new());
        assert_eq!(merged, None);

        let leaf_a = Some(leaf_stats_one_split(split_stats(10, 1, 2)));
        let leaf_b = Some(leaf_stats_one_split(split_stats(20, 3, 4)));

        let merged = merge_leaf_stats_it(vec![&None, &leaf_a, &None, &leaf_b]);
        let merged = merged.unwrap();
        assert_eq!(merged.num_localexec_splits, 2);
        assert_eq!(merged.num_localexec_num_docs, 30);
    }
}
