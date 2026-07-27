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
use quickwit_common::thread_pool::with_priority::ThreadPoolWithPriority;
use quickwit_common::tower::Pool;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, ListSplitsRequest, MetastoreServiceClient,
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
use quickwit_proto::metastore::MetastoreService;
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

fn search_thread_pool() -> &'static ThreadPoolWithPriority {
    static SEARCH_THREAD_POOL: LazyLock<ThreadPoolWithPriority> =
        LazyLock::new(|| ThreadPoolWithPriority::new("search", None));
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
        split_id: split_metadata.split_id.to_string(),
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
    metastore: &MetastoreServiceClient,
) -> crate::Result<Vec<SplitMetadata>> {
    list_relevant_splits(index_uids, None, None, None, metastore).await
}

/// Extract the list of relevant splits for a given request.
pub async fn list_relevant_splits(
    index_uids: Vec<IndexUid>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    tags_filter_opt: Option<TagFilterAst>,
    metastore: &MetastoreServiceClient,
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
    metastore: &MetastoreServiceClient,
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
        &metastore,
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

/// Sum of the per-phase microsecond fields used to rank `split_resources_worst`.
/// Intentionally excludes the two waiting phases (`wait_for_search_permit_microsecs`
/// and `wait_for_cpu_pool_microsecs`) so the ranking reflects "how much work this
/// split actually did" rather than "how long this split queued behind other work".
pub(crate) fn split_phase_sum_microsecs(stats: &SplitResourceStats) -> u64 {
    stats.warmup_microsecs + stats.cpu_search_microsecs
}

/// Field-wise sum of two `SplitResourceStats` (every field is extensive).
pub(crate) fn add_split_stats(acc: &mut SplitResourceStats, other: &SplitResourceStats) {
    acc.split_num_docs += other.split_num_docs;
    acc.input_memory_bytes += other.input_memory_bytes;
    acc.download_num_bytes += other.download_num_bytes;
    acc.download_num_requests += other.download_num_requests;
    acc.matched_num_docs += other.matched_num_docs;
    acc.wait_for_search_permit_microsecs += other.wait_for_search_permit_microsecs;
    acc.warmup_microsecs += other.warmup_microsecs;
    acc.wait_for_cpu_pool_microsecs += other.wait_for_cpu_pool_microsecs;
    acc.cpu_search_microsecs += other.cpu_search_microsecs;
}

/// Min of two `Option<u64>`, treating `None` as "no contribution":
/// `min(None, x) = x`, `min(None, None) = None`.
#[inline]
pub(crate) fn min_opt(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, value) | (value, None) => value,
        (Some(left), Some(right)) => Some(left.min(right)),
    }
}

/// Merge another `LeafResourceStats` into `acc`.
///
/// Every numeric field is summed, with two exceptions:
/// - `split_resources_worst` is selected by `split_phase_sum_microsecs`; `split_resources_sum` is
///   field-wise summed.
/// - `min_wait_for_search_permit_microsecs` and `min_wait_for_cpu_pool_microsecs` are merged with
///   `min_opt` (treating `None` as "no contribution"), so the merged value is the minimum across
///   every locally-executed split that contributed.
///
/// `lambda_bottleneck` is 0 or 1 at the source (a single leaf call) and becomes
/// a count of leaves where lambda was the bottleneck once aggregated.
pub(crate) fn add_leaf_stats(acc: &mut LeafResourceStats, other: &LeafResourceStats) {
    acc.partial_result_cache_num_splits += other.partial_result_cache_num_splits;
    acc.partial_result_cache_num_docs += other.partial_result_cache_num_docs;
    acc.lambda_num_splits += other.lambda_num_splits;
    acc.lambda_num_docs += other.lambda_num_docs;
    acc.lambda_success_num_splits += other.lambda_success_num_splits;
    acc.lambda_success_num_docs += other.lambda_success_num_docs;
    acc.lambda_bottleneck += other.lambda_bottleneck;
    acc.localexec_num_splits += other.localexec_num_splits;
    acc.localexec_num_docs += other.localexec_num_docs;
    acc.wall_time_microsecs += other.wall_time_microsecs;
    acc.search_pool_cpu_threads += other.search_pool_cpu_threads;
    acc.min_wait_for_search_permit_microsecs = min_opt(
        acc.min_wait_for_search_permit_microsecs,
        other.min_wait_for_search_permit_microsecs,
    );
    acc.min_wait_for_cpu_pool_microsecs = min_opt(
        acc.min_wait_for_cpu_pool_microsecs,
        other.min_wait_for_cpu_pool_microsecs,
    );
    if let Some(other_split) = &other.split_resources_sum {
        let acc_split = acc
            .split_resources_sum
            .get_or_insert_with(SplitResourceStats::default);
        add_split_stats(acc_split, other_split);
    }
    acc.split_resources_worst = [acc.split_resources_worst, other.split_resources_worst]
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
            localexec_num_splits: 1,
            localexec_num_docs: split.split_num_docs,
            split_resources_sum: Some(split),
            split_resources_worst: Some(split),
            ..Default::default()
        }
    }

    /// `add_split_stats` is an "every field is extensive" merger. Adding a
    /// new field to the proto without summing it in `add_split_stats` should
    /// fail this test: the `let SplitResourceStats { ... } = other;`
    /// destructure forces the test to be updated whenever a field is added.
    #[test]
    fn test_add_split_stats_sums_every_field() {
        let mut acc = SplitResourceStats {
            split_num_docs: 1,
            input_memory_bytes: 10,
            download_num_bytes: 100,
            download_num_requests: 1_000,
            matched_num_docs: 10_000,
            wait_for_search_permit_microsecs: 100_000,
            warmup_microsecs: 1_000_000,
            wait_for_cpu_pool_microsecs: 10_000_000,
            cpu_search_microsecs: 100_000_000,
        };
        let other = SplitResourceStats {
            split_num_docs: 2,
            input_memory_bytes: 20,
            download_num_bytes: 200,
            download_num_requests: 2_000,
            matched_num_docs: 20_000,
            wait_for_search_permit_microsecs: 200_000,
            warmup_microsecs: 2_000_000,
            wait_for_cpu_pool_microsecs: 20_000_000,
            cpu_search_microsecs: 200_000_000,
        };
        // Destructure on the proto type itself so a newly-added field forces
        // an update to this test (otherwise the assertion below would silently
        // miss it).
        let SplitResourceStats {
            split_num_docs: _,
            input_memory_bytes: _,
            download_num_bytes: _,
            download_num_requests: _,
            matched_num_docs: _,
            wait_for_search_permit_microsecs: _,
            warmup_microsecs: _,
            wait_for_cpu_pool_microsecs: _,
            cpu_search_microsecs: _,
        } = other;

        add_split_stats(&mut acc, &other);

        assert_eq!(acc.split_num_docs, 3);
        assert_eq!(acc.input_memory_bytes, 30);
        assert_eq!(acc.download_num_bytes, 300);
        assert_eq!(acc.download_num_requests, 3_000);
        assert_eq!(acc.matched_num_docs, 30_000);
        assert_eq!(acc.wait_for_search_permit_microsecs, 300_000);
        assert_eq!(acc.warmup_microsecs, 3_000_000);
        assert_eq!(acc.wait_for_cpu_pool_microsecs, 30_000_000);
        assert_eq!(acc.cpu_search_microsecs, 300_000_000);
    }

    #[test]
    fn test_add_split_stats_with_zero_is_identity() {
        let mut acc = SplitResourceStats {
            split_num_docs: 42,
            download_num_bytes: 1_024,
            cpu_search_microsecs: 1_000,
            ..Default::default()
        };
        let snapshot = acc;
        add_split_stats(&mut acc, &SplitResourceStats::default());
        assert_eq!(acc, snapshot);
    }

    /// `add_leaf_stats` is "every numeric field is extensive" with the
    /// exception of `min_wait_for_*_microsecs` (merged with `min_opt`).
    /// The fully-enumerated `LeafResourceStats { ... }` literal (no
    /// `..Default::default()`) forces this test to be updated whenever a
    /// field is added — and the per-field assertions below document each
    /// field's aggregation behavior.
    #[test]
    fn test_add_leaf_stats_sums_every_field() {
        let split_a = SplitResourceStats {
            split_num_docs: 1,
            cpu_search_microsecs: 100,
            ..Default::default()
        };
        let split_b = SplitResourceStats {
            split_num_docs: 2,
            cpu_search_microsecs: 200,
            ..Default::default()
        };

        let mut acc = LeafResourceStats {
            partial_result_cache_num_splits: 1,
            partial_result_cache_num_docs: 10,
            lambda_num_splits: 100,
            lambda_num_docs: 1_000,
            lambda_success_num_splits: 10_000,
            lambda_success_num_docs: 100_000,
            lambda_bottleneck: 0,
            localexec_num_splits: 1_000_000,
            localexec_num_docs: 10_000_000,
            split_resources_worst: Some(split_a),
            split_resources_sum: Some(split_a),
            min_wait_for_search_permit_microsecs: Some(100),
            min_wait_for_cpu_pool_microsecs: Some(2_000),
            wall_time_microsecs: 100_000_000,
            search_pool_cpu_threads: 8,
        };
        let other = LeafResourceStats {
            partial_result_cache_num_splits: 2,
            partial_result_cache_num_docs: 20,
            lambda_num_splits: 200,
            lambda_num_docs: 2_000,
            lambda_success_num_splits: 20_000,
            lambda_success_num_docs: 200_000,
            lambda_bottleneck: 1,
            localexec_num_splits: 2_000_000,
            localexec_num_docs: 20_000_000,
            split_resources_worst: Some(split_b),
            split_resources_sum: Some(split_b),
            min_wait_for_search_permit_microsecs: Some(50),
            min_wait_for_cpu_pool_microsecs: Some(1_000),
            wall_time_microsecs: 200_000_000,
            search_pool_cpu_threads: 16,
        };

        add_leaf_stats(&mut acc, &other);

        assert_eq!(acc.partial_result_cache_num_splits, 3);
        assert_eq!(acc.partial_result_cache_num_docs, 30);
        assert_eq!(acc.lambda_num_splits, 300);
        assert_eq!(acc.lambda_num_docs, 3_000);
        assert_eq!(acc.lambda_success_num_splits, 30_000);
        assert_eq!(acc.lambda_success_num_docs, 300_000);
        // `lambda_bottleneck` is summed post-refactor (a count at aggregate
        // levels), not maxed.
        assert_eq!(acc.lambda_bottleneck, 1);
        assert_eq!(acc.localexec_num_splits, 3_000_000);
        assert_eq!(acc.localexec_num_docs, 30_000_000);
        // `wall_time_microsecs` is summed post-refactor, not maxed.
        assert_eq!(acc.wall_time_microsecs, 300_000_000);
        assert_eq!(acc.search_pool_cpu_threads, 24);

        // `min_wait_for_*_microsecs` is MIN, not sum — the exception to the
        // extensive-sum rule.
        assert_eq!(acc.min_wait_for_search_permit_microsecs, Some(50));
        assert_eq!(acc.min_wait_for_cpu_pool_microsecs, Some(1_000));

        // `split_resources_sum` field-wise sums every contributing split.
        let summed = acc.split_resources_sum.unwrap();
        assert_eq!(summed.split_num_docs, 3);
        assert_eq!(summed.cpu_search_microsecs, 300);

        // `split_resources_worst` is the split with the larger phase-sum:
        // split_b (cpu_search 200) beats split_a (cpu_search 100).
        let worst = acc.split_resources_worst.unwrap();
        assert_eq!(worst.split_num_docs, 2);
    }

    /// When the accumulator already has `split_resources_*` and the other
    /// side has none, the accumulator's values must be preserved.
    #[test]
    fn test_add_leaf_stats_other_with_no_split_resources_keeps_acc() {
        let split = split_stats(5, 10, 20);
        let mut acc = leaf_stats_one_split(split);
        let other = LeafResourceStats {
            localexec_num_splits: 1,
            localexec_num_docs: 7,
            // No split_resources_sum / split_resources_worst.
            ..Default::default()
        };
        add_leaf_stats(&mut acc, &other);
        assert_eq!(acc.localexec_num_splits, 2);
        assert_eq!(acc.localexec_num_docs, 12);
        // Both fields must still point at split_a.
        assert_eq!(acc.split_resources_sum.unwrap().split_num_docs, 5);
        assert_eq!(acc.split_resources_worst.unwrap().split_num_docs, 5);
    }

    /// When the accumulator has no `split_resources_*` and the other side
    /// does, the result must adopt the other side's values.
    #[test]
    fn test_add_leaf_stats_acc_empty_adopts_other_split_resources() {
        let mut acc = LeafResourceStats::default();
        let split = split_stats(9, 11, 13);
        let other = leaf_stats_one_split(split);
        add_leaf_stats(&mut acc, &other);
        assert_eq!(acc.localexec_num_splits, 1);
        assert_eq!(acc.split_resources_sum.unwrap().split_num_docs, 9);
        assert_eq!(acc.split_resources_worst.unwrap().split_num_docs, 9);
    }

    /// Adding a default `LeafResourceStats` should be an identity operation
    /// on every field.
    #[test]
    fn test_add_leaf_stats_with_default_is_identity() {
        let split = split_stats(3, 4, 5);
        let mut acc = LeafResourceStats {
            partial_result_cache_num_splits: 1,
            lambda_num_splits: 2,
            lambda_bottleneck: 1,
            localexec_num_splits: 1,
            localexec_num_docs: 3,
            split_resources_sum: Some(split),
            split_resources_worst: Some(split),
            // Set the min fields explicitly so this test verifies that
            // `min_opt(Some(x), None) = Some(x)` keeps them unchanged.
            min_wait_for_search_permit_microsecs: Some(7),
            min_wait_for_cpu_pool_microsecs: Some(11),
            wall_time_microsecs: 42,
            ..Default::default()
        };
        let snapshot = acc;
        add_leaf_stats(&mut acc, &LeafResourceStats::default());
        assert_eq!(acc, snapshot);
    }

    #[test]
    fn test_min_opt() {
        assert_eq!(min_opt(None, None), None);
        assert_eq!(min_opt(Some(5), None), Some(5));
        assert_eq!(min_opt(None, Some(7)), Some(7));
        assert_eq!(min_opt(Some(5), Some(7)), Some(5));
        assert_eq!(min_opt(Some(7), Some(5)), Some(5));
        // 0 is a real value, not a sentinel.
        assert_eq!(min_opt(Some(0), Some(5)), Some(0));
        assert_eq!(min_opt(Some(0), None), Some(0));
    }

    #[test]
    fn test_merge_leaf_stats_it() {
        let merged = merge_leaf_stats_it(Vec::<&Option<LeafResourceStats>>::new());
        assert_eq!(merged, None);

        let leaf_a = Some(leaf_stats_one_split(split_stats(10, 1, 2)));
        let leaf_b = Some(leaf_stats_one_split(split_stats(20, 3, 4)));

        let merged = merge_leaf_stats_it(vec![&None, &leaf_a, &None, &leaf_b]);
        let merged = merged.unwrap();
        assert_eq!(merged.localexec_num_splits, 2);
        assert_eq!(merged.localexec_num_docs, 30);
    }
}
