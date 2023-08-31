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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::shared_consts::{DELETION_GRACE_PERIOD, SCROLL_BATCH_LEN};
use quickwit_common::uri::Uri;
use quickwit_config::{build_doc_mapper, IndexConfig};
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_doc_mapper::{DocMapper, DYNAMIC_FIELD_NAME};
use quickwit_metastore::{IndexMetadata, ListIndexesQuery, Metastore, SplitMetadata};
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, Hit, LeafHit, LeafListTermsRequest, LeafListTermsResponse,
    LeafSearchRequest, LeafSearchResponse, ListTermsRequest, ListTermsResponse, PartialHit,
    SearchRequest, SearchResponse, SnippetRequest, SortField, SplitIdAndFooterOffsets,
};
use quickwit_proto::IndexUid;
use quickwit_query::query_ast::{
    BoolQuery, QueryAst, QueryAstVisitor, RangeQuery, TermQuery, TermSetQuery,
};
use serde::{Deserialize, Serialize};
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::Collector;
use tantivy::schema::{FieldType, Schema};
use tantivy::TantivyError;
use tracing::{debug, error, info_span, instrument};

use crate::cluster_client::ClusterClient;
use crate::collector::{make_merge_collector, QuickwitAggregations};
use crate::find_trace_ids_collector::Span;
use crate::scroll_context::{ScrollContext, ScrollKeyAndStartOffset};
use crate::search_job_placer::Job;
use crate::service::SearcherContext;
use crate::{
    extract_split_and_footer_offsets, list_relevant_splits, SearchError, SearchJobPlacer,
    SearchServiceClient,
};

/// Maximum accepted scroll TTL.
const MAX_SCROLL_TTL: Duration = Duration::from_secs(DELETION_GRACE_PERIOD.as_secs() - 60 * 2);

/// SearchJob to be assigned to search clients by the [`SearchJobPlacer`].
#[derive(Debug, Clone, PartialEq)]
pub struct SearchJob {
    index_uid: IndexUid,
    cost: usize,
    offsets: SplitIdAndFooterOffsets,
}

impl SearchJob {
    #[cfg(test)]
    pub fn for_test(split_id: &str, cost: usize) -> SearchJob {
        SearchJob {
            index_uid: IndexUid::from("test-index".to_string()),
            cost,
            offsets: SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                ..Default::default()
            },
        }
    }
}

impl From<SearchJob> for SplitIdAndFooterOffsets {
    fn from(search_job: SearchJob) -> Self {
        search_job.offsets
    }
}

impl<'a> From<&'a SplitMetadata> for SearchJob {
    fn from(split_metadata: &'a SplitMetadata) -> Self {
        SearchJob {
            index_uid: split_metadata.index_uid.clone(),
            cost: compute_split_cost(split_metadata),
            offsets: extract_split_and_footer_offsets(split_metadata),
        }
    }
}

impl Job for SearchJob {
    fn split_id(&self) -> &str {
        &self.offsets.split_id
    }

    fn cost(&self) -> usize {
        self.cost
    }
}

pub struct FetchDocsJob {
    index_uid: IndexUid,
    offsets: SplitIdAndFooterOffsets,
    pub partial_hits: Vec<PartialHit>,
}

impl Job for FetchDocsJob {
    fn split_id(&self) -> &str {
        &self.offsets.split_id
    }

    fn cost(&self) -> usize {
        self.partial_hits.len()
    }
}

impl From<FetchDocsJob> for SplitIdAndFooterOffsets {
    fn from(fetch_docs_job: FetchDocsJob) -> SplitIdAndFooterOffsets {
        fetch_docs_job.offsets
    }
}

/// Index metas needed for executing a leaf search request.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMetasForLeafSearch {
    /// Index URI.
    pub index_uri: Uri,
    /// Doc mapper json string.
    pub doc_mapper_str: String,
}

pub(crate) type IndexesMetasForLeafSearch = HashMap<IndexUid, IndexMetasForLeafSearch>;
type TimestampFieldOpt = Option<String>;

/// Validates request against each index's doc mapper and ensures that:
/// - timestamp fields (if any) are equal across indexes.
/// - resolved query ASTs are the same across indexes.
/// Returns the timestamp field, the resolved query AST and the indexes metadatas
/// needed for leaf search requests.
/// Note: the requirements on timestamp fields and resolved query ASTs can be lifted
/// but it adds complexity that does not seem needed right now.
fn validate_request_and_build_metadatas(
    indexes_metadata: &[IndexMetadata],
    search_request: &SearchRequest,
) -> crate::Result<(TimestampFieldOpt, QueryAst, IndexesMetasForLeafSearch)> {
    let mut metadatas_for_leaf: HashMap<IndexUid, IndexMetasForLeafSearch> = HashMap::new();
    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let mut query_ast_resolved_opt: Option<QueryAst> = None;
    let mut timestamp_field_opt: Option<String> = None;

    for index_metadata in indexes_metadata {
        let doc_mapper = build_doc_mapper(
            &index_metadata.index_config.doc_mapping,
            &index_metadata.index_config.search_settings,
        )
        .map_err(|err| {
            SearchError::Internal(format!("Failed to build doc mapper. Cause: {err}"))
        })?;
        let query_ast_resolved_for_index = query_ast
            .clone()
            .parse_user_query(doc_mapper.default_search_fields())
            // We convert the error to return a 400 to the user (and not a 500).
            .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;

        // Validate uniqueness of resolved query AST.
        if let Some(query_ast_resolved) = &query_ast_resolved_opt {
            if query_ast_resolved != &query_ast_resolved_for_index {
                return Err(SearchError::InvalidQuery(
                    "Resolved query ASTs must be the same across indexes. Resolving queries with \
                     different default fields are different between indexes is not supported."
                        .to_string(),
                ));
            }
        } else {
            query_ast_resolved_opt = Some(query_ast_resolved_for_index.clone());
        }

        // Validate uniqueness of timestamp field if any.
        if let Some(timestamp_field_for_index) = doc_mapper.timestamp_field_name() {
            match timestamp_field_opt {
                Some(timestamp_field) if timestamp_field != timestamp_field_for_index => {
                    return Err(SearchError::InvalidQuery(
                        "The timestamp field (if present) must be the same for all indexes."
                            .to_string(),
                    ));
                }
                None => {
                    timestamp_field_opt = Some(timestamp_field_for_index.to_string());
                }
                _ => {}
            }
        }

        validate_request(&*doc_mapper, search_request)?;
        // Validates the query by effectively building it against the current schema.
        doc_mapper.query(doc_mapper.schema(), &query_ast_resolved_for_index, true)?;

        let index_metadata_for_leaf_search = IndexMetasForLeafSearch {
            index_uri: index_metadata.index_uri().clone(),
            doc_mapper_str: serde_json::to_string(&doc_mapper).map_err(|err| {
                SearchError::Internal(format!("Failed to serialize doc mapper. Cause: {err}"))
            })?,
        };
        metadatas_for_leaf.insert(
            index_metadata.index_uid.clone(),
            index_metadata_for_leaf_search,
        );
    }

    let query_ast_resolved = query_ast_resolved_opt.ok_or_else(|| {
        SearchError::Internal(
            "Resolved query AST must be present. This should never happen.".to_string(),
        )
    })?;

    Ok((timestamp_field_opt, query_ast_resolved, metadatas_for_leaf))
}

fn validate_requested_snippet_fields(
    schema: &Schema,
    snippet_fields: &[String],
) -> anyhow::Result<()> {
    for field_name in snippet_fields {
        let field_entry = schema
            .get_field(field_name)
            .map(|field| schema.get_field_entry(field))?;
        match field_entry.field_type() {
            FieldType::Str(text_options) => {
                if !text_options.is_stored() {
                    return Err(anyhow::anyhow!(
                        "The snippet field `{}` must be stored.",
                        field_name
                    ));
                }
            }
            other => {
                return Err(anyhow::anyhow!(
                    "The snippet field `{}` must be of type `Str`, got `{}`.",
                    field_name,
                    other.value_type().name()
                ))
            }
        }
    }
    Ok(())
}

fn validate_sort_by_fields(sort_fields: &[SortField], schema: &Schema) -> crate::Result<()> {
    if sort_fields.is_empty() {
        return Ok(());
    }
    if sort_fields.len() > 2 {
        return Err(SearchError::InvalidArgument(format!(
            "Sort by field must be up to 2 fields {:?}.",
            sort_fields
        )));
    }
    for sort in sort_fields {
        validate_sort_by_field(&sort.field_name, schema)?;
    }

    Ok(())
}

fn simplify_search_request_for_scroll_api(req: &SearchRequest) -> SearchRequest {
    // We do not mutate
    SearchRequest {
        index_id_patterns: req.index_id_patterns.clone(),
        query_ast: req.query_ast.clone(),
        start_timestamp: req.start_timestamp,
        end_timestamp: req.end_timestamp,
        max_hits: req.max_hits,
        start_offset: req.start_offset,
        sort_fields: req.sort_fields.clone(),
        // We remove all aggregation request.
        // The aggregation will not be computed for each scroll request.
        aggregation_request: None,
        // We remove the snippet fields. This feature is not supported for scroll requests.
        snippet_fields: Vec::new(),
        // We remove the scroll ttl parameter. It is irrelevant to process later request
        scroll_ttl_secs: None,
    }
}

fn validate_sort_by_field(field_name: &str, schema: &Schema) -> crate::Result<()> {
    if field_name == "_score" {
        return Ok(());
    }
    let dynamic_field_opt = schema.get_field(DYNAMIC_FIELD_NAME).ok();
    let (sort_by_field, _json_path) = schema
        .find_field_with_default(field_name, dynamic_field_opt)
        .ok_or_else(|| {
            SearchError::InvalidArgument(format!("Unknown field used in `sort by`: {field_name}"))
        })?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);
    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        return Err(SearchError::InvalidArgument(format!(
            "Sort by field on type text is currently not supported `{field_name}`."
        )));
    }
    if !sort_by_field_entry.is_fast() {
        return Err(SearchError::InvalidArgument(format!(
            "Sort by field must be a fast field, please add the fast property to your field \
             `{field_name}`.",
        )));
    }
    Ok(())
}

fn validate_request(
    doc_mapper: &dyn DocMapper,
    search_request: &SearchRequest,
) -> crate::Result<()> {
    let schema = doc_mapper.schema();

    validate_requested_snippet_fields(&schema, &search_request.snippet_fields)?;

    validate_sort_by_fields(&search_request.sort_fields, &schema)?;

    if let Some(agg) = search_request.aggregation_request.as_ref() {
        let _aggs: QuickwitAggregations = serde_json::from_str(agg).map_err(|_err| {
            let err = serde_json::from_str::<tantivy::aggregation::agg_req::Aggregations>(agg)
                .unwrap_err();
            SearchError::InvalidAggregationRequest(err.to_string())
        })?;
    };

    if search_request.start_offset > 10_000 {
        return Err(SearchError::InvalidArgument(format!(
            "max value for start_offset is 10_000, but got {}",
            search_request.start_offset
        )));
    }

    if search_request.max_hits > 10_000 {
        return Err(SearchError::InvalidArgument(format!(
            "max value for max_hits is 10_000, but got {}",
            search_request.max_hits
        )));
    }

    Ok(())
}

fn get_scroll_ttl_duration(search_request: &SearchRequest) -> crate::Result<Option<Duration>> {
    let Some(scroll_ttl_secs) = search_request.scroll_ttl_secs else {
        return Ok(None);
    };
    let scroll_ttl: Duration = Duration::from_secs(scroll_ttl_secs as u64);
    if scroll_ttl > MAX_SCROLL_TTL {
        return Err(SearchError::InvalidArgument(format!(
            "Quickwit only supports scroll TTL period up to {} secs.",
            MAX_SCROLL_TTL.as_secs()
        )));
    }
    Ok(Some(scroll_ttl))
}

#[instrument(skip(search_request, indexes_metas_for_leaf_search, cluster_client))]
async fn search_partial_hits_phase_with_scroll(
    searcher_context: &SearcherContext,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    mut search_request: SearchRequest,
    split_metadatas: &[SplitMetadata],
    cluster_client: &ClusterClient,
) -> crate::Result<(LeafSearchResponse, Option<ScrollKeyAndStartOffset>)> {
    let scroll_ttl_opt = get_scroll_ttl_duration(&search_request)?;

    if let Some(scroll_ttl) = scroll_ttl_opt {
        let max_hits = search_request.max_hits;
        // This is a scroll request.
        //
        // We increase max hits to add populate the scroll cache.
        search_request.max_hits = SCROLL_BATCH_LEN as u64;
        search_request.scroll_ttl_secs = None;
        let mut leaf_search_resp = search_partial_hits_phase(
            searcher_context,
            indexes_metas_for_leaf_search,
            &search_request,
            split_metadatas,
            cluster_client,
        )
        .await?;
        let cached_partial_hits = leaf_search_resp.partial_hits.clone();
        leaf_search_resp.partial_hits.truncate(max_hits as usize);

        let scroll_context_search_request = simplify_search_request_for_scroll_api(&search_request);
        let scroll_ctx = ScrollContext {
            indexes_metas_for_leaf_search: indexes_metas_for_leaf_search.clone(),
            split_metadatas: split_metadatas.to_vec(),
            search_request: scroll_context_search_request,
            total_num_hits: leaf_search_resp.num_hits,
            max_hits_per_page: max_hits,
            cached_partial_hits_start_offset: search_request.start_offset,
            cached_partial_hits,
        };
        let scroll_key_and_start_offset: ScrollKeyAndStartOffset =
            ScrollKeyAndStartOffset::new_with_start_offset(
                scroll_ctx.search_request.start_offset,
                max_hits as u32,
            )
            .next_page(leaf_search_resp.partial_hits.len() as u64);

        let payload: Vec<u8> = scroll_ctx.serialize();
        let scroll_key = scroll_key_and_start_offset.scroll_key();
        cluster_client
            .put_kv(&scroll_key, &payload, scroll_ttl)
            .await;
        Ok((leaf_search_resp, Some(scroll_key_and_start_offset)))
    } else {
        let leaf_search_resp = search_partial_hits_phase(
            searcher_context,
            indexes_metas_for_leaf_search,
            &search_request,
            split_metadatas,
            cluster_client,
        )
        .await?;
        Ok((leaf_search_resp, None))
    }
}

#[instrument(skip(search_request, indexes_metas_for_leaf_search, cluster_client))]
pub(crate) async fn search_partial_hits_phase(
    searcher_context: &SearcherContext,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    search_request: &SearchRequest,
    split_metadatas: &[SplitMetadata],
    cluster_client: &ClusterClient,
) -> crate::Result<LeafSearchResponse> {
    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let mut leaf_request_tasks = Vec::new();
    for (client, client_jobs) in assigned_leaf_search_jobs {
        let leaf_requests =
            jobs_to_leaf_requests(search_request, indexes_metas_for_leaf_search, client_jobs)?;
        for leaf_request in leaf_requests {
            leaf_request_tasks.push(cluster_client.leaf_search(leaf_request, client.clone()));
        }
    }
    let leaf_search_responses: Vec<LeafSearchResponse> = try_join_all(leaf_request_tasks).await?;

    // Creates a collector which merges responses into one
    let merge_collector =
        make_merge_collector(search_request, &searcher_context.get_aggregation_limits())?;

    // Merging is a cpu-bound task.
    // It should be executed by Tokio's blocking threads.

    // Wrap into result for merge_fruits
    let leaf_search_responses: Vec<tantivy::Result<LeafSearchResponse>> =
        leaf_search_responses.into_iter().map(Ok).collect_vec();
    let span = info_span!("merge_fruits");
    let leaf_search_response = crate::run_cpu_intensive(move || {
        let _span_guard = span.enter();
        merge_collector.merge_fruits(leaf_search_responses)
    })
    .await
    .context("failed to merge leaf search responses")?
    .map_err(|error: TantivyError| crate::SearchError::Internal(error.to_string()))?;
    debug!(leaf_search_response = ?leaf_search_response, "Merged leaf search response.");

    if !leaf_search_response.failed_splits.is_empty() {
        error!(failed_splits = ?leaf_search_response.failed_splits, "Leaf search response contains at least one failed split.");
        let errors: String = leaf_search_response.failed_splits.iter().join(", ");
        return Err(SearchError::Internal(errors));
    }
    Ok(leaf_search_response)
}

pub(crate) fn get_snippet_request(search_request: &SearchRequest) -> Option<SnippetRequest> {
    if search_request.snippet_fields.is_empty() {
        return None;
    }
    Some(SnippetRequest {
        snippet_fields: search_request.snippet_fields.clone(),
        query_ast_resolved: search_request.query_ast.clone(),
    })
}

pub(crate) async fn fetch_docs_phase(
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    partial_hits: &[PartialHit],
    split_metadatas: &[SplitMetadata],
    snippet_request_opt: Option<SnippetRequest>,
    cluster_client: &ClusterClient,
) -> crate::Result<Vec<Hit>> {
    let hit_order: HashMap<(String, u32, u32), usize> = partial_hits
        .iter()
        .enumerate()
        .map(|(position, partial_hit)| {
            let key = (
                partial_hit.split_id.clone(),
                partial_hit.segment_ord,
                partial_hit.doc_id,
            );
            (key, position)
        })
        .collect();

    let assigned_fetch_docs_jobs = assign_client_fetch_docs_jobs(
        partial_hits,
        split_metadatas,
        &cluster_client.search_job_placer,
    )
    .await?;

    let mut fetch_docs_tasks = Vec::new();
    for (client, client_jobs) in assigned_fetch_docs_jobs {
        let fetch_jobs_requests = jobs_to_fetch_docs_requests(
            snippet_request_opt.clone(),
            indexes_metas_for_leaf_search,
            client_jobs,
        )?;
        for fetch_docs_request in fetch_jobs_requests {
            fetch_docs_tasks.push(cluster_client.fetch_docs(fetch_docs_request, client.clone()));
        }
    }
    let fetch_docs_responses: Vec<FetchDocsResponse> = try_join_all(fetch_docs_tasks).await?;

    // Merge the fetched docs.
    let leaf_hits = fetch_docs_responses
        .into_iter()
        .flat_map(|response| response.hits.into_iter());

    let mut hits_with_position: Vec<(usize, Hit)> = leaf_hits
        .flat_map(|leaf_hit: LeafHit| {
            let partial_hit_ref = leaf_hit.partial_hit.as_ref()?;
            let key = (
                partial_hit_ref.split_id.clone(),
                partial_hit_ref.segment_ord,
                partial_hit_ref.doc_id,
            );
            let position = *hit_order.get(&key)?;
            Some((
                position,
                Hit {
                    json: leaf_hit.leaf_json,
                    partial_hit: leaf_hit.partial_hit,
                    snippet: leaf_hit.leaf_snippet_json,
                },
            ))
        })
        .collect();

    hits_with_position.sort_by_key(|(position, _)| *position);
    let hits: Vec<Hit> = hits_with_position
        .into_iter()
        .map(|(_position, hit)| hit)
        .collect();

    Ok(hits)
}

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip(
    searcher_context,
    indexes_metas_for_leaf_search,
    search_request,
    cluster_client
))]
async fn root_search_aux(
    searcher_context: &SearcherContext,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    search_request: SearchRequest,
    split_metadatas: Vec<SplitMetadata>,
    cluster_client: &ClusterClient,
) -> crate::Result<SearchResponse> {
    let (first_phase_result, scroll_key_and_start_offset_opt): (
        LeafSearchResponse,
        Option<ScrollKeyAndStartOffset>,
    ) = search_partial_hits_phase_with_scroll(
        searcher_context,
        indexes_metas_for_leaf_search,
        search_request.clone(),
        &split_metadatas[..],
        cluster_client,
    )
    .await?;

    let snippet_request: Option<SnippetRequest> = get_snippet_request(&search_request);
    let hits = fetch_docs_phase(
        indexes_metas_for_leaf_search,
        &first_phase_result.partial_hits,
        &split_metadatas[..],
        snippet_request,
        cluster_client,
    )
    .await?;

    let aggregation_result_json_opt = finalize_aggregation_if_any(
        &search_request,
        first_phase_result.intermediate_aggregation_result,
        searcher_context,
    )?;

    Ok(SearchResponse {
        aggregation: aggregation_result_json_opt,
        num_hits: first_phase_result.num_hits,
        hits,
        elapsed_time_micros: 0u64,
        errors: Vec::new(),
        scroll_id: scroll_key_and_start_offset_opt
            .as_ref()
            .map(ToString::to_string),
    })
}

fn finalize_aggregation(
    intermediate_aggregation_result_bytes: &[u8],
    aggregations: QuickwitAggregations,
    searcher_context: &SearcherContext,
) -> crate::Result<String> {
    let merge_aggregation_result = match aggregations {
        QuickwitAggregations::FindTraceIdsAggregation(_) => {
            // The merge collector has already merged the intermediate results.
            let aggs: Vec<Span> = postcard::from_bytes(intermediate_aggregation_result_bytes)?;
            serde_json::to_string(&aggs)?
        }
        QuickwitAggregations::TantivyAggregations(aggregations) => {
            let intermediate_aggregation_results: IntermediateAggregationResults =
                postcard::from_bytes(intermediate_aggregation_result_bytes)?;
            let final_aggregation_results: AggregationResults = intermediate_aggregation_results
                .into_final_result(aggregations, &searcher_context.get_aggregation_limits())?;
            serde_json::to_string(&final_aggregation_results)?
        }
    };
    Ok(merge_aggregation_result)
}

fn finalize_aggregation_if_any(
    search_request: &SearchRequest,
    intermediate_aggregation_result_bytes_opt: Option<Vec<u8>>,
    searcher_context: &SearcherContext,
) -> crate::Result<Option<String>> {
    let Some(aggregations_json) = search_request.aggregation_request.as_ref() else {
        return Ok(None);
    };
    let aggregations: QuickwitAggregations = serde_json::from_str(aggregations_json)?;
    let Some(intermediate_result_bytes) = intermediate_aggregation_result_bytes_opt else {
        return Ok(None);
    };
    let aggregation_result_json = finalize_aggregation(
        &intermediate_result_bytes[..],
        aggregations,
        searcher_context,
    )?;
    Ok(Some(aggregation_result_json))
}

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip(search_request, cluster_client, metastore))]
pub async fn root_search(
    searcher_context: &SearcherContext,
    mut search_request: SearchRequest,
    metastore: &dyn Metastore,
    cluster_client: &ClusterClient,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let indexes_metadata = metastore
        .list_indexes_metadatas(ListIndexesQuery::IndexIdPatterns(
            search_request.index_id_patterns.clone(),
        ))
        .await?;
    if indexes_metadata.is_empty() {
        return Err(SearchError::IndexesNotFound {
            index_id_patterns: search_request.index_id_patterns,
        });
    }
    let index_uids = indexes_metadata
        .iter()
        .map(|index_metadata| index_metadata.index_uid.clone())
        .collect_vec();
    let (timestamp_field_opt, query_ast_resolved, indexes_metas_for_leaf_search) =
        validate_request_and_build_metadatas(&indexes_metadata, &search_request)?;
    search_request.query_ast = serde_json::to_string(&query_ast_resolved)?;

    if let Some(timestamp_field) = &timestamp_field_opt {
        refine_start_end_timestamp_from_ast(
            &query_ast_resolved,
            timestamp_field,
            &mut search_request.start_timestamp,
            &mut search_request.end_timestamp,
        );
    }
    let tag_filter_ast = extract_tags_from_query(query_ast_resolved);

    let split_metadatas: Vec<SplitMetadata> = list_relevant_splits(
        index_uids,
        search_request.start_timestamp,
        search_request.end_timestamp,
        tag_filter_ast,
        metastore,
    )
    .await?;

    let mut search_response = root_search_aux(
        searcher_context,
        &indexes_metas_for_leaf_search,
        search_request,
        split_metadatas,
        cluster_client,
    )
    .await?;

    search_response.elapsed_time_micros = start_instant.elapsed().as_micros() as u64;
    Ok(search_response)
}

pub(crate) fn refine_start_end_timestamp_from_ast(
    query_ast: &QueryAst,
    timestamp_field: &str,
    start_timestamp: &mut Option<i64>,
    end_timestamp: &mut Option<i64>,
) {
    let mut timestamp_range_extractor = ExtractTimestampRange {
        timestamp_field,
        start_timestamp: *start_timestamp,
        end_timestamp: *end_timestamp,
    };
    timestamp_range_extractor
        .visit(query_ast)
        .expect("can't fail unwrapping Infallible");
    *start_timestamp = timestamp_range_extractor.start_timestamp;
    *end_timestamp = timestamp_range_extractor.end_timestamp;
}

/// Boundaries identified as being implied by the QueryAst.
///
/// `start_timestamp` is to be interpreted as Inclusive (or Unbounded)
/// `end_timestamp` is to be interpreted as Exclusive (or Unbounded)
/// In other word, this is a `[start_timestamp..end_timestamp)` interval.
struct ExtractTimestampRange<'a> {
    timestamp_field: &'a str,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

impl<'a> ExtractTimestampRange<'a> {
    fn update_start_timestamp(
        &mut self,
        lower_bound: &quickwit_query::JsonLiteral,
        included: bool,
    ) {
        use quickwit_query::InterpretUserInput;
        let Some(lower_bound) = tantivy::DateTime::interpret_json(lower_bound) else {
            return;
        };
        let mut lower_bound = lower_bound.into_timestamp_secs();
        if !included {
            // TODO saturating isn't exactly right, we should replace the RangeQuery with
            // a match_none, but the visitor doesn't allow mutation.
            lower_bound = lower_bound.saturating_add(1);
        }
        self.start_timestamp = Some(
            self.start_timestamp
                .map_or(lower_bound, |current| current.max(lower_bound)),
        );
    }

    fn update_end_timestamp(&mut self, upper_bound: &quickwit_query::JsonLiteral, included: bool) {
        use quickwit_query::InterpretUserInput;
        let Some(upper_bound_timestamp) = tantivy::DateTime::interpret_json(upper_bound) else {
            return;
        };
        let mut upper_bound = upper_bound_timestamp.into_timestamp_secs();
        let round_up = (upper_bound_timestamp.into_timestamp_nanos() % 1_000_000_000) != 0;
        if included || round_up {
            // TODO saturating isn't exactly right, we should replace the RangeQuery with
            // a match_none, but the visitor doesn't allow mutation.
            upper_bound = upper_bound.saturating_add(1);
        }
        self.end_timestamp = Some(
            self.end_timestamp
                .map_or(upper_bound, |current| current.min(upper_bound)),
        );
    }
}

impl<'a, 'b> QueryAstVisitor<'b> for ExtractTimestampRange<'a> {
    type Err = std::convert::Infallible;

    fn visit_bool(&mut self, bool_query: &'b BoolQuery) -> Result<(), Self::Err> {
        // we only want to visit sub-queries which are strict (positive) requirements
        for ast in bool_query.must.iter().chain(bool_query.filter.iter()) {
            self.visit(ast)?;
        }
        Ok(())
    }

    fn visit_range(&mut self, range_query: &'b RangeQuery) -> Result<(), Self::Err> {
        use std::ops::Bound;

        if range_query.field == self.timestamp_field {
            match &range_query.lower_bound {
                Bound::Included(lower_bound) => self.update_start_timestamp(lower_bound, true),
                Bound::Excluded(lower_bound) => self.update_start_timestamp(lower_bound, false),
                Bound::Unbounded => (),
            }
            match &range_query.upper_bound {
                Bound::Included(upper_bound) => self.update_end_timestamp(upper_bound, true),
                Bound::Excluded(upper_bound) => self.update_end_timestamp(upper_bound, false),
                Bound::Unbounded => (),
            }
        }
        Ok(())
    }

    // if we visit a term, limit the range to DATE..=DATE
    fn visit_term(&mut self, term_query: &'b TermQuery) -> Result<(), Self::Err> {
        if term_query.field == self.timestamp_field {
            // TODO when fixing #3323, this may need to be modified to support numbers too
            let json_term = quickwit_query::JsonLiteral::String(term_query.value.clone());
            self.update_start_timestamp(&json_term, true);
            self.update_end_timestamp(&json_term, true);
        }
        Ok(())
    }

    // if we visit a termset, limit the range to LOWEST..=HIGHEST
    fn visit_term_set(&mut self, term_query: &'b TermSetQuery) -> Result<(), Self::Err> {
        if let Some(term_set) = term_query.terms_per_field.get(self.timestamp_field) {
            // rfc3339 is lexicographically ordered if YEAR <= 9999, so we can use string
            // ordering to get the start and end quickly.
            if let Some(first) = term_set.first() {
                let json_term = quickwit_query::JsonLiteral::String(first.clone());
                self.update_start_timestamp(&json_term, true);
            }
            if let Some(last) = term_set.last() {
                let json_term = quickwit_query::JsonLiteral::String(last.clone());
                self.update_end_timestamp(&json_term, true);
            }
        }
        Ok(())
    }
}

/// Performs a distributed list terms.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
/// this is much simpler than `root_search` as it doesn't need to get actual docs.
#[instrument(skip(list_terms_request, cluster_client, metastore))]
pub async fn root_list_terms(
    list_terms_request: &ListTermsRequest,
    metastore: &dyn Metastore,
    cluster_client: &ClusterClient,
) -> crate::Result<ListTermsResponse> {
    let start_instant = tokio::time::Instant::now();

    let index_metadata = metastore
        .index_metadata(&list_terms_request.index_id)
        .await?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config: IndexConfig = index_metadata.into_index_config();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::Internal(format!("Failed to build doc mapper. Cause: {err}"))
        })?;

    let schema = doc_mapper.schema();
    let field = schema.get_field(&list_terms_request.field).map_err(|_| {
        SearchError::InvalidQuery(format!(
            "Failed to list terms in `{}`, field doesn't exist",
            list_terms_request.field
        ))
    })?;

    let field_entry = schema.get_field_entry(field);
    if !field_entry.is_indexed() {
        return Err(SearchError::InvalidQuery(
            "Trying to list terms on field which isn't indexed".to_string(),
        ));
    }

    let mut query = quickwit_metastore::ListSplitsQuery::for_index(index_uid)
        .with_split_state(quickwit_metastore::SplitState::Published);

    if let Some(start_ts) = list_terms_request.start_timestamp {
        query = query.with_time_range_start_gte(start_ts);
    }

    if let Some(end_ts) = list_terms_request.end_timestamp {
        query = query.with_time_range_end_lt(end_ts);
    }

    let split_metadatas = metastore
        .list_splits(query)
        .await?
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>();

    let index_uri = &index_config.index_uri;

    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let leaf_search_responses: Vec<LeafListTermsResponse> =
        try_join_all(assigned_leaf_search_jobs.map(|(client, client_jobs)| {
            cluster_client.leaf_list_terms(
                LeafListTermsRequest {
                    list_terms_request: Some(list_terms_request.clone()),
                    split_offsets: client_jobs.into_iter().map(|job| job.offsets).collect(),
                    index_uri: index_uri.to_string(),
                },
                client,
            )
        }))
        .await?;

    let failed_splits: Vec<_> = leaf_search_responses
        .iter()
        .flat_map(|leaf_search_response| &leaf_search_response.failed_splits)
        .collect();

    if !failed_splits.is_empty() {
        error!(failed_splits = ?failed_splits, "Leaf search response contains at least one failed split.");
        let errors: String = failed_splits
            .iter()
            .map(|splits| splits.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SearchError::Internal(errors));
    }

    // Merging is a cpu-bound task, but probably fast enough to not require
    // spawning it on a blocking thread.
    let merged_iter = leaf_search_responses
        .into_iter()
        .map(|leaf_search_response| leaf_search_response.terms)
        .kmerge()
        .dedup();
    let leaf_list_terms_response: Vec<Vec<u8>> = if let Some(limit) = list_terms_request.max_hits {
        merged_iter.take(limit as usize).collect()
    } else {
        merged_iter.collect()
    };

    debug!(leaf_list_terms_response = ?leaf_list_terms_response, "Merged leaf search response.");

    let elapsed = start_instant.elapsed();

    Ok(ListTermsResponse {
        num_hits: leaf_list_terms_response.len() as u64,
        terms: leaf_list_terms_response,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: Vec::new(),
    })
}

async fn assign_client_fetch_docs_jobs(
    partial_hits: &[PartialHit],
    split_metadatas: &[SplitMetadata],
    client_pool: &SearchJobPlacer,
) -> crate::Result<impl Iterator<Item = (SearchServiceClient, Vec<FetchDocsJob>)>> {
    let index_uids_and_split_offsets_map: HashMap<String, (IndexUid, SplitIdAndFooterOffsets)> =
        split_metadatas
            .iter()
            .map(|metadata| {
                (
                    metadata.split_id().to_string(),
                    (
                        metadata.index_uid.clone(),
                        extract_split_and_footer_offsets(metadata),
                    ),
                )
            })
            .collect();

    // Group the partial hits per split
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }

    let mut fetch_docs_req_jobs: Vec<FetchDocsJob> = Vec::new();
    for (split_id, partial_hits) in partial_hits_map {
        let (index_uid, offsets) = index_uids_and_split_offsets_map
            .get(&split_id)
            .ok_or_else(|| {
                crate::SearchError::Internal(format!(
                    "Received partial hit from an unknown split {split_id}"
                ))
            })?
            .clone();
        let fetch_docs_job = FetchDocsJob {
            index_uid: index_uid.clone(),
            offsets,
            partial_hits,
        };
        fetch_docs_req_jobs.push(fetch_docs_job);
    }

    let assigned_jobs = client_pool
        .assign_jobs(fetch_docs_req_jobs, &HashSet::new())
        .await?;

    Ok(assigned_jobs)
}

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> usize {
    // TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

/// Builds a list of [`LeafSearchRequest`], one per index, from a list of [`SearchJob`].
pub fn jobs_to_leaf_requests(
    request: &SearchRequest,
    search_indexes_metadatas: &IndexesMetasForLeafSearch,
    jobs: Vec<SearchJob>,
) -> crate::Result<Vec<LeafSearchRequest>> {
    let mut search_request_for_leaf = request.clone();
    search_request_for_leaf.start_offset = 0;
    search_request_for_leaf.max_hits += request.start_offset;
    let mut leaf_search_requests = Vec::new();
    // Group jobs by index uid.
    for (index_uid, job_group) in &jobs.into_iter().group_by(|job| job.index_uid.clone()) {
        let search_index_meta = search_indexes_metadatas.get(&index_uid).ok_or_else(|| {
            SearchError::Internal(format!(
                "Received search job for an unknown index {index_uid}. It should never happen."
            ))
        })?;
        let leaf_search_request = LeafSearchRequest {
            search_request: Some(search_request_for_leaf.clone()),
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
            doc_mapper: search_index_meta.doc_mapper_str.clone(),
            index_uri: search_index_meta.index_uri.to_string(),
        };
        leaf_search_requests.push(leaf_search_request);
    }
    Ok(leaf_search_requests)
}

/// Builds a list of [`FetchDocsRequest`], one per index, from a list of [`FetchDocsJob`].
pub fn jobs_to_fetch_docs_requests(
    snippet_request_opt: Option<SnippetRequest>,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    jobs: Vec<FetchDocsJob>,
) -> crate::Result<Vec<FetchDocsRequest>> {
    let mut fetch_docs_requests = Vec::new();
    // Group jobs by index uid.
    for (index_uid, job_group) in &jobs.into_iter().group_by(|job| job.index_uid.clone()) {
        let index_meta = indexes_metas_for_leaf_search
            .get(&index_uid)
            .ok_or_else(|| {
                SearchError::Internal(format!(
                    "Received search job for an unknown index {index_uid}"
                ))
            })?;
        let fetch_docs_jobs: Vec<FetchDocsJob> = job_group.collect();
        let partial_hits: Vec<PartialHit> = fetch_docs_jobs
            .iter()
            .flat_map(|fetch_doc_job| fetch_doc_job.partial_hits.iter().cloned())
            .collect();
        let split_offsets: Vec<SplitIdAndFooterOffsets> = fetch_docs_jobs
            .into_iter()
            .map(|fetch_doc_job| fetch_doc_job.into())
            .collect();
        let fetch_docs_req = FetchDocsRequest {
            partial_hits,
            split_offsets,
            index_uri: index_meta.index_uri.to_string(),
            snippet_request: snippet_request_opt.clone(),
            doc_mapper: index_meta.doc_mapper_str.clone(),
        };
        fetch_docs_requests.push(fetch_docs_req);
    }
    Ok(fetch_docs_requests)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};

    use quickwit_common::shared_consts::SCROLL_BATCH_LEN;
    use quickwit_config::{DocMapping, IndexingSettings, SearchSettings};
    use quickwit_indexing::MockSplitBuilder;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::search::{ScrollRequest, SortOrder, SortValue, SplitSearchError};
    use quickwit_query::query_ast::{qast_helper, qast_json_helper, query_ast_from_user_text};
    use tantivy::schema::{FAST, STORED, TEXT};

    use super::*;
    use crate::{searcher_pool_for_test, MockSearchService};

    #[track_caller]
    fn check_snippet_fields_validation(snippet_fields: &[String]) -> anyhow::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        let schema = schema_builder.build();
        validate_requested_snippet_fields(&schema, snippet_fields)
    }

    #[test]
    fn test_validate_requested_snippet_fields() {
        check_snippet_fields_validation(&["desc".to_string()]).unwrap();
        let field_not_stored_err =
            check_snippet_fields_validation(&["title".to_string()]).unwrap_err();
        assert_eq!(
            field_not_stored_err.to_string(),
            "The snippet field `title` must be stored."
        );
        let field_doesnotexist_err =
            check_snippet_fields_validation(&["doesnotexist".to_string()]).unwrap_err();
        assert_eq!(
            field_doesnotexist_err.to_string(),
            "The field does not exist: 'doesnotexist'"
        );
        let field_is_not_text_err =
            check_snippet_fields_validation(&["ip".to_string()]).unwrap_err();
        assert_eq!(
            field_is_not_text_err.to_string(),
            "The snippet field `ip` must be of type `Str`, got `IpAddr`."
        );
    }

    fn index_metadata_for_multi_indexes_test(index_id: &str, index_uri: &str) -> IndexMetadata {
        let index_uri = Uri::from_str(index_uri).unwrap();
        let doc_mapping_json = r#"{
            "mode": "lenient",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                }
            ],
            "timestamp_field": "timestamp",
            "store_source": true
        }"#;
        let doc_mapping = serde_json::from_str(doc_mapping_json).unwrap();
        let indexing_settings = IndexingSettings::default();
        let search_settings = SearchSettings {
            default_search_fields: vec!["body".to_string()],
        };
        IndexMetadata::new(IndexConfig {
            index_id: index_id.to_string(),
            index_uri,
            doc_mapping,
            indexing_settings,
            search_settings,
            retention_policy: Default::default(),
        })
    }

    #[test]
    fn test_validate_request_and_build_metadatas_ok() {
        let request_query_ast = qast_helper("body:test", &[]);
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: serde_json::to_string(&request_query_ast).unwrap(),
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let index_metadata = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_metadata_with_other_config =
            index_metadata_for_multi_indexes_test("test-index-2", "ram:///test-index-2");
        let mut index_metadata_no_timestamp =
            IndexMetadata::for_test("test-index-3", "ram:///test-index-3");
        index_metadata_no_timestamp
            .index_config
            .doc_mapping
            .timestamp_field = None;
        let (timestamp_field, query_ast, indexes_metas_for_leaf_req) =
            validate_request_and_build_metadatas(
                &[
                    index_metadata,
                    index_metadata_with_other_config,
                    index_metadata_no_timestamp,
                ],
                &search_request,
            )
            .unwrap();
        assert_eq!(timestamp_field, Some("timestamp".to_string()));
        assert_eq!(query_ast, request_query_ast);
        assert_eq!(indexes_metas_for_leaf_req.len(), 3);
    }

    #[test]
    fn test_validate_request_and_build_metadatas_fail_with_different_timestamps() {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let index_metadata_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let mut index_metadata_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
        let doc_mapping_json_2 = r#"{
            "mode": "lenient",
            "field_mappings": [
                {
                    "name": "timestamp-2",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text"
                }
            ],
            "timestamp_field": "timestamp-2",
            "store_source": true
        }"#;
        let doc_mapping_2: DocMapping = serde_json::from_str(doc_mapping_json_2).unwrap();
        index_metadata_2.index_config.doc_mapping = doc_mapping_2;
        index_metadata_2
            .index_config
            .search_settings
            .default_search_fields = Vec::new();
        let timestamp_field_different = validate_request_and_build_metadatas(
            &[index_metadata_1, index_metadata_2],
            &search_request,
        )
        .unwrap_err();
        assert_eq!(
            timestamp_field_different.to_string(),
            "The timestamp field (if present) must be the same for all indexes."
        );
    }

    #[test]
    fn test_validate_request_and_build_metadatas_fail_with_different_resolved_qast() {
        let qast = query_ast_from_user_text("test", None);
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: serde_json::to_string(&qast).unwrap(),
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let index_metadata_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let mut index_metadata_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
        index_metadata_2
            .index_config
            .search_settings
            .default_search_fields = vec!["owner".to_string()];
        let timestamp_field_different = validate_request_and_build_metadatas(
            &[index_metadata_1, index_metadata_2],
            &search_request,
        )
        .unwrap_err();
        assert_eq!(
            timestamp_field_different.to_string(),
            "Resolved query ASTs must be the same across indexes. Resolving queries with \
             different default fields are different between indexes is not supported."
        );
    }

    fn mock_partial_hit(
        split_id: &str,
        sort_value: u64,
        doc_id: u32,
    ) -> quickwit_proto::search::PartialHit {
        quickwit_proto::search::PartialHit {
            sort_value: Some(SortValue::U64(sort_value).into()),
            sort_value2: None,
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn get_doc_for_fetch_req(
        fetch_docs_req: quickwit_proto::search::FetchDocsRequest,
    ) -> Vec<quickwit_proto::search::LeafHit> {
        fetch_docs_req
            .partial_hits
            .into_iter()
            .map(|req| quickwit_proto::search::LeafHit {
                leaf_json: serde_json::to_string_pretty(&serde_json::json!({
                    "title": [req.doc_id.to_string()],
                    "body": ["test 1"],
                    "url": ["http://127.0.0.1/1"]
                }))
                .expect("Json serialization should not fail"),
                partial_hit: Some(req),
                leaf_snippet_json: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_root_search_offset_out_of_bounds_1085() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query: ListIndexesQuery| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 2, 2),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split2", 3, 1),
                        mock_partial_hit("split2", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 2, 2),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let searcher_context = SearcherContext::for_test();
        let search_response = root_search(
            &searcher_context,
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_sort_heteregeneous_field_ascending(
    ) -> anyhow::Result<()> {
        let mut search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        if let Some(sort_field) = search_request.sort_fields.get_mut(0) {
            sort_field.set_sort_order(SortOrder::Asc);
        }
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::U64(2u64).into()),
                            sort_value2: None,
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: None,
                            sort_value2: None,
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::I64(-1i64).into()),
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::I64(1i64).into()),
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: None,
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 2,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request.clone(),
            &metastore,
            &cluster_client,
        )
        .await?;

        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 5);
        assert_eq!(
            search_response.hits[2].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: Some(SortValue::I64(-1i64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[1].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::I64(1i64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[0].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::U64(2u64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[4].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: None,
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[3].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 2,
                sort_value: None,
                sort_value2: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_sort_heteregeneous_field_descending(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::U64(2u64).into()),
                            sort_value2: None,
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: None,
                            sort_value2: None,
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::I64(1i64).into()),
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: Some(SortValue::I64(-1i64).into()),
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                        quickwit_proto::search::PartialHit {
                            sort_value: None,
                            sort_value2: None,
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 2,
                        },
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request.clone(),
            &metastore,
            &cluster_client,
        )
        .await?;

        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 5);
        assert_eq!(
            search_response.hits[0].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::U64(2u64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[1].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::I64(1i64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[2].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: Some(SortValue::I64(-1i64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[3].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 2,
                sort_value: None,
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[4].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: None,
                sort_value2: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_other_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });

        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .times(1)
            .returning(
                |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        // requests from split 2 arrive here - simulate failure
                        num_hits: 0,
                        partial_hits: Vec::new(),
                        failed_splits: vec![SplitSearchError {
                            error: "mock_error".to_string(),
                            split_id: "split2".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                },
            );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_leaf_search()
            .times(2)
            .returning(
                |leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                    let split_ids: Vec<&str> = leaf_search_req
                        .split_offsets
                        .iter()
                        .map(|metadata| metadata.split_id.as_str())
                        .collect();
                    if split_ids == ["split1"] {
                        Ok(quickwit_proto::search::LeafSearchResponse {
                            num_hits: 2,
                            partial_hits: vec![
                                mock_partial_hit("split1", 3, 1),
                                mock_partial_hit("split1", 1, 3),
                            ],
                            failed_splits: Vec::new(),
                            num_attempted_splits: 1,
                            ..Default::default()
                        })
                    } else if split_ids == ["split2"] {
                        // RETRY REQUEST!
                        Ok(quickwit_proto::search::LeafSearchResponse {
                            num_hits: 1,
                            partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                            failed_splits: Vec::new(),
                            num_attempted_splits: 1,
                            ..Default::default()
                        })
                    } else {
                        panic!("unexpected request in test {split_ids:?}");
                    }
                },
            );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_all_nodes() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
            .return_once(|_| {
                // requests from split 2 arrive here - simulate failure.
                // a retry will be made on the second service.
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 0,
                    partial_hits: Vec::new(),
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split2".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service_1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split1")
            .return_once(|_| {
                // RETRY REQUEST from split1
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
            .return_once(|_| {
                // retry for split 2 arrive here, simulate success.
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service_2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split1")
            .return_once(|_| {
                // requests from split 1 arrive here - simulate failure, then success.
                Ok(quickwit_proto::search::LeafSearchResponse {
                    // requests from split 2 arrive here - simulate failure
                    num_hits: 0,
                    partial_hits: Vec::new(),
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });
        let mut first_call = true;
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().times(2).returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                // requests from split 2 arrive here - simulate failure, then success
                if first_call {
                    first_call = false;
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: 0,
                        partial_hits: Vec::new(),
                        failed_splits: vec![SplitSearchError {
                            error: "mock_error".to_string(),
                            split_id: "split1".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                } else {
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                }
            },
        );
        mock_search_service.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node_fails() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });

        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().times(2).returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 0,
                    partial_hits: Vec::new(),
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Err(SearchError::Internal("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await;
        assert!(search_response.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_one_splits_two_nodes_but_one_is_failing_for_split(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });
        // Service1 - broken node.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                // retry requests from split 1 arrive here
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - working node.
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 0,
                    partial_hits: Vec::new(),
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Err(SearchError::Internal("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_one_splits_two_nodes_but_one_is_failing_completely(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });

        // Service1 - working node.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Ok(quickwit_proto::search::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - broken node.
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                Err(SearchError::Internal("mockerr search".to_string()))
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Err(SearchError::Internal("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_queries() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split")
                .with_index_uid(&index_uid)
                .build()])
        });

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let searcher_context = SearcherContext::for_test();

        assert!(root_search(
            &searcher_context,
            quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index".to_string()],
                query_ast: qast_json_helper("invalid_field:\"test\"", &["body"]),
                max_hits: 10,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
        )
        .await
        .is_err());

        assert!(root_search(
            &searcher_context,
            quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index".to_string()],
                query_ast: qast_json_helper("test", &["invalid_field"]),
                max_hits: 10,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
        )
        .await
        .is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_aggregation() -> anyhow::Result<()> {
        let agg_req = r#"
            {
                "expensive_colors": {
                    "termss": {
                        "field": "color",
                        "order": {
                            "price_stats.max": "desc"
                        }
                    },
                    "aggs": {
                        "price_stats" : {
                            "stats": {
                                "field": "price"
                            }
                        }
                    }
                }
            }"#;

        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            aggregation_request: Some(agg_req.to_string()),
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid aggregation request: unknown variant `termss`, expected one of `range`, \
             `histogram`, `date_histogram`, `terms`, `avg`, `value_count`, `max`, `min`, `stats`, \
             `sum`, `percentiles` at line 18 column 13"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_request() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            start_offset: 20_000,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| Ok(vec![index_metadata.clone()]));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![MockSplitBuilder::new("split1")
                .with_index_uid(&index_uid)
                .build()])
        });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid argument: max value for start_offset is 10_000, but got 20000",
        );

        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 20_000,
            ..Default::default()
        };

        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid argument: max value for max_hits is 10_000, but got 20000",
        );

        Ok(())
    }

    #[test]
    fn test_extract_timestamp_range_from_ast() {
        use std::ops::Bound;

        use quickwit_query::JsonLiteral;

        let timestamp_field = "timestamp";

        let simple_range = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Excluded(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();

        // direct range
        let mut timestamp_range_extractor = ExtractTimestampRange {
            timestamp_field,
            start_timestamp: None,
            end_timestamp: None,
        };
        timestamp_range_extractor.visit(&simple_range).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618353941));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1620283879));

        // range inside a must bool query
        let bool_query_must = quickwit_query::query_ast::BoolQuery {
            must: vec![simple_range.clone()],
            ..Default::default()
        };
        timestamp_range_extractor.start_timestamp = None;
        timestamp_range_extractor.end_timestamp = None;
        timestamp_range_extractor
            .visit(&bool_query_must.into())
            .unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618353941));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1620283879));

        // range inside a should bool query
        let bool_query_should = quickwit_query::query_ast::BoolQuery {
            should: vec![simple_range.clone()],
            ..Default::default()
        };
        timestamp_range_extractor.start_timestamp = Some(123);
        timestamp_range_extractor.end_timestamp = None;
        timestamp_range_extractor
            .visit(&bool_query_should.into())
            .unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(123));
        assert_eq!(timestamp_range_extractor.end_timestamp, None);

        // start bound was already more restrictive
        timestamp_range_extractor.start_timestamp = Some(1618601297);
        timestamp_range_extractor.end_timestamp = Some(i64::MAX);
        timestamp_range_extractor.visit(&simple_range).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618601297));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1620283879));

        // end bound was already more restrictive
        timestamp_range_extractor.start_timestamp = Some(1);
        timestamp_range_extractor.end_timestamp = Some(1618601297);
        timestamp_range_extractor.visit(&simple_range).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618353941));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1618601297));

        // bounds are (start..end] instead of [start..end)
        let unusual_bounds = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Excluded(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Included(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();
        timestamp_range_extractor.start_timestamp = None;
        timestamp_range_extractor.end_timestamp = None;
        timestamp_range_extractor.visit(&unusual_bounds).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618353942));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1620283880));

        let wrong_field = quickwit_query::query_ast::RangeQuery {
            field: "other_field".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("2021-04-13T22:45:41Z".to_owned())),
            upper_bound: Bound::Excluded(JsonLiteral::String("2021-05-06T06:51:19Z".to_owned())),
        }
        .into();
        timestamp_range_extractor.start_timestamp = None;
        timestamp_range_extractor.end_timestamp = None;
        timestamp_range_extractor.visit(&wrong_field).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, None);
        assert_eq!(timestamp_range_extractor.end_timestamp, None);

        let high_precision = quickwit_query::query_ast::RangeQuery {
            field: timestamp_field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String(
                "2021-04-13T22:45:41.001Z".to_owned(),
            )),
            upper_bound: Bound::Excluded(JsonLiteral::String(
                "2021-05-06T06:51:19.001Z".to_owned(),
            )),
        }
        .into();

        // the upper bound should be rounded up as to includes documents from X.000 to X.001
        let mut timestamp_range_extractor = ExtractTimestampRange {
            timestamp_field,
            start_timestamp: None,
            end_timestamp: None,
        };
        timestamp_range_extractor.visit(&high_precision).unwrap();
        assert_eq!(timestamp_range_extractor.start_timestamp, Some(1618353941));
        assert_eq!(timestamp_range_extractor.end_timestamp, Some(1620283880));
    }

    fn create_search_resp(index_uri: &str, hit_range: Range<usize>) -> LeafSearchResponse {
        let (num_total_hits, split_id) = match index_uri {
            "ram:///test-index-1" => (TOTAL_NUM_HITS_INDEX_1, "split1"),
            "ram:///test-index-2" => (TOTAL_NUM_HITS_INDEX_2, "split2"),
            _ => panic!("unexpected index uri"),
        };
        let truncate_range = hit_range.start.min(num_total_hits)..hit_range.end.min(num_total_hits);
        quickwit_proto::search::LeafSearchResponse {
            num_hits: num_total_hits as u64,
            partial_hits: truncate_range
                .map(|doc_id| {
                    let sort_value = match index_uri {
                        "ram:///test-index-1" => u64::MAX - doc_id as u64,
                        "ram:///test-index-2" => (TOTAL_NUM_HITS_INDEX_2 - doc_id) as u64,
                        _ => panic!("unexpected index uri"),
                    };
                    mock_partial_hit(split_id, sort_value, doc_id as u32)
                })
                .collect(),
            num_attempted_splits: 1,
            ..Default::default()
        }
    }

    const TOTAL_NUM_HITS_INDEX_1: usize = 2_005;
    const TOTAL_NUM_HITS_INDEX_2: usize = 10;
    const MAX_HITS_PER_PAGE: usize = 93;

    #[tokio::test]
    async fn test_root_search_with_scroll() {
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_uid = index_metadata.index_uid.clone();
        let index_metadata_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
        let index_uid_2 = index_metadata_2.index_uid.clone();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move |_index_ids_query| {
                Ok(vec![index_metadata.clone(), index_metadata_2.clone()])
            });
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
                    .with_index_uid(&index_uid_2)
                    .build(),
            ])
        });
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().times(2).returning(
            |req: quickwit_proto::search::LeafSearchRequest| {
                let search_req: &SearchRequest = req.search_request.as_ref().unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                Ok(create_search_resp(
                    &req.index_uri,
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                ))
            },
        );
        mock_search_service.expect_leaf_search().times(2).returning(
            |req: quickwit_proto::search::LeafSearchRequest| {
                let search_req: &SearchRequest = req.search_request.as_ref().unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, 2 * SCROLL_BATCH_LEN);
                Ok(create_search_resp(
                    &req.index_uri,
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                ))
            },
        );
        mock_search_service.expect_leaf_search().times(2).returning(
            |req: quickwit_proto::search::LeafSearchRequest| {
                let search_req: &SearchRequest = req.search_request.as_ref().unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, 3 * SCROLL_BATCH_LEN);
                Ok(create_search_resp(
                    &req.index_uri,
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                ))
            },
        );
        let kv: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> = Default::default();
        let kv_clone = kv.clone();
        mock_search_service
            .expect_put_kv()
            .returning(move |put_kv_req| {
                kv_clone
                    .write()
                    .unwrap()
                    .insert(put_kv_req.key, put_kv_req.payload);
            });
        mock_search_service
            .expect_get_kv()
            .returning(move |get_kv_req| kv.read().unwrap().get(&get_kv_req.key).cloned());
        mock_search_service.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                assert!(fetch_docs_req.partial_hits.len() <= MAX_HITS_PER_PAGE);
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let searcher_context = SearcherContext::for_test();
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let mut count_seen_hits = 0;

        let mut scroll_id: String = {
            let search_request = quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index".to_string()],
                query_ast: qast_json_helper("test", &["body"]),
                max_hits: MAX_HITS_PER_PAGE as u64,
                scroll_ttl_secs: Some(60),
                ..Default::default()
            };
            let search_response = root_search(
                &searcher_context,
                search_request,
                &metastore,
                &cluster_client,
            )
            .await
            .unwrap();
            assert_eq!(
                search_response.num_hits,
                (TOTAL_NUM_HITS_INDEX_1 + TOTAL_NUM_HITS_INDEX_2) as u64
            );
            assert_eq!(search_response.hits.len(), MAX_HITS_PER_PAGE);
            for (i, hit) in search_response.hits.iter().enumerate() {
                assert_eq!(
                    hit.partial_hit.as_ref().unwrap(),
                    &mock_partial_hit("split1", u64::MAX - i as u64, i as u32)
                );
            }
            count_seen_hits += search_response.hits.len();
            search_response.scroll_id.unwrap()
        };
        for page in 1.. {
            let scroll_req = ScrollRequest {
                scroll_id,
                scroll_ttl_secs: Some(60),
            };
            let scroll_resp =
                crate::service::scroll(scroll_req, &cluster_client, &searcher_context)
                    .await
                    .unwrap();
            assert_eq!(
                scroll_resp.num_hits,
                (TOTAL_NUM_HITS_INDEX_1 + TOTAL_NUM_HITS_INDEX_2) as u64
            );
            for (i, hit) in scroll_resp.hits.iter().enumerate() {
                let doc = (page * MAX_HITS_PER_PAGE as u64) + i as u64;
                if doc < TOTAL_NUM_HITS_INDEX_1 as u64 {
                    assert_eq!(
                        hit.partial_hit.as_ref().unwrap(),
                        &mock_partial_hit("split1", u64::MAX - doc, doc as u32)
                    );
                } else {
                    // Docs from index 2 come after the ones from index 1.
                    let doc = doc - TOTAL_NUM_HITS_INDEX_1 as u64;
                    assert_eq!(
                        hit.partial_hit.as_ref().unwrap(),
                        &mock_partial_hit(
                            "split2",
                            TOTAL_NUM_HITS_INDEX_2 as u64 - doc,
                            doc as u32
                        )
                    );
                }
            }
            scroll_id = scroll_resp.scroll_id.unwrap();
            count_seen_hits += scroll_resp.hits.len();
            if scroll_resp.hits.is_empty() {
                break;
            }
        }

        assert_eq!(
            count_seen_hits,
            TOTAL_NUM_HITS_INDEX_1 + TOTAL_NUM_HITS_INDEX_2
        );
    }

    #[tokio::test]
    async fn test_root_search_multi_indices() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index-*".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        let index_metadata_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_uid_1 = index_metadata_1.index_uid.clone();
        let index_metadata_2 =
            index_metadata_for_multi_indexes_test("test-index-2", "ram:///test-index-2");
        let index_uid_2 = index_metadata_2.index_uid.clone();
        let index_metadata_3 =
            index_metadata_for_multi_indexes_test("test-index-3", "ram:///test-index-3");
        let index_uid_3 = index_metadata_3.index_uid.clone();
        metastore.expect_list_indexes_metadatas().return_once(
            move |index_ids_query: ListIndexesQuery| {
                match index_ids_query {
                    ListIndexesQuery::IndexIdPatterns(index_ids_query) => {
                        assert_eq!(index_ids_query, vec!["test-index-*".to_string()]);
                    }
                    ListIndexesQuery::All => {
                        panic!("Unexpected empty index_ids_query");
                    }
                }
                Ok(vec![index_metadata_1, index_metadata_2, index_metadata_3])
            },
        );
        metastore
            .expect_list_splits()
            .return_once(move |list_splits_query| {
                assert!(
                    list_splits_query.index_uids
                        == vec![
                            index_uid_1.clone(),
                            index_uid_2.clone(),
                            index_uid_3.clone()
                        ]
                );
                Ok(vec![
                    MockSplitBuilder::new("index-1-split-1")
                        .with_index_uid(&index_uid_1)
                        .build(),
                    MockSplitBuilder::new("index-1-split-2")
                        .with_index_uid(&index_uid_1)
                        .build(),
                    MockSplitBuilder::new("index-2-split-1")
                        .with_index_uid(&index_uid_2)
                        .build(),
                ])
            });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .times(2)
            .withf(|leaf_search_req| {
                (leaf_search_req.index_uri == "ram:///test-index-1"
                    && leaf_search_req.split_offsets.len() == 2)
                    || (leaf_search_req.index_uri == "ram:///test-index-2"
                        && leaf_search_req.split_offsets[0].split_id == "index-2-split-1")
            })
            .returning(
                |leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                    let partial_hits = leaf_search_req
                        .split_offsets
                        .iter()
                        .map(|split_offset| mock_partial_hit(&split_offset.split_id, 3, 1))
                        .collect_vec();
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: leaf_search_req.split_offsets.len() as u64,
                        partial_hits,
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                },
            );
        mock_search_service_1
            .expect_fetch_docs()
            .times(2)
            .withf(|fetch_docs_req: &FetchDocsRequest| {
                (fetch_docs_req.index_uri == "ram:///test-index-1"
                    && fetch_docs_req.partial_hits.len() == 2)
                    || (fetch_docs_req.index_uri == "ram:///test-index-2"
                        && fetch_docs_req.partial_hits[0].split_id == "index-2-split-1")
            })
            .returning(|fetch_docs_req| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service_1)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            &metastore,
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }
}
