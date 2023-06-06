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

use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_config::{build_doc_mapper, IndexConfig};
use quickwit_doc_mapper::{DocMapper, DYNAMIC_FIELD_NAME};
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResponse, Hit, LeafHit, LeafListTermsRequest, LeafListTermsResponse,
    LeafSearchRequest, LeafSearchResponse, ListTermsRequest, ListTermsResponse, PartialHit,
    SearchRequest, SearchResponse, SplitIdAndFooterOffsets,
};
use quickwit_query::query_ast::{
    BoolQuery, QueryAst, QueryAstVisitor, RangeQuery, TermQuery, TermSetQuery,
};
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::Collector;
use tantivy::schema::{FieldType, Schema};
use tantivy::TantivyError;
use tracing::{debug, error, info_span, instrument};

use crate::cluster_client::ClusterClient;
use crate::collector::{make_merge_collector, QuickwitAggregations};
use crate::find_trace_ids_collector::Span;
use crate::search_job_placer::Job;
use crate::service::SearcherContext;
use crate::{
    extract_split_and_footer_offsets, list_relevant_splits, SearchError, SearchJobPlacer,
    SearchServiceClient,
};

/// SearchJob to be assigned to search clients by the [`SearchJobPlacer`].
#[derive(Debug, Clone, PartialEq)]
pub struct SearchJob {
    cost: usize,
    offsets: SplitIdAndFooterOffsets,
}

impl SearchJob {
    #[cfg(test)]
    pub fn for_test(split_id: &str, cost: usize) -> SearchJob {
        SearchJob {
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

pub(crate) struct FetchDocsJob {
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

pub(crate) fn validate_request(
    doc_mapper: &dyn DocMapper,
    search_request: &SearchRequest,
) -> crate::Result<()> {
    let schema = doc_mapper.schema();

    validate_requested_snippet_fields(&schema, &search_request.snippet_fields)?;

    if let Some(sort_by_field) = &search_request.sort_by_field {
        validate_sort_by_field(sort_by_field, &schema)?;
    }

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

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip(search_request, cluster_client, search_job_placer, metastore))]
pub async fn root_search(
    searcher_context: &SearcherContext,
    mut search_request: SearchRequest,
    metastore: &dyn Metastore,
    cluster_client: &ClusterClient,
    search_job_placer: &SearchJobPlacer,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();

    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config = index_metadata.into_index_config();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::InternalError(format!("Failed to build doc mapper. Cause: {err}"))
        })?;

    validate_request(&*doc_mapper, &search_request)?;

    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let query_ast_resolved = query_ast.parse_user_query(doc_mapper.default_search_fields())?;

    if let Some(timestamp_field) = doc_mapper.timestamp_field_name() {
        refine_start_end_timestamp_from_ast(
            &query_ast_resolved,
            timestamp_field,
            &mut search_request.start_timestamp,
            &mut search_request.end_timestamp,
        );
    }

    // Validates the query by effectively building it against the current schema.
    doc_mapper.query(doc_mapper.schema(), &query_ast_resolved, true)?;

    search_request.query_ast = serde_json::to_string(&query_ast_resolved).map_err(|err| {
        SearchError::InternalError(format!("Failed to serialize query ast: Cause {err}"))
    })?;

    let doc_mapper_str = serde_json::to_string(&doc_mapper).map_err(|err| {
        SearchError::InternalError(format!("Failed to serialize doc mapper: Cause {err}"))
    })?;

    let split_metadatas: Vec<SplitMetadata> =
        list_relevant_splits(index_uid, &search_request, metastore).await?;

    let split_offsets_map: HashMap<String, SplitIdAndFooterOffsets> = split_metadatas
        .iter()
        .map(|metadata| {
            (
                metadata.split_id().to_string(),
                extract_split_and_footer_offsets(metadata),
            )
        })
        .collect();

    let index_uri = &index_config.index_uri;

    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();

    let assigned_leaf_search_jobs = search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let leaf_search_responses: Vec<LeafSearchResponse> =
        try_join_all(assigned_leaf_search_jobs.map(|(client, client_jobs)| {
            let leaf_request = jobs_to_leaf_request(
                &search_request,
                &doc_mapper_str,
                index_uri.as_ref(),
                client_jobs,
            );
            cluster_client.leaf_search(leaf_request, client)
        }))
        .await?;

    // Creates a collector which merges responses into one
    let merge_collector =
        make_merge_collector(&search_request, &searcher_context.get_aggregation_limits())?;
    let aggregations = merge_collector.aggregation.clone();

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
    .context("failed to merge fruits")?
    .map_err(|merge_error: TantivyError| {
        crate::SearchError::InternalError(format!("{merge_error}"))
    })?;
    debug!(leaf_search_response = ?leaf_search_response, "Merged leaf search response.");

    if !leaf_search_response.failed_splits.is_empty() {
        error!(failed_splits = ?leaf_search_response.failed_splits, "Leaf search response contains at least one failed split.");
        let errors: String = leaf_search_response
            .failed_splits
            .iter()
            .map(|splits| format!("{splits}"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SearchError::InternalError(errors));
    }

    let hit_order: HashMap<(String, u32, u32), usize> = leaf_search_response
        .partial_hits
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

    let client_fetch_docs_task: Vec<(SearchServiceClient, Vec<FetchDocsJob>)> =
        assign_client_fetch_doc_tasks(
            &leaf_search_response.partial_hits,
            &split_offsets_map,
            search_job_placer,
        )
        .await?;

    let fetch_docs_resp_futures =
        client_fetch_docs_task
            .into_iter()
            .map(|(client, fetch_docs_jobs)| {
                let partial_hits: Vec<PartialHit> = fetch_docs_jobs
                    .iter()
                    .flat_map(|fetch_doc_job| fetch_doc_job.partial_hits.iter().cloned())
                    .collect();
                let split_offsets: Vec<SplitIdAndFooterOffsets> = fetch_docs_jobs
                    .into_iter()
                    .map(|fetch_doc_job| fetch_doc_job.into())
                    .collect();

                let search_request_opt = if search_request.snippet_fields.is_empty() {
                    None
                } else {
                    Some(search_request.clone())
                };
                let fetch_docs_req = FetchDocsRequest {
                    partial_hits,
                    index_id: search_request.index_id.to_string(),
                    split_offsets,
                    index_uri: index_uri.to_string(),
                    search_request: search_request_opt,
                    doc_mapper: doc_mapper_str.clone(),
                };
                cluster_client.fetch_docs(fetch_docs_req, client)
            });

    let fetch_docs_resps: Vec<FetchDocsResponse> = try_join_all(fetch_docs_resp_futures).await?;

    // Merge the fetched docs.
    let leaf_hits = fetch_docs_resps
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
    let hits = hits_with_position
        .into_iter()
        .map(|(_position, hit)| hit)
        .collect();

    let elapsed = start_instant.elapsed();

    let aggregation: Option<String> = finalize_aggregation(
        leaf_search_response.intermediate_aggregation_result,
        aggregations,
        searcher_context,
    )?;

    Ok(SearchResponse {
        aggregation,
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: Vec::new(),
    })
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
        let Some(lower_bound) = tantivy::DateTime::interpret_json(lower_bound) else { return };
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
        let Some(upper_bound_timestamp) = tantivy::DateTime::interpret_json(upper_bound) else { return };
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

pub fn finalize_aggregation(
    intermediate_aggregation_result: Option<Vec<u8>>,
    aggregations: Option<QuickwitAggregations>,
    searcher_context: &SearcherContext,
) -> crate::Result<Option<String>> {
    let aggregation = if let Some(intermediate_aggregation_result) = intermediate_aggregation_result
    {
        match aggregations.expect(
            "Aggregation should be present since we are processing an intermediate aggregation \
             result.",
        ) {
            QuickwitAggregations::FindTraceIdsAggregation(_) => {
                // The merge collector has already merged the intermediate results.
                let aggs: Vec<Span> =
                    postcard::from_bytes(intermediate_aggregation_result.as_slice())?;
                Some(serde_json::to_string(&aggs)?)
            }
            QuickwitAggregations::TantivyAggregations(aggregations) => {
                let res: IntermediateAggregationResults =
                    postcard::from_bytes(intermediate_aggregation_result.as_slice())?;
                let res: AggregationResults = res
                    .into_final_result(aggregations, &searcher_context.get_aggregation_limits())?;
                Some(serde_json::to_string(&res)?)
            }
        }
    } else {
        None
    };
    Ok(aggregation)
}

/// Performs a distributed list terms.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
/// this is much simpler than `root_search` as it doesn't need to get actual docs.
#[instrument(skip(list_terms_request, cluster_client, search_job_placer, metastore))]
pub async fn root_list_terms(
    list_terms_request: &ListTermsRequest,
    metastore: &dyn Metastore,
    cluster_client: &ClusterClient,
    search_job_placer: &SearchJobPlacer,
) -> crate::Result<ListTermsResponse> {
    let start_instant = tokio::time::Instant::now();

    let index_metadata = metastore
        .index_metadata(&list_terms_request.index_id)
        .await?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config: IndexConfig = index_metadata.into_index_config();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::InternalError(format!("Failed to build doc mapper. Cause: {err}"))
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
    let assigned_leaf_search_jobs = search_job_placer
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
        return Err(SearchError::InternalError(errors));
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

async fn assign_client_fetch_doc_tasks(
    partial_hits: &[PartialHit],
    split_offsets_map: &HashMap<String, SplitIdAndFooterOffsets>,
    client_pool: &SearchJobPlacer,
) -> crate::Result<Vec<(SearchServiceClient, Vec<FetchDocsJob>)>> {
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
        let offsets = split_offsets_map
            .get(&split_id)
            .ok_or_else(|| {
                crate::SearchError::InternalError(format!(
                    "Received partial hit from an Unknown split {split_id}"
                ))
            })?
            .clone();
        let fetch_docs_job = FetchDocsJob {
            offsets,
            partial_hits,
        };
        fetch_docs_req_jobs.push(fetch_docs_job);
    }

    let assigned_jobs: Vec<(SearchServiceClient, Vec<FetchDocsJob>)> = client_pool
        .assign_jobs(fetch_docs_req_jobs, &HashSet::new())
        .await?
        .collect();
    Ok(assigned_jobs)
}

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> usize {
    // TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

/// Builds a [`LeafSearchRequest`] from a list of [`SearchJob`].
pub fn jobs_to_leaf_request(
    request: &SearchRequest,
    doc_mapper_str: &str,
    index_uri: &str, // TODO make Uri
    jobs: Vec<SearchJob>,
) -> LeafSearchRequest {
    let mut request_with_offset_0 = request.clone();
    request_with_offset_0.start_offset = 0;
    request_with_offset_0.max_hits += request.start_offset;
    LeafSearchRequest {
        search_request: Some(request_with_offset_0),
        split_offsets: jobs.into_iter().map(|job| job.offsets).collect(),
        doc_mapper: doc_mapper_str.to_string(),
        index_uri: index_uri.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_config::SearcherConfig;
    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::{qast_helper, SortOrder, SortValue, SplitSearchError};
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

    fn mock_partial_hit(
        split_id: &str,
        sort_value: u64,
        doc_id: u32,
    ) -> quickwit_proto::PartialHit {
        quickwit_proto::PartialHit {
            sort_value: Some(SortValue::U64(sort_value)),
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn get_doc_for_fetch_req(
        fetch_docs_req: quickwit_proto::FetchDocsRequest,
    ) -> Vec<quickwit_proto::LeafHit> {
        fetch_docs_req
            .partial_hits
            .into_iter()
            .map(|req| quickwit_proto::LeafHit {
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
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &Arc::new(SearcherContext::new(SearcherConfig::default())),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
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
        let mut search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        search_request.set_sort_order(SortOrder::Asc);
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::U64(2u64)),
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: None,
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::I64(-1i64)),
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::I64(1i64)),
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: None,
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &SearcherContext::new(SearcherConfig::default()),
            search_request.clone(),
            &metastore,
            &cluster_client,
            &search_job_placer,
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
                sort_value: Some(SortValue::I64(-1i64)),
            }
        );
        assert_eq!(
            search_response.hits[1].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::I64(1i64)),
            }
        );
        assert_eq!(
            search_response.hits[0].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::U64(2u64)),
            }
        );
        assert_eq!(
            search_response.hits[4].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: None,
            }
        );
        assert_eq!(
            search_response.hits[3].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 2,
                sort_value: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_sort_heteregeneous_field_descending(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::U64(2u64)),
                            split_id: "split1".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: None,
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 3,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::I64(1i64)),
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 0,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: Some(SortValue::I64(-1i64)),
                            split_id: "split2".to_string(),
                            segment_ord: 0,
                            doc_id: 1,
                        },
                        quickwit_proto::PartialHit {
                            sort_value: None,
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &SearcherContext::new(SearcherConfig::default()),
            search_request.clone(),
            &metastore,
            &cluster_client,
            &search_job_placer,
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
                sort_value: Some(SortValue::U64(2u64)),
            }
        );
        assert_eq!(
            search_response.hits[1].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::I64(1i64)),
            }
        );
        assert_eq!(
            search_response.hits[2].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: Some(SortValue::I64(-1i64)),
            }
        );
        assert_eq!(
            search_response.hits[3].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split2".to_string(),
                segment_ord: 0,
                doc_id: 2,
                sort_value: None,
            }
        );
        assert_eq!(
            search_response.hits[4].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_other_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));

        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .times(1)
            .returning(|_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            });
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_leaf_search()
            .times(2)
            .returning(|leaf_search_req: quickwit_proto::LeafSearchRequest| {
                let split_ids: Vec<&str> = leaf_search_req
                    .split_offsets
                    .iter()
                    .map(|metadata| metadata.split_id.as_str())
                    .collect();
                if split_ids == ["split1"] {
                    Ok(quickwit_proto::LeafSearchResponse {
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
                    Ok(quickwit_proto::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                } else {
                    panic!("unexpected request in test {split_ids:?}");
                }
            });
        mock_search_service_2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_all_nodes() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1"), mock_split("split2")]));
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
            .return_once(|_| {
                println!("request from service1 split2?");
                // requests from split 2 arrive here - simulate failure.
                // a retry will be made on the second service.
                Ok(quickwit_proto::LeafSearchResponse {
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
                println!("request from service1 split1?");
                // RETRY REQUEST from split1
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
            .return_once(|_| {
                println!("request from service2 split2?");
                // retry for split 2 arrive here, simulate success.
                Ok(quickwit_proto::LeafSearchResponse {
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
                println!("request from service2 split1?");
                // requests from split 1 arrive here - simulate failure, then success.
                Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));
        let mut first_call = true;
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().times(2).returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // requests from split 2 arrive here - simulate failure, then success
                if first_call {
                    first_call = false;
                    Ok(quickwit_proto::LeafSearchResponse {
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
                    Ok(quickwit_proto::LeafSearchResponse {
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
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node_fails() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));

        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().times(2).returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await;
        assert!(search_response.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_one_splits_two_nodes_but_one_is_failing_for_split(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));
        // Service1 - broken node.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // retry requests from split 1 arrive here
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - working node.
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
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
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));

        // Service1 - working node.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - broken node.
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Err(SearchError::InternalError("mockerr search".to_string()))
            },
        );
        mock_search_service_2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
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
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split")]));

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        assert!(root_search(
            &SearcherContext::new(SearcherConfig::default()),
            quickwit_proto::SearchRequest {
                index_id: "test-index".to_string(),
                query_ast: qast_helper("invalid_field:\"test\"", &["body"]),
                max_hits: 10,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await
        .is_err());

        assert!(root_search(
            &SearcherContext::new(SearcherConfig::default()),
            quickwit_proto::SearchRequest {
                index_id: "test-index".to_string(),
                query_ast: qast_helper("test", &["invalid_field"]),
                max_hits: 10,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
            &search_job_placer,
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

        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            aggregation_request: Some(agg_req.to_string()),
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid aggregation request: no variant of enum AggregationVariants found in \
             flattened data at line 17 column 17"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_request() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 10,
            start_offset: 20_000,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_splits()
            .returning(|_filter| Ok(vec![mock_split("split1")]));
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
        )
        .await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid argument: max value for start_offset is 10_000, but got 20000",
        );

        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_helper("test", &["body"]),
            max_hits: 20_000,
            ..Default::default()
        };

        let search_response = root_search(
            &SearcherContext::new(SearcherConfig::default()),
            search_request,
            &metastore,
            &cluster_client,
            &search_job_placer,
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
}
