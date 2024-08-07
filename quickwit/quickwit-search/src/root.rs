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

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::pretty::PrettySample;
use quickwit_common::shared_consts;
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_doc_mapper::DYNAMIC_FIELD_NAME;
use quickwit_metastore::{IndexMetadata, ListIndexesMetadataResponseExt, SplitMetadata};
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, Hit, LeafHit, LeafRequestRef, LeafSearchRequest,
    LeafSearchResponse, PartialHit, SearchPlanResponse, SearchRequest, SearchResponse,
    SnippetRequest, SortDatetimeFormat, SortField, SortValue, SplitIdAndFooterOffsets,
};
use quickwit_proto::types::{IndexUid, SplitId};
use quickwit_query::query_ast::{
    BoolQuery, QueryAst, QueryAstVisitor, RangeQuery, TermQuery, TermSetQuery,
};
use serde::{Deserialize, Serialize};
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::Collector;
use tantivy::schema::{FieldEntry, FieldType, Schema};
use tantivy::TantivyError;
use tracing::{debug, error, info, info_span, instrument};

use crate::cluster_client::ClusterClient;
use crate::collector::{make_merge_collector, QuickwitAggregations};
use crate::find_trace_ids_collector::Span;
use crate::scroll_context::{ScrollContext, ScrollKeyAndStartOffset};
use crate::search_job_placer::{group_by, group_jobs_by_index_id, Job};
use crate::search_response_rest::StorageRequestCount;
use crate::service::SearcherContext;
use crate::{
    extract_split_and_footer_offsets, list_relevant_splits, SearchError, SearchJobPlacer,
    SearchPlanResponseRest, SearchServiceClient,
};

/// Maximum accepted scroll TTL.
fn max_scroll_ttl() -> Duration {
    static MAX_SCROLL_TTL_LOCK: OnceLock<Duration> = OnceLock::new();
    *MAX_SCROLL_TTL_LOCK.get_or_init(|| {
        let split_deletion_grace_period = shared_consts::split_deletion_grace_period();
        assert!(
            split_deletion_grace_period >= shared_consts::MINIMUM_DELETION_GRACE_PERIOD,
            "The split deletion grace period is too short ({split_deletion_grace_period:?}). This \
             should not happen."
        );
        // We remove an extra margin of 2minutes from the split deletion grace period.
        split_deletion_grace_period - Duration::from_secs(60 * 2)
    })
}

const SORT_DOC_FIELD_NAMES: &[&str] = &["_shard_doc", "_doc"];

/// SearchJob to be assigned to search clients by the [`SearchJobPlacer`].
#[derive(Debug, Clone, PartialEq)]
pub struct SearchJob {
    /// The index UID.
    pub index_uid: IndexUid,
    cost: usize,
    /// The split ID and footer offsets of the split.
    pub offsets: SplitIdAndFooterOffsets,
}

impl SearchJob {
    #[cfg(test)]
    pub fn for_test(split_id: &str, cost: usize) -> SearchJob {
        use std::str::FromStr;
        SearchJob {
            index_uid: IndexUid::from_str("test-index:00000000000000000000000000").unwrap(),
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

#[derive(Debug)]
struct RequestMetadata {
    timestamp_field_opt: Option<String>,
    query_ast_resolved: QueryAst,
    indexes_meta_for_leaf_search: IndexesMetasForLeafSearch,
    sort_fields_is_datetime: HashMap<String, bool>,
}

/// Validates request against each index's doc mapper and ensures that:
/// - timestamp fields (if any) are equal across indexes.
/// - resolved query ASTs are the same across indexes.
/// - if a sort field is of type datetime, it must be a datetime field on all indexes. This
///   constraint come from the need to support datetime formatting on sort values.
/// Returns the timestamp field, the resolved query AST and the indexes metadatas
/// needed for leaf search requests.
/// Note: the requirements on timestamp fields and resolved query ASTs can be lifted
/// but it adds complexity that does not seem needed right now.
fn validate_request_and_build_metadata(
    indexes_metadata: &[IndexMetadata],
    search_request: &SearchRequest,
) -> crate::Result<RequestMetadata> {
    validate_sort_by_fields_and_search_after(
        &search_request.sort_fields,
        &search_request.search_after,
    )?;
    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let mut indexes_meta_for_leaf_search: HashMap<IndexUid, IndexMetasForLeafSearch> =
        HashMap::new();
    let mut query_ast_resolved_opt: Option<QueryAst> = None;
    let mut timestamp_field_opt: Option<String> = None;
    let mut sort_fields_is_datetime: HashMap<String, bool> = HashMap::new();

    for index_metadata in indexes_metadata {
        let doc_mapper = build_doc_mapper(
            &index_metadata.index_config.doc_mapping,
            &index_metadata.index_config.search_settings,
        )
        .map_err(|err| {
            SearchError::Internal(format!("failed to build doc mapper. cause: {err}"))
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
                    "resolved query ASTs must be the same across indexes. resolving queries with \
                     different default fields are different between indexes is not supported"
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
                        "the timestamp field (if present) must be the same for all indexes"
                            .to_string(),
                    ));
                }
                None => {
                    timestamp_field_opt = Some(timestamp_field_for_index.to_string());
                }
                _ => {}
            }
        }

        // Validate request against the current index schema.
        let schema = doc_mapper.schema();
        validate_request(&schema, &doc_mapper.timestamp_field_name(), search_request)?;

        validate_sort_field_types(
            &schema,
            &search_request.sort_fields,
            &mut sort_fields_is_datetime,
        )?;

        // Validates the query by effectively building it against the current schema.
        doc_mapper.query(doc_mapper.schema(), &query_ast_resolved_for_index, true)?;

        let index_metadata_for_leaf_search = IndexMetasForLeafSearch {
            index_uri: index_metadata.index_uri().clone(),
            doc_mapper_str: serde_json::to_string(&doc_mapper).map_err(|err| {
                SearchError::Internal(format!("failed to serialize doc mapper. cause: {err}"))
            })?,
        };
        indexes_meta_for_leaf_search.insert(
            index_metadata.index_uid.clone(),
            index_metadata_for_leaf_search,
        );
    }

    let query_ast_resolved = query_ast_resolved_opt.ok_or_else(|| {
        SearchError::Internal(
            "resolved query AST must be present. this should never happen".to_string(),
        )
    })?;

    Ok(RequestMetadata {
        timestamp_field_opt,
        query_ast_resolved,
        indexes_meta_for_leaf_search,
        sort_fields_is_datetime,
    })
}

/// Validate sort field types.
fn validate_sort_field_types(
    schema: &Schema,
    sort_fields: &[SortField],
    sort_field_is_datetime: &mut HashMap<String, bool>,
) -> crate::Result<()> {
    for sort_field in sort_fields.iter() {
        if let Some(sort_field_entry) = get_sort_by_field_entry(&sort_field.field_name, schema)? {
            validate_sort_by_field_type(
                sort_field_entry,
                sort_field.sort_datetime_format.is_some(),
            )?;
            // If sort field type is a date, ensure it's true for all indexes.
            if let Some(is_datetime) = sort_field_is_datetime.get(&sort_field.field_name) {
                if *is_datetime != sort_field_entry.field_type().is_date() {
                    return Err(SearchError::InvalidQuery(format!(
                        "sort datetime field `{}` must be of type datetime on all indexes",
                        sort_field_entry.name(),
                    )));
                }
            } else {
                sort_field_is_datetime.insert(
                    sort_field.field_name.to_string(),
                    sort_field_entry.field_type().is_date(),
                );
            }
        } else {
            sort_field_is_datetime.insert(sort_field.field_name.to_string(), false);
        }
    }
    Ok(())
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
                        "the snippet field `{}` must be stored",
                        field_name
                    ));
                }
            }
            other => {
                return Err(anyhow::anyhow!(
                    "the snippet field `{}` must be of type `Str`, got `{}`",
                    field_name,
                    other.value_type().name()
                ))
            }
        }
    }
    Ok(())
}

fn simplify_search_request_for_scroll_api(req: &SearchRequest) -> crate::Result<SearchRequest> {
    if req.search_after.is_some() {
        return Err(SearchError::InvalidArgument(
            "search_after cannot be used in a scroll context".to_string(),
        ));
    }

    // We do not mutate
    Ok(SearchRequest {
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
        search_after: None,
        // request is simplified after initial query, and we cache the hit count, so we don't need
        // to recompute it afterward.
        count_hits: quickwit_proto::search::CountHits::Underestimate as i32,
    })
}

/// Validates sort fields and search after values.
/// - validate sort fields length.
/// - search after values must be set for all sort fields.
fn validate_sort_by_fields_and_search_after(
    sort_fields: &[SortField],
    search_after: &Option<PartialHit>,
) -> crate::Result<()> {
    if sort_fields.is_empty() {
        return Ok(());
    }
    if sort_fields.len() > 2 {
        return Err(SearchError::InvalidArgument(format!(
            "sort by field must be up to 2 fields, got {}",
            sort_fields.len()
        )));
    }
    let Some(search_after_partial_hit) = search_after.as_ref() else {
        return Ok(());
    };

    let sort_fields_without_doc_count = sort_fields
        .iter()
        .filter(|sort_field| !SORT_DOC_FIELD_NAMES.contains(&sort_field.field_name.as_str()))
        .count();
    let has_doc_sort_field = sort_fields_without_doc_count != sort_fields.len();
    if has_doc_sort_field && search_after_partial_hit.split_id.is_empty() {
        return Err(SearchError::InvalidArgument(
            "search_after with a sort field `_doc` must define a split ID, segment ID and doc ID \
             values"
                .to_string(),
        ));
    }

    let mut search_after_sort_value_count = 0;
    // TODO: we could validate if the search after sort value types of consistent with the sort
    // field types.
    if let Some(sort_by_value) = search_after_partial_hit.sort_value.as_ref() {
        sort_by_value.sort_value.context("sort value must be set")?;
        search_after_sort_value_count += 1;
    }
    if let Some(sort_by_value_2) = search_after_partial_hit.sort_value2.as_ref() {
        sort_by_value_2
            .sort_value
            .context("sort value must be set")?;
        search_after_sort_value_count += 1;
    }
    if search_after_sort_value_count != sort_fields_without_doc_count {
        return Err(SearchError::InvalidArgument(format!(
            "`search_after` must have the same number of sort values as sort by fields {:?}",
            sort_fields
                .iter()
                .map(|sort_field| &sort_field.field_name)
                .collect_vec()
        )));
    }
    Ok(())
}

fn get_sort_by_field_entry<'a>(
    field_name: &str,
    schema: &'a Schema,
) -> crate::Result<Option<&'a FieldEntry>> {
    if "_score" == field_name || SORT_DOC_FIELD_NAMES.contains(&field_name) {
        return Ok(None);
    }
    let dynamic_field_opt = schema.get_field(DYNAMIC_FIELD_NAME).ok();
    let (sort_by_field, _json_path) = schema
        .find_field_with_default(field_name, dynamic_field_opt)
        .ok_or_else(|| {
            SearchError::InvalidArgument(format!("unknown field used in `sort by`: {field_name}"))
        })?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);
    Ok(Some(sort_by_field_entry))
}

/// Validates sort field type.
fn validate_sort_by_field_type(
    sort_by_field_entry: &FieldEntry,
    has_timestamp_format: bool,
) -> crate::Result<()> {
    let field_name = sort_by_field_entry.name();
    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        return Err(SearchError::InvalidArgument(format!(
            "sort by field on type text is currently not supported `{field_name}`"
        )));
    }
    if !sort_by_field_entry.is_fast() {
        return Err(SearchError::InvalidArgument(format!(
            "sort by field must be a fast field, please add the fast property to your field \
             `{field_name}`",
        )));
    }
    if has_timestamp_format && !sort_by_field_entry.field_type().is_date() {
        return Err(SearchError::InvalidArgument(format!(
            "sort by field with a timestamp format must be a datetime field and the field \
             `{field_name}` is not",
        )));
    }
    Ok(())
}

fn validate_request(
    schema: &Schema,
    timestamp_field_name: &Option<&str>,
    search_request: &SearchRequest,
) -> crate::Result<()> {
    if timestamp_field_name.is_none()
        && (search_request.start_timestamp.is_some() || search_request.end_timestamp.is_some())
    {
        return Err(SearchError::InvalidQuery(format!(
            "the timestamp field is not set in index: {:?} definition but start-timestamp or \
             end-timestamp are set in the query",
            search_request.index_id_patterns
        )));
    }

    validate_requested_snippet_fields(schema, &search_request.snippet_fields)?;

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
    let max_scroll_ttl = max_scroll_ttl();
    if scroll_ttl > max_scroll_ttl {
        return Err(SearchError::InvalidArgument(format!(
            "Quickwit only supports scroll TTL period up to {} secs",
            max_scroll_ttl.as_secs()
        )));
    }
    Ok(Some(scroll_ttl))
}

#[instrument(level = "debug", skip_all)]
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
        search_request.max_hits = search_request
            .max_hits
            .max(shared_consts::SCROLL_BATCH_LEN as u64);
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
        let last_hit = leaf_search_resp
            .partial_hits
            .last()
            .cloned()
            .unwrap_or_default();

        let scroll_context_search_request =
            simplify_search_request_for_scroll_api(&search_request)?;
        let mut scroll_ctx = ScrollContext {
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
                last_hit.clone(),
            )
            .next_page(leaf_search_resp.partial_hits.len() as u64, last_hit);

        scroll_ctx.clear_cache_if_unneeded();
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

/// Check if the request is a count request without any filters, so we can just return the split
/// metadata count.
///
/// This is done by exclusion, so we will need to keep it up to date if fields are added.
pub fn is_metadata_count_request(request: &SearchRequest) -> bool {
    let query_ast: QueryAst = serde_json::from_str(&request.query_ast).unwrap();
    is_metadata_count_request_with_ast(&query_ast, request)
}

/// Check if the request is a count request without any filters, so we can just return the split
/// metadata count.
///
/// This is done by exclusion, so we will need to keep it up to date if fields are added.
///
/// The passed query_ast should match the serialized on in request.
pub fn is_metadata_count_request_with_ast(query_ast: &QueryAst, request: &SearchRequest) -> bool {
    if query_ast != &QueryAst::MatchAll {
        return false;
    }
    if request.max_hits != 0 {
        return false;
    }

    // If the start and end timestamp encompass the whole split, it is still a count query.
    // We remove this currently on the leaf level, but not yet on the root level.
    // There's a small advantage when we would do this on the root level, since we have the
    // counts available on the split. On the leaf it is currently required to open the split
    // to get the count.
    if request.start_timestamp.is_some() || request.end_timestamp.is_some() {
        return false;
    }
    if request.aggregation_request.is_some() || !request.snippet_fields.is_empty() {
        return false;
    }
    true
}

/// Get a leaf search response that returns the num_docs of the split
pub fn get_count_from_metadata(split_metadatas: &[SplitMetadata]) -> Vec<LeafSearchResponse> {
    split_metadatas
        .iter()
        .map(|metadata| LeafSearchResponse {
            num_hits: metadata.num_docs as u64,
            partial_hits: Vec::new(),
            failed_splits: Vec::new(),
            num_attempted_splits: 1,
            intermediate_aggregation_result: None,
        })
        .collect()
}

#[instrument(level = "debug", skip_all)]
pub(crate) async fn search_partial_hits_phase(
    searcher_context: &SearcherContext,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    search_request: &SearchRequest,
    split_metadatas: &[SplitMetadata],
    cluster_client: &ClusterClient,
) -> crate::Result<LeafSearchResponse> {
    let leaf_search_responses: Vec<LeafSearchResponse> =
        if is_metadata_count_request(search_request) {
            get_count_from_metadata(split_metadatas)
        } else {
            let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
            let assigned_leaf_search_jobs = cluster_client
                .search_job_placer
                .assign_jobs(jobs, &HashSet::default())
                .await?;
            let mut leaf_request_tasks = Vec::new();
            for (client, client_jobs) in assigned_leaf_search_jobs {
                let leaf_request = jobs_to_leaf_request(
                    search_request,
                    indexes_metas_for_leaf_search,
                    client_jobs,
                )?;
                leaf_request_tasks.push(cluster_client.leaf_search(leaf_request, client.clone()));
            }
            try_join_all(leaf_request_tasks).await?
        };

    // Creates a collector which merges responses into one
    let merge_collector = make_merge_collector(
        search_request,
        &searcher_context.create_new_aggregation_limits(),
    )?;

    // Merging is a cpu-bound task.
    // It should be executed by Tokio's blocking threads.

    // Wrap into result for merge_fruits
    let leaf_search_responses: Vec<tantivy::Result<LeafSearchResponse>> =
        leaf_search_responses.into_iter().map(Ok).collect_vec();
    let span = info_span!("merge_fruits");
    let leaf_search_response = crate::search_thread_pool()
        .run_cpu_intensive(move || {
            let _span_guard = span.enter();
            merge_collector.merge_fruits(leaf_search_responses)
        })
        .await
        .context("failed to merge leaf search responses")?
        .map_err(|error: TantivyError| crate::SearchError::Internal(error.to_string()))?;
    debug!(
        num_hits = leaf_search_response.num_hits,
        failed_splits = ?leaf_search_response.failed_splits,
        num_attempted_splits = leaf_search_response.num_attempted_splits,
        has_intermediate_aggregation_result = leaf_search_response.intermediate_aggregation_result.is_some(),
        "Merged leaf search response."
    );
    if !leaf_search_response.failed_splits.is_empty() {
        error!(failed_splits = ?leaf_search_response.failed_splits, "leaf search response contains at least one failed split");
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

#[instrument(skip_all, fields(partial_hits_num=partial_hits.len()))]
pub(crate) async fn fetch_docs_phase(
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    partial_hits: &[PartialHit],
    split_metadatas: &[SplitMetadata],
    search_request: &SearchRequest,
    cluster_client: &ClusterClient,
) -> crate::Result<Vec<Hit>> {
    let snippet_request: Option<SnippetRequest> = get_snippet_request(search_request);
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
            snippet_request.clone(),
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

    // Build map of Split ID > index ID to add the index ID to the hits.
    // Used for ES compatibility.
    let split_id_to_index_id_map: HashMap<&SplitId, &str> = split_metadatas
        .iter()
        .map(|split_metadata| {
            (
                &split_metadata.split_id,
                split_metadata.index_uid.index_id.as_str(),
            )
        })
        .collect();
    let mut sort_field_iter = search_request.sort_fields.iter();
    let sort_field_1_datetime_format_opt: Option<SortDatetimeFormat> =
        get_sort_field_datetime_format(sort_field_iter.next())?;
    let sort_field_2_datetime_format_opt: Option<SortDatetimeFormat> =
        get_sort_field_datetime_format(sort_field_iter.next())?;
    let mut hits_with_position: Vec<(usize, Hit)> = leaf_hits
        .map(|leaf_hit| {
            build_hit_with_position(
                leaf_hit,
                &split_id_to_index_id_map,
                &hit_order,
                &sort_field_1_datetime_format_opt,
                &sort_field_2_datetime_format_opt,
            )
        })
        .try_collect()?;

    hits_with_position.sort_by_key(|(position, _)| *position);
    let hits: Vec<Hit> = hits_with_position
        .into_iter()
        .map(|(_position, hit)| hit)
        .collect();

    Ok(hits)
}

fn build_hit_with_position(
    mut leaf_hit: LeafHit,
    split_id_to_index_id_map: &HashMap<&SplitId, &str>,
    hit_order: &HashMap<(String, u32, u32), usize>,
    sort_field_1_datetime_format_opt: &Option<SortDatetimeFormat>,
    sort_field_2_datetime_format_opt: &Option<SortDatetimeFormat>,
) -> crate::Result<(usize, Hit)> {
    let partial_hit_ref = leaf_hit
        .partial_hit
        .as_mut()
        .expect("partial hit must be present");
    let key = (
        partial_hit_ref.split_id.clone(),
        partial_hit_ref.segment_ord,
        partial_hit_ref.doc_id,
    );
    let sort_value_opt = partial_hit_ref
        .sort_value
        .as_mut()
        .and_then(|sort_field| sort_field.sort_value.as_mut());
    if let Some(sort_by_value) = sort_value_opt {
        if let Some(output_datetime_format) = &sort_field_1_datetime_format_opt {
            convert_sort_datetime_value(sort_by_value, *output_datetime_format)?;
        }
    }
    let sort_value_2_opt = partial_hit_ref
        .sort_value2
        .as_mut()
        .and_then(|sort_field| sort_field.sort_value.as_mut());
    if let Some(sort_by_value) = sort_value_2_opt {
        if let Some(output_datetime_format) = &sort_field_2_datetime_format_opt {
            convert_sort_datetime_value(sort_by_value, *output_datetime_format)?;
        }
    }
    let position = *hit_order.get(&key).expect("hit order must be present");
    let index_id = split_id_to_index_id_map
        .get(&partial_hit_ref.split_id)
        .map(|split_id| split_id.to_string())
        .unwrap_or_default();

    Result::<(usize, Hit), SearchError>::Ok((
        position,
        Hit {
            json: leaf_hit.leaf_json,
            partial_hit: leaf_hit.partial_hit,
            snippet: leaf_hit.leaf_snippet_json,
            index_id,
        },
    ))
}

fn get_sort_field_datetime_format(
    sort_field: Option<&SortField>,
) -> crate::Result<Option<SortDatetimeFormat>> {
    if let Some(sort_field) = sort_field {
        if let Some(sort_field_datetime_format_int) = &sort_field.sort_datetime_format {
            let sort_field_datetime_format =
                SortDatetimeFormat::try_from(*sort_field_datetime_format_int)
                    .context("invalid sort datetime format")?;
            return Ok(Some(sort_field_datetime_format));
        }
    }
    Ok(None)
}

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip_all, fields(num_splits=%split_metadatas.len()))]
async fn root_search_aux(
    searcher_context: &SearcherContext,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    search_request: SearchRequest,
    split_metadatas: Vec<SplitMetadata>,
    cluster_client: &ClusterClient,
) -> crate::Result<SearchResponse> {
    debug!(split_metadatas = ?PrettySample::new(&split_metadatas, 5));
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

    let hits = fetch_docs_phase(
        indexes_metas_for_leaf_search,
        &first_phase_result.partial_hits,
        &split_metadatas[..],
        &search_request,
        cluster_client,
    )
    .await?;

    let mut aggregation_result_json_opt = finalize_aggregation_if_any(
        &search_request,
        first_phase_result.intermediate_aggregation_result,
        searcher_context,
    )?;
    // In case there is no index, we don't want the response to contain any aggregation structure
    if indexes_metas_for_leaf_search.is_empty() {
        aggregation_result_json_opt = None;
    }

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
    intermediate_aggregation_result_bytes_opt: Option<Vec<u8>>,
    aggregations: QuickwitAggregations,
    searcher_context: &SearcherContext,
) -> crate::Result<Option<String>> {
    let merge_aggregation_result = match aggregations {
        QuickwitAggregations::FindTraceIdsAggregation(_) => {
            let Some(intermediate_aggregation_result_bytes) =
                intermediate_aggregation_result_bytes_opt
            else {
                return Ok(None);
            };
            // The merge collector has already merged the intermediate results.
            let aggs: Vec<Span> = postcard::from_bytes(&intermediate_aggregation_result_bytes)?;
            serde_json::to_string(&aggs)?
        }
        QuickwitAggregations::TantivyAggregations(aggregations) => {
            let intermediate_aggregation_results =
                if let Some(intermediate_aggregation_result_bytes) =
                    intermediate_aggregation_result_bytes_opt
                {
                    let intermediate_aggregation_results: IntermediateAggregationResults =
                        postcard::from_bytes(&intermediate_aggregation_result_bytes)?;
                    intermediate_aggregation_results
                } else {
                    // Default, to return correct structure
                    Default::default()
                };
            let final_aggregation_results: AggregationResults = intermediate_aggregation_results
                .into_final_result(
                    aggregations,
                    &searcher_context.create_new_aggregation_limits(),
                )?;
            serde_json::to_string(&final_aggregation_results)?
        }
    };
    Ok(Some(merge_aggregation_result))
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
    let aggregation_result_json = finalize_aggregation(
        intermediate_aggregation_result_bytes_opt,
        aggregations,
        searcher_context,
    )?;
    Ok(aggregation_result_json)
}

/// Checks that all of the index researched as found.
///
/// An index pattern (= containing a wildcard) not matching is not an error.
/// A specific index id however must be found.
///
/// We put this check here and not in the metastore to make sure the logic is independent
/// of the metastore implementation, and some different use cases could require different
/// behaviors. This specification was principally motivated by #4042.
pub fn check_all_index_metadata_found(
    index_metadatas: &[IndexMetadata],
    index_id_patterns: &[String],
) -> crate::Result<()> {
    let mut index_ids: HashSet<&str> = index_id_patterns
        .iter()
        .map(|index_ptn| index_ptn.as_str())
        .filter(|index_ptn| !index_ptn.contains('*') && !index_ptn.starts_with('-'))
        .collect();

    if index_ids.is_empty() {
        // All of the patterns are wildcard patterns.
        return Ok(());
    }

    for index_metadata in index_metadatas {
        index_ids.remove(index_metadata.index_uid.index_id.as_str());
    }

    if !index_ids.is_empty() {
        let missing_index_ids = index_ids
            .into_iter()
            .map(|missing_index_id| missing_index_id.to_string())
            .collect();
        return Err(SearchError::IndexesNotFound {
            index_ids: missing_index_ids,
        });
    }

    Ok(())
}

async fn refine_and_list_matches(
    metastore: &mut MetastoreServiceClient,
    search_request: &mut SearchRequest,
    indexes_metadata: Vec<IndexMetadata>,
    query_ast_resolved: QueryAst,
    sort_fields_is_datetime: HashMap<String, bool>,
    timestamp_field_opt: Option<String>,
) -> crate::Result<Vec<SplitMetadata>> {
    let index_uids = indexes_metadata
        .iter()
        .map(|index_metadata| index_metadata.index_uid.clone())
        .collect_vec();
    search_request.query_ast = serde_json::to_string(&query_ast_resolved)?;

    // convert search_after datetime values from input datetime format to nanos.
    convert_search_after_datetime_values(search_request, &sort_fields_is_datetime)?;

    // update_search_after_datetime_in_nanos(&mut search_request)?;
    if let Some(timestamp_field) = &timestamp_field_opt {
        refine_start_end_timestamp_from_ast(
            &query_ast_resolved,
            timestamp_field,
            &mut search_request.start_timestamp,
            &mut search_request.end_timestamp,
        );
    }
    let tag_filter_ast = extract_tags_from_query(query_ast_resolved);

    // TODO if search after is set, we sort by timestamp and we don't want to count all results,
    // we can refine more here. Same if we sort by _shard_doc
    let split_metadatas: Vec<SplitMetadata> = list_relevant_splits(
        index_uids,
        search_request.start_timestamp,
        search_request.end_timestamp,
        tag_filter_ast,
        metastore,
    )
    .await?;
    Ok(split_metadatas)
}

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip_all)]
pub async fn root_search(
    searcher_context: &SearcherContext,
    mut search_request: SearchRequest,
    mut metastore: MetastoreServiceClient,
    cluster_client: &ClusterClient,
) -> crate::Result<SearchResponse> {
    info!(searcher_context = ?searcher_context, search_request = ?search_request);
    let start_instant = tokio::time::Instant::now();
    let list_indexes_metadatas_request = ListIndexesMetadataRequest {
        index_id_patterns: search_request.index_id_patterns.clone(),
    };
    let indexes_metadata: Vec<IndexMetadata> = metastore
        .list_indexes_metadata(list_indexes_metadatas_request)
        .await?
        .deserialize_indexes_metadata()
        .await?;

    check_all_index_metadata_found(&indexes_metadata[..], &search_request.index_id_patterns[..])?;

    if indexes_metadata.is_empty() {
        // We go through root_search_aux instead of directly
        // returning an empty response to make sure we generate
        // a (pretty useless) scroll id if requested.
        let mut search_response = root_search_aux(
            searcher_context,
            &HashMap::default(),
            search_request,
            Vec::new(),
            cluster_client,
        )
        .await?;
        search_response.elapsed_time_micros = start_instant.elapsed().as_micros() as u64;
        return Ok(search_response);
    }

    let request_metadata = validate_request_and_build_metadata(&indexes_metadata, &search_request)?;
    let split_metadatas = refine_and_list_matches(
        &mut metastore,
        &mut search_request,
        indexes_metadata,
        request_metadata.query_ast_resolved,
        request_metadata.sort_fields_is_datetime,
        request_metadata.timestamp_field_opt,
    )
    .await?;

    let mut search_response = root_search_aux(
        searcher_context,
        &request_metadata.indexes_meta_for_leaf_search,
        search_request,
        split_metadatas,
        cluster_client,
    )
    .await?;

    search_response.elapsed_time_micros = start_instant.elapsed().as_micros() as u64;
    Ok(search_response)
}

/// Returns details on how a query would be executed
pub async fn search_plan(
    mut search_request: SearchRequest,
    mut metastore: MetastoreServiceClient,
) -> crate::Result<SearchPlanResponse> {
    let list_indexes_metadatas_request = ListIndexesMetadataRequest {
        index_id_patterns: search_request.index_id_patterns.clone(),
    };
    let indexes_metadata: Vec<IndexMetadata> = metastore
        .list_indexes_metadata(list_indexes_metadatas_request)
        .await?
        .deserialize_indexes_metadata()
        .await?;

    check_all_index_metadata_found(&indexes_metadata[..], &search_request.index_id_patterns[..])?;
    if indexes_metadata.is_empty() {
        return Ok(SearchPlanResponse {
            result: serde_json::to_string(&SearchPlanResponseRest {
                quickwit_ast: QueryAst::MatchAll,
                tantivy_ast: String::new(),
                searched_splits: Vec::new(),
                storage_requests: StorageRequestCount::default(),
            })?,
        });
    }
    let doc_mapper = build_doc_mapper(
        &indexes_metadata[0].index_config.doc_mapping,
        &indexes_metadata[0].index_config.search_settings,
    )
    .map_err(|err| SearchError::Internal(format!("failed to build doc mapper. cause: {err}")))?;

    let request_metadata = validate_request_and_build_metadata(&indexes_metadata, &search_request)?;
    let split_metadatas = refine_and_list_matches(
        &mut metastore,
        &mut search_request,
        indexes_metadata,
        request_metadata.query_ast_resolved.clone(),
        request_metadata.sort_fields_is_datetime,
        request_metadata.timestamp_field_opt,
    )
    .await?;

    let (query, mut warmup_info) = doc_mapper.query(
        doc_mapper.schema(),
        &request_metadata.query_ast_resolved,
        true,
    )?;
    let merge_collector = make_merge_collector(&search_request, &Default::default())?;
    warmup_info.merge(merge_collector.warmup_info());
    warmup_info.simplify();

    let split_ids = split_metadatas
        .into_iter()
        .map(|split| format!("{}/{}", split.index_uid.index_id, split.split_id))
        .collect();
    // this is an upper bound, we'd need access to a hotdir for more precise results
    let fieldnorm_query_count = if warmup_info.field_norms {
        doc_mapper
            .schema()
            .fields()
            .filter(|(_, entry)| entry.has_fieldnorms())
            .count()
    } else {
        0
    };
    let sstable_query_count = warmup_info.term_dict_fields.len()
        + warmup_info
            .terms_grouped_by_field
            .values()
            .map(|terms: &HashMap<tantivy::Term, bool>| terms.len())
            .sum::<usize>()
        + warmup_info
            .term_ranges_grouped_by_field
            .values()
            .map(|terms: &HashMap<_, bool>| terms.len())
            .sum::<usize>();
    let position_query_count = warmup_info
        .terms_grouped_by_field
        .values()
        .map(|terms: &HashMap<tantivy::Term, bool>| {
            terms
                .values()
                .filter(|load_position| **load_position)
                .count()
        })
        .sum::<usize>()
        + warmup_info
            .term_ranges_grouped_by_field
            .values()
            .map(|terms: &HashMap<_, bool>| {
                terms
                    .values()
                    .filter(|load_position| **load_position)
                    .count()
            })
            .sum::<usize>();
    Ok(SearchPlanResponse {
        result: serde_json::to_string(&SearchPlanResponseRest {
            quickwit_ast: request_metadata.query_ast_resolved,
            tantivy_ast: format!("{query:#?}"),
            searched_splits: split_ids,
            storage_requests: StorageRequestCount {
                footer: 1,
                fastfield: warmup_info.fast_field_names.len(),
                fieldnorm: fieldnorm_query_count,
                sstable: sstable_query_count,
                posting: sstable_query_count,
                position: position_query_count,
            },
        })?,
    })
}

/// Converts search after with datetime format to nanoseconds (representation in tantivy).
/// If the sort field is a datetime field and no datetime format is set, the default format is
/// milliseconds.
/// `sort_fields_are_datetime_opt` must be of the same length as `search_request.sort_fields`.
fn convert_search_after_datetime_values(
    search_request: &mut SearchRequest,
    sort_fields_is_datetime: &HashMap<String, bool>,
) -> crate::Result<()> {
    for sort_field in search_request.sort_fields.iter_mut() {
        if *sort_fields_is_datetime
            .get(&sort_field.field_name)
            .unwrap_or(&false)
            && sort_field.sort_datetime_format.is_none()
        {
            sort_field.sort_datetime_format = Some(SortDatetimeFormat::UnixTimestampMillis as i32);
        }
    }
    if let Some(partial_hit) = search_request.search_after.as_mut() {
        let search_after_values = [
            partial_hit.sort_value.as_mut(),
            partial_hit.sort_value2.as_mut(),
        ];
        for (sort_field, search_after_value_opt) in
            search_request.sort_fields.iter().zip(search_after_values)
        {
            let Some(search_after_sort_by_value) = search_after_value_opt else {
                continue;
            };
            let Some(search_after_sort_value) = search_after_sort_by_value.sort_value.as_mut()
            else {
                continue;
            };
            let Some(datetime_format_int) = sort_field.sort_datetime_format else {
                continue;
            };
            let input_datetime_format = SortDatetimeFormat::try_from(datetime_format_int)
                .context("invalid sort datetime format")?;
            convert_sort_datetime_value_into_nanos(search_after_sort_value, input_datetime_format)?;
        }
    }
    Ok(())
}

/// Convert sort values from input datetime format into nanoseconds.
/// The conversion is done only for U64 and I64 sort values, an error is returned for other types.
fn convert_sort_datetime_value_into_nanos(
    sort_value: &mut SortValue,
    input_format: SortDatetimeFormat,
) -> crate::Result<()> {
    match sort_value {
        SortValue::U64(value) => match input_format {
            SortDatetimeFormat::UnixTimestampMillis => {
                *value = value.checked_mul(1_000_000).ok_or_else(|| {
                    SearchError::Internal(format!(
                        "sort value defined in milliseconds is too large and cannot be converted \
                         into nanoseconds: {}",
                        value
                    ))
                })?;
            }
            SortDatetimeFormat::UnixTimestampNanos => {
                // Nothing to do as the internal format is nanos.
            }
        },
        SortValue::I64(value) => match input_format {
            SortDatetimeFormat::UnixTimestampMillis => {
                *value = value.checked_mul(1_000_000).ok_or_else(|| {
                    SearchError::Internal(format!(
                        "sort value defined in milliseconds is too large and cannot be converted \
                         into nanoseconds: {}",
                        value
                    ))
                })?;
            }
            SortDatetimeFormat::UnixTimestampNanos => {
                // Nothing to do as the internal format is nanos.
            }
        },
        _ => {
            return Err(SearchError::Internal(format!(
                "datetime conversion are only support for u64 and i64 sort values, not `{:?}`",
                sort_value
            )));
        }
    }
    Ok(())
}

/// Convert sort values from nanoseconds to the requested output format.
/// The conversion is done only for U64 and I64 sort values, an error is returned for other types.
fn convert_sort_datetime_value(
    sort_value: &mut SortValue,
    output_format: SortDatetimeFormat,
) -> crate::Result<()> {
    match sort_value {
        SortValue::U64(value) => match output_format {
            SortDatetimeFormat::UnixTimestampMillis => {
                *value /= 1_000_000;
            }
            SortDatetimeFormat::UnixTimestampNanos => {
                // Nothing todo as the internal format is in nanos.
            }
        },
        SortValue::I64(value) => match output_format {
            SortDatetimeFormat::UnixTimestampMillis => {
                *value /= 1_000_000;
            }
            SortDatetimeFormat::UnixTimestampNanos => {
                // Nothing todo as the internal format is in nanos.
            }
        },
        _ => {
            return Err(SearchError::Internal(format!(
                "datetime conversion are only support for u64 and i64 sort values, not `{:?}`",
                sort_value
            )));
        }
    }
    Ok(())
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
            .or_default()
            .push(partial_hit.clone());
    }

    let mut fetch_docs_req_jobs: Vec<FetchDocsJob> = Vec::new();
    for (split_id, partial_hits) in partial_hits_map {
        let (index_uid, offsets) = index_uids_and_split_offsets_map
            .get(&split_id)
            .ok_or_else(|| {
                crate::SearchError::Internal(format!(
                    "received partial hit from an unknown split {split_id}"
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
fn compute_split_cost(split_metadata: &SplitMetadata) -> usize {
    // TODO this formula could be tuned a lot more. The general idea is that there is a fixed
    // cost to searching a split, plus a somewhat-linear cost depending on the size of the split
    5 + split_metadata.num_docs / 100_000
}

/// Builds a LeafSearchRequest to one node, from a list of [`SearchJob`].
pub fn jobs_to_leaf_request(
    request: &SearchRequest,
    search_indexes_metadatas: &IndexesMetasForLeafSearch,
    jobs: Vec<SearchJob>,
) -> crate::Result<LeafSearchRequest> {
    let mut search_request_for_leaf = request.clone();
    search_request_for_leaf.start_offset = 0;
    search_request_for_leaf.max_hits += request.start_offset;

    let mut leaf_search_request = LeafSearchRequest {
        search_request: Some(search_request_for_leaf),
        leaf_requests: Vec::new(),
        doc_mappers: Vec::new(),
        index_uris: Vec::new(),
    };

    let mut added_doc_mappers: HashMap<&str, u32> = HashMap::new();
    // Group jobs by index uid, as the split offsets are relative to the index.
    group_jobs_by_index_id(jobs, |job_group| {
        let index_uid = &job_group[0].index_uid;
        let search_index_meta = search_indexes_metadatas.get(index_uid).ok_or_else(|| {
            SearchError::Internal(format!(
                "received job for an unknown index {index_uid}. it should never happen"
            ))
        })?;
        let doc_mapper_ord = *added_doc_mappers
            .entry(&search_index_meta.doc_mapper_str)
            .or_insert_with(|| {
                let ord = leaf_search_request.doc_mappers.len();
                leaf_search_request
                    .doc_mappers
                    .push(search_index_meta.doc_mapper_str.to_string());
                ord as u32
            });
        let index_uri_ord = leaf_search_request.index_uris.len() as u32;
        leaf_search_request
            .index_uris
            .push(search_index_meta.index_uri.to_string());

        let leaf_search_request_ref = LeafRequestRef {
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
            doc_mapper_ord,
            index_uri_ord,
        };
        leaf_search_request
            .leaf_requests
            .push(leaf_search_request_ref);
        Ok(())
    })?;
    Ok(leaf_search_request)
}

/// Builds a list of [`FetchDocsRequest`], one per index, from a list of [`FetchDocsJob`].
pub fn jobs_to_fetch_docs_requests(
    snippet_request_opt: Option<SnippetRequest>,
    indexes_metas_for_leaf_search: &IndexesMetasForLeafSearch,
    jobs: Vec<FetchDocsJob>,
) -> crate::Result<Vec<FetchDocsRequest>> {
    let mut fetch_docs_requests = Vec::new();
    // Group jobs by index uid.
    group_by(
        jobs,
        |job| &job.index_uid,
        |fetch_docs_jobs| {
            let index_uid = &fetch_docs_jobs[0].index_uid;

            let index_meta = indexes_metas_for_leaf_search
                .get(index_uid)
                .ok_or_else(|| {
                    SearchError::Internal(format!(
                        "received search job for an unknown index {index_uid}"
                    ))
                })?;
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

            Ok(())
        },
    )?;
    Ok(fetch_docs_requests)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};

    use quickwit_common::shared_consts::SCROLL_BATCH_LEN;
    use quickwit_common::ServiceStream;
    use quickwit_config::{DocMapping, IndexConfig, IndexingSettings, SearchSettings};
    use quickwit_indexing::MockSplitBuilder;
    use quickwit_metastore::{IndexMetadata, ListSplitsRequestExt, ListSplitsResponseExt};
    use quickwit_proto::metastore::{
        ListIndexesMetadataResponse, ListSplitsResponse, MockMetastoreService,
    };
    use quickwit_proto::search::{
        ScrollRequest, SortByValue, SortOrder, SortValue, SplitSearchError,
    };
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
            "the snippet field `title` must be stored"
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
            "the snippet field `ip` must be of type `Str`, got `IpAddr`"
        );
    }

    #[test]
    fn test_get_sort_by_field_entry() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_u64_field("timestamp", FAST | STORED);
        let schema = schema_builder.build();
        get_sort_by_field_entry("timestamp", &schema)
            .unwrap()
            .unwrap();
        let sort_by_field_entry_err = get_sort_by_field_entry("doesnotexist", &schema).unwrap_err();
        assert_eq!(
            sort_by_field_entry_err.to_string(),
            "Invalid argument: unknown field used in `sort by`: doesnotexist"
        );
        for sort_field_name in &["_doc", "_score", "_shard_doc"] {
            assert!(get_sort_by_field_entry(sort_field_name, &schema)
                .unwrap()
                .is_none());
        }
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
            retention_policy_opt: Default::default(),
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
            sort_fields: vec![
                SortField {
                    field_name: "timestamp".to_string(),
                    sort_order: SortOrder::Desc as i32,
                    sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
                },
                SortField {
                    field_name: "_doc".to_string(),
                    sort_order: SortOrder::Asc as i32,
                    sort_datetime_format: None,
                },
            ],
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
        let request_metadata = validate_request_and_build_metadata(
            &[
                index_metadata,
                index_metadata_with_other_config,
                index_metadata_no_timestamp,
            ],
            &search_request,
        )
        .unwrap();
        assert_eq!(
            request_metadata.timestamp_field_opt,
            Some("timestamp".to_string())
        );
        assert_eq!(request_metadata.query_ast_resolved, request_query_ast);
        assert_eq!(request_metadata.indexes_meta_for_leaf_search.len(), 3);
        assert_eq!(request_metadata.sort_fields_is_datetime.len(), 2);
        assert_eq!(
            request_metadata.sort_fields_is_datetime.get("timestamp"),
            Some(&true)
        );
        assert_eq!(
            request_metadata.sort_fields_is_datetime.get("_doc"),
            Some(&false)
        );
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
        let timestamp_field_different = validate_request_and_build_metadata(
            &[index_metadata_1, index_metadata_2],
            &search_request,
        )
        .unwrap_err();
        assert_eq!(
            timestamp_field_different.to_string(),
            "the timestamp field (if present) must be the same for all indexes"
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
        let timestamp_field_different = validate_request_and_build_metadata(
            &[index_metadata_1, index_metadata_2],
            &search_request,
        )
        .unwrap_err();
        assert_eq!(
            timestamp_field_different.to_string(),
            "resolved query ASTs must be the same across indexes. resolving queries with \
             different default fields are different between indexes is not supported"
        );
    }

    fn index_metadata_for_multi_indexes_test_with_incompatible_sort_type(
        index_id: &str,
        index_uri: &str,
    ) -> IndexMetadata {
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
                },
                {
                    "name": "response_date",
                    "type": "i64",
                    "stored": true,
                    "fast": true
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
            retention_policy_opt: Default::default(),
        })
    }

    #[test]
    fn test_validate_request_and_build_metadatas_fail_with_incompatible_sort_field_types() {
        let request_query_ast = qast_helper("body:test", &[]);
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: serde_json::to_string(&request_query_ast).unwrap(),
            max_hits: 10,
            start_offset: 10,
            sort_fields: vec![SortField {
                field_name: "response_date".to_string(),
                sort_order: SortOrder::Desc as i32,
                sort_datetime_format: None,
            }],
            ..Default::default()
        };
        let index_metadata = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_metadata_with_other_config =
            index_metadata_for_multi_indexes_test_with_incompatible_sort_type(
                "test-index-2",
                "ram:///test-index-2",
            );
        let search_error = validate_request_and_build_metadata(
            &[index_metadata, index_metadata_with_other_config],
            &search_request,
        )
        .unwrap_err();
        assert_eq!(
            search_error.to_string(),
            "sort datetime field `response_date` must be of type datetime on all indexes"
        );
    }

    #[test]
    fn test_convert_sort_datetime_value() {
        let mut sort_value = SortValue::U64(1617000000000000000);
        convert_sort_datetime_value(&mut sort_value, SortDatetimeFormat::UnixTimestampMillis)
            .unwrap();
        assert_eq!(sort_value, SortValue::U64(1617000000000));
        let mut sort_value = SortValue::I64(1617000000000000000);
        convert_sort_datetime_value(&mut sort_value, SortDatetimeFormat::UnixTimestampMillis)
            .unwrap();
        assert_eq!(sort_value, SortValue::I64(1617000000000));

        // conversion with float values should fail.
        let mut sort_value = SortValue::F64(1617000000000000000.0);
        let error =
            convert_sort_datetime_value(&mut sort_value, SortDatetimeFormat::UnixTimestampMillis)
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            "internal error: `datetime conversion are only support for u64 and i64 sort values, \
             not `F64(1.617e18)``"
        );
    }

    #[test]
    fn test_convert_sort_datetime_value_into_nanos() {
        let mut sort_value = SortValue::U64(1617000000000);
        convert_sort_datetime_value_into_nanos(
            &mut sort_value,
            SortDatetimeFormat::UnixTimestampMillis,
        )
        .unwrap();
        assert_eq!(sort_value, SortValue::U64(1617000000000000000));
        let mut sort_value = SortValue::I64(1617000000000);
        convert_sort_datetime_value_into_nanos(
            &mut sort_value,
            SortDatetimeFormat::UnixTimestampMillis,
        )
        .unwrap();
        assert_eq!(sort_value, SortValue::I64(1617000000000000000));

        // conversion with a too large millisecond value should fail.
        let mut sort_value = SortValue::I64(1617000000000000);
        let error = convert_sort_datetime_value_into_nanos(
            &mut sort_value,
            SortDatetimeFormat::UnixTimestampMillis,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "internal error: `sort value defined in milliseconds is too large and cannot be \
             converted into nanoseconds: 1617000000000000`"
        );
        // conversion with float values should fail.
        let mut sort_value = SortValue::F64(1617000000000000.0);
        let error = convert_sort_datetime_value_into_nanos(
            &mut sort_value,
            SortDatetimeFormat::UnixTimestampMillis,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "internal error: `datetime conversion are only support for u64 and i64 sort values, \
             not `F64(1617000000000000.0)``"
        );
    }

    #[test]
    fn test_validate_sort_field_types_with_doc_and_shard_doc() {
        let sort_fields = vec![
            SortField {
                field_name: "_doc".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
            SortField {
                field_name: "_shard_doc".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("timestamp", FAST);
        schema_builder.add_u64_field("id", FAST);
        let schema = schema_builder.build();
        let mut sort_field_are_datetime = HashMap::new();
        validate_sort_field_types(&schema, &sort_fields, &mut sort_field_are_datetime).unwrap();
        assert_eq!(sort_field_are_datetime.get("_doc"), Some(&false));
        assert_eq!(sort_field_are_datetime.get("_shard_doc"), Some(&false));
    }

    #[test]
    fn test_validate_sort_field_types_valid() {
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("timestamp", FAST);
        schema_builder.add_u64_field("id", FAST);
        let schema = schema_builder.build();
        let mut sort_field_are_datetime = HashMap::new();
        validate_sort_field_types(&schema, &sort_fields, &mut sort_field_are_datetime).unwrap();
        assert_eq!(sort_field_are_datetime.get("timestamp"), Some(&true));
        assert_eq!(sort_field_are_datetime.get("id"), Some(&false));
    }

    #[test]
    fn test_validate_sort_field_types_with_inconsistent_datetime_type() {
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("timestamp", FAST);
        schema_builder.add_u64_field("id", FAST);
        let schema = schema_builder.build();
        {
            let mut sort_field_are_datetime = HashMap::new();
            sort_field_are_datetime.insert("timestamp".to_string(), false);
            sort_field_are_datetime.insert("id".to_string(), false);
            let error =
                validate_sort_field_types(&schema, &sort_fields, &mut sort_field_are_datetime)
                    .unwrap_err();
            assert_eq!(
                error.to_string(),
                "sort datetime field `timestamp` must be of type datetime on all indexes"
            );
        }
        {
            let mut sort_field_are_datetime = HashMap::new();
            sort_field_are_datetime.insert("id".to_string(), true);
            let error =
                validate_sort_field_types(&schema, &sort_fields, &mut sort_field_are_datetime)
                    .unwrap_err();
            assert_eq!(
                error.to_string(),
                "sort datetime field `id` must be of type datetime on all indexes"
            );
        }
    }

    #[test]
    fn test_validate_sort_by_fields_with_datetime_format_ok() {
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        validate_sort_by_fields_and_search_after(&sort_fields, &None).unwrap();
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_after_ok() {
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let partial_hit = PartialHit {
            sort_value: Some(SortByValue {
                sort_value: Some(SortValue::U64(1)),
            }),
            sort_value2: Some(SortByValue {
                sort_value: Some(SortValue::U64(2)),
            }),
            split_id: "".to_string(),
            segment_ord: 0,
            doc_id: 0,
        };
        validate_sort_by_fields_and_search_after(&sort_fields, &Some(partial_hit)).unwrap();
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_after_ok_with_doc_sort_field() {
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "_doc".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let partial_hit = PartialHit {
            sort_value: Some(SortByValue {
                sort_value: Some(SortValue::U64(1)),
            }),
            sort_value2: None,
            split_id: "split1".to_string(),
            segment_ord: 1,
            doc_id: 1,
        };
        validate_sort_by_fields_and_search_after(&sort_fields, &Some(partial_hit)).unwrap();
    }

    #[test]
    fn test_validate_sort_by_field_type() {
        let mut schema_builder = Schema::builder();
        let timestamp_field = schema_builder.add_date_field("timestamp", FAST);
        let id_field = schema_builder.add_u64_field("id", FAST);
        let no_fast_field = schema_builder.add_u64_field("no_fast", STORED);
        let text_field = schema_builder.add_text_field("text", STORED);
        let schema = schema_builder.build();
        {
            let sort_by_field_entry = schema.get_field_entry(timestamp_field);
            validate_sort_by_field_type(sort_by_field_entry, false).unwrap();
            validate_sort_by_field_type(sort_by_field_entry, true).unwrap();
        }
        {
            let sort_by_field_entry = schema.get_field_entry(id_field);
            validate_sort_by_field_type(sort_by_field_entry, false).unwrap();
            let error = validate_sort_by_field_type(sort_by_field_entry, true).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Invalid argument: sort by field with a timestamp format must be a datetime field \
                 and the field `id` is not"
            );
        }
        {
            let sort_by_field_entry = schema.get_field_entry(no_fast_field);
            let error = validate_sort_by_field_type(sort_by_field_entry, true).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Invalid argument: sort by field must be a fast field, please add the fast \
                 property to your field `no_fast`"
            );
        }
        {
            let sort_by_field_entry = schema.get_field_entry(text_field);
            let error = validate_sort_by_field_type(sort_by_field_entry, true).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Invalid argument: sort by field on type text is currently not supported `text`"
            );
        }
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_after_invalid_1() {
        // 2 sort fields + search after with only one sort value is invalid.
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let partial_hit = PartialHit {
            sort_value: Some(SortByValue {
                sort_value: Some(SortValue::U64(1)),
            }),
            sort_value2: None,
            split_id: "split1".to_string(),
            segment_ord: 1,
            doc_id: 1,
        };
        let error =
            validate_sort_by_fields_and_search_after(&sort_fields, &Some(partial_hit)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument: `search_after` must have the same number of sort values as sort by \
             fields [\"timestamp\", \"id\"]"
        );
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_after_invalid_with_missing_split_id() {
        // 2 sort fields + search after with only one sort value is invalid.
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "_doc".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let partial_hit = PartialHit {
            sort_value: Some(SortByValue {
                sort_value: Some(SortValue::U64(1)),
            }),
            sort_value2: None,
            split_id: "".to_string(),
            segment_ord: 1,
            doc_id: 1,
        };
        let error =
            validate_sort_by_fields_and_search_after(&sort_fields, &Some(partial_hit)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument: search_after with a sort field `_doc` must define a split ID, \
             segment ID and doc ID values"
        );
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_valid_1() {
        // 2 sort fields + search after with only one sort value is invalid.
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "id".to_string(),
                sort_order: 0,
                sort_datetime_format: None,
            },
        ];
        let partial_hit = PartialHit {
            sort_value: Some(SortByValue {
                sort_value: Some(SortValue::U64(1)),
            }),
            sort_value2: None,
            split_id: "split1".to_string(),
            segment_ord: 1,
            doc_id: 1,
        };
        let error =
            validate_sort_by_fields_and_search_after(&sort_fields, &Some(partial_hit)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument: `search_after` must have the same number of sort values as sort by \
             fields [\"timestamp\", \"id\"]"
        );
    }

    #[test]
    fn test_validate_sort_by_field_type_invalid() {
        // sort non-datetime field with a datetime format is invalid.
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("timestamp", FAST);
        let schema = schema_builder.build();
        let field_entry = schema.get_field_entry(field);
        let error = validate_sort_by_field_type(field_entry, true).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument: sort by field with a timestamp format must be a datetime field and \
             the field `timestamp` is not"
        );
    }

    #[test]
    fn test_validate_sort_by_fields_and_search_after_invalid_3() {
        // 3 sort fields is not possible.
        let sort_fields = vec![
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
            SortField {
                field_name: "timestamp".to_string(),
                sort_order: 0,
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampMillis as i32),
            },
        ];
        let error = validate_sort_by_fields_and_search_after(&sort_fields, &None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument: sort by field must be up to 2 fields, got 3"
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

    fn mock_partial_hit_opt_sort_value(
        split_id: &str,
        sort_value: Option<u64>,
        doc_id: u32,
    ) -> quickwit_proto::search::PartialHit {
        quickwit_proto::search::PartialHit {
            sort_value: sort_value.map(|sort_value| SortValue::U64(sort_value).into()),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_indexes_metadata_request| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_list_splits_request| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_with_failure() -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 2,
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_leaf_search().returning(
            |leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                if leaf_search_req.leaf_requests[0].split_offsets.len() == 2 {
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: 2,
                        partial_hits: vec![
                            mock_partial_hit("split1", 3, 1),
                            mock_partial_hit("split1", 1, 3),
                        ],
                        failed_splits: vec![SplitSearchError {
                            error: "some error".to_string(),
                            split_id: "split2".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 2,
                        ..Default::default()
                    })
                } else {
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                }
            },
        );
        mock_search_service_1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service_1)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            MetastoreServiceClient::from_mock(mock_metastore),
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_sort_heteregeneous_field_ascending(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            sort_fields: vec![SortField {
                field_name: "response_date".to_string(),
                sort_order: SortOrder::Asc.into(),
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampNanos as i32),
            }],
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
            &cluster_client,
        )
        .await?;

        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 5);
        assert_eq!(
            search_response.hits[0].partial_hit.as_ref().unwrap(),
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
            search_response.hits[2].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 0,
                sort_value: Some(SortValue::U64(2u64).into()),
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[3].partial_hit.as_ref().unwrap(),
            &PartialHit {
                split_id: "split1".to_string(),
                segment_ord: 0,
                doc_id: 1,
                sort_value: None,
                sort_value2: None,
            }
        );
        assert_eq!(
            search_response.hits[4].partial_hit.as_ref().unwrap(),
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
            sort_fields: vec![SortField {
                field_name: "response_date".to_string(),
                sort_order: SortOrder::Desc.into(),
                sort_datetime_format: Some(SortDatetimeFormat::UnixTimestampNanos as i32),
            }],
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });

        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .times(2)
            .returning(
                |leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                    let split_ids: Vec<&str> = leaf_search_req.leaf_requests[0]
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_indexes_metadata_request| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .withf(|leaf_search_req| {
                leaf_search_req.leaf_requests[0].split_offsets[0].split_id == "split2"
            })
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
            .withf(|leaf_search_req| {
                leaf_search_req.leaf_requests[0].split_offsets[0].split_id == "split1"
            })
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
            .withf(|leaf_search_req| {
                leaf_search_req.leaf_requests[0].split_offsets[0].split_id == "split2"
            })
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
            .withf(|leaf_search_req| {
                leaf_search_req.leaf_requests[0].split_offsets[0].split_id == "split1"
            })
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_list_splits_request| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
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
            MetastoreServiceClient::from_mock(mock_metastore),
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let searcher_context = SearcherContext::for_test();
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);

        assert!(root_search(
            &searcher_context,
            quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index".to_string()],
                query_ast: qast_json_helper("invalid_field:\"test\"", &["body"]),
                max_hits: 10,
                ..Default::default()
            },
            metastore.clone(),
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
            metastore,
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            MetastoreServiceClient::from_mock(mock_metastore),
            &cluster_client,
        )
        .await;
        assert!(search_response.is_err());
        assert!(search_response
            .unwrap_err()
            .to_string()
            .starts_with("invalid aggregation request: unknown variant `termss`, expected one of"));
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build()];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let search_response = root_search(
            &SearcherContext::for_test(),
            search_request,
            metastore.clone(),
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
            metastore,
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

    #[tokio::test]
    async fn test_search_plan_multiple_splits() -> anyhow::Result<()> {
        use quickwit_query::query_ast::{FullTextMode, FullTextParams, FullTextQuery};
        use quickwit_query::MatchAllOrNone;

        let search_request = quickwit_proto::search::SearchRequest {
            index_id_patterns: vec!["test-index".to_string()],
            query_ast: qast_json_helper("test-query", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata.clone()
                ]))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let search_response = search_plan(
            search_request,
            MetastoreServiceClient::from_mock(mock_metastore),
        )
        .await
        .unwrap();
        let response: SearchPlanResponseRest =
            serde_json::from_str(&search_response.result).unwrap();
        assert_eq!(
            response,
            SearchPlanResponseRest {
                quickwit_ast: QueryAst::FullText(FullTextQuery {
                    field: "body".to_string(),
                    text: "test-query".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::PhraseFallbackToIntersection,
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                },),
                tantivy_ast: r#"BooleanQuery {
    subqueries: [
        (
            Must,
            TermQuery(Term(field=3, type=Str, "test")),
        ),
        (
            Must,
            TermQuery(Term(field=3, type=Str, "query")),
        ),
    ],
    minimum_number_should_match: 0,
}"#
                .to_string(),
                searched_splits: vec![
                    "test-index/split1".to_string(),
                    "test-index/split2".to_string()
                ],
                storage_requests: StorageRequestCount {
                    footer: 1,
                    fastfield: 0,
                    fieldnorm: 0,
                    sstable: 2,
                    posting: 2,
                    position: 0,
                },
            }
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

    fn create_search_resp(
        index_uri: &str,
        hit_range: Range<usize>,
        search_after: Option<PartialHit>,
    ) -> LeafSearchResponse {
        let (num_total_hits, split_id) = match index_uri {
            "ram:///test-index-1" => (TOTAL_NUM_HITS_INDEX_1, "split1"),
            "ram:///test-index-2" => (TOTAL_NUM_HITS_INDEX_2, "split2"),
            _ => panic!("unexpected index uri"),
        };

        let doc_ids = (0..num_total_hits)
            .rev()
            .filter(|elem| {
                if let Some(search_after) = &search_after {
                    if split_id == search_after.split_id {
                        *elem < (search_after.doc_id as usize)
                    } else {
                        split_id < search_after.split_id.as_str()
                    }
                } else {
                    true
                }
            })
            .skip(hit_range.start)
            .take(hit_range.end - hit_range.start);
        quickwit_proto::search::LeafSearchResponse {
            num_hits: num_total_hits as u64,
            partial_hits: doc_ids
                .map(|doc_id| mock_partial_hit_opt_sort_value(split_id, None, doc_id as u32))
                .collect(),
            num_attempted_splits: 1,
            ..Default::default()
        }
    }

    const TOTAL_NUM_HITS_INDEX_1: usize = 2_005;
    const TOTAL_NUM_HITS_INDEX_2: usize = 10;
    const MAX_HITS_PER_PAGE: usize = 93;
    const MAX_HITS_PER_PAGE_LARGE: usize = 1_005;

    #[tokio::test]
    async fn test_root_search_with_scroll() {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_uid = index_metadata.index_uid.clone();
        let index_metadata_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
        let index_uid_2 = index_metadata_2.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                let indexes_metadata = vec![index_metadata.clone(), index_metadata_2.clone()];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid_2)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        // We add two mock_search_service to simulate a multi node environment, where the requests
        // are forwarded two node.
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_none());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });

        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_none());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, SCROLL_BATCH_LEN);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });

        let kv: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> = Default::default();
        let kv_clone = kv.clone();
        mock_search_service1
            .expect_put_kv()
            .returning(move |put_kv_req| {
                kv_clone
                    .write()
                    .unwrap()
                    .insert(put_kv_req.key, put_kv_req.payload);
            });
        mock_search_service1
            .expect_get_kv()
            .returning(move |get_kv_req| kv.read().unwrap().get(&get_kv_req.key).cloned());

        let kv: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> = Default::default();
        let kv_clone = kv.clone();
        mock_search_service2
            .expect_put_kv()
            .returning(move |put_kv_req| {
                kv_clone
                    .write()
                    .unwrap()
                    .insert(put_kv_req.key, put_kv_req.payload);
            });
        mock_search_service2
            .expect_get_kv()
            .returning(move |get_kv_req| kv.read().unwrap().get(&get_kv_req.key).cloned());

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                assert!(fetch_docs_req.partial_hits.len() <= MAX_HITS_PER_PAGE);
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                assert!(fetch_docs_req.partial_hits.len() <= MAX_HITS_PER_PAGE);
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service1),
            ("127.0.0.1:1002", mock_search_service2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let searcher_context = SearcherContext::for_test();
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let mut count_seen_hits = 0;

        let mut scroll_id: String = {
            let search_request = quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index-*".to_string()],
                query_ast: qast_json_helper("test", &["body"]),
                max_hits: MAX_HITS_PER_PAGE as u64,
                scroll_ttl_secs: Some(60),
                ..Default::default()
            };
            let search_response = root_search(
                &searcher_context,
                search_request,
                MetastoreServiceClient::from_mock(mock_metastore),
                &cluster_client,
            )
            .await
            .unwrap();
            assert_eq!(
                search_response.num_hits,
                (TOTAL_NUM_HITS_INDEX_1 + TOTAL_NUM_HITS_INDEX_2) as u64
            );
            assert_eq!(search_response.hits.len(), MAX_HITS_PER_PAGE);
            let expected = (0..TOTAL_NUM_HITS_INDEX_2)
                .rev()
                .zip(std::iter::repeat("split2"))
                .chain(
                    (0..TOTAL_NUM_HITS_INDEX_1)
                        .rev()
                        .zip(std::iter::repeat("split1")),
                );
            for (hit, (doc_id, split)) in search_response.hits.iter().zip(expected) {
                assert_eq!(
                    hit.partial_hit.as_ref().unwrap(),
                    &mock_partial_hit_opt_sort_value(split, None, doc_id as u32)
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
            let expected = (0..TOTAL_NUM_HITS_INDEX_2)
                .rev()
                .zip(std::iter::repeat("split2"))
                .chain(
                    (0..TOTAL_NUM_HITS_INDEX_1)
                        .rev()
                        .zip(std::iter::repeat("split1")),
                )
                .skip(page * MAX_HITS_PER_PAGE);
            for (hit, (doc_id, split)) in scroll_resp.hits.iter().zip(expected) {
                assert_eq!(
                    hit.partial_hit.as_ref().unwrap(),
                    &mock_partial_hit_opt_sort_value(split, None, doc_id as u32)
                );
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
    async fn test_root_search_with_scroll_large_page() {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_uid = index_metadata.index_uid.clone();
        let index_metadata_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
        let index_uid_2 = index_metadata_2.index_uid.clone();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_index_ids_query| {
                let indexes_metadata = vec![index_metadata.clone(), index_metadata_2.clone()];
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });
        mock_metastore
            .expect_list_splits()
            .returning(move |_filter| {
                let splits = vec![
                    MockSplitBuilder::new("split1")
                        .with_index_uid(&index_uid)
                        .build(),
                    MockSplitBuilder::new("split2")
                        .with_index_uid(&index_uid_2)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        // We add two mock_search_service to simulate a multi node environment, where the requests
        // are forwarded two nodes.
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_none());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        let kv: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> = Default::default();
        let kv_clone = kv.clone();
        mock_search_service1
            .expect_put_kv()
            .returning(move |put_kv_req| {
                kv_clone
                    .write()
                    .unwrap()
                    .insert(put_kv_req.key, put_kv_req.payload);
            });
        mock_search_service1
            .expect_get_kv()
            .returning(move |get_kv_req| kv.read().unwrap().get(&get_kv_req.key).cloned());
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                assert!(fetch_docs_req.partial_hits.len() <= MAX_HITS_PER_PAGE_LARGE);
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_none());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        mock_search_service2
            .expect_leaf_search()
            .times(1)
            .returning(|req: quickwit_proto::search::LeafSearchRequest| {
                let search_req = req.search_request.unwrap();
                // the leaf request does not need to know about the scroll_ttl.
                assert_eq!(search_req.start_offset, 0u64);
                assert!(search_req.scroll_ttl_secs.is_none());
                assert_eq!(search_req.max_hits as usize, MAX_HITS_PER_PAGE_LARGE);
                assert!(search_req.search_after.is_some());
                Ok(create_search_resp(
                    &req.index_uris[0],
                    search_req.start_offset as usize
                        ..(search_req.start_offset + search_req.max_hits) as usize,
                    search_req.search_after,
                ))
            });
        let kv: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> = Default::default();
        let kv_clone = kv.clone();
        mock_search_service2
            .expect_put_kv()
            .returning(move |put_kv_req| {
                kv_clone
                    .write()
                    .unwrap()
                    .insert(put_kv_req.key, put_kv_req.payload);
            });
        mock_search_service2
            .expect_get_kv()
            .returning(move |get_kv_req| kv.read().unwrap().get(&get_kv_req.key).cloned());
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::search::FetchDocsRequest| {
                assert!(fetch_docs_req.partial_hits.len() <= MAX_HITS_PER_PAGE_LARGE);
                Ok(quickwit_proto::search::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service1),
            ("127.0.0.1:1002", mock_search_service2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let searcher_context = SearcherContext::for_test();
        let cluster_client = ClusterClient::new(search_job_placer.clone());

        let mut count_seen_hits = 0;

        let mut scroll_id: String = {
            let search_request = quickwit_proto::search::SearchRequest {
                index_id_patterns: vec!["test-index-*".to_string()],
                query_ast: qast_json_helper("test", &["body"]),
                max_hits: MAX_HITS_PER_PAGE_LARGE as u64,
                scroll_ttl_secs: Some(60),
                ..Default::default()
            };
            let search_response = root_search(
                &searcher_context,
                search_request,
                MetastoreServiceClient::from_mock(mock_metastore),
                &cluster_client,
            )
            .await
            .unwrap();
            assert_eq!(
                search_response.num_hits,
                (TOTAL_NUM_HITS_INDEX_1 + TOTAL_NUM_HITS_INDEX_2) as u64
            );
            assert_eq!(search_response.hits.len(), MAX_HITS_PER_PAGE_LARGE);
            let expected = (0..TOTAL_NUM_HITS_INDEX_2)
                .rev()
                .zip(std::iter::repeat("split2"))
                .chain(
                    (0..TOTAL_NUM_HITS_INDEX_1)
                        .rev()
                        .zip(std::iter::repeat("split1")),
                );
            for (hit, (doc_id, split)) in search_response.hits.iter().zip(expected) {
                assert_eq!(
                    hit.partial_hit.as_ref().unwrap(),
                    &mock_partial_hit_opt_sort_value(split, None, doc_id as u32)
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
            let expected = (0..TOTAL_NUM_HITS_INDEX_2)
                .rev()
                .zip(std::iter::repeat("split2"))
                .chain(
                    (0..TOTAL_NUM_HITS_INDEX_1)
                        .rev()
                        .zip(std::iter::repeat("split1")),
                )
                .skip(page * MAX_HITS_PER_PAGE_LARGE);
            for (hit, (doc_id, split)) in scroll_resp.hits.iter().zip(expected) {
                assert_eq!(
                    hit.partial_hit.as_ref().unwrap(),
                    &mock_partial_hit_opt_sort_value(split, None, doc_id as u32)
                );
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
        let index_uid_1 = index_metadata_1.index_uid.clone();
        let index_metadata_2 =
            index_metadata_for_multi_indexes_test("test-index-2", "ram:///test-index-2");
        let index_uid_2 = index_metadata_2.index_uid.clone();
        let index_metadata_3 =
            index_metadata_for_multi_indexes_test("test-index-3", "ram:///test-index-3");
        let index_uid_3 = index_metadata_3.index_uid.clone();
        mock_metastore.expect_list_indexes_metadata().return_once(
            move |list_indexes_metadata_request: ListIndexesMetadataRequest| {
                let index_id_patterns = list_indexes_metadata_request.index_id_patterns;
                assert_eq!(&index_id_patterns, &["test-index-*".to_string()]);
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    index_metadata_1,
                    index_metadata_2,
                    index_metadata_3,
                ]))
            },
        );
        mock_metastore
            .expect_list_splits()
            .return_once(move |list_splits_request| {
                let list_splits_query =
                    list_splits_request.deserialize_list_splits_query().unwrap();
                assert!(
                    list_splits_query.index_uids
                        == vec![
                            index_uid_1.clone(),
                            index_uid_2.clone(),
                            index_uid_3.clone()
                        ]
                );
                let splits = vec![
                    MockSplitBuilder::new("index-1-split-1")
                        .with_index_uid(&index_uid_1)
                        .build(),
                    MockSplitBuilder::new("index-1-split-2")
                        .with_index_uid(&index_uid_1)
                        .build(),
                    MockSplitBuilder::new("index-2-split-1")
                        .with_index_uid(&index_uid_2)
                        .build(),
                ];
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search()
            .times(1)
            .withf(|leaf_search_req| {
                (&leaf_search_req.index_uris[0] == "ram:///test-index-1"
                    && leaf_search_req.leaf_requests[0].split_offsets.len() == 2)
                    || (leaf_search_req.index_uris[0] == "ram:///test-index-2"
                        && leaf_search_req.leaf_requests[0].split_offsets[0].split_id
                            == "index-2-split-1")
            })
            .returning(
                |leaf_search_req: quickwit_proto::search::LeafSearchRequest| {
                    let mut partial_hits = leaf_search_req.leaf_requests[0]
                        .split_offsets
                        .iter()
                        .map(|split_offset| mock_partial_hit(&split_offset.split_id, 3, 1))
                        .collect_vec();
                    let partial_hits2 = leaf_search_req.leaf_requests[1]
                        .split_offsets
                        .iter()
                        .map(|split_offset| mock_partial_hit(&split_offset.split_id, 3, 1))
                        .collect_vec();
                    partial_hits.extend_from_slice(&partial_hits2);
                    Ok(quickwit_proto::search::LeafSearchResponse {
                        num_hits: leaf_search_req.leaf_requests[0].split_offsets.len() as u64
                            + leaf_search_req.leaf_requests[1].split_offsets.len() as u64,
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
            MetastoreServiceClient::from_mock(mock_metastore),
            &cluster_client,
        )
        .await
        .unwrap();
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
        assert_eq!(
            search_response
                .hits
                .iter()
                .map(|hit| &hit.index_id)
                .collect_vec(),
            vec!["test-index-1", "test-index-1", "test-index-2"]
        );
        Ok(())
    }
}
