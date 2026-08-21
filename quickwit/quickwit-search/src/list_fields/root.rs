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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use anyhow::Context;
use futures::future::try_join_all;
use quickwit_common::rate_limited_warn;
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{
    LeafListFieldsRequest, ListFieldsEntry, ListFieldsRequest, ListFieldsResponse,
};
use quickwit_proto::types::{IndexId, IndexUid};
use quickwit_query::query_ast::QueryAst;
use tracing::{Span, instrument};

use crate::list_fields::{merge_entries, sort_and_dedup};
use crate::search_job_placer::group_jobs_by_index_id;
use crate::{
    ClusterClient, SearchError, SearchJob, list_relevant_splits, resolve_index_patterns,
    search_thread_pool,
};

/// Index metas needed for executing a leaf list fields request.
#[derive(Clone, Debug)]
struct IndexMetasForLeafSearch {
    /// Index id.
    index_id: IndexId,
    /// Index URI.
    index_uri: Uri,
}

/// Performs a distributed list fields request.
/// 1. Sends leaf requests over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
#[instrument(skip_all, fields(index_id_patterns = ?list_fields_req.index_id_patterns))]
pub async fn root_list_fields(
    list_fields_req: ListFieldsRequest,
    cluster_client: &ClusterClient,
    metastore: &MetastoreServiceClient,
) -> crate::Result<ListFieldsResponse> {
    let indexes_metadata =
        resolve_index_patterns(&list_fields_req.index_id_patterns[..], metastore).await?;

    // The request contains a wildcard, but couldn't find any index.
    if indexes_metadata.is_empty() {
        return Ok(ListFieldsResponse::default());
    }
    // Build index metadata map and extract timestamp field for time range refinement
    let mut index_uid_to_index_meta: HashMap<IndexUid, IndexMetasForLeafSearch> = HashMap::new();
    let mut index_uids: Vec<IndexUid> = Vec::new();
    let mut timestamp_field_opt: Option<String> = None;

    for index_metadata in indexes_metadata {
        // Extract timestamp field for time range refinement (use first index's field)
        if timestamp_field_opt.is_none()
            && list_fields_req.query_ast.is_some()
            && let Ok(doc_mapper) = build_doc_mapper(
                &index_metadata.index_config.doc_mapping,
                &index_metadata.index_config.search_settings,
            )
        {
            timestamp_field_opt = doc_mapper.timestamp_field_name().map(|s| s.to_string());
        }
        let index_metadata_for_leaf_search = IndexMetasForLeafSearch {
            index_uri: index_metadata.index_uri().clone(),
            index_id: index_metadata.index_config.index_id.to_string(),
        };
        index_uids.push(index_metadata.index_uid.clone());

        index_uid_to_index_meta.insert(
            index_metadata.index_uid.clone(),
            index_metadata_for_leaf_search,
        );
    }
    // Extract tags and refine time range from query_ast for split pruning
    let mut start_timestamp = list_fields_req.start_timestamp;
    let mut end_timestamp = list_fields_req.end_timestamp;
    let tags_filter_opt = if let Some(ref query_ast_json) = list_fields_req.query_ast {
        let query_ast: QueryAst = serde_json::from_str(query_ast_json)
            .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;

        // Refine time range from query AST if timestamp field is available
        if let Some(ref timestamp_field) = timestamp_field_opt {
            crate::root::refine_start_end_timestamp_from_ast(
                &query_ast,
                timestamp_field,
                &mut start_timestamp,
                &mut end_timestamp,
            );
        }
        extract_tags_from_query(query_ast)
    } else {
        None
    };
    let split_metadatas: Vec<SplitMetadata> = list_relevant_splits(
        index_uids,
        start_timestamp,
        end_timestamp,
        tags_filter_opt,
        metastore,
    )
    .await?;

    // Build requests for each index id
    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;

    let mut leaf_request_futures = Vec::new();
    // For each node, forward to a node with an affinity for that index id.
    for (client, client_jobs) in assigned_leaf_search_jobs {
        let leaf_requests =
            jobs_to_leaf_requests(&list_fields_req, &index_uid_to_index_meta, client_jobs)?;
        for leaf_request in leaf_requests {
            leaf_request_futures
                .push(cluster_client.leaf_list_fields(leaf_request, client.clone()));
        }
    }
    let leaf_responses: Vec<ListFieldsResponse> = try_join_all(leaf_request_futures).await?;
    let leaf_entries: Vec<Vec<ListFieldsEntry>> = leaf_responses
        .into_iter()
        .map(|response| response.entries)
        .collect();
    let merged_entries = merge_fields_metadata(leaf_entries).await?;
    let response = ListFieldsResponse {
        entries: merged_entries,
    };
    Ok(response)
}

/// Builds a list of [`LeafListFieldsRequest`], one per index, from a list of [`SearchJob`].
fn jobs_to_leaf_requests(
    request: &ListFieldsRequest,
    index_uid_to_id: &HashMap<IndexUid, IndexMetasForLeafSearch>,
    jobs: Vec<SearchJob>,
) -> crate::Result<Vec<LeafListFieldsRequest>> {
    let search_request_for_leaf = request.clone();
    let mut leaf_search_requests = Vec::new();
    // Group jobs by index uid.
    group_jobs_by_index_id(jobs, |job_group| {
        let index_uid = &job_group[0].index_uid;
        let index_meta = index_uid_to_id.get(index_uid).ok_or_else(|| {
            SearchError::Internal(format!(
                "received list fields job for an unknown index {index_uid}. it should never happen"
            ))
        })?;

        let leaf_search_request = LeafListFieldsRequest {
            index_id: index_meta.index_id.to_string(),
            index_uri: index_meta.index_uri.to_string(),
            field_patterns: search_request_for_leaf.field_patterns.clone(),
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
        };
        leaf_search_requests.push(leaf_search_request);
        Ok(())
    })?;

    Ok(leaf_search_requests)
}

#[instrument(skip_all, fields(num_leaves = entry_groups.len()))]
async fn merge_fields_metadata(
    mut entry_groups: Vec<Vec<ListFieldsEntry>>,
) -> crate::Result<Vec<ListFieldsEntry>> {
    let parent_span = Span::current();
    search_thread_pool()
        .run_cpu_intensive(move || {
            parent_span.in_scope(|| {
                for entry_group in &mut entry_groups {
                    if !entry_group.is_sorted_by(|left, right| {
                        // Checking on less ensure that this is both sorted AND that there are no
                        // duplicates
                        left.cmp_by_name_and_type(right) == Ordering::Less
                    }) {
                        rate_limited_warn!(
                            limit_per_min = 1,
                            "fields metadata returned by a leaf is not strictly sorted, please \
                             report"
                        );
                        sort_and_dedup(entry_group);
                    }
                }
                merge_entries(entry_groups)
            })
        })
        .await
        .context("failed to merge leaf list fields responses")?
}
