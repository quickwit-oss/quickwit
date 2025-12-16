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

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use itertools::{Either, Itertools};
use quickwit_common::pretty::PrettySample;
use quickwit_config::build_doc_mapper;
use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::search::{
    LeafListTermsRequest, LeafListTermsResponse, ListTermsRequest, ListTermsResponse,
    SplitIdAndFooterOffsets, SplitSearchError,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{ByteRangeCache, Storage};
use tantivy::schema::{Field, FieldType};
use tantivy::{ReloadPolicy, Term};
use tracing::{debug, error, info, instrument};

use crate::leaf::open_index_with_caches;
use crate::search_job_placer::group_jobs_by_index_id;
use crate::search_permit_provider::compute_initial_memory_allocation;
use crate::{ClusterClient, SearchError, SearchJob, SearcherContext, resolve_index_patterns};

/// Performs a distributed list terms.
/// 1. Sends leaf requests over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
/// this is much simpler than `root_search` as it doesn't need to get actual docs.
#[instrument(skip(list_terms_request, cluster_client, metastore))]
pub async fn root_list_terms(
    list_terms_request: &ListTermsRequest,
    mut metastore: MetastoreServiceClient,
    cluster_client: &ClusterClient,
) -> crate::Result<ListTermsResponse> {
    let start_instant = tokio::time::Instant::now();
    let indexes_metadata =
        resolve_index_patterns(&list_terms_request.index_id_patterns, &mut metastore).await?;
    // The request contains a wildcard, but couldn't find any index.
    if indexes_metadata.is_empty() {
        return Ok(ListTermsResponse {
            num_hits: 0,
            terms: Vec::new(),
            elapsed_time_micros: 0,
            errors: Vec::new(),
        });
    }

    for index_metadata in indexes_metadata.iter() {
        let index_config = &index_metadata.index_config;
        let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
            .map_err(|err| {
                SearchError::Internal(format!("failed to build doc mapper. cause: {err}"))
            })?;
        let schema = doc_mapper.schema();
        let field = schema.get_field(&list_terms_request.field).map_err(|_| {
            SearchError::InvalidQuery(format!(
                "failed to list terms in `{}`, field doesn't exist",
                list_terms_request.field
            ))
        })?;
        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_indexed() {
            return Err(SearchError::InvalidQuery(
                "trying to list terms on field which isn't indexed".to_string(),
            ));
        }
    }
    let index_uids: Vec<IndexUid> = indexes_metadata
        .iter()
        .map(|index_metadata| index_metadata.index_uid.clone())
        .collect();

    let Some(mut query) = quickwit_metastore::ListSplitsQuery::try_from_index_uids(index_uids)
    else {
        return Ok(ListTermsResponse::default());
    };
    query = query.with_split_state(quickwit_metastore::SplitState::Published);

    if let Some(start_ts) = list_terms_request.start_timestamp {
        query = query.with_time_range_start_gte(start_ts);
    }

    if let Some(end_ts) = list_terms_request.end_timestamp {
        query = query.with_time_range_end_lt(end_ts);
    }
    let index_uid_to_index_uri: HashMap<IndexUid, String> = indexes_metadata
        .iter()
        .map(|index_metadata| {
            (
                index_metadata.index_uid.clone(),
                index_metadata.index_uri().to_string(),
            )
        })
        .collect();
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let split_metadatas: Vec<SplitMetadata> = metastore
        .clone()
        .list_splits(list_splits_request)
        .await?
        .collect_splits_metadata()
        .await?;

    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let mut leaf_request_tasks = Vec::new();
    // For each node, forward to a node with an affinity for that index id.
    for (client, client_jobs) in assigned_leaf_search_jobs {
        let leaf_requests =
            jobs_to_leaf_requests(list_terms_request, &index_uid_to_index_uri, client_jobs)?;
        for leaf_request in leaf_requests {
            leaf_request_tasks.push(cluster_client.leaf_list_terms(leaf_request, client.clone()));
        }
    }
    let leaf_search_responses: Vec<LeafListTermsResponse> =
        try_join_all(leaf_request_tasks).await?;

    let failed_splits: Vec<_> = leaf_search_responses
        .iter()
        .flat_map(|leaf_search_response| &leaf_search_response.failed_splits)
        .collect();

    if !failed_splits.is_empty() {
        error!(failed_splits = ?failed_splits, "leaf search response contains at least one failed split");
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

    debug!(
        leaf_list_terms_response_count = leaf_list_terms_response.len(),
        "Merged leaf search response."
    );

    let elapsed = start_instant.elapsed();

    Ok(ListTermsResponse {
        num_hits: leaf_list_terms_response.len() as u64,
        terms: leaf_list_terms_response,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: Vec::new(),
    })
}

/// Builds a list of [`LeafListTermsRequest`], one per index, from a list of [`SearchJob`].
pub fn jobs_to_leaf_requests(
    request: &ListTermsRequest,
    index_uid_to_uri: &HashMap<IndexUid, String>,
    jobs: Vec<SearchJob>,
) -> crate::Result<Vec<LeafListTermsRequest>> {
    let search_request_for_leaf = request.clone();
    let mut leaf_search_requests = Vec::new();
    group_jobs_by_index_id(jobs, |job_group| {
        let index_uid = &job_group[0].index_uid;
        let index_uri = index_uid_to_uri.get(index_uid).ok_or_else(|| {
            SearchError::Internal(format!(
                "received list fields job for an unknown index {index_uid}. it should never happen"
            ))
        })?;

        let leaf_search_request = LeafListTermsRequest {
            list_terms_request: Some(search_request_for_leaf.clone()),
            index_uri: index_uri.to_string(),
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
        };
        leaf_search_requests.push(leaf_search_request);
        Ok(())
    })?;
    Ok(leaf_search_requests)
}

/// Apply a leaf list terms on a single split.
#[instrument(skip_all, fields(split_id = split.split_id))]
#[allow(deprecated)]
async fn leaf_list_terms_single_split(
    searcher_context: &SearcherContext,
    search_request: &ListTermsRequest,
    storage: Arc<dyn Storage>,
    split: SplitIdAndFooterOffsets,
) -> crate::Result<LeafListTermsResponse> {
    let cache =
        ByteRangeCache::with_infinite_capacity(&quickwit_storage::STORAGE_METRICS.shortlived_cache);
    let (index, _) =
        open_index_with_caches(searcher_context, storage, &split, None, Some(cache)).await?;
    let split_schema = index.schema();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    let field = split_schema
        .get_field(&search_request.field)
        .with_context(|| {
            format!(
                "couldn't get field named {:?} from schema to list terms",
                search_request.field
            )
        })?;

    let field_type = split_schema.get_field_entry(field).field_type();
    let start_term: Option<Term> = search_request
        .start_key
        .as_ref()
        .map(|data| term_from_data(field, field_type, data));
    let end_term: Option<Term> = search_request
        .end_key
        .as_ref()
        .map(|data| term_from_data(field, field_type, data));

    let mut segment_results = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let inverted_index = segment_reader.inverted_index(field)?.clone();
        let dict = inverted_index.terms();
        dict.file_slice_for_range(
            (
                start_term
                    .as_ref()
                    .map(Term::serialized_value_bytes)
                    .map(Bound::Included)
                    .unwrap_or(Bound::Unbounded),
                end_term
                    .as_ref()
                    .map(Term::serialized_value_bytes)
                    .map(Bound::Excluded)
                    .unwrap_or(Bound::Unbounded),
            ),
            search_request.max_hits,
        )
        .read_bytes_async()
        .await
        .with_context(|| "failed to load sstable range")?;

        let mut range = dict.range();
        if let Some(limit) = search_request.max_hits {
            range = range.limit(limit);
        }
        if let Some(start_term) = &start_term {
            range = range.ge(start_term.serialized_value_bytes())
        }
        if let Some(end_term) = &end_term {
            range = range.lt(end_term.serialized_value_bytes())
        }
        let mut stream = range
            .into_stream()
            .with_context(|| "failed to create stream over sstable")?;
        let mut segment_result: Vec<Vec<u8>> =
            Vec::with_capacity(search_request.max_hits.unwrap_or(0) as usize);
        while stream.advance() {
            segment_result.push(term_to_data(field, field_type, stream.key()));
        }
        segment_results.push(segment_result);
    }

    let merged_iter = segment_results.into_iter().kmerge().dedup();
    let merged_results: Vec<Vec<u8>> = if let Some(limit) = search_request.max_hits {
        merged_iter.take(limit as usize).collect()
    } else {
        merged_iter.collect()
    };

    Ok(LeafListTermsResponse {
        num_hits: merged_results.len() as u64,
        terms: merged_results,
        num_attempted_splits: 1,
        failed_splits: Vec::new(),
    })
}

fn term_from_data(field: Field, field_type: &FieldType, data: &[u8]) -> Term {
    let mut term = Term::from_field_bool(field, false);
    term.clear_with_type(field_type.value_type());
    term.append_bytes(data);
    term
}

#[allow(deprecated)]
fn term_to_data(field: Field, field_type: &FieldType, field_value: &[u8]) -> Vec<u8> {
    let mut term = Term::from_field_bool(field, false);
    term.clear_with_type(field_type.value_type());
    term.append_bytes(field_value);
    term.serialized_term().to_vec()
}

/// `leaf` step of list terms.
#[instrument(skip_all)]
pub async fn leaf_list_terms(
    searcher_context: Arc<SearcherContext>,
    request: &ListTermsRequest,
    index_storage: Arc<dyn Storage>,
    splits: &[SplitIdAndFooterOffsets],
) -> Result<LeafListTermsResponse, SearchError> {
    info!(split_offsets = ?PrettySample::new(splits, 5));
    let permit_sizes = splits.iter().map(|split| {
        compute_initial_memory_allocation(
            split,
            searcher_context
                .searcher_config
                .warmup_single_split_initial_allocation,
        )
    });
    let permits = searcher_context
        .search_permit_provider
        .get_permits(permit_sizes)
        .await;
    let leaf_search_single_split_futures: Vec<_> = splits
        .iter()
        .zip(permits.into_iter())
        .map(|(split, search_permit_recv)| {
            let index_storage_clone = index_storage.clone();
            let searcher_context_clone = searcher_context.clone();
            async move {
                let leaf_split_search_permit = search_permit_recv.await;
                // TODO dedicated counter and timer?
                crate::SEARCH_METRICS.leaf_list_terms_splits_total.inc();
                let timer = crate::SEARCH_METRICS
                    .leaf_search_split_duration_secs
                    .start_timer();
                let leaf_search_single_split_res = leaf_list_terms_single_split(
                    &searcher_context_clone,
                    request,
                    index_storage_clone,
                    split.clone(),
                )
                .await;
                timer.observe_duration();

                // Explicitly drop the permit for readability.
                // This should always happen after the ephemeral search cache is dropped.
                std::mem::drop(leaf_split_search_permit);

                leaf_search_single_split_res.map_err(|err| (split.split_id.clone(), err))
            }
        })
        .collect();

    let split_search_results = futures::future::join_all(leaf_search_single_split_futures).await;

    let (split_search_responses, errors): (Vec<LeafListTermsResponse>, Vec<(String, SearchError)>) =
        split_search_results
            .into_iter()
            .partition_map(|split_search_res| match split_search_res {
                Ok(split_search_resp) => Either::Left(split_search_resp),
                Err(err) => Either::Right(err),
            });

    let merged_iter = split_search_responses
        .into_iter()
        .map(|leaf_search_response| leaf_search_response.terms)
        .kmerge()
        .dedup();
    let terms: Vec<Vec<u8>> = if let Some(limit) = request.max_hits {
        merged_iter.take(limit as usize).collect()
    } else {
        merged_iter.collect()
    };

    let failed_splits = errors
        .into_iter()
        .map(|(split_id, err)| SplitSearchError {
            split_id,
            error: err.to_string(),
            retryable_error: true,
        })
        .collect();
    let merged_search_response = LeafListTermsResponse {
        num_hits: terms.len() as u64,
        terms,
        num_attempted_splits: splits.len() as u64,
        failed_splits,
    };

    Ok(merged_search_response)
}
