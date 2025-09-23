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
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use futures::future;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::rate_limited_warn;
use quickwit_common::shared_consts::{FIELD_PRESENCE_FIELD_NAME, SPLIT_FIELDS_FILE_NAME};
use quickwit_common::uri::Uri;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::search::{
    LeafListFieldsRequest, ListFields, ListFieldsEntryResponse, ListFieldsRequest,
    ListFieldsResponse, SplitIdAndFooterOffsets, deserialize_split_fields,
};
use quickwit_proto::types::{IndexId, IndexUid};
use quickwit_storage::Storage;

use crate::leaf::open_split_bundle;
use crate::search_job_placer::group_jobs_by_index_id;
use crate::service::SearcherContext;
use crate::{
    ClusterClient, SearchError, SearchJob, list_relevant_splits, resolve_index_patterns,
    search_thread_pool,
};

/// QW_FIELD_LIST_SIZE_LIMIT defines a hard limit on the number of fields that
/// can be returned (error otherwise).
///
/// Having many fields can happen when a user is creating fields dynamically in
/// a JSON type with random field names. This leads to huge memory consumption
/// when building the response. This is a workaround until a way is found to
/// prune the long tail of rare fields.
static FIELD_LIST_SIZE_LIMIT: LazyLock<usize> =
    LazyLock::new(|| quickwit_common::get_from_env("QW_FIELD_LIST_SIZE_LIMIT", 100_000, false));

const DYNAMIC_FIELD_PREFIX: &str = "_dynamic.";

/// Get the list of fields in the given split.
/// The returned list is guaranteed to be strictly sorted by (field_name, field_type).
async fn get_fields_from_split(
    searcher_context: &SearcherContext,
    index_id: IndexId,
    split_and_footer_offsets: &SplitIdAndFooterOffsets,
    index_storage: Arc<dyn Storage>,
) -> anyhow::Result<Vec<ListFieldsEntryResponse>> {
    if let Some(list_fields) = searcher_context
        .list_fields_cache
        .get(split_and_footer_offsets.clone())
    {
        return Ok(list_fields.fields);
    }
    let (_, split_bundle) =
        open_split_bundle(searcher_context, index_storage, split_and_footer_offsets).await?;

    let serialized_split_fields = split_bundle
        .get_all(Path::new(SPLIT_FIELDS_FILE_NAME))
        .await?;
    let serialized_split_fields_len = serialized_split_fields.len();
    let list_fields_proto =
        deserialize_split_fields(serialized_split_fields).with_context(|| {
            format!("could not read split fields (serialized len: {serialized_split_fields_len})",)
        })?;

    let mut list_fields = list_fields_proto.fields;
    list_fields.retain(|list_field_entry| list_field_entry.field_name != FIELD_PRESENCE_FIELD_NAME);

    for list_field_entry in list_fields.iter_mut() {
        list_field_entry.index_ids = vec![index_id.to_string()];

        if list_field_entry
            .field_name
            .starts_with(DYNAMIC_FIELD_PREFIX)
        {
            list_field_entry
                .field_name
                .replace_range(..DYNAMIC_FIELD_PREFIX.len(), "");
        }
    }

    // We sort our fields, as the removal of dynamic_field prefix could have caused them to be out
    // of order. We also defensively make sure there are no duplicates here.
    make_sorted_and_dedup(&mut list_fields);

    // Put result into cache
    searcher_context.list_fields_cache.put(
        split_and_footer_offsets.clone(),
        ListFields {
            fields: list_fields.clone(),
        },
    );

    Ok(list_fields)
}

fn field_order(
    left: &ListFieldsEntryResponse,
    right: &ListFieldsEntryResponse,
) -> std::cmp::Ordering {
    left.field_name
        .cmp(&right.field_name)
        .then_with(|| left.field_type.cmp(&right.field_type))
}

// Sorts and deduplicates the list of fields.
//
// If somehow we end up with duplicate fields, only the first one is kept,
// and we log a warning.
fn make_sorted_and_dedup(list_fields: &mut Vec<ListFieldsEntryResponse>) {
    list_fields.sort_unstable_by(field_order);

    // We defensively make sure there are no duplicates here.
    list_fields.dedup_by(|left, right| {
        if left.field_name == right.field_name && left.field_type == right.field_type {
            rate_limited_warn!(
                limit_per_min = 1,
                left.field_name,
                "duplicate fields found, please report"
            );
            true
        } else {
            false
        }
    });
}

/// `current_group` needs to contain at least one element.
/// The group needs to be of the same field name and type.
fn merge_same_field_group(
    current_group: &mut Vec<ListFieldsEntryResponse>,
) -> ListFieldsEntryResponse {
    // Make sure all fields have the same name and type in current_group
    assert!(!current_group.is_empty());
    assert!(
        current_group
            .windows(2)
            .all(|window| window[0].field_name == window[1].field_name
                && window[0].field_type == window[1].field_type)
    );

    if current_group.len() == 1 {
        return current_group
            .pop()
            .expect("`current_group` should not be empty");
    }
    let metadata = current_group
        .last()
        .expect("`current_group` should not be empty");
    let searchable = current_group.iter().any(|entry| entry.searchable);
    let aggregatable = current_group.iter().any(|entry| entry.aggregatable);
    let field_name = metadata.field_name.to_string();
    let field_type = metadata.field_type;
    let mut non_searchable_index_ids = if searchable {
        // We need to combine the non_searchable_index_ids + index_ids where searchable is set to
        // false (as they are all non_searchable)
        current_group
            .iter()
            .flat_map(|entry| {
                if !entry.searchable {
                    entry.index_ids.iter().cloned()
                } else {
                    entry.non_searchable_index_ids.iter().cloned()
                }
            })
            .collect()
    } else {
        // Not searchable => no need to list all the indices
        Vec::new()
    };
    non_searchable_index_ids.sort_unstable();
    non_searchable_index_ids.dedup();

    let mut non_aggregatable_index_ids = if aggregatable {
        // We need to combine the non_aggregatable_index_ids + index_ids where aggregatable is set
        // to false (as they are all non_aggregatable)
        current_group
            .iter()
            .flat_map(|entry| {
                if !entry.aggregatable {
                    entry.index_ids.iter().cloned()
                } else {
                    entry.non_aggregatable_index_ids.iter().cloned()
                }
            })
            .collect()
    } else {
        // Not aggregatable => no need to list all the indices
        Vec::new()
    };
    non_aggregatable_index_ids.sort_unstable();
    non_aggregatable_index_ids.dedup();
    let mut index_ids: Vec<String> = current_group
        .drain(..)
        .flat_map(|entry| entry.index_ids.into_iter())
        .collect();
    index_ids.sort_unstable();
    index_ids.dedup();

    ListFieldsEntryResponse {
        field_name,
        field_type,
        searchable,
        aggregatable,
        non_searchable_index_ids,
        non_aggregatable_index_ids,
        index_ids,
    }
}

/// Merge iterators of ListFieldsEntryResponse into a `Vec<ListFieldsEntryResponse>`.
///
/// The iterators need to be sorted by (field_name, fieldtype)
fn merge_leaf_list_fields(
    iterators: Vec<impl Iterator<Item = ListFieldsEntryResponse>>,
) -> crate::Result<Vec<ListFieldsEntryResponse>> {
    let merged = iterators
        .into_iter()
        .kmerge_by(|a, b| (&a.field_name, a.field_type) <= (&b.field_name, b.field_type));
    let mut responses = Vec::new();

    let mut current_group: Vec<ListFieldsEntryResponse> = Vec::new();
    // Build ListFieldsEntryResponse from current group
    let flush_group = |responses: &mut Vec<_>, current_group: &mut Vec<ListFieldsEntryResponse>| {
        let entry = merge_same_field_group(current_group);
        responses.push(entry);
        current_group.clear();
    };

    for entry in merged {
        if let Some(last) = current_group.last()
            && (last.field_name != entry.field_name || last.field_type != entry.field_type)
        {
            flush_group(&mut responses, &mut current_group);
        }
        if responses.len() >= *FIELD_LIST_SIZE_LIMIT {
            return Err(SearchError::Internal(format!(
                "list fields response exceeded {} fields",
                *FIELD_LIST_SIZE_LIMIT
            )));
        }
        current_group.push(entry);
    }
    if !current_group.is_empty() {
        flush_group(&mut responses, &mut current_group);
    }

    Ok(responses)
}

// Returns true if any of the patterns match the field name.
fn matches_any_pattern(field_name: &str, field_patterns: &[FieldPattern]) -> bool {
    field_patterns
        .iter()
        .any(|pattern| pattern.matches(field_name))
}

enum FieldPattern {
    Match { field: String },
    Wildcard { prefix: String, suffix: String },
}

impl FromStr for FieldPattern {
    type Err = crate::SearchError;

    fn from_str(field_pattern: &str) -> crate::Result<Self> {
        match field_pattern.find('*') {
            None => Ok(FieldPattern::Match {
                field: field_pattern.to_string(),
            }),
            Some(pos) => {
                let prefix = field_pattern[..pos].to_string();
                let suffix = field_pattern[pos + 1..].to_string();
                if suffix.contains("*") {
                    return Err(crate::SearchError::InvalidArgument(format!(
                        "invalid field pattern `{field_pattern}`: we only support one wildcard"
                    )));
                }
                Ok(FieldPattern::Wildcard { prefix, suffix })
            }
        }
    }
}

impl FieldPattern {
    pub fn matches(&self, field_name: &str) -> bool {
        match self {
            FieldPattern::Match { field } => field == field_name,
            FieldPattern::Wildcard { prefix, suffix } => {
                field_name.starts_with(prefix) && field_name.ends_with(suffix)
            }
        }
    }
}

/// `leaf` step of list fields.
pub async fn leaf_list_fields(
    index_id: IndexId,
    index_storage: Arc<dyn Storage>,
    searcher_context: &SearcherContext,
    split_ids: &[SplitIdAndFooterOffsets],
    field_patterns_str: &[String],
) -> crate::Result<ListFieldsResponse> {
    let field_patterns: Vec<FieldPattern> = field_patterns_str
        .iter()
        .map(|pattern_str| FieldPattern::from_str(pattern_str))
        .collect::<crate::Result<_>>()?;

    let single_split_list_fields_futures: Vec<_> = split_ids
        .iter()
        .map(|split_id| {
            get_fields_from_split(
                searcher_context,
                index_id.to_string(),
                split_id,
                index_storage.clone(),
            )
        })
        .collect();

    let mut single_split_list_fields_vec: Vec<Vec<ListFieldsEntryResponse>> =
        future::try_join_all(single_split_list_fields_futures).await?;

    let fields = search_thread_pool()
        .run_cpu_intensive(move || {
            for single_split_list_fields in &mut single_split_list_fields_vec {
                // This contract is enforced on a different node, etc. so we defensively check that
                // the fields are sorted and deduplicated.
                if !single_split_list_fields.is_sorted_by(|left, right| {
                    // Checking on less ensure that this is both sorted AND that there are no
                    // duplicates
                    field_order(left, right) == std::cmp::Ordering::Less
                }) {
                    rate_limited_warn!(
                        limit_per_min = 1,
                        "contract breach: fields returned by a leaf are not strictly sorted! \
                         please report"
                    );
                    make_sorted_and_dedup(single_split_list_fields);
                }
            }

            let filtered_list_fields_sorted_iters: Vec<_> = single_split_list_fields_vec
                .into_iter()
                .map(|list_fields_sorted| {
                    list_fields_sorted.into_iter().filter(|field| {
                        if field_patterns.is_empty() {
                            true
                        } else {
                            matches_any_pattern(&field.field_name, &field_patterns)
                        }
                    })
                })
                .collect();
            merge_leaf_list_fields(filtered_list_fields_sorted_iters)
        })
        .await
        .context("failed to merge single split list fields")??;
    Ok(ListFieldsResponse { fields })
}

/// Index metas needed for executing a leaf search request.
#[derive(Clone, Debug)]
pub struct IndexMetasForLeafSearch {
    /// Index id.
    pub index_id: IndexId,
    /// Index URI.
    pub index_uri: Uri,
}

/// Performs a distributed list fields request.
/// 1. Sends leaf requests over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
pub async fn root_list_fields(
    list_fields_req: ListFieldsRequest,
    cluster_client: &ClusterClient,
    mut metastore: MetastoreServiceClient,
) -> crate::Result<ListFieldsResponse> {
    let indexes_metadata =
        resolve_index_patterns(&list_fields_req.index_id_patterns[..], &mut metastore).await?;
    // The request contains a wildcard, but couldn't find any index.
    if indexes_metadata.is_empty() {
        return Ok(ListFieldsResponse { fields: Vec::new() });
    }
    let index_uid_to_index_meta: HashMap<IndexUid, IndexMetasForLeafSearch> = indexes_metadata
        .iter()
        .map(|index_metadata| {
            let index_metadata_for_leaf_search = IndexMetasForLeafSearch {
                index_uri: index_metadata.index_uri().clone(),
                index_id: index_metadata.index_config.index_id.to_string(),
            };

            (
                index_metadata.index_uid.clone(),
                index_metadata_for_leaf_search,
            )
        })
        .collect();
    let index_uids: Vec<IndexUid> = indexes_metadata
        .into_iter()
        .map(|index_metadata| index_metadata.index_uid)
        .collect();
    let split_metadatas: Vec<SplitMetadata> = list_relevant_splits(
        index_uids,
        list_fields_req.start_timestamp,
        list_fields_req.end_timestamp,
        None,
        &mut metastore,
    )
    .await?;

    // Build requests for each index id
    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let mut leaf_request_tasks = Vec::new();
    // For each node, forward to a node with an affinity for that index id.
    for (client, client_jobs) in assigned_leaf_search_jobs {
        let leaf_requests =
            jobs_to_leaf_requests(&list_fields_req, &index_uid_to_index_meta, client_jobs)?;
        for leaf_request in leaf_requests {
            leaf_request_tasks.push(cluster_client.leaf_list_fields(leaf_request, client.clone()));
        }
    }
    let leaf_list_fields_protos: Vec<ListFieldsResponse> = try_join_all(leaf_request_tasks).await?;
    let fields = search_thread_pool()
        .run_cpu_intensive(move || {
            let leaf_list_fields = leaf_list_fields_protos
                .into_iter()
                .map(|leaf_list_fields_proto| leaf_list_fields_proto.fields.into_iter())
                .collect();
            merge_leaf_list_fields(leaf_list_fields)
        })
        .await
        .context("failed to merge leaf list fields responses")??;

    Ok(ListFieldsResponse { fields })
}

/// Builds a list of [`LeafListFieldsRequest`], one per index, from a list of [`SearchJob`].
pub fn jobs_to_leaf_requests(
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
            fields: search_request_for_leaf.fields.clone(),
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
        };
        leaf_search_requests.push(leaf_search_request);
        Ok(())
    })?;

    Ok(leaf_search_requests)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{ListFieldType, ListFieldsEntryResponse};

    use super::*;

    #[test]
    fn merge_leaf_list_fields_identical_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1]);
    }
    #[test]
    fn merge_leaf_list_fields_different_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field2".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1, entry2]);
    }
    #[test]
    fn merge_leaf_list_fields_non_searchable_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index2".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string(), "index2".to_string()],
        };
        assert_eq!(resp, vec![expected]);
    }
    #[test]
    fn merge_leaf_list_fields_non_aggregatable_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: false,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: vec!["index2".to_string()],
            index_ids: vec!["index1".to_string(), "index2".to_string()],
        };
        assert_eq!(resp, vec![expected]);
    }
    #[test]
    fn merge_leaf_list_fields_mixed_types1() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry2.clone()].into_iter(),
            vec![entry3.clone()].into_iter(),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_mixed_types2() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry3.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_multiple_field_names() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field2".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry3.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_non_aggregatable_list_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index1".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec![
                "index1".to_string(),
                "index2".to_string(),
                "index3".to_string(),
            ],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index4".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter(),
            vec![entry2.clone()].into_iter(),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: ListFieldType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index1".to_string(), "index4".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec![
                "index1".to_string(),
                "index2".to_string(),
                "index3".to_string(),
                "index4".to_string(),
            ],
        };
        assert_eq!(resp, vec![expected]);
    }

    #[test]
    fn test_field_pattern() {
        let prefix_pattern = FieldPattern::from_str("toto*").unwrap();
        assert!(!prefix_pattern.matches(""));
        assert!(!prefix_pattern.matches("tot3"));
        assert!(!prefix_pattern.matches("atoto"));
        assert!(prefix_pattern.matches("toto"));
        assert!(prefix_pattern.matches("totowhatever"));

        let suffix_pattern = FieldPattern::from_str("*toto").unwrap();
        assert!(!suffix_pattern.matches(""));
        assert!(!suffix_pattern.matches("3tot"));
        assert!(!suffix_pattern.matches("totoa"));
        assert!(suffix_pattern.matches("toto"));
        assert!(suffix_pattern.matches("whatevertoto"));

        let inner_pattern = FieldPattern::from_str("to*ti").unwrap();
        assert!(!inner_pattern.matches(""));
        assert!(!inner_pattern.matches("tot"));
        assert!(!inner_pattern.matches("totia"));
        assert!(!inner_pattern.matches("atoti"));
        assert!(inner_pattern.matches("toti"));
        assert!(!inner_pattern.matches("tito"));
        assert!(inner_pattern.matches("towhateverti"));

        assert!(FieldPattern::from_str("to**").is_err());
    }
}
