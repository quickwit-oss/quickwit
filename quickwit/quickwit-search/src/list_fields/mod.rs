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

mod cache;
mod leaf;
mod patterns;
mod root;

use std::cmp::Ordering;
use std::sync::LazyLock;

pub use cache::ListFieldsCache;
use itertools::Itertools;
use quickwit_common::rate_limited_warn;
use quickwit_proto::search::ListFieldsEntry;
use tracing::instrument;

use crate::SearchError;
pub use crate::list_fields::leaf::leaf_list_fields;
pub use crate::list_fields::root::root_list_fields;

/// QW_FIELD_LIST_SIZE_LIMIT defines a hard limit on the number of fields that
/// can be returned (error otherwise).
///
/// Having many fields can happen when a user is creating fields dynamically in
/// a JSON type with random field names. This leads to huge memory consumption
/// when building the response. This is a workaround until a way is found to
/// prune the long tail of rare fields.
static FIELD_LIST_SIZE_LIMIT: LazyLock<usize> =
    LazyLock::new(|| quickwit_common::get_from_env("QW_FIELD_LIST_SIZE_LIMIT", 100_000, false));

// Sorts and deduplicates the list of fields.
//
// If somehow we end up with duplicate fields, only the first one is kept,
// and we log a warning.
#[instrument(skip_all)]
fn sort_and_dedup(entries: &mut Vec<ListFieldsEntry>) {
    entries.sort_unstable_by(|left, right| left.cmp_by_name_and_type(right));

    // We defensively make sure there are no duplicates here.
    entries.dedup_by(|left, right| {
        if left.cmp_by_name_and_type(right) == std::cmp::Ordering::Equal {
            rate_limited_warn!(
                limit_per_min = 1,
                field_name = %left.field_name,
                field_type = %left.field_type,
                "found duplicate fields, please report"
            );
            true
        } else {
            false
        }
    });
}

fn merge_entries(entry_groups: Vec<Vec<ListFieldsEntry>>) -> crate::Result<Vec<ListFieldsEntry>> {
    let merged_entries = entry_groups
        .into_iter()
        .kmerge_by(|left, right| left.cmp_by_name_and_type(right) == Ordering::Less);
    let mut entries = Vec::new();

    let mut current_group: Vec<ListFieldsEntry> = Vec::new();
    // Build ListFieldsEntry from current group
    let flush_group = |responses: &mut Vec<_>, current_group: &mut Vec<ListFieldsEntry>| {
        let entry = merge_same_entry_group(current_group);
        responses.push(entry);
        current_group.clear();
    };
    for entry in merged_entries {
        if let Some(last) = current_group.last()
            && (last.field_name != entry.field_name || last.field_type != entry.field_type)
        {
            flush_group(&mut entries, &mut current_group);
        }
        if entries.len() >= *FIELD_LIST_SIZE_LIMIT {
            return Err(SearchError::Internal(format!(
                "list fields response exceeded {} fields",
                *FIELD_LIST_SIZE_LIMIT
            )));
        }
        current_group.push(entry);
    }
    if !current_group.is_empty() {
        flush_group(&mut entries, &mut current_group);
    }
    Ok(entries)
}

/// `current_group` needs to contain at least one element.
/// The group needs to be of the same field name and type.
fn merge_same_entry_group(current_group: &mut Vec<ListFieldsEntry>) -> ListFieldsEntry {
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

    let index_ids: Vec<String> = current_group
        .drain(..)
        .flat_map(|entry| entry.index_ids.into_iter())
        .sorted_unstable()
        .dedup()
        .collect();

    ListFieldsEntry {
        field_name,
        field_type,
        searchable,
        aggregatable,
        non_searchable_index_ids,
        non_aggregatable_index_ids,
        index_ids,
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{ListFieldsEntry, ListFieldsType};

    use super::*;

    #[test]
    fn merge_leaf_list_fields_identical_test() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        assert_eq!(resp, vec![entry1]);
    }

    #[test]
    fn merge_leaf_list_fields_different_test() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field2".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        assert_eq!(resp, vec![entry1, entry2]);
    }

    #[test]
    fn merge_leaf_list_fields_non_searchable_test() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        let expected = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
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
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: false,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        let expected = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
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
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry2.clone()],
            vec![entry3.clone()],
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }

    #[test]
    fn merge_leaf_list_fields_mixed_types2() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry3.clone()],
            vec![entry2.clone()],
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }

    #[test]
    fn merge_leaf_list_fields_multiple_field_names() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntry {
            field_name: "field2".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry3.clone()],
            vec![entry2.clone()],
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }

    #[test]
    fn merge_leaf_list_fields_non_aggregatable_list_test() {
        let entry1 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
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
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index4".to_string()],
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        let expected = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
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
}
