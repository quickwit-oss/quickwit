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

pub use cache::ListFieldsCache;
use itertools::Itertools;
use quickwit_common::rate_limited_warn;
use quickwit_proto::search::ListFieldsEntry;
use tracing::instrument;

pub use crate::list_fields::leaf::leaf_list_fields;
pub use crate::list_fields::root::root_list_fields;

/// QW_FIELD_LIST_SIZE_LIMIT defines a hard limit on the number of fields that
/// can be returned. When the limit is exceeded, the fields present in the most
/// splits are retained.
///
/// Having many fields can happen when a user is creating fields dynamically in
/// a JSON type with random field names. Retaining the most common fields bounds
/// response memory while pruning the long tail of rare fields. The default is
/// 10,000 because responses with 100,000 fields may exceed gRPC message size limits.
fn field_list_size_limit() -> usize {
    quickwit_common::get_from_env_cached!(usize, "QW_FIELD_LIST_SIZE_LIMIT", 10_000, false)
}

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
    Ok(merge_entries_with_limit(
        entry_groups,
        field_list_size_limit(),
    ))
}

fn merge_entries_with_limit(
    entry_groups: Vec<Vec<ListFieldsEntry>>,
    limit: usize,
) -> Vec<ListFieldsEntry> {
    let merged_entries = entry_groups
        .into_iter()
        .kmerge_by(|left, right| left.cmp_by_name_and_type(right) == Ordering::Less);
    let mut entries = Vec::new();
    let mut current_group: Vec<ListFieldsEntry> = Vec::new();

    let truncate_entries = |entries: &mut Vec<ListFieldsEntry>| {
        entries.sort_unstable_by(|left, right| {
            right
                .num_splits
                .cmp(&left.num_splits)
                .then_with(|| left.cmp_by_name_and_type(right))
        });
        entries.truncate(limit);
    };
    let flush_group = |entries: &mut Vec<ListFieldsEntry>,
                       current_group: &mut Vec<ListFieldsEntry>| {
        entries.push(merge_same_entry_group(current_group));
        current_group.clear();
        if entries.len() >= limit * 2 {
            truncate_entries(entries);
        }
    };

    for entry in merged_entries {
        if let Some(last) = current_group.last()
            && (last.field_name != entry.field_name || last.field_type != entry.field_type)
        {
            flush_group(&mut entries, &mut current_group);
        }
        current_group.push(entry);
    }
    if !current_group.is_empty() {
        flush_group(&mut entries, &mut current_group);
    }

    truncate_entries(&mut entries);
    entries.sort_unstable_by(ListFieldsEntry::cmp_by_name_and_type);
    entries
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
    let num_splits = current_group.iter().map(|entry| entry.num_splits).sum();
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
        num_splits,
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let resp = merge_entries(vec![vec![entry1.clone()], vec![entry2.clone()]]).unwrap();
        let mut expected = entry1;
        expected.num_splits = 2;
        assert_eq!(resp, vec![expected]);
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field2".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
            num_splits: 1,
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
            num_splits: 2,
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: false,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
            num_splits: 1,
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
            num_splits: 2,
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let entry3 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry2.clone()],
            vec![entry3.clone()],
        ])
        .unwrap();
        let mut expected_entry1 = entry1;
        expected_entry1.num_splits = 2;
        assert_eq!(resp, vec![expected_entry1, entry3]);
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let entry3 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::U64 as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry3.clone()],
            vec![entry2.clone()],
        ])
        .unwrap();
        let mut expected_entry1 = entry1;
        expected_entry1.num_splits = 2;
        assert_eq!(resp, vec![expected_entry1, entry3]);
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let entry3 = ListFieldsEntry {
            field_name: "field2".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
            num_splits: 1,
        };
        let resp = merge_entries(vec![
            vec![entry1.clone(), entry3.clone()],
            vec![entry2.clone()],
        ])
        .unwrap();
        let mut expected_entry1 = entry1;
        expected_entry1.num_splits = 2;
        assert_eq!(resp, vec![expected_entry1, entry3]);
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
            num_splits: 1,
        };
        let entry2 = ListFieldsEntry {
            field_name: "field1".to_string(),
            field_type: ListFieldsType::Str as i32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index4".to_string()],
            num_splits: 1,
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
            num_splits: 2,
        };
        assert_eq!(resp, vec![expected]);
    }

    #[test]
    fn merge_entries_truncates_by_split_count() {
        let entry = |field_name: &str, num_splits: u64| ListFieldsEntry {
            field_name: field_name.to_string(),
            num_splits,
            ..Default::default()
        };
        let entries = vec![
            entry("early-a", 2),
            entry("early-b", 1),
            entry("early-c", 1),
            entry("early-d", 1),
            entry("late", 3),
        ];

        let entries = merge_entries_with_limit(vec![entries], 2);

        let field_names: Vec<&str> = entries
            .iter()
            .map(|entry| entry.field_name.as_str())
            .collect();
        assert_eq!(field_names, ["early-a", "late"]);
        assert_eq!(entries[1].num_splits, 3);
    }

    #[test]
    fn merge_entries_carries_split_counts_across_merge_levels() {
        let leaf_entry = ListFieldsEntry {
            field_name: "field".to_string(),
            num_splits: 3,
            ..Default::default()
        };
        let other_leaf_entry = ListFieldsEntry {
            field_name: "field".to_string(),
            num_splits: 2,
            ..Default::default()
        };

        let entries = merge_entries_with_limit(vec![vec![leaf_entry], vec![other_leaf_entry]], 10);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].num_splits, 5);
    }
}
