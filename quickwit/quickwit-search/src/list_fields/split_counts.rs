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

use std::collections::hash_map::Entry;

use quickwit_proto::search::ListFieldsEntry;
use rustc_hash::FxHashMap;

/// Counts splits by field name and type within one index.
#[derive(Default)]
pub(super) struct SplitCountsByField {
    by_name_and_type: FxHashMap<(String, i32), SplitCounts>,
}

#[derive(Default)]
struct SplitCounts {
    num_splits: u32,
    searchable_splits: u32,
    aggregatable_splits: u32,
}

impl SplitCountsByField {
    pub fn len(&self) -> usize {
        self.by_name_and_type.len()
    }

    /// Fields must be unique by name and type within this split. Once full, finish counting
    /// retained fields so their capabilities reflect the entire last split.
    pub fn count_split(mut self, fields: Vec<ListFieldsEntry>, limit: usize) -> Self {
        for field in fields {
            let has_room = self.len() < limit;
            let counts = match self
                .by_name_and_type
                .entry((field.field_name, field.field_type))
            {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) if has_room => entry.insert(SplitCounts::default()),
                Entry::Vacant(_) => continue,
            };
            counts.num_splits += 1;
            counts.searchable_splits += u32::from(field.searchable);
            counts.aggregatable_splits += u32::from(field.aggregatable);
        }
        self
    }

    pub fn into_entries(self, index_id: &str) -> Vec<ListFieldsEntry> {
        let mut fields: Vec<_> = self
            .by_name_and_type
            .into_iter()
            .map(|((field_name, field_type), counts)| {
                let index_id_if_support_varies = |supporting_splits| {
                    if supporting_splits > 0 && supporting_splits < counts.num_splits {
                        vec![index_id.to_string()]
                    } else {
                        Vec::new()
                    }
                };
                ListFieldsEntry {
                    field_name,
                    field_type,
                    searchable: counts.searchable_splits > 0,
                    aggregatable: counts.aggregatable_splits > 0,
                    index_ids: vec![index_id.to_string()],
                    non_searchable_index_ids: index_id_if_support_varies(counts.searchable_splits),
                    non_aggregatable_index_ids: index_id_if_support_varies(
                        counts.aggregatable_splits,
                    ),
                    num_splits: u64::from(counts.num_splits),
                }
            })
            .collect();
        fields.sort_unstable_by(ListFieldsEntry::cmp_by_name_and_type);
        fields
    }
}
