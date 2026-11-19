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

use std::collections::HashMap;

use itertools::Itertools;
use quickwit_proto::search::ListFieldsEntry;

/// All splits in a leaf request belong to the same index. Keep only counts until we know which
/// fields to return. Dropping rare fields before the last split could change the top fields.
#[derive(Default)]
pub(super) struct LeafFieldCounts {
    by_name_and_type: HashMap<(String, i32), SplitCounts>,
}

#[derive(Default)]
struct SplitCounts {
    total: u64,
    searchable: u64,
    aggregatable: u64,
}

impl LeafFieldCounts {
    /// The split's normalized entries must be unique by (name, type).
    pub fn with_split(mut self, entries: Vec<ListFieldsEntry>) -> Self {
        for entry in entries {
            let counts = self
                .by_name_and_type
                .entry((entry.field_name, entry.field_type))
                .or_default();
            counts.total += 1;
            counts.searchable += u64::from(entry.searchable);
            counts.aggregatable += u64::from(entry.aggregatable);
        }
        self
    }

    pub fn into_entries(self, index_id: &str, limit: usize) -> Vec<ListFieldsEntry> {
        let mut entries: Vec<_> = self
            .by_name_and_type
            .into_iter()
            .k_smallest_by(limit, |(key_a, counts_a), (key_b, counts_b)| {
                counts_b
                    .total
                    .cmp(&counts_a.total)
                    .then_with(|| key_a.cmp(key_b))
            })
            .map(|((field_name, field_type), counts)| {
                // The exception arrays are present only if some, but not all, splits support the
                // capability. For a leaf they can contain only this index.
                let exceptions = |supported| {
                    if supported > 0 && supported < counts.total {
                        vec![index_id.to_string()]
                    } else {
                        Vec::new()
                    }
                };
                ListFieldsEntry {
                    field_name,
                    field_type,
                    searchable: counts.searchable > 0,
                    aggregatable: counts.aggregatable > 0,
                    index_ids: vec![index_id.to_string()],
                    non_searchable_index_ids: exceptions(counts.searchable),
                    non_aggregatable_index_ids: exceptions(counts.aggregatable),
                    num_splits: counts.total,
                }
            })
            .collect();
        entries.sort_unstable_by(ListFieldsEntry::cmp_by_name_and_type);
        entries
    }
}
