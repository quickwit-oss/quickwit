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

use fnv::FnvHashMap;
use quickwit_proto::indexing::IndexingTask;
use serde::Serialize;

/// A [`PhysicalIndexingPlan`] defines the list of indexing tasks
/// each indexer, identified by its node ID, should run.
/// TODO(fmassot): a metastore version number will be attached to the plan
/// to identify if the plan is up to date with the metastore.
#[derive(Debug, PartialEq, Clone, Serialize, Default)]
pub struct PhysicalIndexingPlan {
    indexing_tasks_per_node_id: FnvHashMap<String, Vec<IndexingTask>>,
}

impl PhysicalIndexingPlan {
    pub fn add_indexing_task(&mut self, node_id: &str, indexing_task: IndexingTask) {
        self.indexing_tasks_per_node_id
            .entry(node_id.to_string())
            .or_default()
            .push(indexing_task);
    }

    /// Returns the hashmap of (node ID, indexing tasks).
    pub fn indexing_tasks_per_node(&self) -> &FnvHashMap<String, Vec<IndexingTask>> {
        &self.indexing_tasks_per_node_id
    }

    /// Returns the hashmap of (node ID, indexing tasks).
    pub fn node(&self, node_id: &str) -> Option<&[IndexingTask]> {
        self.indexing_tasks_per_node_id
            .get(node_id)
            .map(Vec::as_slice)
    }

    pub fn normalize(&mut self) {
        for tasks in self.indexing_tasks_per_node_id.values_mut() {
            tasks.sort_by(|left, right| {
                left.index_uid
                    .cmp(&right.index_uid)
                    .then_with(|| left.source_id.cmp(&right.source_id))
            });
        }
    }
}
