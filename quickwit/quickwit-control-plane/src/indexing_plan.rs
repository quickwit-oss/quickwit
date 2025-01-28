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

use fnv::FnvHashMap;
use quickwit_proto::indexing::IndexingTask;
use serde::Serialize;

/// A [`PhysicalIndexingPlan`] defines the list of indexing tasks
/// each indexer, identified by its node ID, should run.
/// TODO(fmassot): a metastore version number will be attached to the plan
/// to identify if the plan is up to date with the metastore.
#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct PhysicalIndexingPlan {
    indexing_tasks_per_indexer_id: FnvHashMap<String, Vec<IndexingTask>>,
}

impl PhysicalIndexingPlan {
    pub fn with_indexer_ids(indexer_ids: &[String]) -> PhysicalIndexingPlan {
        PhysicalIndexingPlan {
            indexing_tasks_per_indexer_id: indexer_ids
                .iter()
                .map(|indexer_id| (indexer_id.clone(), Vec::new()))
                .collect(),
        }
    }

    pub fn add_indexing_task(&mut self, indexer_id: &str, indexing_task: IndexingTask) {
        self.indexing_tasks_per_indexer_id
            .entry(indexer_id.to_string())
            .or_default()
            .push(indexing_task);
    }

    /// Returns the hashmap of (indexer ID, indexing tasks).
    pub fn indexing_tasks_per_indexer(&self) -> &FnvHashMap<String, Vec<IndexingTask>> {
        &self.indexing_tasks_per_indexer_id
    }

    pub fn num_indexers(&self) -> usize {
        self.indexing_tasks_per_indexer_id.len()
    }

    /// Returns the hashmap of (indexer ID, indexing tasks).
    pub fn indexing_tasks_per_indexer_mut(&mut self) -> &mut FnvHashMap<String, Vec<IndexingTask>> {
        &mut self.indexing_tasks_per_indexer_id
    }

    /// Returns the hashmap of (indexer ID, indexing tasks).
    pub fn indexer(&self, indexer_id: &str) -> Option<&[IndexingTask]> {
        self.indexing_tasks_per_indexer_id
            .get(indexer_id)
            .map(Vec::as_slice)
    }

    pub fn normalize(&mut self) {
        for tasks in self.indexing_tasks_per_indexer_id.values_mut() {
            for task in tasks.iter_mut() {
                task.shard_ids.sort_unstable();
            }
            tasks.sort_unstable_by(|left, right| {
                left.index_uid
                    .cmp(&right.index_uid)
                    .then_with(|| left.source_id.cmp(&right.source_id))
                    .then_with(|| left.shard_ids.first().cmp(&right.shard_ids.first()))
                    .then_with(|| left.pipeline_uid.cmp(&right.pipeline_uid))
            });
        }
    }
}
