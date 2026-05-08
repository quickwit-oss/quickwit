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

mod compaction_planner;
mod compaction_state;
mod index_config_metastore;
pub(crate) mod metrics;

use std::collections::BinaryHeap;

pub use compaction_planner::CompactionPlanner;
use quickwit_indexing::merge_policy::MergeOperation;

use crate::planner::metrics::COMPACTION_PLANNER_METRICS;
use crate::source_uid_metrics_label;

/// Max-heap of merge operations awaiting assignment, ordered by
/// `MergeOperation`'s score-based `Ord`. The `pending_merge_operations` gauge
/// is maintained inline; push/pop are the only mutation paths so the metric
/// stays consistent with `len()`.
#[derive(Debug)]
struct PendingOperations {
    inner: BinaryHeap<MergeOperation>,
}

impl PendingOperations {
    fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    fn push(&mut self, operation: MergeOperation) {
        Self::adjust_gauge(&operation, 1);
        self.inner.push(operation);
    }

    fn pop(&mut self) -> Option<MergeOperation> {
        let operation = self.inner.pop()?;
        Self::adjust_gauge(&operation, -1);
        Some(operation)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = &MergeOperation> {
        self.inner.iter()
    }

    fn adjust_gauge(operation: &MergeOperation, delta: i64) {
        let source_uid_label =
            source_uid_metrics_label(&operation.index_uid, &operation.source_id);
        let merge_level = operation.merge_level().to_string();
        COMPACTION_PLANNER_METRICS
            .pending_merge_operations
            .with_label_values([source_uid_label.as_str(), merge_level.as_str()])
            .add(delta);
    }
}