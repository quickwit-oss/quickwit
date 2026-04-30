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
#[allow(dead_code)]
mod compaction_state;
#[allow(dead_code)]
mod index_config_metastore;
pub(crate) mod metrics;

pub use compaction_planner::CompactionPlanner;

use std::collections::VecDeque;

use quickwit_indexing::merge_policy::MergeOperation;

use crate::planner::compaction_state::CompactionPartitionKey;
use crate::planner::metrics::COMPACTION_PLANNER_METRICS;
use crate::source_uid_metrics_label;

/// Queue of merge operations awaiting assignment, with the
/// `pending_merge_operations` gauge maintained inline. Push/pop are the only
/// mutation paths so the metric stays consistent with `len()`.
struct PendingOperations {
    inner: VecDeque<(CompactionPartitionKey, MergeOperation)>,
}

impl PendingOperations {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn push(&mut self, partition_key: CompactionPartitionKey, operation: MergeOperation) {
        Self::adjust_gauge(&partition_key, &operation, 1);
        self.inner.push_back((partition_key, operation));
    }

    fn pop(&mut self) -> Option<(CompactionPartitionKey, MergeOperation)> {
        let (partition_key, operation) = self.inner.pop_front()?;
        Self::adjust_gauge(&partition_key, &operation, -1);
        Some((partition_key, operation))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = &(CompactionPartitionKey, MergeOperation)> {
        self.inner.iter()
    }

    fn adjust_gauge(
        partition_key: &CompactionPartitionKey,
        operation: &MergeOperation,
        delta: i64,
    ) {
        let source_uid_label =
            source_uid_metrics_label(&partition_key.index_uid, &partition_key.source_id);
        let merge_level = operation.merge_level().to_string();
        COMPACTION_PLANNER_METRICS
            .pending_merge_operations
            .with_label_values([source_uid_label.as_str(), merge_level.as_str()])
            .add(delta);
    }
}
