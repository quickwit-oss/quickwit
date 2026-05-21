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

use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub use compaction_planner::CompactionPlanner;
use quickwit_indexing::merge_policy::{MergeOperation, compute_merge_score};
use quickwit_proto::types::{IndexUid, SourceId};

use quickwit_metrics::{gauge, label_values};

use crate::planner::metrics::{PENDING_MERGE_OPERATIONS, SOURCE_UID_MERGE_LEVEL};
use crate::source_uid_metrics_label;

/// A `MergeOperation` waiting to be assigned, with `priority_score` used to order the heap.
#[derive(Debug)]
pub(super) struct PendingMerge {
    pub(super) operation: MergeOperation,
    pub(super) index_uid: IndexUid,
    pub(super) source_id: SourceId,
    pub(super) priority_score: u64,
}

impl PendingMerge {
    pub(super) fn new(operation: MergeOperation) -> Self {
        let first_split = operation
            .splits
            .first()
            .expect("merge operation must have splits");
        let index_uid = first_split.index_uid.clone();
        let source_id = first_split.source_id.clone();
        let total_num_bytes: u64 = operation
            .splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        let priority_score = compute_merge_score(operation.splits.len(), total_num_bytes);
        Self {
            operation,
            index_uid,
            source_id,
            priority_score,
        }
    }
}

impl PartialEq for PendingMerge {
    fn eq(&self, other: &Self) -> bool {
        self.operation.merge_split_id == other.operation.merge_split_id
    }
}

impl Eq for PendingMerge {}

impl PartialOrd for PendingMerge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingMerge {
    /// Higher `priority_score` sorts first (max-heap); ties broken by
    /// `merge_split_id` so `Ord::cmp == Equal` agrees with `PartialEq`.
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority_score
            .cmp(&other.priority_score)
            .then_with(|| self.operation.merge_split_id.cmp(&other.operation.merge_split_id))
    }
}

/// Max-heap of pending merges awaiting assignment, ordered by
/// `PendingMerge`'s priority score. The `pending_merge_operations` gauge is
/// maintained inline; push/pop are the only mutation paths so the metric
/// stays consistent with `len()`.
#[derive(Debug)]
pub(super) struct PendingOperations {
    inner: BinaryHeap<PendingMerge>,
}

impl PendingOperations {
    pub(super) fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    pub(super) fn push(&mut self, pending: PendingMerge) {
        Self::adjust_gauge(&pending, 1);
        self.inner.push(pending);
    }

    pub(super) fn pop(&mut self) -> Option<PendingMerge> {
        let pending = self.inner.pop()?;
        Self::adjust_gauge(&pending, -1);
        Some(pending)
    }

    pub(super) fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = &PendingMerge> {
        self.inner.iter()
    }

    fn adjust_gauge(pending: &PendingMerge, delta: i64) {
        let source_uid_label = source_uid_metrics_label(&pending.index_uid, &pending.source_id);
        let merge_level = pending.operation.merge_level().to_string();
        let labels = label_values!(SOURCE_UID_MERGE_LEVEL => source_uid_label, merge_level);
        gauge!(parent: PENDING_MERGE_OPERATIONS, labels: [labels]).inc_by(delta as f64);
    }
}
