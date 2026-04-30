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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    IntCounter, IntCounterVec, IntGaugeVec, new_counter, new_counter_vec, new_gauge_vec,
};

pub struct CompactionPlannerMetrics {
    pub new_splits_scanned: IntCounterVec<1>,
    pub splits_needing_compaction: IntGaugeVec<1>,
    pub pending_merge_operations: IntGaugeVec<2>,
    pub timed_out_operations: IntCounter,
    pub metastore_errors: IntCounterVec<1>,
}

impl Default for CompactionPlannerMetrics {
    fn default() -> Self {
        CompactionPlannerMetrics {
            new_splits_scanned: new_counter_vec(
                "new_splits_scanned",
                "cumulative number of immature splits scanned from the metastore",
                "compaction_planner",
                &[],
                ["source_uid"],
            ),
            splits_needing_compaction: new_gauge_vec(
                "splits_needing_compaction",
                "number of splits currently tracked as needing compaction",
                "compaction_planner",
                &[],
                ["source_uid"],
            ),
            pending_merge_operations: new_gauge_vec(
                "pending_merge_operations",
                "number of pending merge operations awaiting assignment",
                "compaction_planner",
                &[],
                ["source_uid", "merge_level"],
            ),
            timed_out_operations: new_counter(
                "timed_out_operations",
                "cumulative number of merge operations that timed out waiting for a worker \
                 heartbeat",
                "compaction_planner",
                &[],
            ),
            metastore_errors: new_counter_vec(
                "metastore_errors",
                "cumulative number of metastore errors encountered by the compaction planner",
                "compaction_planner",
                &[],
                ["operation"],
            ),
        }
    }
}

pub static COMPACTION_PLANNER_METRICS: Lazy<CompactionPlannerMetrics> =
    Lazy::new(CompactionPlannerMetrics::default);
