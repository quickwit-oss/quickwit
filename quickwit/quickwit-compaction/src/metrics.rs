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
    HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets, new_counter_vec,
    new_gauge, new_gauge_vec, new_histogram_vec,
};

pub struct CompactorMetrics {
    pub compactions_in_progress: IntGaugeVec<2>,
    pub compactions_failed: IntCounterVec<2>,
    pub compactions_succeeded: IntCounterVec<2>,
    pub available_slots: IntGauge,
    pub compaction_duration: HistogramVec<2>,
}

fn compaction_duration_buckets() -> Vec<f64> {
    exponential_buckets(0.5, 2.0, 14).expect("compaction duration buckets should be valid")
}

impl Default for CompactorMetrics {
    fn default() -> Self {
        CompactorMetrics {
            compactions_in_progress: new_gauge_vec(
                "compactions_in_progress",
                "number of compaction merge operations currently running on this compactor",
                "compactor",
                &[],
                ["source_uid", "merge_level"],
            ),
            compactions_failed: new_counter_vec(
                "compactions_failed",
                "total number of compaction merge operations that have failed",
                "compactor",
                &[],
                ["source_uid", "merge_level"],
            ),
            compactions_succeeded: new_counter_vec(
                "compactions_succeeded",
                "total number of compaction merge operations that have completed successfully",
                "compactor",
                &[],
                ["source_uid", "merge_level"],
            ),
            available_slots: new_gauge(
                "available_slots",
                "number of compaction slots currently available on this compactor",
                "compactor",
                &[],
            ),
            compaction_duration: new_histogram_vec(
                "compaction_duration_seconds",
                "duration of compaction merge operations in seconds",
                "compactor",
                &[],
                ["source_uid", "merge_level"],
                compaction_duration_buckets(),
            ),
        }
    }
}

pub static COMPACTOR_METRICS: Lazy<CompactorMetrics> = Lazy::new(CompactorMetrics::default);
