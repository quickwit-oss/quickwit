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

pub struct JanitorMetrics {
    pub ongoing_num_delete_operations_total: IntGaugeVec<1>,
    pub gc_deleted_splits: IntCounterVec<1>,
    pub gc_deleted_bytes: IntCounter,
    pub gc_runs: IntCounterVec<1>,
    pub gc_seconds_total: IntCounter,
    // TODO having a current run duration which is 0|undefined out of run, and returns `now -
    // start_time` during a run would be nice
}

impl Default for JanitorMetrics {
    fn default() -> Self {
        JanitorMetrics {
            ongoing_num_delete_operations_total: new_gauge_vec(
                "ongoing_num_delete_operations_total",
                "Num of ongoing delete operations (per index).",
                "quickwit_janitor",
                &[],
                ["index"],
            ),
            gc_deleted_splits: new_counter_vec(
                "gc_deleted_splits_total",
                "Total number of splits deleted by the garbage collector.",
                "quickwit_janitor",
                &[],
                ["result"],
            ),
            gc_deleted_bytes: new_counter(
                "gc_deleted_bytes_total",
                "Total number of bytes deleted by the garbage collector.",
                "quickwit_janitor",
                &[],
            ),
            gc_runs: new_counter_vec(
                "gc_runs_total",
                "Total number of garbage collector execition.",
                "quickwit_janitor",
                &[],
                ["result"],
            ),
            gc_seconds_total: new_counter(
                "gc_seconds_total",
                "Total time spent running the garbage collector",
                "quickwit_janitor",
                &[],
            ),
        }
    }
}

/// `JANITOR_METRICS` exposes a bunch of related metrics through a prometheus
/// endpoint.
pub static JANITOR_METRICS: Lazy<JanitorMetrics> = Lazy::new(JanitorMetrics::default);
