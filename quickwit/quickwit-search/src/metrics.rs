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

// See https://prometheus.io/docs/practices/naming/

use std::fmt;

use bytesize::ByteSize;
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, exponential_buckets,
    linear_buckets, new_counter, new_counter_vec, new_gauge, new_gauge_vec, new_histogram,
    new_histogram_vec,
};

fn print_if_not_null(
    field_name: &'static str,
    counter: &IntCounter,
    f: &mut fmt::Formatter,
) -> fmt::Result {
    let val = counter.get();
    if val > 0 {
        write!(f, "{}={} ", field_name, val)?;
    }
    Ok(())
}

pub struct SplitSearchOutcomeCounters {
    pub cancel_before_warmup: IntCounter,
    pub cache_hit: IntCounter,
    pub pruned_before_warmup: IntCounter,
    pub cancel_warmup: IntCounter,
    pub pruned_after_warmup: IntCounter,
    pub cancel_cpu_queue: IntCounter,
    pub cancel_cpu: IntCounter,
    pub success: IntCounter,
}

impl fmt::Display for SplitSearchOutcomeCounters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        print_if_not_null("cancel_before_warmup", &self.cancel_before_warmup, f)?;
        print_if_not_null("cache_hit", &self.cache_hit, f)?;
        print_if_not_null("pruned_before_warmup", &self.pruned_before_warmup, f)?;
        print_if_not_null("cancel_warmup", &self.cancel_warmup, f)?;
        print_if_not_null("pruned_after_warmup", &self.pruned_after_warmup, f)?;
        print_if_not_null("cancel_cpu_queue", &self.cancel_cpu_queue, f)?;
        print_if_not_null("cancel_cpu", &self.cancel_cpu, f)?;
        print_if_not_null("success", &self.success, f)?;
        Ok(())
    }
}

impl SplitSearchOutcomeCounters {
    /// Create a new SplitSearchOutcomeCounters instance, registered in prometheus.
    pub fn new_registered() -> Self {
        let search_split_outcome_vec = new_counter_vec(
            "split_search_outcome",
            "Count the state in which each leaf search split ended",
            "search",
            &[],
            ["category"],
        );
        Self::new_from_counter_vec(search_split_outcome_vec)
    }

    /// Create a new SplitSearchOutcomeCounters instance, but this one won't be reported to
    /// prometheus.
    pub fn new_unregistered() -> Self {
        let search_split_outcome_vec = IntCounterVec::new(
            "split_search_outcome",
            "Count the state in which each leaf search split ended",
            "search",
            &[],
            ["category"],
        );
        Self::new_from_counter_vec(search_split_outcome_vec)
    }

    pub fn new_from_counter_vec(search_split_outcome_vec: IntCounterVec<1>) -> Self {
        SplitSearchOutcomeCounters {
            cancel_before_warmup: search_split_outcome_vec
                .with_label_values(["cancel_before_warmup"]),
            cache_hit: search_split_outcome_vec.with_label_values(["cache_hit"]),
            pruned_before_warmup: search_split_outcome_vec
                .with_label_values(["pruned_before_warmup"]),
            cancel_warmup: search_split_outcome_vec.with_label_values(["cancel_warmup"]),
            pruned_after_warmup: search_split_outcome_vec
                .with_label_values(["pruned_after_warmup"]),
            cancel_cpu_queue: search_split_outcome_vec.with_label_values(["cancel_cpu_queue"]),
            cancel_cpu: search_split_outcome_vec.with_label_values(["cancel_cpu"]),
            success: search_split_outcome_vec.with_label_values(["success"]),
        }
    }
}

pub struct SearchMetrics {
    pub root_search_requests_total: IntCounterVec<1>,
    pub root_search_request_duration_seconds: HistogramVec<1>,
    pub root_search_targeted_splits: HistogramVec<1>,
    pub leaf_search_requests_total: IntCounterVec<1>,
    pub leaf_search_request_duration_seconds: HistogramVec<1>,
    pub leaf_search_targeted_splits: HistogramVec<1>,
    pub leaf_list_terms_splits_total: IntCounter,
    pub split_search_outcome_total: SplitSearchOutcomeCounters,
    pub leaf_search_split_duration_secs: Histogram,
    pub job_assigned_total: IntCounterVec<1>,
    pub leaf_search_single_split_tasks_pending: IntGauge,
    pub leaf_search_single_split_tasks_ongoing: IntGauge,
    pub leaf_search_single_split_warmup_num_bytes: Histogram,
    pub searcher_local_kv_store_size_bytes: IntGauge,
}

/// From 0.008s to 131.072s
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.008, 2.0, 15).unwrap()
}

impl Default for SearchMetrics {
    fn default() -> Self {
        let targeted_splits_buckets: Vec<f64> = [
            linear_buckets(0.0, 10.0, 10).unwrap(),
            linear_buckets(100.0, 100.0, 9).unwrap(),
            linear_buckets(1000.0, 1000.0, 9).unwrap(),
            linear_buckets(10000.0, 10000.0, 10).unwrap(),
        ]
        .iter()
        .flatten()
        .copied()
        .collect();

        let pseudo_exponential_bytes_buckets = vec![
            ByteSize::mb(10).as_u64() as f64,
            ByteSize::mb(20).as_u64() as f64,
            ByteSize::mb(50).as_u64() as f64,
            ByteSize::mb(100).as_u64() as f64,
            ByteSize::mb(200).as_u64() as f64,
            ByteSize::mb(500).as_u64() as f64,
            ByteSize::gb(1).as_u64() as f64,
            ByteSize::gb(2).as_u64() as f64,
            ByteSize::gb(5).as_u64() as f64,
        ];

        let leaf_search_single_split_tasks = new_gauge_vec::<1>(
            "leaf_search_single_split_tasks",
            "Number of single split search tasks pending or ongoing",
            "search",
            &[],
            ["status"], // takes values "ongoing" or "pending"
        );

        SearchMetrics {
            root_search_requests_total: new_counter_vec(
                "root_search_requests_total",
                "Total number of root search gRPC requests processed.",
                "search",
                &[("kind", "server")],
                ["status"],
            ),
            root_search_request_duration_seconds: new_histogram_vec(
                "root_search_request_duration_seconds",
                "Duration of root search gRPC requests in seconds.",
                "search",
                &[("kind", "server")],
                ["status"],
                duration_buckets(),
            ),
            root_search_targeted_splits: new_histogram_vec(
                "root_search_targeted_splits",
                "Number of splits targeted per root search GRPC request.",
                "search",
                &[],
                ["status"],
                targeted_splits_buckets.clone(),
            ),
            leaf_search_requests_total: new_counter_vec(
                "leaf_search_requests_total",
                "Total number of leaf search gRPC requests processed.",
                "search",
                &[("kind", "server")],
                ["status"],
            ),
            leaf_search_request_duration_seconds: new_histogram_vec(
                "leaf_search_request_duration_seconds",
                "Duration of leaf search gRPC requests in seconds.",
                "search",
                &[("kind", "server")],
                ["status"],
                duration_buckets(),
            ),
            leaf_search_targeted_splits: new_histogram_vec(
                "leaf_search_targeted_splits",
                "Number of splits targeted per leaf search GRPC request.",
                "search",
                &[],
                ["status"],
                targeted_splits_buckets,
            ),

            leaf_list_terms_splits_total: new_counter(
                "leaf_list_terms_splits_total",
                "Number of list terms splits total",
                "search",
                &[],
            ),
            split_search_outcome_total: SplitSearchOutcomeCounters::new_registered(),

            leaf_search_split_duration_secs: new_histogram(
                "leaf_search_split_duration_secs",
                "Number of seconds required to run a leaf search over a single split. The timer \
                 starts after the semaphore is obtained.",
                "search",
                duration_buckets(),
            ),
            leaf_search_single_split_tasks_ongoing: leaf_search_single_split_tasks
                .with_label_values(["ongoing"]),
            leaf_search_single_split_tasks_pending: leaf_search_single_split_tasks
                .with_label_values(["pending"]),
            leaf_search_single_split_warmup_num_bytes: new_histogram(
                "leaf_search_single_split_warmup_num_bytes",
                "Size of the short lived cache for a single split once the warmup is done.",
                "search",
                pseudo_exponential_bytes_buckets,
            ),
            job_assigned_total: new_counter_vec(
                "job_assigned_total",
                "Number of job assigned to searchers, per affinity rank.",
                "search",
                &[],
                ["affinity"],
            ),
            searcher_local_kv_store_size_bytes: new_gauge(
                "searcher_local_kv_store_size_bytes",
                "Size of the searcher kv store in bytes. This store is used to cache scroll \
                 contexts.",
                "search",
                &[],
            ),
        }
    }
}

/// `SEARCH_METRICS` exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static SEARCH_METRICS: Lazy<SearchMetrics> = Lazy::new(SearchMetrics::default);
