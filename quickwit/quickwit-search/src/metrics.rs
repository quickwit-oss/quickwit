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

use bytesize::ByteSize;
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    exponential_buckets, linear_buckets, new_counter, new_counter_vec, new_gauge_vec,
    new_histogram, new_histogram_vec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
};

pub struct SearchMetrics {
    pub root_search_requests_total: IntCounterVec<1>,
    pub root_search_request_duration_seconds: HistogramVec<1>,
    pub root_search_targeted_splits: HistogramVec<1>,
    pub leaf_search_requests_total: IntCounterVec<1>,
    pub leaf_search_request_duration_seconds: HistogramVec<1>,
    pub leaf_search_targeted_splits: HistogramVec<1>,
    pub leaf_searches_splits_total: IntCounter,
    pub leaf_search_split_duration_secs: Histogram,
    pub job_assigned_total: IntCounterVec<1>,
    pub leaf_search_single_split_tasks_pending: IntGauge,
    pub leaf_search_single_split_tasks_ongoing: IntGauge,
    pub leaf_search_single_split_warmup_num_bytes: Histogram,
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
                exponential_buckets(0.001, 2.0, 15).unwrap(),
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
                exponential_buckets(0.001, 2.0, 15).unwrap(),
            ),
            leaf_search_targeted_splits: new_histogram_vec(
                "leaf_search_targeted_splits",
                "Number of splits targeted per leaf search GRPC request.",
                "search",
                &[],
                ["status"],
                targeted_splits_buckets,
            ),
            leaf_searches_splits_total: new_counter(
                "leaf_searches_splits_total",
                "Number of leaf searches (count of splits) started.",
                "search",
                &[],
            ),
            leaf_search_split_duration_secs: new_histogram(
                "leaf_search_split_duration_secs",
                "Number of seconds required to run a leaf search over a single split. The timer \
                 starts after the semaphore is obtained.",
                "search",
                exponential_buckets(0.001, 2.0, 15).unwrap(),
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
        }
    }
}

/// `SEARCH_METRICS` exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static SEARCH_METRICS: Lazy<SearchMetrics> = Lazy::new(SearchMetrics::default);
