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

use std::sync::LazyLock;

use quickwit_common::metrics::{
    Histogram, HistogramVec, IntCounterVec, exponential_buckets, new_counter_vec,
};

/// From 100ms to 73s seconds
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.100, 3f64.sqrt(), 13).unwrap()
}

/// From 1KB to 16MB
fn payload_size_buckets() -> Vec<f64> {
    exponential_buckets(1024.0, 4.0, 8).unwrap()
}

pub struct LambdaMetrics {
    pub leaf_search_requests_total: IntCounterVec<1>,
    pub leaf_search_duration_seconds: HistogramVec<1>,
    pub leaf_search_request_payload_size_bytes: Histogram,
    pub leaf_search_response_payload_size_bytes: Histogram,
}

quickwit_common::define_histogram_vec! {
    LEAF_SEARCH_DURATION_SECONDS,
    name: "leaf_search_duration_seconds",
    help: "Duration of Lambda leaf search invocations in seconds.",
    subsystem: "lambda",
    const_labels: [],
    labels: ["status"],
    buckets: duration_buckets(),
}

quickwit_common::define_histogram! {
    LEAF_SEARCH_REQUEST_PAYLOAD_SIZE_BYTES,
    name: "leaf_search_request_payload_size_bytes",
    help: "Size of the request payload sent to Lambda in bytes.",
    subsystem: "lambda",
    buckets: payload_size_buckets(),
}

quickwit_common::define_histogram! {
    LEAF_SEARCH_RESPONSE_PAYLOAD_SIZE_BYTES,
    name: "leaf_search_response_payload_size_bytes",
    help: "Size of the response payload received from Lambda in bytes.",
    subsystem: "lambda",
    buckets: payload_size_buckets(),
}

impl Default for LambdaMetrics {
    fn default() -> Self {
        LambdaMetrics {
            leaf_search_requests_total: new_counter_vec(
                "leaf_search_requests_total",
                "Total number of Lambda leaf search invocations.",
                "lambda",
                &[],
                ["status"],
            ),
            leaf_search_duration_seconds: LEAF_SEARCH_DURATION_SECONDS.clone(),
            leaf_search_request_payload_size_bytes: LEAF_SEARCH_REQUEST_PAYLOAD_SIZE_BYTES.clone(),
            leaf_search_response_payload_size_bytes: LEAF_SEARCH_RESPONSE_PAYLOAD_SIZE_BYTES
                .clone(),
        }
    }
}

pub static LAMBDA_METRICS: LazyLock<LambdaMetrics> = LazyLock::new(LambdaMetrics::default);
