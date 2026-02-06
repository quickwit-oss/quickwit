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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    Histogram, HistogramVec, IntCounterVec, exponential_buckets, new_counter_vec, new_histogram,
    new_histogram_vec,
};

/// From 0.008s to 131s
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.008, 2.0, 15).unwrap()
}

/// From 1KB to 50MB
fn payload_size_buckets() -> Vec<f64> {
    exponential_buckets(1024.0, 4.0, 8).unwrap()
}

pub struct LambdaMetrics {
    pub leaf_search_requests_total: IntCounterVec<1>,
    pub leaf_search_duration_seconds: HistogramVec<1>,
    pub leaf_search_request_payload_size_bytes: Histogram,
    pub leaf_search_response_payload_size_bytes: Histogram,
    pub deploy_total: IntCounterVec<1>,
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
            leaf_search_duration_seconds: new_histogram_vec(
                "leaf_search_duration_seconds",
                "Duration of Lambda leaf search invocations in seconds.",
                "lambda",
                &[],
                ["status"],
                duration_buckets(),
            ),
            leaf_search_request_payload_size_bytes: new_histogram(
                "leaf_search_request_payload_size_bytes",
                "Size of the request payload sent to Lambda in bytes.",
                "lambda",
                payload_size_buckets(),
            ),
            leaf_search_response_payload_size_bytes: new_histogram(
                "leaf_search_response_payload_size_bytes",
                "Size of the response payload received from Lambda in bytes.",
                "lambda",
                payload_size_buckets(),
            ),
            deploy_total: new_counter_vec(
                "deploy_total",
                "Total number of Lambda deployment attempts.",
                "lambda",
                &[],
                ["status"],
            ),
        }
    }
}

pub static LAMBDA_METRICS: Lazy<LambdaMetrics> = Lazy::new(LambdaMetrics::default);
