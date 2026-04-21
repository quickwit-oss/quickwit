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

use std::sync::LazyLock;

use quickwit_common::metrics::{HistogramVec, IntCounterVec, exponential_buckets, new_counter_vec};

pub struct JaegerServiceMetrics {
    pub requests_total: IntCounterVec<2>,
    pub request_errors_total: IntCounterVec<2>,
    pub request_duration_seconds: HistogramVec<3>,
    pub fetched_traces_total: IntCounterVec<2>,
    pub fetched_spans_total: IntCounterVec<2>,
    pub transferred_bytes_total: IntCounterVec<2>,
}

quickwit_common::define_histogram_vec! {
    REQUEST_DURATION_SECONDS,
    name: "request_duration_seconds",
    help: "Duration of requests",
    subsystem: "jaeger",
    const_labels: [],
    labels: ["operation", "index", "error"],
    buckets: exponential_buckets(0.02, 2.0, 8).unwrap(),
}

impl Default for JaegerServiceMetrics {
    fn default() -> Self {
        Self {
            requests_total: new_counter_vec(
                "requests_total",
                "Number of requests",
                "jaeger",
                &[],
                ["operation", "index"],
            ),
            request_errors_total: new_counter_vec(
                "request_errors_total",
                "Number of failed requests",
                "jaeger",
                &[],
                ["operation", "index"],
            ),
            request_duration_seconds: REQUEST_DURATION_SECONDS.clone(),
            fetched_traces_total: new_counter_vec(
                "fetched_traces_total",
                "Number of traces retrieved from storage",
                "jaeger",
                &[],
                ["operation", "index"],
            ),
            fetched_spans_total: new_counter_vec(
                "fetched_spans_total",
                "Number of spans retrieved from storage",
                "jaeger",
                &[],
                ["operation", "index"],
            ),
            transferred_bytes_total: new_counter_vec(
                "transferred_bytes_total",
                "Number of bytes transferred",
                "jaeger",
                &[],
                ["operation", "index"],
            ),
        }
    }
}

pub static JAEGER_SERVICE_METRICS: LazyLock<JaegerServiceMetrics> =
    LazyLock::new(JaegerServiceMetrics::default);
