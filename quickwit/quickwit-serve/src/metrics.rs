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
    HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, new_counter, new_counter_vec,
    new_gauge_vec, new_histogram_vec,
};

pub struct ServeMetrics {
    pub http_requests_total: IntCounterVec<2>,
    pub request_duration_secs: HistogramVec<2>,
    pub ongoing_requests: IntGaugeVec<1>,
    pub pending_requests: IntGaugeVec<1>,
    pub circuit_break_total: IntCounter,
}

impl Default for ServeMetrics {
    fn default() -> Self {
        let circuit_break_total = new_counter(
            "circuit_break_total",
            "Circuit breaker counter",
            "grpc",
            &[],
        );
        ServeMetrics {
            http_requests_total: new_counter_vec(
                "http_requests_total",
                "Total number of HTTP requests processed.",
                "",
                &[],
                ["method", "status_code"],
            ),
            request_duration_secs: new_histogram_vec(
                "request_duration_secs",
                "Response time in seconds",
                "",
                &[],
                ["method", "status_code"],
                // last bucket is 163.84s
                quickwit_common::metrics::exponential_buckets(0.02, 2.0, 14).unwrap(),
            ),
            ongoing_requests: new_gauge_vec(
                "ongoing_requests",
                "Number of ongoing requests.",
                "",
                &[],
                ["endpoint_group"],
            ),
            pending_requests: new_gauge_vec(
                "pending_requests",
                "Number of pending requests.",
                "",
                &[],
                ["endpoint_group"],
            ),
            circuit_break_total,
        }
    }
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static SERVE_METRICS: Lazy<ServeMetrics> = Lazy::new(ServeMetrics::default);
