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
    HistogramVec, IntCounterVec, exponential_buckets, new_counter_vec, new_histogram_vec,
};

pub struct OtlpServiceMetrics {
    pub requests_total: IntCounterVec<4>,
    pub request_errors_total: IntCounterVec<4>,
    pub request_duration_seconds: HistogramVec<5>,
    pub ingested_log_records_total: IntCounterVec<4>,
    pub ingested_spans_total: IntCounterVec<4>,
    pub ingested_bytes_total: IntCounterVec<4>,
}

impl Default for OtlpServiceMetrics {
    fn default() -> Self {
        Self {
            requests_total: new_counter_vec(
                "requests_total",
                "Number of requests",
                "otlp",
                &[],
                ["service", "index", "transport", "format"],
            ),
            request_errors_total: new_counter_vec(
                "request_errors_total",
                "Number of failed requests",
                "otlp",
                &[],
                ["service", "index", "transport", "format"],
            ),
            request_duration_seconds: new_histogram_vec(
                "request_duration_seconds",
                "Duration of requests",
                "otlp",
                &[],
                ["service", "index", "transport", "format", "error"],
                exponential_buckets(0.02, 2.0, 8).unwrap(),
            ),
            ingested_log_records_total: new_counter_vec(
                "ingested_log_records_total",
                "Number of log records ingested",
                "otlp",
                &[],
                ["service", "index", "transport", "format"],
            ),
            ingested_spans_total: new_counter_vec(
                "ingested_spans_total",
                "Number of spans ingested",
                "otlp",
                &[],
                ["service", "index", "transport", "format"],
            ),
            ingested_bytes_total: new_counter_vec(
                "ingested_bytes_total",
                "Number of bytes ingested",
                "otlp",
                &[],
                ["service", "index", "transport", "format"],
            ),
        }
    }
}

/// `OTLP_SERVICE_METRICS` exposes metrics for each OTLP service.
pub static OTLP_SERVICE_METRICS: Lazy<OtlpServiceMetrics> = Lazy::new(OtlpServiceMetrics::default);
