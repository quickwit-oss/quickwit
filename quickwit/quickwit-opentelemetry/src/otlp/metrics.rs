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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{Counter, Histogram, counter, histogram};

pub struct OtlpServiceMetrics {
    pub requests_total: Counter,
    pub request_errors_total: Counter,
    pub request_duration_seconds: Histogram,
    pub ingested_log_records_total: Counter,
    pub ingested_spans_total: Counter,
    pub ingested_data_points_total: Counter,
    pub ingested_bytes_total: Counter,
}

static REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "requests_total",
        description: "Number of requests",
        subsystem: "otlp",
    )
});

static REQUEST_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "request_errors_total",
        description: "Number of failed requests",
        subsystem: "otlp",
    )
});

static REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "request_duration_seconds",
        description: "Duration of requests",
        subsystem: "otlp",
        buckets: exponential_buckets(0.02, 2.0, 8).unwrap(),
    )
});

static INGESTED_LOG_RECORDS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_log_records_total",
        description: "Number of log records ingested",
        subsystem: "otlp",
    )
});

static INGESTED_SPANS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_spans_total",
        description: "Number of spans ingested",
        subsystem: "otlp",
    )
});

static INGESTED_DATA_POINTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_data_points_total",
        description: "Number of metric data points ingested",
        subsystem: "otlp",
    )
});

static INGESTED_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_bytes_total",
        description: "Number of bytes ingested",
        subsystem: "otlp",
    )
});

impl Default for OtlpServiceMetrics {
    fn default() -> Self {
        Self {
            requests_total: REQUESTS_TOTAL.clone(),
            request_errors_total: REQUEST_ERRORS_TOTAL.clone(),
            request_duration_seconds: REQUEST_DURATION_SECONDS.clone(),
            ingested_log_records_total: INGESTED_LOG_RECORDS_TOTAL.clone(),
            ingested_spans_total: INGESTED_SPANS_TOTAL.clone(),
            ingested_data_points_total: INGESTED_DATA_POINTS_TOTAL.clone(),
            ingested_bytes_total: INGESTED_BYTES_TOTAL.clone(),
        }
    }
}

/// `OTLP_SERVICE_METRICS` exposes metrics for each OTLP service.
pub static OTLP_SERVICE_METRICS: LazyLock<OtlpServiceMetrics> =
    LazyLock::new(OtlpServiceMetrics::default);
