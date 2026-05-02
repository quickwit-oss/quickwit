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

pub struct JaegerServiceMetrics {
    pub requests_total: Counter,
    pub request_errors_total: Counter,
    pub request_duration_seconds: Histogram,
    pub fetched_traces_total: Counter,
    pub fetched_spans_total: Counter,
    pub transferred_bytes_total: Counter,
}

static REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "requests_total",
        description: "Number of requests",
        subsystem: "jaeger",
    )
});

static REQUEST_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "request_errors_total",
        description: "Number of failed requests",
        subsystem: "jaeger",
    )
});

static REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "request_duration_seconds",
        description: "Duration of requests",
        subsystem: "jaeger",
        buckets: exponential_buckets(0.02, 2.0, 8).unwrap(),
    )
});

static FETCHED_TRACES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "fetched_traces_total",
        description: "Number of traces retrieved from storage",
        subsystem: "jaeger",
    )
});

static FETCHED_SPANS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "fetched_spans_total",
        description: "Number of spans retrieved from storage",
        subsystem: "jaeger",
    )
});

static TRANSFERRED_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "transferred_bytes_total",
        description: "Number of bytes transferred",
        subsystem: "jaeger",
    )
});

impl Default for JaegerServiceMetrics {
    fn default() -> Self {
        Self {
            requests_total: REQUESTS_TOTAL.clone(),
            request_errors_total: REQUEST_ERRORS_TOTAL.clone(),
            request_duration_seconds: REQUEST_DURATION_SECONDS.clone(),
            fetched_traces_total: FETCHED_TRACES_TOTAL.clone(),
            fetched_spans_total: FETCHED_SPANS_TOTAL.clone(),
            transferred_bytes_total: TRANSFERRED_BYTES_TOTAL.clone(),
        }
    }
}

pub static JAEGER_SERVICE_METRICS: LazyLock<JaegerServiceMetrics> =
    LazyLock::new(JaegerServiceMetrics::default);
