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
use quickwit_metrics::{Counter, Histogram, LabelNames, counter, histogram, label_names};

pub(crate) const OTLP_GRPC_LABELS: LabelNames<4> =
    label_names!("service", "index", "transport", "format");
pub(crate) const OTLP_GRPC_ERROR_LABELS: LabelNames<5> =
    label_names!("service", "index", "transport", "format", "error");

pub(crate) static REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "requests_total",
        description: "Number of requests",
        subsystem: "otlp",
    )
});

pub(crate) static REQUEST_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "request_errors_total",
        description: "Number of failed requests",
        subsystem: "otlp",
    )
});

pub(crate) static REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "request_duration_seconds",
        description: "Duration of requests",
        subsystem: "otlp",
        buckets: exponential_buckets(0.02, 2.0, 8).unwrap(),
    )
});

pub(crate) static INGESTED_LOG_RECORDS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_log_records_total",
        description: "Number of log records ingested",
        subsystem: "otlp",
    )
});

pub(crate) static INGESTED_SPANS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_spans_total",
        description: "Number of spans ingested",
        subsystem: "otlp",
    )
});

pub(crate) static INGESTED_DATA_POINTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_data_points_total",
        description: "Number of metric data points ingested",
        subsystem: "otlp",
    )
});

pub(crate) static INGESTED_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingested_bytes_total",
        description: "Number of bytes ingested",
        subsystem: "otlp",
    )
});
