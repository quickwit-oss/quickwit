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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{
    LabelNames, LazyCounter, LazyHistogram, label_names, lazy_counter, lazy_histogram,
};

pub(crate) const OPERATION_INDEX_LABEL_NAMES: LabelNames<2> = label_names!("operation", "index");
pub(crate) const OPERATION_INDEX_ERROR_LABEL_NAMES: LabelNames<3> =
    label_names!("operation", "index", "error");

pub(crate) static REQUESTS_TOTAL: LazyCounter = lazy_counter!(
        name: "requests_total",
        description: "Number of requests",
        subsystem: "jaeger",
);

pub(crate) static REQUEST_ERRORS_TOTAL: LazyCounter = lazy_counter!(
        name: "request_errors_total",
        description: "Number of failed requests",
        subsystem: "jaeger",
);

pub(crate) static REQUEST_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
        name: "request_duration_seconds",
        description: "Duration of requests",
        subsystem: "jaeger",
        buckets: exponential_buckets(0.02, 2.0, 8).unwrap(),
);

pub(crate) static FETCHED_TRACES_TOTAL: LazyCounter = lazy_counter!(
        name: "fetched_traces_total",
        description: "Number of traces retrieved from storage",
        subsystem: "jaeger",
);

pub(crate) static FETCHED_SPANS_TOTAL: LazyCounter = lazy_counter!(
        name: "fetched_spans_total",
        description: "Number of spans retrieved from storage",
        subsystem: "jaeger",
);

pub(crate) static TRANSFERRED_BYTES_TOTAL: LazyCounter = lazy_counter!(
        name: "transferred_bytes_total",
        description: "Number of bytes transferred",
        subsystem: "jaeger",
);
