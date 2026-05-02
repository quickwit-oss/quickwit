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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{Counter, Histogram, counter, histogram};

/// From 100ms to 73s seconds
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.100, 3f64.sqrt(), 13).unwrap()
}

/// From 1KB to 16MB
fn payload_size_buckets() -> Vec<f64> {
    exponential_buckets(1024.0, 4.0, 8).unwrap()
}

pub(crate) static LEAF_SEARCH_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "leaf_search_requests_total",
        description: "Total number of Lambda leaf search invocations.",
        subsystem: "lambda",
    )
});

pub(crate) static LEAF_SEARCH_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_duration_seconds",
        description: "Duration of Lambda leaf search invocations in seconds.",
        subsystem: "lambda",
        buckets: duration_buckets(),
    )
});

pub(crate) static LEAF_SEARCH_REQUEST_PAYLOAD_SIZE_BYTES: LazyLock<Histogram> =
    LazyLock::new(|| {
        histogram!(
            name: "leaf_search_request_payload_size_bytes",
            description: "Size of the request payload sent to Lambda in bytes.",
            subsystem: "lambda",
            buckets: payload_size_buckets(),
        )
    });

pub(crate) static LEAF_SEARCH_RESPONSE_PAYLOAD_SIZE_BYTES: LazyLock<Histogram> =
    LazyLock::new(|| {
        histogram!(
            name: "leaf_search_response_payload_size_bytes",
            description: "Size of the response payload received from Lambda in bytes.",
            subsystem: "lambda",
            buckets: payload_size_buckets(),
        )
    });
