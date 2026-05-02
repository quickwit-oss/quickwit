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
use quickwit_metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};

pub(crate) static HTTP_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "http_requests_total",
        description: "Total number of HTTP requests processed.",
        subsystem: "",
    )
});

pub(crate) static REQUEST_DURATION_SECS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "request_duration_secs",
        description: "Response time in seconds",
        subsystem: "",
        // last bucket is 163.84s
        buckets: exponential_buckets(0.02, 2.0, 14).unwrap(),
    )
});

pub(crate) static ONGOING_REQUESTS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "ongoing_requests",
        description: "Number of ongoing requests.",
        subsystem: "",
    )
});

pub(crate) static PENDING_REQUESTS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "pending_requests",
        description: "Number of pending requests.",
        subsystem: "",
    )
});

pub(crate) static CIRCUIT_BREAK_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "circuit_break_total",
        description: "Circuit breaker counter",
        subsystem: "grpc",
    )
});
