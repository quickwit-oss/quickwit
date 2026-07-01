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

//! Prometheus metrics for the KQL parser and lowering passes.
//!
//! All counters live under the `quickwit_kql_*` namespace so SRE can pin
//! dashboards on KQL traffic distinctly from the Tantivy-grammar path.

use quickwit_metrics::{LazyCounter, LazyHistogram, lazy_counter, lazy_histogram};

/// Buckets in seconds covering sub-millisecond up to 100ms — a single KQL
/// parse should never exceed a few milliseconds; anything past that points to
/// a pathological input.
fn parse_duration_buckets() -> Vec<f64> {
    vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
}

/// Incremented every time a KQL query reaches the parse-and-lower stage,
/// regardless of outcome.
pub(crate) static KQL_PARSE_TOTAL: LazyCounter = lazy_counter!(
    name: "parse_total",
    description: "Total number of KQL parse attempts.",
    subsystem: "kql",
);

/// Incremented when a parse or lowering pass returns an error. Together with
/// `KQL_PARSE_TOTAL` this exposes the parse-failure rate.
pub(crate) static KQL_PARSE_FAILURES_TOTAL: LazyCounter = lazy_counter!(
    name: "parse_failures_total",
    description: "Total number of KQL parse / lower failures.",
    subsystem: "kql",
);

/// Wall-clock duration spent in `KqlQuery::parse_user_query`. Lets SRE
/// alert on parser regressions or pathological inputs.
pub(crate) static KQL_PARSE_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
    name: "parse_duration_seconds",
    description: "Duration of KQL parse + lowering in seconds.",
    subsystem: "kql",
    buckets: parse_duration_buckets(),
);
