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

mod rest_handler;

use std::sync::LazyLock;

use metrics::{Counter, Label, counter};
use quickwit_common::dd_metrics::{DD_STATUS_CODES, DDCounters, DDHistograms};
pub(crate) use rest_handler::byoc_api_handlers;

pub(crate) struct ByocApiMetrics {
    pub log_requests_total: DDCounters,
    pub log_request_duration_seconds: DDHistograms,
    pub log_bytes_total: Counter,
    pub log_unmatched_events_total: Counter,
    pub metric_requests_total: DDCounters,
    pub metric_request_duration_seconds: DDHistograms,
    pub metric_bytes_total: Counter,
    pub trace_requests_total: DDCounters,
    pub trace_request_duration_seconds: DDHistograms,
    pub trace_bytes_total: Counter,
}

impl Default for ByocApiMetrics {
    fn default() -> Self {
        let log = Label::new("signal", "log");
        let metric = Label::new("signal", "metric");
        let trace = Label::new("signal", "trace");

        Self {
            log_requests_total: DDCounters::new(
                "byoc_ingest_requests.count",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&log),
            ),
            log_request_duration_seconds: DDHistograms::new(
                "byoc_ingest_requests.duration_seconds",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&log),
            ),
            log_bytes_total: counter!("byoc_ingest_bytes.count", vec![log.clone()]),
            log_unmatched_events_total: counter!("byoc_ingest_unmatched_events.count", vec![log]),

            metric_requests_total: DDCounters::new(
                "byoc_ingest_requests.count",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&metric),
            ),
            metric_request_duration_seconds: DDHistograms::new(
                "byoc_ingest_requests.duration_seconds",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&metric),
            ),
            metric_bytes_total: counter!("byoc_ingest_bytes.count", vec![metric]),

            trace_requests_total: DDCounters::new(
                "byoc_ingest_requests.count",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&trace),
            ),
            trace_request_duration_seconds: DDHistograms::new(
                "byoc_ingest_requests.duration_seconds",
                "status_code",
                DD_STATUS_CODES,
                std::slice::from_ref(&trace),
            ),
            trace_bytes_total: counter!("byoc_ingest_bytes.count", vec![trace]),
        }
    }
}

pub(crate) static BYOC_METRICS: LazyLock<ByocApiMetrics> = LazyLock::new(ByocApiMetrics::default);
