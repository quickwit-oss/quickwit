// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter_vec, new_histogram_vec, HistogramVec, IntCounterVec};

pub struct JaegerServiceMetrics {
    pub requests_total: IntCounterVec<2>,
    pub request_errors_total: IntCounterVec<2>,
    pub request_duration_seconds: HistogramVec<3>,
    pub fetched_traces_total: IntCounterVec<2>,
    pub fetched_spans_total: IntCounterVec<2>,
    pub transferred_bytes_total: IntCounterVec<2>,
}

impl Default for JaegerServiceMetrics {
    fn default() -> Self {
        Self {
            requests_total: new_counter_vec(
                "requests_total",
                "Number of requests",
                "quickwit_jaeger",
                ["operation", "index"],
            ),
            request_errors_total: new_counter_vec(
                "request_errors_total",
                "Number of failed requests",
                "quickwit_jaeger",
                ["operation", "index"],
            ),
            request_duration_seconds: new_histogram_vec(
                "request_duration_seconds",
                "Duration of requests",
                "quickwit_jaeger",
                ["operation", "index", "error"],
            ),
            fetched_traces_total: new_counter_vec(
                "fetched_traces_total",
                "Number of traces retrieved from storage",
                "quickwit_jaeger",
                ["operation", "index"],
            ),
            fetched_spans_total: new_counter_vec(
                "fetched_spans_total",
                "Number of spans retrieved from storage",
                "quickwit_jaeger",
                ["operation", "index"],
            ),
            transferred_bytes_total: new_counter_vec(
                "transferred_bytes_total",
                "Number of bytes transferred",
                "quickwit_jaeger",
                ["operation", "index"],
            ),
        }
    }
}

pub static JAEGER_SERVICE_METRICS: Lazy<JaegerServiceMetrics> =
    Lazy::new(JaegerServiceMetrics::default);
