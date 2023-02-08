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

pub struct MetastoreMetrics {
    pub requests_total: IntCounterVec<2>,
    pub request_errors_total: IntCounterVec<2>,
    pub request_duration_seconds: HistogramVec<3>,
}

impl Default for MetastoreMetrics {
    fn default() -> Self {
        Self {
            requests_total: new_counter_vec(
                "requests_total",
                "Number of requests",
                "quickwit_metastore",
                ["operation", "index"],
            ),
            request_errors_total: new_counter_vec(
                "request_errors_total",
                "Number of failed requests",
                "quickwit_metastore",
                ["operation", "index"],
            ),
            request_duration_seconds: new_histogram_vec(
                "request_duration_seconds",
                "Duration of requests",
                "quickwit_metastore",
                ["operation", "index", "error"],
            ),
        }
    }
}

/// `METASTORE_METRICS` exposes a bunch of metastore-related metrics through a Prometheus
/// endpoint.
pub static METASTORE_METRICS: Lazy<MetastoreMetrics> = Lazy::new(MetastoreMetrics::default);
