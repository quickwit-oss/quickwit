// Copyright (C) 2022 Quickwit, Inc.
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

// See https://prometheus.io/docs/practices/naming/

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter, new_gauge, new_histogram, Histogram, IntCounter, IntGauge,
};

pub struct SearchMetrics {
    pub leaf_searches_splits_total: IntCounter,
    pub leaf_search_split_duration_secs: Histogram,
    pub active_search_threads_count: IntGauge,
}

impl Default for SearchMetrics {
    fn default() -> Self {
        SearchMetrics {
            leaf_searches_splits_total: new_counter(
                "leaf_searches_splits_total",
                "Number of leaf search (count of splits) started.",
                "quickwit_search",
            ),
            leaf_search_split_duration_secs: new_histogram(
                "leaf_search_split_duration_secs",
                "Number of seconds required to run a leaf search over a single split. The timer \
                 starts after the semaphore is obtained.",
                "quickwit_search",
            ),
            active_search_threads_count: new_gauge(
                "active_search_threads_count",
                "Number of threads in use in the CPU thread pool",
                "quickwit_search",
            ),
        }
    }
}

/// `SEARCH_METRICS` exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static SEARCH_METRICS: Lazy<SearchMetrics> = Lazy::new(SearchMetrics::default);
