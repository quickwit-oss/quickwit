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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter, IntCounter};

pub struct RestMetrics {
    pub http_requests_total: IntCounter,
}

impl Default for RestMetrics {
    fn default() -> Self {
        RestMetrics {
            http_requests_total: new_counter(
                "http_requests_total",
                "Total number of HTTP requests received",
                "quickwit",
            ),
        }
    }
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static SERVE_METRICS: Lazy<RestMetrics> = Lazy::new(RestMetrics::default);
