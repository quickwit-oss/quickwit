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
use quickwit_common::metrics::{new_gauge_vec, IntGaugeVec};

pub struct JanitorMetrics {
    pub ongoing_num_delete_operations_total: IntGaugeVec<1>,
}

impl Default for JanitorMetrics {
    fn default() -> Self {
        JanitorMetrics {
            ongoing_num_delete_operations_total: new_gauge_vec(
                "ongoing_num_delete_operations_total",
                "Num of ongoing delete operations (per index).",
                "quickwit_janitor",
                ["index"],
            ),
        }
    }
}

/// `JANITOR_METRICS` exposes a bunch of related metrics through a prometheus
/// endpoint.
pub static JANITOR_METRICS: Lazy<JanitorMetrics> = Lazy::new(JanitorMetrics::default);
