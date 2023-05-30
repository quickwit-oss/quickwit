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
use quickwit_common::metrics::{new_histogram_vec, HistogramVec};

pub struct CliMetrics {
    pub thread_unpark_duration_microseconds: HistogramVec<0>,
}

impl Default for CliMetrics {
    fn default() -> Self {
        CliMetrics {
            thread_unpark_duration_microseconds: new_histogram_vec(
                "thread_unpark_duration_microseconds",
                "Duration for which a thread of the main tokio runtime is unparked.",
                "quickwit_cli",
                [],
            ),
        }
    }
}

/// Serve counters exposes a bunch a set of metrics about the request received to quickwit.
pub static CLI_METRICS: Lazy<CliMetrics> = Lazy::new(CliMetrics::default);
