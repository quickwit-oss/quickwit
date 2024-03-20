// Copyright (C) 2024 Quickwit, Inc.
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
use quickwit_common::metrics::{new_gauge, IntGauge};

#[derive(Clone)]
pub(super) struct PostgresMetrics {
    pub acquire_connections: IntGauge,
    pub active_connections: IntGauge,
    pub idle_connections: IntGauge,
}

impl Default for PostgresMetrics {
    fn default() -> Self {
        Self {
            acquire_connections: new_gauge(
                "acquire_connections",
                "Number of connections being acquired.",
                "metastore",
                &[],
            ),
            active_connections: new_gauge(
                "active_connections",
                "Number of active (used + idle) connections.",
                "metastore",
                &[],
            ),
            idle_connections: new_gauge(
                "idle_connections",
                "Number of idle connections.",
                "metastore",
                &[],
            ),
        }
    }
}

pub(super) static POSTGRES_METRICS: Lazy<PostgresMetrics> = Lazy::new(PostgresMetrics::default);
