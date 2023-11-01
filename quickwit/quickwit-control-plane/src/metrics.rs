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
use quickwit_common::metrics::{new_counter, IntCounter};

pub struct ControlPlaneMetrics {
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,
    pub metastore_error_aborted: IntCounter,
    pub metastore_error_maybe_executed: IntCounter,
}

impl Default for ControlPlaneMetrics {
    fn default() -> Self {
        ControlPlaneMetrics {
            restart_total: new_counter(
                "restart_total",
                "Number of control plane restart.",
                "quickwit_control_plane",
            ),
            schedule_total: new_counter(
                "schedule_total",
                "Number of control plane `schedule` operation.",
                "quickwit_control_plane",
            ),
            metastore_error_aborted: new_counter(
                "metastore_error_aborted",
                "Number of aborted metastore transaction (= do not trigger a control plane \
                 restart)",
                "quickwit_control_plane",
            ),
            metastore_error_maybe_executed: new_counter(
                "metastore_error_maybe_executed",
                "Number of metastore transaction with an uncertain outcome (= do trigger a \
                 control plane restart)",
                "quickwit_control_plane",
            ),
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> =
    Lazy::new(ControlPlaneMetrics::default);
