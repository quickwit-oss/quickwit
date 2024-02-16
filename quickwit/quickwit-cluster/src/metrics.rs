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
use quickwit_common::metrics::{new_counter, IntCounter};

pub struct ClusterMetrics {
    pub gossip_recv_total: IntCounter,
    pub gossip_recv_bytes_total: IntCounter,
    pub gossip_send_total: IntCounter,
    pub gossip_send_bytes_total: IntCounter,
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        ClusterMetrics {
            gossip_recv_total: new_counter(
                "gossip_recv_total",
                "Total number of gossip messages received.",
                "cluster",
            ),
            gossip_recv_bytes_total: new_counter(
                "gossip_recv_bytes_total",
                "Total amount of gossip data received in bytes.",
                "cluster",
            ),
            gossip_send_total: new_counter(
                "gossip_send_total",
                "Total number of gossip messages sent.",
                "cluster",
            ),
            gossip_send_bytes_total: new_counter(
                "gossip_send_bytes_total",
                "Total amount of gossip data sent in bytes.",
                "cluster",
            ),
        }
    }
}

pub static CLUSTER_METRICS: Lazy<ClusterMetrics> = Lazy::new(ClusterMetrics::default);
