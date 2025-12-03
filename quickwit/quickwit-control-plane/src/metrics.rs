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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    IntCounter, IntGauge, IntGaugeVec, new_counter, new_gauge, new_gauge_vec,
};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    pub num_remote_shards: usize,
    pub num_local_shards: usize,
}

pub struct ControlPlaneMetrics {
    // Indexes and shards tracked by the control plane.
    pub indexes_total: IntGauge,
    pub open_shards: IntGaugeVec<1>,
    pub closed_shards: IntGaugeVec<1>,

    // Operations performed by the control plane.
    pub apply_plan_total: IntCounter,
    pub rebalance_shards: IntGauge,
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,

    // Metastore errors.
    pub metastore_error_aborted: IntCounter,
    pub metastore_error_maybe_executed: IntCounter,

    // Indexing plan metrics.
    pub local_shards: IntGauge,
    pub remote_shards: IntGauge,
}

impl ControlPlaneMetrics {
    pub fn set_shard_locality_metrics(&self, shard_locality_metrics: ShardLocalityMetrics) {
        self.local_shards
            .set(shard_locality_metrics.num_local_shards as i64);
        self.remote_shards
            .set(shard_locality_metrics.num_remote_shards as i64);
    }
}

impl Default for ControlPlaneMetrics {
    fn default() -> Self {
        let open_shards = new_gauge_vec(
            "shards",
            "Number of open and closed shards tracked by the ingest controller",
            "control_plane",
            &[("state", "open")],
            ["index_id"],
        );
        let closed_shards = new_gauge_vec(
            "shards",
            "Number of open and closed shards tracked by the ingest controller",
            "control_plane",
            &[("state", "closed")],
            ["index_id"],
        );
        let indexed_shards = new_gauge_vec(
            "indexed_shards",
            "Number of (remote/local) shards in the indexing plan",
            "control_plane",
            &[],
            ["locality"],
        );
        let local_shards = indexed_shards.with_label_values(["local"]);
        let remote_shards = indexed_shards.with_label_values(["remote"]);

        ControlPlaneMetrics {
            indexes_total: new_gauge(
                "indexes_total",
                "Number of indexes tracked by the control plane.",
                "control_plane",
                &[],
            ),
            open_shards,
            closed_shards,
            apply_plan_total: new_counter(
                "apply_plan_total",
                "Number of control plane `apply plan` operations.",
                "control_plane",
                &[],
            ),
            rebalance_shards: new_gauge(
                "rebalance_shards",
                "Number of shards rebalanced by the control plane.",
                "control_plane",
                &[],
            ),
            restart_total: new_counter(
                "restart_total",
                "Number of control plane restarts.",
                "control_plane",
                &[],
            ),
            schedule_total: new_counter(
                "schedule_total",
                "Number of control plane `schedule` operations.",
                "control_plane",
                &[],
            ),
            metastore_error_aborted: new_counter(
                "metastore_error_aborted",
                "Number of aborted metastore transaction (= do not trigger a control plane \
                 restart)",
                "control_plane",
                &[],
            ),
            metastore_error_maybe_executed: new_counter(
                "metastore_error_maybe_executed",
                "Number of metastore transaction with an uncertain outcome (= do trigger a \
                 control plane restart)",
                "control_plane",
                &[],
            ),
            local_shards,
            remote_shards,
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> =
    Lazy::new(ControlPlaneMetrics::default);
