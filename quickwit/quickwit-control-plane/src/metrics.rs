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
    new_counter, new_gauge, new_gauge_vec, IntCounter, IntGauge, IntGaugeVec,
};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    pub num_remote_shards: usize,
    pub num_local_shards: usize,
}

pub struct ControlPlaneMetrics {
    pub indexes_total: IntGauge,
    pub restart_total: IntCounter,
    pub schedule_total: IntCounter,
    pub apply_total: IntCounter,
    pub metastore_error_aborted: IntCounter,
    pub metastore_error_maybe_executed: IntCounter,
    pub open_shards_total: IntGaugeVec<1>,
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
        let shards = new_gauge_vec(
            "shards",
            "Number of (remote/local) shards in the indexing plan",
            "control_plane",
            &[],
            ["locality"],
        );
        let local_shards = shards.with_label_values(["local"]);
        let remote_shards = shards.with_label_values(["remote"]);
        ControlPlaneMetrics {
            indexes_total: new_gauge("indexes_total", "Number of indexes.", "control_plane", &[]),
            restart_total: new_counter(
                "restart_total",
                "Number of control plane restart.",
                "control_plane",
                &[],
            ),
            schedule_total: new_counter(
                "schedule_total",
                "Number of control plane `schedule` operations.",
                "control_plane",
                &[],
            ),
            apply_total: new_counter(
                "apply_total",
                "Number of control plane `apply plan` operations.",
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
            open_shards_total: new_gauge_vec(
                "open_shards_total",
                "Number of open shards per source.",
                "control_plane",
                &[],
                ["index_id"],
            ),
            local_shards,
            remote_shards,
        }
    }
}

pub static CONTROL_PLANE_METRICS: Lazy<ControlPlaneMetrics> =
    Lazy::new(ControlPlaneMetrics::default);
