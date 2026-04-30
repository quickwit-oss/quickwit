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

use std::sync::LazyLock;

use quickwit_common::metrics::{Counter, Gauge, counter, gauge};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    pub num_remote_shards: usize,
    pub num_local_shards: usize,
}

pub struct ControlPlaneMetrics {
    // Indexes and shards tracked by the control plane.
    pub indexes_total: Gauge,
    pub open_shards: Gauge,
    pub closed_shards: Gauge,

    // Operations performed by the control plane.
    pub apply_plan_total: Counter,
    pub rebalance_shards: Gauge,
    pub restart_total: Counter,
    pub schedule_total: Counter,

    // Metastore errors.
    pub metastore_error_aborted: Counter,
    pub metastore_error_maybe_executed: Counter,

    // Indexing plan metrics.
    pub local_shards: Gauge,
    pub remote_shards: Gauge,
}

impl ControlPlaneMetrics {
    pub fn set_shard_locality_metrics(&self, shard_locality_metrics: ShardLocalityMetrics) {
        self.local_shards
            .set(shard_locality_metrics.num_local_shards as f64);
        self.remote_shards
            .set(shard_locality_metrics.num_remote_shards as f64);
    }
}

static INDEXES_TOTAL: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "indexes_total",
        description: "Number of indexes tracked by the control plane.",
        subsystem: "control_plane",
    )
});

static SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "shards",
        description: "Number of open and closed shards tracked by the ingest controller",
        subsystem: "control_plane",
    )
});

static INDEXED_SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "indexed_shards",
        description: "Number of (remote/local) shards in the indexing plan",
        subsystem: "control_plane",
    )
});

static APPLY_PLAN_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "apply_plan_total",
        description: "Number of control plane `apply plan` operations.",
        subsystem: "control_plane",
    )
});

static REBALANCE_SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "rebalance_shards",
        description: "Number of shards rebalanced by the control plane.",
        subsystem: "control_plane",
    )
});

static RESTART_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "restart_total",
        description: "Number of control plane restarts.",
        subsystem: "control_plane",
    )
});

static SCHEDULE_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "schedule_total",
        description: "Number of control plane `schedule` operations.",
        subsystem: "control_plane",
    )
});

static METASTORE_ERROR_ABORTED: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "metastore_error_aborted",
        description: "Number of aborted metastore transaction (= do not trigger a control plane restart)",
        subsystem: "control_plane",
    )
});

static METASTORE_ERROR_MAYBE_EXECUTED: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "metastore_error_maybe_executed",
        description: "Number of metastore transaction with an uncertain outcome (= do trigger a control plane restart)",
        subsystem: "control_plane",
    )
});

impl Default for ControlPlaneMetrics {
    fn default() -> Self {
        let open_shards = gauge!(parent: &*SHARDS, "state" => "open");
        let closed_shards = gauge!(parent: &*SHARDS, "state" => "closed");
        let local_shards = gauge!(parent: &*INDEXED_SHARDS, "locality" => "local");
        let remote_shards = gauge!(parent: &*INDEXED_SHARDS, "locality" => "remote");

        ControlPlaneMetrics {
            indexes_total: INDEXES_TOTAL.clone(),
            open_shards,
            closed_shards,
            apply_plan_total: APPLY_PLAN_TOTAL.clone(),
            rebalance_shards: REBALANCE_SHARDS.clone(),
            restart_total: RESTART_TOTAL.clone(),
            schedule_total: SCHEDULE_TOTAL.clone(),
            metastore_error_aborted: METASTORE_ERROR_ABORTED.clone(),
            metastore_error_maybe_executed: METASTORE_ERROR_MAYBE_EXECUTED.clone(),
            local_shards,
            remote_shards,
        }
    }
}

pub static CONTROL_PLANE_METRICS: LazyLock<ControlPlaneMetrics> =
    LazyLock::new(ControlPlaneMetrics::default);
