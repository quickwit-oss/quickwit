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

use quickwit_metrics::{Counter, Gauge, Labels, counter, gauge};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    pub num_remote_shards: usize,
    pub num_local_shards: usize,
}

impl ShardLocalityMetrics {
    pub fn publish(self) {
        LOCAL_SHARDS.set(self.num_local_shards as f64);
        REMOTE_SHARDS.set(self.num_remote_shards as f64);
    }
}

pub(crate) static INDEXES_TOTAL: LazyLock<Gauge> = LazyLock::new(|| {
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

pub(crate) static OPEN_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: SHARDS, "state" => "open"));

pub(crate) static CLOSED_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: SHARDS, "state" => "closed"));

pub(crate) const INDEX_ID_LABELS: Labels<1> = Labels::new(["index_id"]);

static INDEXED_SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "indexed_shards",
        description: "Number of (remote/local) shards in the indexing plan",
        subsystem: "control_plane",
    )
});

pub(crate) static LOCAL_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: INDEXED_SHARDS, "locality" => "local"));

pub(crate) static REMOTE_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: INDEXED_SHARDS, "locality" => "remote"));

pub(crate) static APPLY_PLAN_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "apply_plan_total",
        description: "Number of control plane `apply plan` operations.",
        subsystem: "control_plane",
    )
});

pub(crate) static REBALANCE_SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "rebalance_shards",
        description: "Number of shards rebalanced by the control plane.",
        subsystem: "control_plane",
    )
});

pub(crate) static RESTART_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "restart_total",
        description: "Number of control plane restarts.",
        subsystem: "control_plane",
    )
});

pub(crate) static SCHEDULE_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "schedule_total",
        description: "Number of control plane `schedule` operations.",
        subsystem: "control_plane",
    )
});

pub(crate) static METASTORE_ERROR_ABORTED: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "metastore_error_aborted",
        description: "Number of aborted metastore transaction (= do not trigger a control plane restart)",
        subsystem: "control_plane",
    )
});

pub(crate) static METASTORE_ERROR_MAYBE_EXECUTED: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "metastore_error_maybe_executed",
        description: "Number of metastore transaction with an uncertain outcome (= do trigger a control plane restart)",
        subsystem: "control_plane",
    )
});
