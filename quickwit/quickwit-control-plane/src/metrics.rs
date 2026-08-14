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

use quickwit_metrics::{LabelNames, LazyCounter, LazyGauge, label_names, lazy_counter, lazy_gauge};

#[derive(Debug, Clone, Copy)]
pub struct ShardLocalityMetrics {
    // shards on other indexers if az-awareness is off; cross-az if its on
    pub num_remote_shards: usize,
    // not used if az-awareness is off; same-az if its on
    pub num_nearby_shards: usize,
    // shards hosted on this indexer
    pub num_local_shards: usize,
}

impl ShardLocalityMetrics {
    /// Share of shards indexed without crossing an availability zone, as a percentage.
    pub fn locality_percent(self) -> u32 {
        let num_shards =
            self.num_local_shards + self.num_nearby_shards + self.num_remote_shards;
        if num_shards == 0 {
            return 100;
        }
        let num_local_or_nearby_shards = self.num_local_shards + self.num_nearby_shards;
        (num_local_or_nearby_shards * 100 / num_shards) as u32
    }

    pub fn publish(self) {
        LOCAL_SHARDS.set(self.num_local_shards as f64);
        NEARBY_SHARDS.set(self.num_nearby_shards as f64);
        REMOTE_SHARDS.set(self.num_remote_shards as f64);
    }
}

pub(crate) static INDEXES_TOTAL: LazyGauge = lazy_gauge!(
        name: "indexes_total",
        description: "Number of indexes tracked by the control plane.",
        subsystem: "control_plane",
);

static SHARDS: LazyGauge = lazy_gauge!(
        name: "shards",
        description: "Number of open and closed shards tracked by the ingest controller",
        subsystem: "control_plane",
);

pub(crate) static OPEN_SHARDS: LazyGauge = lazy_gauge!(parent: SHARDS, "state" => "open");

pub(crate) static CLOSED_SHARDS: LazyGauge = lazy_gauge!(parent: SHARDS, "state" => "closed");

pub(crate) const INDEX_ID_LABEL_NAMES: LabelNames<1> = label_names!("index_id");

static INDEXED_SHARDS: LazyGauge = lazy_gauge!(
        name: "indexed_shards",
        description: "Number of (remote/nearby/local) shards in the indexing plan",
        subsystem: "control_plane",
);

pub(crate) static LOCAL_SHARDS: LazyGauge =
    lazy_gauge!(parent: INDEXED_SHARDS, "locality" => "local");

pub(crate) static NEARBY_SHARDS: LazyGauge =
    lazy_gauge!(parent: INDEXED_SHARDS, "locality" => "nearby");

pub(crate) static REMOTE_SHARDS: LazyGauge =
    lazy_gauge!(parent: INDEXED_SHARDS, "locality" => "remote");

pub(crate) static APPLY_PLAN_TOTAL: LazyCounter = lazy_counter!(
        name: "apply_plan_total",
        description: "Number of control plane `apply plan` operations.",
        subsystem: "control_plane",
);

pub(crate) static REBALANCE_SHARDS: LazyGauge = lazy_gauge!(
        name: "rebalance_shards",
        description: "Number of shards rebalanced by the control plane.",
        subsystem: "control_plane",
);

pub(crate) static RESTART_TOTAL: LazyCounter = lazy_counter!(
        name: "restart_total",
        description: "Number of control plane restarts.",
        subsystem: "control_plane",
);

pub(crate) static SCHEDULE_TOTAL: LazyCounter = lazy_counter!(
        name: "schedule_total",
        description: "Number of control plane `schedule` operations.",
        subsystem: "control_plane",
);

pub(crate) static METASTORE_ERROR_ABORTED: LazyCounter = lazy_counter!(
        name: "metastore_error_aborted",
        description: "Number of aborted metastore transaction (= do not trigger a control plane restart)",
        subsystem: "control_plane",
);

pub(crate) static METASTORE_ERROR_MAYBE_EXECUTED: LazyCounter = lazy_counter!(
        name: "metastore_error_maybe_executed",
        description: "Number of metastore transaction with an uncertain outcome (= do trigger a control plane restart)",
        subsystem: "control_plane",
);
