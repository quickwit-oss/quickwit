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

use fnv::FnvHashMap;
use quickwit_metrics::{
    LabelNames, LazyCounter, LazyGauge, gauge, label_names, label_values, lazy_counter, lazy_gauge,
};
use quickwit_proto::types::{NodeId, PipelineUid, ShardId};

use crate::indexing_plan::PhysicalIndexingPlan;

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
        let num_shards = self.num_local_shards + self.num_nearby_shards + self.num_remote_shards;
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

const INDEXER_ID_NUM_SHARDS_LABEL_NAMES: LabelNames<2> =
    label_names!("indexer_id", "num_shards");

static INDEXING_PIPELINES: LazyGauge = lazy_gauge!(
        name: "indexing_pipelines",
        description: "Number of pipelines in the latest applied indexing plan, grouped by indexer and assigned shard count.",
        subsystem: "control_plane",
);

static INDEXING_PIPELINE_RESETS_TOTAL: LazyCounter = lazy_counter!(
        name: "indexing_pipeline_resets_total",
        description: "Number of running pipelines whose live shard assignment was reset by an indexing plan change.",
        subsystem: "control_plane",
);

static INDEXING_PIPELINE_RESETS_FROM_MOVES: LazyCounter =
    lazy_counter!(parent: INDEXING_PIPELINE_RESETS_TOTAL, "reason" => "shard_moved");

static INDEXING_PIPELINE_RESETS_FROM_REPACKING: LazyCounter =
    lazy_counter!(parent: INDEXING_PIPELINE_RESETS_TOTAL, "reason" => "pipeline_repacked");

static INDEXING_SHARD_MOVES_TOTAL: LazyCounter = lazy_counter!(
        name: "indexing_shard_moves_total",
        description: "Number of live shards moved to another indexer by an indexing plan change.",
        subsystem: "control_plane",
);

#[derive(Default)]
struct IndexingPlanChurn {
    num_moved_shards: usize,
    num_pipeline_resets_from_moves: usize,
    num_pipeline_resets_from_repacking: usize,
}

type PipelineKey = (NodeId, PipelineUid);

fn pipeline_counts_by_indexer_and_shard_count(
    plan: &PhysicalIndexingPlan,
) -> FnvHashMap<(NodeId, usize), usize> {
    let mut pipeline_counts = FnvHashMap::default();
    for (indexer_id, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            *pipeline_counts
                .entry((indexer_id.clone(), task.shard_ids.len()))
                .or_default() += 1;
        }
    }
    pipeline_counts
}

fn plan_assignments(
    plan: &PhysicalIndexingPlan,
) -> (
    FnvHashMap<PipelineKey, Vec<ShardId>>,
    FnvHashMap<ShardId, PipelineKey>,
) {
    let mut shards_per_pipeline: FnvHashMap<PipelineKey, Vec<ShardId>> = FnvHashMap::default();
    let mut pipeline_per_shard = FnvHashMap::default();
    for (indexer_id, tasks) in plan.indexing_tasks_per_indexer() {
        for task in tasks {
            let pipeline = (indexer_id.clone(), task.pipeline_uid());
            shards_per_pipeline
                .entry(pipeline.clone())
                .or_default()
                .extend(task.shard_ids.iter().cloned());
            for shard_id in &task.shard_ids {
                pipeline_per_shard.insert(shard_id.clone(), pipeline.clone());
            }
        }
    }
    (shards_per_pipeline, pipeline_per_shard)
}

fn compute_indexing_plan_churn(
    previous_plan: &PhysicalIndexingPlan,
    new_plan: &PhysicalIndexingPlan,
) -> IndexingPlanChurn {
    let (previous_shards_per_pipeline, previous_pipeline_per_shard) =
        plan_assignments(previous_plan);
    let (_, new_pipeline_per_shard) = plan_assignments(new_plan);
    let mut churn = IndexingPlanChurn::default();

    for (shard_id, new_pipeline) in &new_pipeline_per_shard {
        if previous_pipeline_per_shard
            .get(shard_id)
            .is_some_and(|previous_pipeline| previous_pipeline.0 != new_pipeline.0)
        {
            churn.num_moved_shards += 1;
        }
    }

    for (previous_pipeline, previous_shards) in previous_shards_per_pipeline {
        let mut lost_shard_to_another_indexer = false;
        let mut lost_shard_to_another_pipeline = false;
        for shard_id in previous_shards {
            let Some(new_pipeline) = new_pipeline_per_shard.get(&shard_id) else {
                // Completed and deleted shards leave the plan without resetting their pipeline.
                continue;
            };
            if new_pipeline == &previous_pipeline {
                continue;
            }
            lost_shard_to_another_pipeline = true;
            if new_pipeline.0 != previous_pipeline.0 {
                lost_shard_to_another_indexer = true;
                break;
            }
        }
        if lost_shard_to_another_indexer {
            churn.num_pipeline_resets_from_moves += 1;
        } else if lost_shard_to_another_pipeline {
            churn.num_pipeline_resets_from_repacking += 1;
        }
    }
    churn
}

fn set_pipeline_count(indexer_id: &NodeId, num_shards: usize, num_pipelines: usize) {
    let labels = label_values!(
        INDEXER_ID_NUM_SHARDS_LABEL_NAMES =>
        indexer_id.to_string(),
        num_shards.to_string()
    );
    gauge!(parent: INDEXING_PIPELINES, labels: [labels]).set(num_pipelines as f64);
}

pub(crate) fn publish_indexing_plan_metrics(
    previous_plan: Option<&PhysicalIndexingPlan>,
    new_plan: &PhysicalIndexingPlan,
) {
    if let Some(previous_plan) = previous_plan {
        for ((indexer_id, num_shards), _) in
            pipeline_counts_by_indexer_and_shard_count(previous_plan)
        {
            set_pipeline_count(&indexer_id, num_shards, 0);
        }
        let churn = compute_indexing_plan_churn(previous_plan, new_plan);
        INDEXING_SHARD_MOVES_TOTAL.inc_by(churn.num_moved_shards as u64);
        INDEXING_PIPELINE_RESETS_FROM_MOVES
            .inc_by(churn.num_pipeline_resets_from_moves as u64);
        INDEXING_PIPELINE_RESETS_FROM_REPACKING
            .inc_by(churn.num_pipeline_resets_from_repacking as u64);
    }
    for ((indexer_id, num_shards), num_pipelines) in
        pipeline_counts_by_indexer_and_shard_count(new_plan)
    {
        set_pipeline_count(&indexer_id, num_shards, num_pipelines);
    }
}

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
