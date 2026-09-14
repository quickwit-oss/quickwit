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

use std::time::Instant;

use fnv::FnvHashMap;
use quickwit_proto::indexing::IndexingTask;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::NodeId;

use super::{SourceToSchedule, SourceToScheduleType, compute_max_num_shards_per_pipeline};
use crate::IndexerPoolEntry;
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::{
    IndexingSchedulerState, MIN_DURATION_BETWEEN_SCHEDULING, build_indexer_tasks,
    get_indexing_plans_diff,
};

pub(in crate::indexing_scheduler) fn is_plan_eligible_for_optimization(
    indexers: &[IndexerPoolEntry],
    indexer_statuses: &FnvHashMap<NodeId, IngesterStatus>,
    locality_aware: bool,
    state: &mut IndexingSchedulerState,
) -> bool {
    if !locality_aware || !is_plan_repair_due(state) {
        return false;
    }
    let running_indexer_tasks = build_indexer_tasks(indexers);
    if !is_running_plan_stable(&running_indexer_tasks, indexer_statuses, state) {
        return false;
    }
    state.last_plan_repair_attempt_timestamp = Some(Instant::now());
    true
}

pub(super) fn conditionally_optimize_plan(
    physical_plan: &mut PhysicalIndexingPlan,
    previous_plan: Option<&PhysicalIndexingPlan>,
    sources: &[SourceToSchedule],
    can_optimize_plan: bool,
) {
    if !can_optimize_plan || previous_plan != Some(physical_plan) {
        return;
    }
    repair_physical_plan_density(physical_plan, sources);
}

fn is_running_plan_stable(
    current_indexer_tasks: &FnvHashMap<NodeId, Vec<IndexingTask>>,
    current_indexer_statuses: &FnvHashMap<NodeId, IngesterStatus>,
    state: &IndexingSchedulerState,
) -> bool {
    if current_indexer_statuses
        .values()
        .any(|status| *status != IngesterStatus::Ready)
    {
        return false;
    }
    let Some(last_applied_plan) = state.last_applied_physical_plan.as_ref() else {
        return false;
    };
    let current_running_plan_diff = get_indexing_plans_diff(
        current_indexer_tasks,
        last_applied_plan.indexing_tasks_per_indexer(),
        current_indexer_statuses,
        &state.last_applied_indexer_statuses,
    );
    current_running_plan_diff.is_empty()
}

fn is_plan_repair_due(state: &IndexingSchedulerState) -> bool {
    let Some(last_applied_plan_timestamp) = state.last_applied_plan_timestamp else {
        return false;
    };
    if last_applied_plan_timestamp.elapsed() < MIN_DURATION_BETWEEN_SCHEDULING {
        return false;
    }
    let Some(last_attempt) = state.last_plan_repair_attempt_timestamp else {
        return true;
    };
    last_attempt.elapsed() >= MIN_DURATION_BETWEEN_SCHEDULING
}

struct DensityRepair {
    indexer_id: NodeId,
    donor_task_ord: usize,
    receiver_task_ord: usize,
}

fn repair_physical_plan_density(
    physical_plan: &mut PhysicalIndexingPlan,
    sources: &[SourceToSchedule],
) -> bool {
    for source in sources {
        if !matches!(source.source_type, SourceToScheduleType::Sharded { .. }) {
            continue;
        }
        let max_num_shards =
            compute_max_num_shards_per_pipeline(&source.source_type).get() as usize;
        let Some(repair) = select_density_repair(physical_plan, source, max_num_shards) else {
            continue;
        };
        apply_density_repair(physical_plan, repair);
        return true;
    }
    false
}

fn select_density_repair(
    physical_plan: &PhysicalIndexingPlan,
    source: &SourceToSchedule,
    max_num_shards: usize,
) -> Option<DensityRepair> {
    for (indexer_id, indexing_tasks) in physical_plan.indexing_tasks_per_indexer() {
        let mut task_ords: Vec<usize> = indexing_tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| {
                task.index_uid.as_ref() == Some(&source.source_uid.index_uid)
                    && task.source_id == source.source_uid.source_id
                    && task.params_fingerprint == source.params_fingerprint
            })
            .map(|(task_ord, _)| task_ord)
            .collect();
        task_ords.sort_by_key(|task_ord| {
            (indexing_tasks[*task_ord].shard_ids.len(), *task_ord)
        });
        let [donor_task_ord, receiver_task_ord, ..] = task_ords.as_slice() else {
            continue;
        };
        let combined_num_shards = indexing_tasks[*donor_task_ord].shard_ids.len()
            + indexing_tasks[*receiver_task_ord].shard_ids.len();
        if combined_num_shards <= max_num_shards {
            return Some(DensityRepair {
                indexer_id: indexer_id.clone(),
                donor_task_ord: *donor_task_ord,
                receiver_task_ord: *receiver_task_ord,
            });
        }
    }
    None
}

fn apply_density_repair(physical_plan: &mut PhysicalIndexingPlan, repair: DensityRepair) {
    let indexing_tasks = physical_plan
        .indexing_tasks_per_indexer_mut()
        .get_mut(&repair.indexer_id)
        .expect("selected density-repair indexer disappeared");
    let donor_shards = std::mem::take(&mut indexing_tasks[repair.donor_task_ord].shard_ids);
    indexing_tasks[repair.receiver_task_ord]
        .shard_ids
        .extend(donor_shards);
    indexing_tasks.remove(repair.donor_task_ord);
    physical_plan.normalize();
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, ShardId, SourceUid};

    use super::repair_physical_plan_density;
    use crate::indexing_plan::PhysicalIndexingPlan;
    use crate::indexing_scheduler::scheduling::{SourceToSchedule, SourceToScheduleType};

    fn source_uid() -> SourceUid {
        SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        }
    }

    fn indexing_task(
        source_uid: &SourceUid,
        pipeline_uid: u128,
        shard_ids: Vec<ShardId>,
    ) -> IndexingTask {
        IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            pipeline_uid: Some(PipelineUid::for_test(pipeline_uid)),
            shard_ids,
            params_fingerprint: 7,
        }
    }

    #[test]
    fn test_repair_physical_plan_density() {
        let indexer_id = NodeId::from_str("indexer");
        let source_uid = source_uid();
        let source = SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![ShardId::from(1), ShardId::from(2), ShardId::from(3)],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 7,
        };
        let mut plan = PhysicalIndexingPlan::with_indexer_ids(std::slice::from_ref(&indexer_id));
        plan.add_indexing_task(
            &indexer_id,
            indexing_task(&source_uid, 1, vec![ShardId::from(1)]),
        );
        plan.add_indexing_task(
            &indexer_id,
            indexing_task(&source_uid, 2, vec![ShardId::from(2), ShardId::from(3)]),
        );

        assert!(repair_physical_plan_density(
            &mut plan,
            std::slice::from_ref(&source)
        ));
        assert_eq!(plan.indexer(&indexer_id).unwrap().len(), 1);
        assert_eq!(
            plan.indexer(&indexer_id).unwrap()[0].shard_ids,
            vec![ShardId::from(1), ShardId::from(2), ShardId::from(3)]
        );
        assert_eq!(
            plan.indexer(&indexer_id).unwrap()[0].pipeline_uid,
            Some(PipelineUid::for_test(2))
        );
        assert!(!repair_physical_plan_density(
            &mut plan,
            std::slice::from_ref(&source)
        ));
    }
}
