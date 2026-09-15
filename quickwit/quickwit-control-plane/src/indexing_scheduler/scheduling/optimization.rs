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
use quickwit_proto::types::{NodeId, ShardId, SourceUid};
use rand::Rng;
use rand::seq::SliceRandom;

use super::{
    Eligibility, IndexerInfo, SourceToSchedule, SourceToScheduleType,
    compute_max_num_shards_per_pipeline, shard_availability_zone,
};
use crate::IndexerPoolEntry;
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::{
    IndexingSchedulerState, MIN_DURATION_BETWEEN_SCHEDULING, build_indexer_tasks,
    get_indexing_plans_diff,
};
use crate::metrics::LOCALITY_REPAIRS_TOTAL;
use crate::model::ShardLocations;

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
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
    can_optimize_plan: bool,
) {
    if !can_optimize_plan || previous_plan != Some(physical_plan) {
        return;
    }
    if repair_physical_plan_density(physical_plan, sources) {
        return;
    }
    repair_physical_plan_locality(
        physical_plan,
        shard_locations,
        indexer_infos,
        &mut rand::rng(),
    );
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
        task_ords.sort_by_key(|task_ord| (indexing_tasks[*task_ord].shard_ids.len(), *task_ord));
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

struct LocalityRepairPipeline {
    indexer_id: NodeId,
    task_ord: usize,
    availability_zone: String,
    shard_ids: Vec<ShardId>,
}

fn repair_physical_plan_locality(
    physical_plan: &mut PhysicalIndexingPlan,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
    rng: &mut impl Rng,
) -> bool {
    let mut pipelines_by_source =
        collect_locality_repair_pipelines_by_source(physical_plan, indexer_infos);
    pipelines_by_source.shuffle(rng);

    for source_pipelines in pipelines_by_source {
        let selected_pipelines =
            select_locality_repair_pipelines(source_pipelines, shard_locations, indexer_infos, rng);
        let Some(replacement_shards) =
            arrange_shards_in_home_zones(&selected_pipelines, shard_locations, indexer_infos)
        else {
            continue;
        };
        apply_locality_repair(physical_plan, &selected_pipelines, replacement_shards);
        LOCALITY_REPAIRS_TOTAL.inc();
        return true;
    }
    false
}

fn collect_locality_repair_pipelines_by_source(
    physical_plan: &PhysicalIndexingPlan,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
) -> Vec<Vec<LocalityRepairPipeline>> {
    let mut pipelines_by_source: FnvHashMap<(SourceUid, u64), Vec<LocalityRepairPipeline>> =
        FnvHashMap::default();
    for (indexer_id, indexing_tasks) in physical_plan.indexing_tasks_per_indexer() {
        let indexer_info = &indexer_infos[indexer_id];
        if indexer_info.eligibility != Eligibility::Any {
            continue;
        }
        let Some(availability_zone) = indexer_info.availability_zone.as_ref() else {
            continue;
        };
        for (task_ord, indexing_task) in indexing_tasks.iter().enumerate() {
            if indexing_task.shard_ids.is_empty() {
                continue;
            }
            let source_uid = SourceUid {
                index_uid: indexing_task.index_uid().clone(),
                source_id: indexing_task.source_id.clone(),
            };
            pipelines_by_source
                .entry((source_uid, indexing_task.params_fingerprint))
                .or_default()
                .push(LocalityRepairPipeline {
                    indexer_id: indexer_id.clone(),
                    task_ord,
                    availability_zone: availability_zone.clone(),
                    shard_ids: indexing_task.shard_ids.clone(),
                });
        }
    }
    pipelines_by_source.into_values().collect()
}

fn select_locality_repair_pipelines(
    mut source_pipelines: Vec<LocalityRepairPipeline>,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
    rng: &mut impl Rng,
) -> Vec<LocalityRepairPipeline> {
    source_pipelines.shuffle(rng);

    let mut selected_pipelines: Vec<LocalityRepairPipeline> = Vec::new();
    for pipeline in source_pipelines {
        let availability_zone_selected = selected_pipelines
            .iter()
            .any(|selected| selected.availability_zone == pipeline.availability_zone);
        if availability_zone_selected {
            continue;
        }
        let has_foreign_shard = pipeline.shard_ids.iter().any(|shard_id| {
            let Some(shard_availability_zone) =
                shard_availability_zone(shard_id, shard_locations, indexer_infos)
            else {
                return false;
            };
            shard_availability_zone != pipeline.availability_zone.as_str()
        });
        if has_foreign_shard {
            selected_pipelines.push(pipeline);
        }
    }
    selected_pipelines
}

fn arrange_shards_in_home_zones(
    selected_pipelines: &[LocalityRepairPipeline],
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<NodeId, IndexerInfo>,
) -> Option<Vec<Vec<ShardId>>> {
    if selected_pipelines.len() < 2 {
        return None;
    }

    let mut remaining_shards: Vec<ShardId> = Vec::new();
    let mut replacement_shards: Vec<Vec<ShardId>> = Vec::new();
    for pipeline in selected_pipelines {
        let mut pipeline_replacement: Vec<ShardId> = Vec::new();
        for shard_id in &pipeline.shard_ids {
            let shard_is_local = shard_availability_zone(shard_id, shard_locations, indexer_infos)
                == Some(pipeline.availability_zone.as_str());
            if shard_is_local {
                pipeline_replacement.push(shard_id.clone());
            } else {
                remaining_shards.push(shard_id.clone());
            }
        }
        replacement_shards.push(pipeline_replacement);
    }

    let num_local_shards_before: usize = replacement_shards.iter().map(Vec::len).sum();
    for (pipeline, pipeline_replacement) in selected_pipelines.iter().zip(&mut replacement_shards) {
        while pipeline_replacement.len() < pipeline.shard_ids.len() {
            let Some(shard_position) = remaining_shards.iter().position(|shard_id| {
                shard_availability_zone(shard_id, shard_locations, indexer_infos)
                    == Some(pipeline.availability_zone.as_str())
            }) else {
                break;
            };
            pipeline_replacement.push(remaining_shards.swap_remove(shard_position));
        }
    }
    let num_local_shards_after: usize = replacement_shards.iter().map(Vec::len).sum();
    if num_local_shards_after <= num_local_shards_before {
        return None;
    }

    for (pipeline, pipeline_replacement) in selected_pipelines.iter().zip(&mut replacement_shards) {
        while pipeline_replacement.len() < pipeline.shard_ids.len() {
            let shard_id = remaining_shards
                .pop()
                .expect("selected pipeline capacity must equal the number of selected shards");
            pipeline_replacement.push(shard_id);
        }
    }
    debug_assert!(remaining_shards.is_empty());
    Some(replacement_shards)
}

fn apply_locality_repair(
    physical_plan: &mut PhysicalIndexingPlan,
    selected_pipelines: &[LocalityRepairPipeline],
    replacement_shards: Vec<Vec<ShardId>>,
) {
    for (pipeline, shard_ids) in selected_pipelines.iter().zip(replacement_shards) {
        let indexing_task = &mut physical_plan
            .indexing_tasks_per_indexer_mut()
            .get_mut(&pipeline.indexer_id)
            .expect("selected locality-repair indexer disappeared")[pipeline.task_ord];
        indexing_task.shard_ids = shard_ids;
    }
    physical_plan.normalize();
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::{Duration, Instant};

    use fnv::FnvHashMap;
    use quickwit_proto::indexing::{IndexingTask, mcpu};
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, ShardId, SourceUid};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::{
        LocalityRepairPipeline, arrange_shards_in_home_zones,
        collect_locality_repair_pipelines_by_source, is_plan_repair_due, is_running_plan_stable,
        repair_physical_plan_density, repair_physical_plan_locality,
        select_locality_repair_pipelines,
    };
    use crate::indexing_plan::PhysicalIndexingPlan;
    use crate::indexing_scheduler::scheduling::{
        Eligibility, IndexerInfo, SourceToSchedule, SourceToScheduleType, shard_ids_for_indexer,
    };
    use crate::indexing_scheduler::{IndexingSchedulerState, MIN_DURATION_BETWEEN_SCHEDULING};
    use crate::model::ShardLocations;

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

    fn indexer_info(availability_zone: &str) -> IndexerInfo {
        IndexerInfo {
            cpu_capacity: mcpu(4_000),
            availability_zone: Some(availability_zone.to_string()),
            eligibility: Eligibility::Any,
        }
    }

    fn locality_pipeline(
        indexer_id: &NodeId,
        task_ord: usize,
        availability_zone: &str,
        shard_ids: Vec<ShardId>,
    ) -> LocalityRepairPipeline {
        LocalityRepairPipeline {
            indexer_id: indexer_id.clone(),
            task_ord,
            availability_zone: availability_zone.to_string(),
            shard_ids,
        }
    }

    #[test]
    fn test_collect_locality_repair_pipelines_by_source() {
        let indexer_a = NodeId::from_str("indexer-a");
        let indexer_b = NodeId::from_str("indexer-b");
        let indexer_without_zone = NodeId::from_str("indexer-without-zone");
        let draining_indexer = NodeId::from_str("draining-indexer");
        let source_uid = source_uid();
        let mut plan = PhysicalIndexingPlan::with_indexer_ids(&[
            indexer_a.clone(),
            indexer_b.clone(),
            indexer_without_zone.clone(),
            draining_indexer.clone(),
        ]);
        plan.add_indexing_task(
            &indexer_a,
            indexing_task(&source_uid, 1, vec![ShardId::from(1)]),
        );
        plan.add_indexing_task(
            &indexer_b,
            indexing_task(&source_uid, 2, vec![ShardId::from(2)]),
        );
        let mut different_params = indexing_task(&source_uid, 3, vec![ShardId::from(3)]);
        different_params.params_fingerprint = 8;
        plan.add_indexing_task(&indexer_a, different_params);
        plan.add_indexing_task(
            &indexer_without_zone,
            indexing_task(&source_uid, 4, vec![ShardId::from(4)]),
        );
        plan.add_indexing_task(
            &draining_indexer,
            indexing_task(&source_uid, 5, vec![ShardId::from(5)]),
        );

        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer_a.clone(), indexer_info("az-a"));
        indexer_infos.insert(indexer_b.clone(), indexer_info("az-b"));
        let mut without_zone = indexer_info("unused");
        without_zone.availability_zone = None;
        indexer_infos.insert(indexer_without_zone, without_zone);
        let mut draining = indexer_info("az-c");
        draining.eligibility = Eligibility::SelfHostedOnly;
        indexer_infos.insert(draining_indexer, draining);

        let groups = collect_locality_repair_pipelines_by_source(&plan, &indexer_infos);
        let mut group_sizes: Vec<usize> = groups.iter().map(Vec::len).collect();
        group_sizes.sort();
        assert_eq!(group_sizes, vec![1, 2]);
        assert!(groups.iter().flatten().all(|pipeline| {
            pipeline.indexer_id == indexer_a || pipeline.indexer_id == indexer_b
        }));
    }

    #[test]
    fn test_select_locality_repair_pipelines() {
        let indexer_a = NodeId::from_str("indexer-a");
        let indexer_b = NodeId::from_str("indexer-b");
        let indexer_c = NodeId::from_str("indexer-c");
        let shard_a = ShardId::from(1);
        let shard_b = ShardId::from(2);
        let shard_c = ShardId::from(3);
        let another_shard_b = ShardId::from(4);
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer_a.clone(), indexer_info("az-a"));
        indexer_infos.insert(indexer_b.clone(), indexer_info("az-b"));
        indexer_infos.insert(indexer_c.clone(), indexer_info("az-c"));
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard_a, &indexer_a);
        shard_locations.add_location(&shard_b, &indexer_b);
        shard_locations.add_location(&shard_c, &indexer_c);
        shard_locations.add_location(&another_shard_b, &indexer_b);
        let candidates = vec![
            locality_pipeline(&indexer_a, 0, "az-a", vec![shard_b.clone()]),
            locality_pipeline(&indexer_a, 1, "az-a", vec![another_shard_b.clone()]),
            locality_pipeline(&indexer_b, 0, "az-b", vec![shard_a.clone()]),
            locality_pipeline(&indexer_c, 0, "az-c", vec![shard_c.clone()]),
            locality_pipeline(&indexer_c, 1, "az-c", vec![ShardId::from(99)]),
        ];

        let mut rng = StdRng::seed_from_u64(0);
        let selected = select_locality_repair_pipelines(
            candidates,
            &shard_locations,
            &indexer_infos,
            &mut rng,
        );
        let mut selected_zones: Vec<&str> = selected
            .iter()
            .map(|pipeline| pipeline.availability_zone.as_str())
            .collect();
        selected_zones.sort();
        assert_eq!(selected_zones, vec!["az-a", "az-b"]);
    }

    #[test]
    fn test_arrange_shards_in_home_zones() {
        let indexer_a = NodeId::from_str("indexer-a");
        let indexer_b = NodeId::from_str("indexer-b");
        let shard_a_1 = ShardId::from(1);
        let shard_a_2 = ShardId::from(2);
        let shard_b_1 = ShardId::from(3);
        let shard_b_2 = ShardId::from(4);
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer_a.clone(), indexer_info("az-a"));
        indexer_infos.insert(indexer_b.clone(), indexer_info("az-b"));
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard_a_1, &indexer_a);
        shard_locations.add_location(&shard_a_2, &indexer_a);
        shard_locations.add_location(&shard_b_1, &indexer_b);
        shard_locations.add_location(&shard_b_2, &indexer_b);
        let crossed_pipelines = vec![
            locality_pipeline(
                &indexer_a,
                0,
                "az-a",
                vec![shard_a_1.clone(), shard_b_1.clone()],
            ),
            locality_pipeline(
                &indexer_b,
                0,
                "az-b",
                vec![shard_b_2.clone(), shard_a_2.clone()],
            ),
        ];

        let mut replacements =
            arrange_shards_in_home_zones(&crossed_pipelines, &shard_locations, &indexer_infos)
                .unwrap();
        replacements.iter_mut().for_each(|shards| shards.sort());
        assert_eq!(replacements[0], vec![shard_a_1.clone(), shard_a_2.clone()]);
        assert_eq!(replacements[1], vec![shard_b_1.clone(), shard_b_2.clone()]);

        let local_pipelines = vec![
            locality_pipeline(
                &indexer_a,
                0,
                "az-a",
                vec![shard_a_1.clone(), shard_a_2.clone()],
            ),
            locality_pipeline(
                &indexer_b,
                0,
                "az-b",
                vec![shard_b_1.clone(), shard_b_2.clone()],
            ),
        ];
        assert!(
            arrange_shards_in_home_zones(&local_pipelines, &shard_locations, &indexer_infos,)
                .is_none()
        );
        assert!(
            arrange_shards_in_home_zones(&local_pipelines[..1], &shard_locations, &indexer_infos,)
                .is_none()
        );
    }

    #[test]
    fn test_is_running_plan_stable() {
        let indexer_id = NodeId::from_str("indexer");
        let plan = PhysicalIndexingPlan::with_indexer_ids(std::slice::from_ref(&indexer_id));
        let running_tasks = plan.indexing_tasks_per_indexer().clone();
        let ready_statuses = FnvHashMap::from_iter([(indexer_id.clone(), IngesterStatus::Ready)]);
        let mut state = IndexingSchedulerState::default();

        assert!(!is_running_plan_stable(
            &running_tasks,
            &ready_statuses,
            &state
        ));

        state.last_applied_physical_plan = Some(plan);
        state.last_applied_indexer_statuses = ready_statuses.clone();
        assert!(is_running_plan_stable(
            &running_tasks,
            &ready_statuses,
            &state
        ));

        assert!(!is_running_plan_stable(
            &FnvHashMap::default(),
            &ready_statuses,
            &state
        ));

        let retiring_statuses = FnvHashMap::from_iter([(indexer_id, IngesterStatus::Retiring)]);
        assert!(!is_running_plan_stable(
            &running_tasks,
            &retiring_statuses,
            &state
        ));
    }

    #[test]
    fn test_is_plan_repair_due() {
        let mut state = IndexingSchedulerState::default();
        assert!(!is_plan_repair_due(&state));

        state.last_applied_plan_timestamp = Some(Instant::now());
        assert!(!is_plan_repair_due(&state));

        let elapsed = MIN_DURATION_BETWEEN_SCHEDULING + Duration::from_millis(1);
        state.last_applied_plan_timestamp = Some(Instant::now() - elapsed);
        assert!(is_plan_repair_due(&state));

        state.last_plan_repair_attempt_timestamp = Some(Instant::now());
        assert!(!is_plan_repair_due(&state));

        state.last_plan_repair_attempt_timestamp = Some(Instant::now() - elapsed);
        assert!(is_plan_repair_due(&state));
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

    #[test]
    fn test_repair_physical_plan_locality() {
        let indexer_a = NodeId::from_str("indexer-a");
        let indexer_b = NodeId::from_str("indexer-b");
        let indexer_c = NodeId::from_str("indexer-c");
        let shard_a = ShardId::from(1);
        let shard_b = ShardId::from(2);
        let shard_c = ShardId::from(3);
        let source_uid = source_uid();
        let mut indexer_infos: FnvHashMap<NodeId, IndexerInfo> = FnvHashMap::default();
        indexer_infos.insert(indexer_a.clone(), indexer_info("az-a"));
        indexer_infos.insert(indexer_b.clone(), indexer_info("az-b"));
        indexer_infos.insert(indexer_c.clone(), indexer_info("az-c"));
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard_a, &indexer_a);
        shard_locations.add_location(&shard_b, &indexer_b);
        shard_locations.add_location(&shard_c, &indexer_c);

        let mut plan = PhysicalIndexingPlan::with_indexer_ids(&[
            indexer_a.clone(),
            indexer_b.clone(),
            indexer_c.clone(),
        ]);
        for (pipeline_ord, (indexer_id, shard_id)) in [
            (&indexer_a, shard_b.clone()),
            (&indexer_b, shard_c.clone()),
            (&indexer_c, shard_a.clone()),
        ]
        .into_iter()
        .enumerate()
        {
            plan.add_indexing_task(
                indexer_id,
                indexing_task(&source_uid, pipeline_ord as u128, vec![shard_id]),
            );
        }

        let mut rng = StdRng::seed_from_u64(0);
        assert!(repair_physical_plan_locality(
            &mut plan,
            &shard_locations,
            &indexer_infos,
            &mut rng,
        ));
        assert_eq!(
            shard_ids_for_indexer(&plan, &indexer_a),
            vec![shard_a.clone()]
        );
        assert_eq!(
            shard_ids_for_indexer(&plan, &indexer_b),
            vec![shard_b.clone()]
        );
        assert_eq!(
            shard_ids_for_indexer(&plan, &indexer_c),
            vec![shard_c.clone()]
        );
        assert!(!repair_physical_plan_locality(
            &mut plan,
            &shard_locations,
            &indexer_infos,
            &mut rng,
        ));
    }
}
