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

pub mod scheduling_logic;
pub mod scheduling_logic_model;

use std::num::NonZeroU32;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_proto::indexing::{CpuCapacity, IndexingTask};
use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};
use scheduling_logic_model::{IndexerOrd, SourceOrd};
use tracing::{error, warn};

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling::scheduling_logic_model::{
    IndexerAssignment, SchedulingProblem, SchedulingSolution,
};

/// If we have several pipelines below this threshold we
/// reduce the number of pipelines.
///
/// Note that even for 2 pipelines, this creates an hysteris effect.
///
/// Starting from a single pipeline.
/// An overall load above 80% is enough to trigger the creation of a
/// second pipeline.
///
/// Coming back to a single pipeline requires having a load per pipeline
/// of 30%. Which translates into an overall load of 60%.
const CPU_PER_PIPELINE_LOAD_LOWER_THRESHOLD: CpuCapacity = CpuCapacity::from_cpu_millis(1_200);

/// That's 80% of a period
const MAX_LOAD_PER_PIPELINE: CpuCapacity = CpuCapacity::from_cpu_millis(3_200);

fn populate_problem(
    source: &SourceToSchedule,
    problem: &mut SchedulingProblem,
) -> Option<SourceOrd> {
    match &source.source_type {
        SourceToScheduleType::IngestV1 => {
            // TODO ingest v1 is scheduled differently
            None
        }
        SourceToScheduleType::Sharded {
            shard_ids,
            load_per_shard,
        } => {
            let num_shards = shard_ids.len() as u32;
            let source_ord = problem.add_source(num_shards, *load_per_shard);
            Some(source_ord)
        }
        SourceToScheduleType::NonSharded {
            num_pipelines,
            load_per_pipeline,
        } => {
            let source_ord = problem.add_source(*num_pipelines, *load_per_pipeline);
            Some(source_ord)
        }
    }
}

#[derive(Default)]
struct IdToOrdMap {
    indexer_ids: Vec<String>,
    source_uids: Vec<SourceUid>,
    indexer_id_to_indexer_ord: FnvHashMap<String, IndexerOrd>,
    source_uid_to_source_ord: FnvHashMap<SourceUid, SourceOrd>,
}

impl IdToOrdMap {
    fn add_source_uid(&mut self, source_uid: SourceUid) -> SourceOrd {
        let source_ord = self.source_uid_to_source_ord.len() as SourceOrd;
        self.source_uid_to_source_ord
            .insert(source_uid.clone(), source_ord);
        self.source_uids.push(source_uid);
        source_ord
    }

    fn source_ord(&self, source_uid: &SourceUid) -> Option<SourceOrd> {
        self.source_uid_to_source_ord.get(source_uid).copied()
    }

    fn indexer_ord(&self, indexer_id: &str) -> Option<IndexerOrd> {
        self.indexer_id_to_indexer_ord.get(indexer_id).copied()
    }

    fn add_indexer_id(&mut self, indexer_id: String) -> IndexerOrd {
        let indexer_ord = self.indexer_ids.len() as IndexerOrd;
        self.indexer_id_to_indexer_ord
            .insert(indexer_id.clone(), indexer_ord);
        self.indexer_ids.push(indexer_id);
        indexer_ord
    }
}

fn convert_physical_plan_to_solution(
    plan: &PhysicalIndexingPlan,
    id_to_ord_map: &IdToOrdMap,
    solution: &mut SchedulingSolution,
) {
    for (indexer_id, indexing_tasks) in plan.indexing_tasks_per_indexer() {
        if let Some(indexer_ord) = id_to_ord_map.indexer_ord(indexer_id) {
            let indexer_assignment = &mut solution.indexer_assignments[indexer_ord];
            for indexing_task in indexing_tasks {
                let source_uid = SourceUid {
                    index_uid: IndexUid::from(indexing_task.index_uid.clone()),
                    source_id: indexing_task.source_id.clone(),
                };
                if let Some(source_ord) = id_to_ord_map.source_ord(&source_uid) {
                    indexer_assignment.add_shards(source_ord, indexing_task.shard_ids.len() as u32);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct SourceToSchedule {
    pub source_uid: SourceUid,
    pub source_type: SourceToScheduleType,
}

#[derive(Debug)]
pub enum SourceToScheduleType {
    Sharded {
        shard_ids: Vec<ShardId>,
        load_per_shard: NonZeroU32,
    },
    NonSharded {
        num_pipelines: u32,
        load_per_pipeline: NonZeroU32,
    },
    // deprecated
    IngestV1,
}

fn compute_max_num_shards_per_pipeline(source_type: &SourceToScheduleType) -> NonZeroU32 {
    match &source_type {
        SourceToScheduleType::Sharded { load_per_shard, .. } => {
            NonZeroU32::new(MAX_LOAD_PER_PIPELINE.cpu_millis() / load_per_shard.get())
                .unwrap_or_else(|| {
                    // We throttle shard at ingestion to ensure that a shard does not
                    // exceed 5MB/s.
                    //
                    // This value has been chosen to make sure that one full pipeline
                    // should always be able to handle the load of one shard.
                    //
                    // However it is possible for the system to take more than this
                    // when it is playing catch up.
                    //
                    // This is a transitory state, and not a problem per se.
                    warn!("load per shard is higher than `MAX_LOAD_PER_PIPELINE`");
                    NonZeroU32::MIN // also colloquially known as `1`
                })
        }
        SourceToScheduleType::IngestV1 | SourceToScheduleType::NonSharded { .. } => {
            NonZeroU32::new(1u32).unwrap()
        }
    }
}

fn convert_scheduling_solution_to_physical_plan_single_node_single_source(
    mut remaining_num_shards_to_schedule_on_node: u32,
    // Specific to the source.
    mut previous_tasks: &[&IndexingTask],
    source: &SourceToSchedule,
) -> Vec<IndexingTask> {
    match &source.source_type {
        SourceToScheduleType::Sharded {
            shard_ids,
            load_per_shard,
        } => {
            // For the moment we do something voluntarily suboptimal.
            let max_num_pipelines = quickwit_common::div_ceil_u32(
                shard_ids.len() as u32 * load_per_shard.get(),
                CPU_PER_PIPELINE_LOAD_LOWER_THRESHOLD.cpu_millis(),
            );
            if previous_tasks.len() > max_num_pipelines as usize {
                previous_tasks = &previous_tasks[..max_num_pipelines as usize];
            }
            let max_num_shards_per_pipeline: NonZeroU32 =
                compute_max_num_shards_per_pipeline(&source.source_type);
            let mut new_tasks = Vec::new();
            for previous_task in previous_tasks {
                let max_shard_in_pipeline = max_num_shards_per_pipeline
                    .get()
                    .min(remaining_num_shards_to_schedule_on_node)
                    as usize;
                let shard_ids: Vec<ShardId> = previous_task
                    .shard_ids
                    .iter()
                    .copied()
                    .filter(|shard_id| shard_ids.contains(shard_id))
                    .take(max_shard_in_pipeline)
                    .collect();
                remaining_num_shards_to_schedule_on_node -= shard_ids.len() as u32;
                let new_task = IndexingTask {
                    index_uid: previous_task.index_uid.clone(),
                    source_id: previous_task.source_id.clone(),
                    pipeline_uid: previous_task.pipeline_uid,
                    shard_ids,
                };
                new_tasks.push(new_task);
                if remaining_num_shards_to_schedule_on_node == 0 {
                    break;
                }
            }
            new_tasks
        }
        SourceToScheduleType::NonSharded { .. } => {
            // For non-sharded pipelines, we just need `num_shards` is a number of pipelines.
            let mut indexing_tasks: Vec<IndexingTask> = previous_tasks
                .iter()
                .take(remaining_num_shards_to_schedule_on_node as usize)
                .map(|task| (*task).clone())
                .collect();
            indexing_tasks.resize_with(remaining_num_shards_to_schedule_on_node as usize, || {
                IndexingTask {
                    index_uid: source.source_uid.index_uid.to_string(),
                    source_id: source.source_uid.source_id.clone(),
                    pipeline_uid: Some(PipelineUid::new()),
                    shard_ids: Vec::new(),
                }
            });
            indexing_tasks
        }
        SourceToScheduleType::IngestV1 => {
            // Ingest V1 is simple. One pipeline per indexer node.
            if let Some(indexing_task) = previous_tasks.first() {
                // The pipeline already exists, let's reuse it.
                vec![(*indexing_task).clone()]
            } else {
                // The source is new, we need to create a new task.
                vec![IndexingTask {
                    index_uid: source.source_uid.index_uid.to_string(),
                    source_id: source.source_uid.source_id.clone(),
                    pipeline_uid: Some(PipelineUid::new()),
                    shard_ids: Vec::new(),
                }]
            }
        }
    }
}

fn convert_scheduling_solution_to_physical_plan_single_node(
    indexer_assigment: &IndexerAssignment,
    previous_tasks: &[IndexingTask],
    sources: &[SourceToSchedule],
    id_to_ord_map: &IdToOrdMap,
) -> Vec<IndexingTask> {
    let mut tasks = Vec::new();
    for source in sources {
        let source_num_shards =
            if let Some(source_ord) = id_to_ord_map.source_ord(&source.source_uid) {
                indexer_assigment.num_shards(source_ord)
            } else {
                // This can happen for IngestV1
                1u32
            };
        let source_pipelines: Vec<&IndexingTask> = previous_tasks
            .iter()
            .filter(|task| {
                task.index_uid == source.source_uid.index_uid.as_str()
                    && task.source_id == source.source_uid.source_id
            })
            .collect();
        let source_tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
            source_num_shards,
            &source_pipelines[..],
            source,
        );
        tasks.extend(source_tasks);
    }
    // code goes here.
    tasks.sort_by(|left: &IndexingTask, right: &IndexingTask| {
        left.index_uid
            .cmp(&right.index_uid)
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    tasks
}

fn pick_indexer(capacity_per_node: &[(String, u32)]) -> impl Iterator<Item = &str> {
    capacity_per_node.iter().flat_map(|(node_id, capacity)| {
        std::iter::repeat(node_id.as_str()).take(*capacity as usize)
    })
}

/// This function takes a scheduling solution (which abstracts the notion of pipelines,
/// and shard ids) and builds a physical plan, attempting to make as little change as possible
/// to the existing pipelines.
///
/// We do not support moving shard from one pipeline to another, so if required this function may
/// also return instruction about deleting / adding new shards.
fn convert_scheduling_solution_to_physical_plan(
    mut solution: SchedulingSolution,
    id_to_ord_map: &IdToOrdMap,
    sources: &[SourceToSchedule],
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
) -> PhysicalIndexingPlan {
    let mut new_physical_plan = PhysicalIndexingPlan::with_indexer_ids(&id_to_ord_map.indexer_ids);
    for (indexer_id, indexer_assignment) in id_to_ord_map
        .indexer_ids
        .iter()
        .zip(&mut solution.indexer_assignments)
    {
        let previous_tasks_for_indexer = previous_plan_opt
            .and_then(|previous_plan| previous_plan.indexer(indexer_id))
            .unwrap_or(&[]);
        // First we attempt to recycle existing pipelines.
        let new_plan_indexing_tasks_for_indexer: Vec<IndexingTask> =
            convert_scheduling_solution_to_physical_plan_single_node(
                indexer_assignment,
                previous_tasks_for_indexer,
                sources,
                id_to_ord_map,
            );
        for indexing_task in new_plan_indexing_tasks_for_indexer {
            new_physical_plan.add_indexing_task(indexer_id, indexing_task);
        }
    }

    // We still need to do some extra work for sharded sources: assign missing shards, and possibly
    // adding extra pipelines.
    for source in sources {
        let SourceToScheduleType::Sharded { shard_ids, .. } = &source.source_type else {
            continue;
        };
        let source_ord = id_to_ord_map.source_ord(&source.source_uid).unwrap();
        let mut scheduled_shards: FnvHashSet<ShardId> = FnvHashSet::default();
        let mut remaining_capacity_per_node: Vec<(String, u32)> = Vec::default();
        for (indexer, indexing_tasks) in new_physical_plan.indexing_tasks_per_indexer_mut() {
            let indexer_ord = id_to_ord_map.indexer_ord(indexer).unwrap();
            let mut num_shards_for_indexer_source: u32 =
                solution.indexer_assignments[indexer_ord].num_shards(source_ord);
            for indexing_task in indexing_tasks {
                if indexing_task.index_uid == source.source_uid.index_uid.as_str()
                    && indexing_task.source_id == source.source_uid.source_id
                {
                    indexing_task.shard_ids.retain(|&shard| {
                        let shard_added = scheduled_shards.insert(shard);
                        if shard_added {
                            true
                        } else {
                            error!(
                                "this should never happen. shard was allocated into two pipelines."
                            );
                            false
                        }
                    });
                    num_shards_for_indexer_source -= indexing_task.shard_ids.len() as u32;
                }
            }
            remaining_capacity_per_node.push((indexer.to_string(), num_shards_for_indexer_source));
        }

        // Missing shards is an iterator over the shards that are not scheduled into a pipeline yet.
        let missing_shards = shard_ids
            .iter()
            .filter(|shard_id| !scheduled_shards.contains(shard_id))
            .copied();

        // Let's assign the missing shards.

        // TODO that's the logic that has to change. Eventually we want to remove shards that
        // were previously allocated and create new shards to replace them.
        let max_shard_per_pipeline = compute_max_num_shards_per_pipeline(&source.source_type);
        for (missing_shard, indexer_str) in
            missing_shards.zip(pick_indexer(&remaining_capacity_per_node))
        {
            add_shard_to_indexer(
                missing_shard,
                indexer_str,
                &source.source_uid,
                max_shard_per_pipeline,
                &mut new_physical_plan,
            );
        }
    }

    new_physical_plan.normalize();

    new_physical_plan
}

fn add_shard_to_indexer(
    missing_shard: ShardId,
    indexer: &str,
    source_uid: &SourceUid,
    max_shard_per_pipeline: NonZeroU32,
    new_physical_plan: &mut PhysicalIndexingPlan,
) {
    let indexer_tasks = new_physical_plan
        .indexing_tasks_per_indexer_mut()
        .entry(indexer.to_string())
        .or_default();

    let indexing_task_opt = indexer_tasks
        .iter_mut()
        .filter(|indexing_task| {
            indexing_task.index_uid == source_uid.index_uid.as_str()
                && indexing_task.source_id == source_uid.source_id
        })
        .filter(|task| task.shard_ids.len() < max_shard_per_pipeline.get() as usize)
        .min_by_key(|task| task.shard_ids.len());

    if let Some(indexing_task) = indexing_task_opt {
        indexing_task.shard_ids.push(missing_shard);
    } else {
        // We haven't found any pipeline with remaining room.
        // It is time to create a new pipeline.
        indexer_tasks.push(IndexingTask {
            index_uid: source_uid.index_uid.to_string(),
            source_id: source_uid.source_id.clone(),
            pipeline_uid: Some(PipelineUid::new()),
            shard_ids: vec![missing_shard],
        });
    }
}

/// Creates a physical plan given the current situation of the cluster and the list of sources
/// to schedule.
///
/// The scheduling problem abstracts all notion of shard ids, source types, and node_ids,
/// to transform scheduling into a math problem.
///
/// This function implementation therefore goes
/// - 1) transform our problem into a scheduling problem. Something closer to a well-defined
/// optimization problem. In particular this step removes:
///   - the notion of shard ids, and only considers a number of shards being allocated.
///   - node_ids and shard ids. These are replaced by integers.
/// - 2) convert the current situation of the cluster into something a previous scheduling
/// solution.
/// - 3) compute the new scheduling solution.
/// - 4) convert the new scheduling solution back to the real world by reallocating the shard ids.
///
/// TODO cut into pipelines.
pub fn build_physical_indexing_plan(
    sources: &[SourceToSchedule],
    indexer_id_to_cpu_capacities: &FnvHashMap<String, CpuCapacity>,
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
) -> PhysicalIndexingPlan {
    // Convert our problem to a scheduling problem.
    let mut id_to_ord_map = IdToOrdMap::default();

    // We use a Vec as a `IndexOrd` -> Max load map.
    let mut indexer_cpu_capacities: Vec<CpuCapacity> =
        Vec::with_capacity(indexer_id_to_cpu_capacities.len());
    for (indexer_id, &cpu_capacity) in indexer_id_to_cpu_capacities {
        let indexer_ord = id_to_ord_map.add_indexer_id(indexer_id.clone());
        assert_eq!(indexer_ord, indexer_cpu_capacities.len() as IndexerOrd);
        indexer_cpu_capacities.push(cpu_capacity);
    }

    let mut problem = SchedulingProblem::with_indexer_cpu_capacities(indexer_cpu_capacities);
    for source in sources {
        if let Some(source_ord) = populate_problem(source, &mut problem) {
            let registered_source_ord = id_to_ord_map.add_source_uid(source.source_uid.clone());
            assert_eq!(source_ord, registered_source_ord);
        }
    }

    // Populate the previous solution.
    let mut previous_solution = problem.new_solution();
    if let Some(previous_plan) = previous_plan_opt {
        convert_physical_plan_to_solution(previous_plan, &id_to_ord_map, &mut previous_solution);
    }

    // Compute the new scheduling solution
    let (new_solution, unassigned_shards) = scheduling_logic::solve(&problem, previous_solution);

    if !unassigned_shards.is_empty() {
        // TODO this is probably a bad idea to just not overschedule, as having a single index trail
        // behind will prevent the log GC.
        // A better strategy would probably be to close shard, and start prevent ingestion.
        error!("unable to assign all sources in the cluster");
    }

    // Convert the new scheduling solution back to a physical plan.
    convert_scheduling_solution_to_physical_plan(
        new_solution,
        &id_to_ord_map,
        sources,
        previous_plan_opt,
    )
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use fnv::FnvHashMap;
    use quickwit_proto::indexing::{mcpu, CpuCapacity, IndexingTask};
    use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};

    use super::{build_physical_indexing_plan, SourceToSchedule, SourceToScheduleType};
    use crate::indexing_plan::PhysicalIndexingPlan;

    fn source_id() -> SourceUid {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let index = IndexUid::from_parts("test_index", "0000");
        let source_id = COUNTER.fetch_add(1, Ordering::SeqCst);
        SourceUid {
            index_uid: index,
            source_id: format!("source_{source_id}"),
        }
    }

    #[test]
    fn test_build_physical_plan() {
        let indexer1 = "indexer1".to_string();
        let indexer2 = "indexer2".to_string();
        let source_uid0 = source_id();
        let source_uid1 = source_id();
        let source_uid2 = source_id();
        let source_0 = SourceToSchedule {
            source_uid: source_uid0.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![0, 1, 2, 3, 4, 5, 6, 7],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
        };
        let source_1 = SourceToSchedule {
            source_uid: source_uid1.clone(),
            source_type: SourceToScheduleType::NonSharded {
                num_pipelines: 2,
                load_per_pipeline: NonZeroU32::new(3_200).unwrap(),
            },
        };
        let source_2 = SourceToSchedule {
            source_uid: source_uid2.clone(),
            source_type: SourceToScheduleType::IngestV1,
        };
        let mut indexer_id_to_cpu_capacities = FnvHashMap::default();
        indexer_id_to_cpu_capacities.insert(indexer1.clone(), mcpu(16_000));
        indexer_id_to_cpu_capacities.insert(indexer2.clone(), mcpu(16_000));
        let indexing_plan = build_physical_indexing_plan(
            &[source_0, source_1, source_2],
            &indexer_id_to_cpu_capacities,
            None,
        );
        assert_eq!(indexing_plan.indexing_tasks_per_indexer().len(), 2);

        let node1_plan = indexing_plan.indexer(&indexer1).unwrap();
        let node2_plan = indexing_plan.indexer(&indexer2).unwrap();

        // both non-sharded pipelines get scheduled on the same node.
        assert_eq!(node1_plan.len(), 3);
        assert_eq!(&node1_plan[0].source_id, &source_uid1.source_id);
        assert!(&node1_plan[0].shard_ids.is_empty());
        assert_eq!(&node1_plan[1].source_id, &source_uid1.source_id);
        assert!(&node1_plan[1].shard_ids.is_empty());
        assert_eq!(&node1_plan[2].source_id, &source_uid2.source_id);
        assert!(&node1_plan[2].shard_ids.is_empty());

        assert_eq!(node2_plan.len(), 4);
        assert_eq!(&node2_plan[0].source_id, &source_uid0.source_id);
        assert_eq!(&node2_plan[0].shard_ids, &[0, 1, 2]);
        assert_eq!(&node2_plan[1].source_id, &source_uid0.source_id);
        assert_eq!(&node2_plan[1].shard_ids, &[3, 4, 5]);
        assert_eq!(&node2_plan[2].source_id, &source_uid0.source_id);
        assert_eq!(&node2_plan[2].shard_ids, &[6, 7]);
        assert_eq!(&node2_plan[3].source_id, &source_uid2.source_id);
    }

    #[tokio::test]
    async fn test_build_physical_indexing_plan_with_not_enough_indexers() {
        let source_uid1 = source_id();
        let source_1 = SourceToSchedule {
            source_uid: source_uid1.clone(),
            source_type: SourceToScheduleType::NonSharded {
                num_pipelines: 2,
                load_per_pipeline: NonZeroU32::new(1000).unwrap(),
            },
        };
        let sources = vec![source_1];

        let indexer1 = "indexer1".to_string();
        let mut indexer_max_loads = FnvHashMap::default();
        {
            indexer_max_loads.insert(indexer1.clone(), mcpu(1_999));
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None);
            assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 1);
            let expected_tasks = physical_plan.indexer(&indexer1).unwrap();
            assert_eq!(expected_tasks.len(), 1);
            assert_eq!(&expected_tasks[0].source_id, &source_uid1.source_id);
        }
        {
            indexer_max_loads.insert(indexer1.clone(), mcpu(2_000));
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None);
            assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 1);
            let expected_tasks = physical_plan.indexer(&indexer1).unwrap();
            assert_eq!(expected_tasks.len(), 2);
            assert_eq!(&expected_tasks[0].source_id, &source_uid1.source_id);
            assert!(expected_tasks[0].shard_ids.is_empty());
            assert_eq!(&expected_tasks[1].source_id, &source_uid1.source_id);
            assert!(expected_tasks[1].shard_ids.is_empty());
        }
    }

    fn make_indexing_tasks(
        source_uid: &SourceUid,
        shards: &[(PipelineUid, &[ShardId])],
    ) -> Vec<IndexingTask> {
        let mut plan = Vec::new();
        for (pipeline_uid, shard_ids) in shards {
            plan.push(IndexingTask {
                index_uid: source_uid.index_uid.to_string(),
                source_id: source_uid.source_id.clone(),
                pipeline_uid: Some(*pipeline_uid),
                shard_ids: shard_ids.to_vec(),
            });
        }
        plan
    }

    #[test]
    fn test_group_shards_into_pipeline_simple() {
        let source_uid = source_id();
        let indexing_tasks = make_indexing_tasks(
            &source_uid,
            &[
                (PipelineUid::from_u128(1u128), &[1, 2]),
                (PipelineUid::from_u128(2u128), &[3, 4, 5]),
            ],
        );
        let sources = vec![SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![0, 1, 3, 4, 5],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
        }];
        let mut indexer_id_to_cpu_capacities = FnvHashMap::default();
        indexer_id_to_cpu_capacities.insert("node1".to_string(), mcpu(10_000));
        let mut indexing_plan = PhysicalIndexingPlan::with_indexer_ids(&["node1".to_string()]);
        for indexing_task in indexing_tasks {
            indexing_plan.add_indexing_task("node1", indexing_task);
        }
        let new_plan = build_physical_indexing_plan(
            &sources,
            &indexer_id_to_cpu_capacities,
            Some(&indexing_plan),
        );
        let indexing_tasks = new_plan.indexer("node1").unwrap();
        assert_eq!(indexing_tasks.len(), 2);
        assert_eq!(&indexing_tasks[0].shard_ids, &[0, 1]);
        assert_eq!(&indexing_tasks[1].shard_ids, &[3, 4, 5]);
    }

    fn group_shards_into_pipelines_aux(
        source_uid: &SourceUid,
        shard_ids: &[ShardId],
        previous_pipeline_shards: &[(PipelineUid, &[ShardId])],
        load_per_shard: CpuCapacity,
    ) -> Vec<IndexingTask> {
        let indexing_tasks = make_indexing_tasks(source_uid, previous_pipeline_shards);
        let sources = vec![SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: shard_ids.to_vec(),
                load_per_shard: NonZeroU32::new(load_per_shard.cpu_millis()).unwrap(),
            },
        }];
        const NODE: &str = "node1";
        let mut indexer_id_to_cpu_capacities = FnvHashMap::default();
        indexer_id_to_cpu_capacities.insert(NODE.to_string(), mcpu(10_000));
        let mut indexing_plan = PhysicalIndexingPlan::with_indexer_ids(&["node1".to_string()]);
        for indexing_task in indexing_tasks {
            indexing_plan.add_indexing_task(NODE, indexing_task);
        }
        let new_plan = build_physical_indexing_plan(
            &sources,
            &indexer_id_to_cpu_capacities,
            Some(&indexing_plan),
        );
        let mut indexing_tasks = new_plan.indexer(NODE).unwrap().to_vec();
        // We sort indexing tasks for normalization purpose
        indexing_tasks.sort_by_key(|task| task.shard_ids[0]);
        indexing_tasks
    }

    #[test]
    fn test_group_shards_load_per_shard_too_high() {
        let source_uid = source_id();
        let indexing_tasks =
            group_shards_into_pipelines_aux(&source_uid, &[1, 2], &[], mcpu(4_000));
        assert_eq!(indexing_tasks.len(), 2);
    }

    #[test]
    fn test_group_shards_into_pipeline_hysteresis() {
        let source_uid = source_id();
        let indexing_tasks_1 = group_shards_into_pipelines_aux(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &[],
            mcpu(400),
        );
        assert_eq!(indexing_tasks_1.len(), 2);
        assert_eq!(&indexing_tasks_1[0].shard_ids, &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(&indexing_tasks_1[1].shard_ids, &[8, 9, 10]);

        let pipeline_tasks1: Vec<(PipelineUid, &[ShardId])> = indexing_tasks_1
            .iter()
            .map(|task| (task.pipeline_uid(), &task.shard_ids[..]))
            .collect();

        // With the same set of shards, an increase of load triggers the creation of a new task.
        let indexing_tasks_2 = group_shards_into_pipelines_aux(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &pipeline_tasks1[..],
            mcpu(600),
        );
        assert_eq!(indexing_tasks_2.len(), 3);
        assert_eq!(&indexing_tasks_2[0].shard_ids, &[0, 1, 2, 3, 4]);
        assert_eq!(&indexing_tasks_2[1].shard_ids, &[5, 6, 8, 9, 10]);
        assert_eq!(&indexing_tasks_2[2].shard_ids, &[7]);

        // Now the load comes back to normal
        // The hysteresis takes effect. We do not switch back to 2 pipelines.
        let pipeline_tasks_2: Vec<(PipelineUid, &[ShardId])> = indexing_tasks_2
            .iter()
            .map(|task| (task.pipeline_uid(), &task.shard_ids[..]))
            .collect();
        assert_eq!(indexing_tasks_2.len(), 3);
        let indexing_tasks_3 = group_shards_into_pipelines_aux(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &pipeline_tasks_2,
            mcpu(400),
        );
        assert_eq!(&indexing_tasks_3, &indexing_tasks_2);

        let pipeline_tasks3: Vec<(PipelineUid, &[ShardId])> = indexing_tasks_3
            .iter()
            .map(|task| (task.pipeline_uid(), &task.shard_ids[..]))
            .collect();
        // Now a further lower load.
        let indexing_tasks_4 = group_shards_into_pipelines_aux(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &pipeline_tasks3,
            mcpu(200),
        );
        assert_eq!(indexing_tasks_4.len(), 2);
        assert_eq!(&indexing_tasks_4[0].shard_ids, &[0, 1, 2, 3, 4, 7]);
        assert_eq!(&indexing_tasks_4[1].shard_ids, &[5, 6, 8, 9, 10]);
    }

    /// We want to make sure for small pipelines, we still reschedule them with the same
    /// pipeline uid.
    #[test]
    fn test_group_shards_into_pipeline_single_small_pipeline() {
        let source_uid = source_id();
        let pipeline_uid = PipelineUid::from_u128(1u128);
        let indexing_tasks = group_shards_into_pipelines_aux(
            &source_uid,
            &[12],
            &[(pipeline_uid, &[12])],
            mcpu(100),
        );
        assert_eq!(indexing_tasks.len(), 1);
        let indexing_task = &indexing_tasks[0];
        assert_eq!(&indexing_task.shard_ids, &[12]);
        assert_eq!(indexing_task.pipeline_uid.unwrap(), pipeline_uid);
    }

    #[test]
    fn test_pick_indexer_for_shard() {
        let indexer_capacity = vec![
            ("node1".to_string(), 1),
            ("node2".to_string(), 0),
            ("node3".to_string(), 2),
            ("node4".to_string(), 2),
            ("node5".to_string(), 0),
            ("node6".to_string(), 0),
        ];
        let indexers: Vec<&str> = super::pick_indexer(&indexer_capacity).collect();
        assert_eq!(indexers, &["node1", "node3", "node3", "node4", "node4"]);
    }
}
