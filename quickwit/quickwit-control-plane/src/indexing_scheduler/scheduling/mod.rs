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

use std::collections::hash_map::Entry;
use std::num::NonZeroU32;

use fnv::FnvHashMap;
use quickwit_proto::indexing::IndexingTask;
use quickwit_proto::types::{IndexUid, ShardId};
pub use scheduling_logic_model::Load;
use scheduling_logic_model::{IndexerOrd, SourceOrd};
use tracing::error;
use tracing::log::warn;

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling::scheduling_logic_model::{
    SchedulingProblem, SchedulingSolution,
};
use crate::indexing_scheduler::PIPELINE_FULL_LOAD;
use crate::SourceUid;

/// If we have several pipelines below this threshold we
/// reduce the number of pipelines.
///
/// Note that even for 2 pipelines, this creates an hysteris effet.
///
/// Starting from a single pipeline.
/// An overall load above 80% is enough to trigger the creation of a
/// second pipeline.
///
/// Coming back to a single pipeline requires having a load per pipeline
/// of 30%. Which translates into an overall load of 60%.
const LOAD_PER_PIPELINE_LOW_THRESHOLD: u32 = PIPELINE_FULL_LOAD * 3 / 10;

/// That's 80% of a period
const MAX_LOAD_PER_PIPELINE: u32 = PIPELINE_FULL_LOAD * 8 / 10;

fn indexing_task(source_uid: SourceUid, shard_ids: Vec<ShardId>) -> IndexingTask {
    IndexingTask {
        index_uid: source_uid.index_uid.to_string(),
        source_id: source_uid.source_id,
        shard_ids,
    }
}
fn create_shard_to_indexer_map(
    physical_plan: &PhysicalIndexingPlan,
    id_to_ord_map: &IdToOrdMap,
) -> FnvHashMap<SourceOrd, FnvHashMap<ShardId, IndexerOrd>> {
    let mut source_to_shard_to_indexer: FnvHashMap<SourceOrd, FnvHashMap<ShardId, IndexerOrd>> =
        Default::default();
    for (indexer_id, indexing_tasks) in physical_plan.indexing_tasks_per_indexer().iter() {
        for indexing_task in indexing_tasks {
            let index_uid = IndexUid::from(indexing_task.index_uid.clone());
            let Some(indexer_ord) = id_to_ord_map.indexer_ord(indexer_id) else {
                continue;
            };
            let source_uid = SourceUid {
                index_uid,
                source_id: indexing_task.source_id.clone(),
            };
            let Some(source_ord) = id_to_ord_map.source_ord(&source_uid) else {
                continue;
            };
            for &shard_id in &indexing_task.shard_ids {
                source_to_shard_to_indexer
                    .entry(source_ord)
                    .or_default()
                    .insert(shard_id, indexer_ord);
            }
        }
    }
    source_to_shard_to_indexer
}

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
            shards,
            load_per_shard,
        } => {
            let num_shards = shards.len() as u32;
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
    fn indexer_id(&self, indexer_ord: IndexerOrd) -> &String {
        &self.indexer_ids[indexer_ord]
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

/// Spreads the list of shard_ids optimally amongst the different indexers.
/// This function also receives a `previous_shard_to_indexer_ord` map, informing
/// use of the previous configuration.
///
/// Whenever possible this function tries to keep shards on the same indexer.
///
/// Contract:
/// The sum of the number of shards (values of `indexer_num_shards`) should match
/// the length of shard_ids.
/// Note that all shards are not necessarily in previous_shard_to_indexer_ord.
fn spread_shards_optimally(
    shard_ids: &[ShardId],
    mut indexer_num_shards: FnvHashMap<IndexerOrd, NonZeroU32>,
    previous_shard_to_indexer_ord: FnvHashMap<ShardId, IndexerOrd>,
) -> FnvHashMap<IndexerOrd, Vec<ShardId>> {
    assert_eq!(
        shard_ids.len(),
        indexer_num_shards
            .values()
            .map(|num_shards| num_shards.get() as usize)
            .sum::<usize>(),
    );
    let mut shard_ids_per_indexer: FnvHashMap<IndexerOrd, Vec<ShardId>> = Default::default();
    let mut unassigned_shard_ids: Vec<ShardId> = Vec::new();
    for &shard_id in shard_ids {
        if let Some(previous_indexer_ord) = previous_shard_to_indexer_ord.get(&shard_id).cloned() {
            if let Entry::Occupied(mut num_shards_entry) =
                indexer_num_shards.entry(previous_indexer_ord)
            {
                if let Some(new_num_shards) = NonZeroU32::new(num_shards_entry.get().get() - 1u32) {
                    *num_shards_entry.get_mut() = new_num_shards;
                } else {
                    num_shards_entry.remove();
                }
                // We keep the shard on the indexer it used to be.
                shard_ids_per_indexer
                    .entry(previous_indexer_ord)
                    .or_default()
                    .push(shard_id);
                continue;
            }
        }
        unassigned_shard_ids.push(shard_id);
    }

    // Finally, we need to add the missing shards.
    for (indexer_ord, num_shards) in indexer_num_shards {
        assert!(unassigned_shard_ids.len() >= num_shards.get() as usize);
        shard_ids_per_indexer
            .entry(indexer_ord)
            .or_default()
            .extend(unassigned_shard_ids.drain(..num_shards.get() as usize));
    }

    // At this point, we should have applied all of the missing shards.
    assert!(unassigned_shard_ids.is_empty());

    shard_ids_per_indexer
}

#[derive(Debug)]
pub struct SourceToSchedule {
    pub source_uid: SourceUid,
    pub source_type: SourceToScheduleType,
}

#[derive(Debug)]
pub enum SourceToScheduleType {
    Sharded {
        shards: Vec<ShardId>,
        load_per_shard: NonZeroU32,
    },
    NonSharded {
        num_pipelines: u32,
        load_per_pipeline: NonZeroU32,
    },
    // deprecated
    IngestV1,
}

fn group_shards_into_pipelines(
    source_uid: &SourceUid,
    shard_ids: &[ShardId],
    previous_indexing_tasks: &[IndexingTask],
    load_per_shard: Load,
) -> Vec<IndexingTask> {
    let num_shards = shard_ids.len() as u32;
    if num_shards == 0 {
        return Vec::new();
    }
    let max_num_shards_per_pipeline: NonZeroU32 =
        NonZeroU32::new(MAX_LOAD_PER_PIPELINE / load_per_shard).unwrap_or_else(|| {
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
            NonZeroU32::new(1).unwrap()
        });

    // We compute the number of pipelines we will create, cooking in some hysteresis effect here.
    // We have two different threshold to increase and to decrease the number of pipelines.
    let min_num_pipelines: u32 =
        (num_shards + max_num_shards_per_pipeline.get() - 1) / max_num_shards_per_pipeline;
    assert!(min_num_pipelines > 0);
    let max_num_pipelines: u32 =
        min_num_pipelines.max(num_shards * load_per_shard / LOAD_PER_PIPELINE_LOW_THRESHOLD);
    let previous_num_pipelines = previous_indexing_tasks.len() as u32;
    let num_pipelines: u32 = if previous_num_pipelines > min_num_pipelines {
        previous_num_pipelines.min(max_num_pipelines)
    } else {
        min_num_pipelines
    };

    let mut pipelines: Vec<Vec<ShardId>> = std::iter::repeat_with(Vec::new)
        .take((previous_num_pipelines as usize).max(num_pipelines as usize))
        .collect();

    let mut unassigned_shard_ids: Vec<ShardId> = Vec::new();
    let previous_pipeline_map: FnvHashMap<ShardId, usize> = previous_indexing_tasks
        .iter()
        .enumerate()
        .flat_map(|(pipeline_ord, indexing_task)| {
            indexing_task
                .shard_ids
                .iter()
                .map(move |shard_id| (*shard_id, pipeline_ord))
        })
        .collect();

    for &shard in shard_ids {
        if let Some(pipeline_ord) = previous_pipeline_map.get(&shard).copied() {
            // Whenever possible we allocate to the previous pipeline.
            let best_pipeline_for_shard = &mut pipelines[pipeline_ord];
            if best_pipeline_for_shard.len() < max_num_shards_per_pipeline.get() as usize {
                best_pipeline_for_shard.push(shard);
            } else {
                unassigned_shard_ids.push(shard);
            }
        } else {
            unassigned_shard_ids.push(shard);
        }
    }

    // If needed, let's remove some pipelines. We just remove the pipelines that have
    // the least number of shards.
    pipelines.sort_by_key(|shards| std::cmp::Reverse(shards.len()));
    for removed_pipeline_shards in pipelines.drain(num_pipelines as usize..) {
        unassigned_shard_ids.extend(removed_pipeline_shards);
    }

    // Now we need to allocate the unallocated shards.
    // We just allocate them to the current pipeline that has the lowest load.
    for shard in unassigned_shard_ids {
        let best_pipeline_for_shard: &mut Vec<ShardId> = pipelines
            .iter_mut()
            .min_by_key(|shards| shards.len())
            .unwrap();
        best_pipeline_for_shard.push(shard);
    }

    let mut indexing_tasks: Vec<IndexingTask> = pipelines
        .into_iter()
        .map(|mut shard_ids| {
            shard_ids.sort();
            IndexingTask {
                index_uid: source_uid.index_uid.to_string(),
                source_id: source_uid.source_id.clone(),
                shard_ids,
            }
        })
        .collect();

    indexing_tasks.sort_by_key(|indexing_task| indexing_task.shard_ids[0]);

    indexing_tasks
}

/// This function takes a scheduling solution (which abstracts the notion of pipelines,
/// and shard ids) and builds a physical plan.
fn convert_scheduling_solution_to_physical_plan(
    solution: &SchedulingSolution,
    problem: &SchedulingProblem,
    id_to_ord_map: &IdToOrdMap,
    sources: &[SourceToSchedule],
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
) -> PhysicalIndexingPlan {
    let mut previous_shard_to_indexer_map: FnvHashMap<SourceOrd, FnvHashMap<ShardId, IndexerOrd>> =
        previous_plan_opt
            .map(|previous_plan| create_shard_to_indexer_map(previous_plan, id_to_ord_map))
            .unwrap_or_default();

    let mut physical_indexing_plan =
        PhysicalIndexingPlan::with_indexer_ids(&id_to_ord_map.indexer_ids);

    for source in sources {
        match &source.source_type {
            SourceToScheduleType::Sharded {
                shards,
                load_per_shard: _load_per_shard,
            } => {
                // That's ingest v2.
                // The logic is complicated here. At this point we know the number of shards to
                // be assign to each indexer, but we want to convert that number into a list of
                // shard ids, without moving a shard from a indexer to another
                // whenever possible.
                let source_ord = id_to_ord_map.source_ord(&source.source_uid).unwrap();
                let indexer_num_shards: FnvHashMap<IndexerOrd, NonZeroU32> =
                    solution.indexer_shards(source_ord).collect();

                let shard_to_indexer_ord = previous_shard_to_indexer_map
                    .remove(&source_ord)
                    .unwrap_or_default();

                let load_per_shard = problem.source_load_per_shard(source_ord);
                let shard_ids_per_node: FnvHashMap<IndexerOrd, Vec<ShardId>> =
                    spread_shards_optimally(shards, indexer_num_shards, shard_to_indexer_ord);

                for (node_ord, shard_ids_for_node) in shard_ids_per_node {
                    let node_id = id_to_ord_map.indexer_id(node_ord);
                    let indexing_tasks: &[IndexingTask] = previous_plan_opt
                        .and_then(|previous_plan| previous_plan.indexer(node_id))
                        .unwrap_or(&[]);
                    let indexing_tasks = group_shards_into_pipelines(
                        &source.source_uid,
                        &shard_ids_for_node,
                        indexing_tasks,
                        load_per_shard.get(),
                    );
                    for indexing_task in indexing_tasks {
                        physical_indexing_plan.add_indexing_task(node_id, indexing_task);
                    }
                }
            }
            SourceToScheduleType::NonSharded { .. } => {
                // These are the sources that are not sharded (Kafka-like).
                //
                // Here one shard is one pipeline.
                let source_ord = id_to_ord_map.source_ord(&source.source_uid).unwrap();

                let indexer_num_shards: FnvHashMap<IndexerOrd, NonZeroU32> =
                    solution.indexer_shards(source_ord).collect();

                for (indexer_ord, num_shards) in indexer_num_shards {
                    let indexer_id = id_to_ord_map.indexer_id(indexer_ord);
                    for _ in 0..num_shards.get() {
                        let indexing_task = indexing_task(source.source_uid.clone(), Vec::new());
                        physical_indexing_plan.add_indexing_task(indexer_id, indexing_task);
                    }
                }
                continue;
            }
            SourceToScheduleType::IngestV1 => {
                // Ingest V1 requires to start one pipeline on each indexer.
                // This pipeline is off-the-grid: it is not taken in account in the
                // indexer capacity. We start it to ensure backward compatibility
                // a little, but we want to remove it rapidly.
                for indexer_id in &id_to_ord_map.indexer_ids {
                    let indexing_task = indexing_task(source.source_uid.clone(), Vec::new());
                    physical_indexing_plan.add_indexing_task(indexer_id, indexing_task);
                }
            }
        }
    }

    // We sort the tasks by `source_uid`.
    physical_indexing_plan.normalize();
    physical_indexing_plan
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
    indexer_id_to_max_load: &FnvHashMap<String, Load>,
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
) -> PhysicalIndexingPlan {
    // TODO make the load per node something that can be configured on each node.

    // Convert our problem to a scheduling problem.
    let mut id_to_ord_map = IdToOrdMap::default();

    // We use a Vec as a `IndexOrd` -> Max load map.
    let mut indexer_max_loads: Vec<Load> = Vec::with_capacity(indexer_id_to_max_load.len());
    for (indexer_id, &max_load) in indexer_id_to_max_load {
        let indexer_ord = id_to_ord_map.add_indexer_id(indexer_id.clone());
        assert_eq!(indexer_ord, indexer_max_loads.len() as IndexerOrd);
        indexer_max_loads.push(max_load);
    }

    let mut problem = SchedulingProblem::with_indexer_maximum_load(indexer_max_loads);
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
        error!("unable to assign all sources in the cluster.");
    }

    // Convert the new scheduling solution back to a physical plan.
    convert_scheduling_solution_to_physical_plan(
        &new_solution,
        &problem,
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
    use quickwit_proto::indexing::IndexingTask;
    use quickwit_proto::types::{IndexUid, ShardId};

    use super::{
        build_physical_indexing_plan, group_shards_into_pipelines, indexing_task,
        spread_shards_optimally, SourceToSchedule, SourceToScheduleType,
    };
    use crate::SourceUid;

    #[test]
    fn test_spread_shard_optimally() {
        let mut indexer_num_shards = FnvHashMap::default();
        indexer_num_shards.insert(0, NonZeroU32::new(2).unwrap());
        indexer_num_shards.insert(1, NonZeroU32::new(3).unwrap());
        let mut shard_to_indexer_ord = FnvHashMap::default();
        shard_to_indexer_ord.insert(0, 0);
        shard_to_indexer_ord.insert(1, 2);
        shard_to_indexer_ord.insert(3, 0);
        let indexer_to_shards =
            spread_shards_optimally(&[0, 1, 2, 3, 4], indexer_num_shards, shard_to_indexer_ord);
        assert_eq!(indexer_to_shards.get(&0), Some(&vec![0, 3]));
        assert_eq!(indexer_to_shards.get(&1), Some(&vec![1, 2, 4]));
    }

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
                shards: vec![0, 1, 2, 3, 4, 5, 6, 7],
                load_per_shard: NonZeroU32::new(250).unwrap(),
            },
        };
        let source_1 = SourceToSchedule {
            source_uid: source_uid1.clone(),
            source_type: SourceToScheduleType::NonSharded {
                num_pipelines: 2,
                load_per_pipeline: NonZeroU32::new(800).unwrap(),
            },
        };
        let source_2 = SourceToSchedule {
            source_uid: source_uid2.clone(),
            source_type: SourceToScheduleType::IngestV1,
        };
        let mut indexer_max_load = FnvHashMap::default();
        indexer_max_load.insert(indexer1.clone(), 4_000);
        indexer_max_load.insert(indexer2.clone(), 4_000);
        let indexing_plan =
            build_physical_indexing_plan(&[source_0, source_1, source_2], &indexer_max_load, None);
        assert_eq!(indexing_plan.indexing_tasks_per_indexer().len(), 2);

        let node1_plan = indexing_plan.indexer(&indexer1).unwrap();

        // both non-sharded pipelines get scheduled on the same node.
        assert_eq!(
            &node1_plan,
            &[
                indexing_task(source_uid1.clone(), vec![]),
                indexing_task(source_uid1.clone(), vec![]),
                indexing_task(source_uid2.clone(), vec![]),
            ]
        );

        let node2_plan = indexing_plan.indexer(&indexer2).unwrap();
        assert_eq!(
            &node2_plan,
            &[
                indexing_task(source_uid0.clone(), vec![0, 3, 6]),
                indexing_task(source_uid0.clone(), vec![1, 4, 7]),
                indexing_task(source_uid0.clone(), vec![2, 5]),
                indexing_task(source_uid2.clone(), vec![]),
            ]
        );
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
            indexer_max_loads.insert(indexer1.clone(), 1_999);
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None);
            assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 1);
            let expected_tasks = physical_plan.indexer(&indexer1).unwrap();
            assert_eq!(
                expected_tasks,
                &[indexing_task(source_uid1.clone(), Vec::new())]
            );
        }
        {
            indexer_max_loads.insert(indexer1.clone(), 2_000);
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None);
            assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 1);
            let expected_tasks = physical_plan.indexer(&indexer1).unwrap();
            assert_eq!(
                expected_tasks,
                &[
                    indexing_task(source_uid1.clone(), Vec::new()),
                    indexing_task(source_uid1.clone(), Vec::new()),
                ]
            )
        }
    }

    #[test]
    fn test_group_shards_empty() {
        let source_uid = source_id();
        let indexing_tasks = group_shards_into_pipelines(&source_uid, &[], &[], 250);
        assert!(indexing_tasks.is_empty());
    }

    fn make_indexing_tasks(
        source_uid: &SourceUid,
        shard_ids_grp: &[&[ShardId]],
    ) -> Vec<IndexingTask> {
        shard_ids_grp
            .iter()
            .copied()
            .map(|shard_ids| IndexingTask {
                index_uid: source_uid.index_uid.to_string(),
                source_id: source_uid.source_id.clone(),
                shard_ids: shard_ids.to_vec(),
            })
            .collect::<Vec<IndexingTask>>()
    }

    #[test]
    fn test_group_shards_into_pipeline_simple() {
        let source_uid = source_id();
        let previous_indexing_tasks: Vec<IndexingTask> =
            make_indexing_tasks(&source_uid, &[&[1, 2], &[3, 4, 5]]);
        let indexing_tasks = group_shards_into_pipelines(
            &source_uid,
            &[0, 1, 3, 4, 5],
            &previous_indexing_tasks,
            250,
        );
        assert_eq!(indexing_tasks.len(), 2);
        assert_eq!(&indexing_tasks[0].shard_ids, &[0, 1]);
        assert_eq!(&indexing_tasks[1].shard_ids, &[3, 4, 5]);
    }

    #[test]
    fn test_group_shards_load_per_shard_too_high() {
        let source_uid = source_id();
        let indexing_tasks = group_shards_into_pipelines(&source_uid, &[1, 2], &[], 1_000);
        assert_eq!(indexing_tasks.len(), 2);
    }

    #[test]
    fn test_group_shards_into_pipeline_hysteresis() {
        let source_uid = source_id();
        let previous_indexing_tasks: Vec<IndexingTask> = make_indexing_tasks(&source_uid, &[]);
        let indexing_tasks_1 = group_shards_into_pipelines(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &previous_indexing_tasks,
            100,
        );
        assert_eq!(indexing_tasks_1.len(), 2);
        assert_eq!(&indexing_tasks_1[0].shard_ids, &[0, 2, 4, 6, 8, 10]);
        assert_eq!(&indexing_tasks_1[1].shard_ids, &[1, 3, 5, 7, 9]);
        // With the same set of shards, an increase of load triggers the creation of a new task.
        let indexing_tasks_2 = group_shards_into_pipelines(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &indexing_tasks_1,
            150,
        );
        assert_eq!(indexing_tasks_2.len(), 3);
        assert_eq!(&indexing_tasks_2[0].shard_ids, &[0, 2, 4, 6, 8]);
        assert_eq!(&indexing_tasks_2[1].shard_ids, &[1, 3, 5, 7, 9]);
        assert_eq!(&indexing_tasks_2[2].shard_ids, &[10]);
        // Now the load comes back to normal
        // The hysteresis takes effect. We do not switch back to 2 pipelines.
        let indexing_tasks_3 = group_shards_into_pipelines(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &indexing_tasks_2,
            100,
        );
        assert_eq!(indexing_tasks_3.len(), 3);
        assert_eq!(&indexing_tasks_3[0].shard_ids, &[0, 2, 4, 6, 8]);
        assert_eq!(&indexing_tasks_3[1].shard_ids, &[1, 3, 5, 7, 9]);
        assert_eq!(&indexing_tasks_3[2].shard_ids, &[10]);
        // Now a further lower load..
        let indexing_tasks_4 = group_shards_into_pipelines(
            &source_uid,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &indexing_tasks_3,
            80,
        );
        assert_eq!(indexing_tasks_4.len(), 2);
        assert_eq!(&indexing_tasks_4[0].shard_ids, &[0, 2, 4, 6, 8, 10]);
        assert_eq!(&indexing_tasks_4[1].shard_ids, &[1, 3, 5, 7, 9]);
    }
}
