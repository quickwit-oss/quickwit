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

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling::scheduling_logic_model::{
    SchedulingProblem, SchedulingSolution,
};
use crate::SourceUid;

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

fn convert_scheduling_solution_to_physical_plan(
    solution: SchedulingSolution,
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
                let shard_ids_per_indexer =
                    spread_shards_optimally(shards, indexer_num_shards, shard_to_indexer_ord);

                for (indexer_ord, shard_ids_for_indexer) in shard_ids_per_indexer {
                    let indexer_id = id_to_ord_map.indexer_id(indexer_ord);
                    let indexing_task =
                        indexing_task(source.source_uid.clone(), shard_ids_for_indexer);
                    physical_indexing_plan.add_indexing_task(indexer_id, indexing_task);
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
    use quickwit_proto::types::IndexUid;

    use super::{
        build_physical_indexing_plan, indexing_task, spread_shards_optimally, SourceToSchedule,
        SourceToScheduleType,
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
                indexing_task(source_uid0.clone(), vec![0, 1, 2, 3, 4, 5, 6, 7]),
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
}
