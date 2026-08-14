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

#[cfg(test)]
mod churn_tests;
#[cfg(test)]
mod scale_tests;
pub mod scheduling_logic;
pub mod scheduling_logic_model;

use std::collections::HashMap;
use std::num::NonZeroU32;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_common::rate_limited_debug;
use quickwit_proto::indexing::{CpuCapacity, IndexingTask};
#[cfg(test)]
use quickwit_proto::types::NodeId;
use quickwit_proto::types::{PipelineUid, ShardId, SourceUid};
pub use scheduling_logic_model::Eligibility;
use scheduling_logic_model::{IndexerLocality, IndexerOrd, LocalityGroup, SourceOrd};
use tracing::{error, warn};

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling::scheduling_logic_model::{
    IndexerAssignment, SchedulingProblem, SchedulingSolution,
};
use crate::model::ShardLocations;

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
struct IdToOrdMap<'a> {
    indexer_ids: Vec<String>,
    sources: Vec<&'a SourceToSchedule>,
    indexer_id_to_indexer_ord: FnvHashMap<String, IndexerOrd>,
    source_uid_to_source_ord: FnvHashMap<SourceUid, SourceOrd>,
}

impl<'a> IdToOrdMap<'a> {
    // All source added are required to have a different source uid.
    fn add_source(&mut self, source: &'a SourceToSchedule) -> SourceOrd {
        let source_ord = self.source_uid_to_source_ord.len() as SourceOrd;
        let previous_item = self
            .source_uid_to_source_ord
            .insert(source.source_uid.clone(), source_ord);
        assert!(previous_item.is_none());
        self.sources.push(source);
        source_ord
    }

    fn source_ord(&self, source_uid: &SourceUid) -> Option<SourceOrd> {
        self.source_uid_to_source_ord.get(source_uid).copied()
    }

    fn source(&self, source_uid: &SourceUid) -> Option<(SourceOrd, &'a SourceToSchedule)> {
        let source_ord = self.source_uid_to_source_ord.get(source_uid).copied()?;
        Some((source_ord, self.sources[source_ord as usize]))
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
                    index_uid: indexing_task.index_uid().clone(),
                    source_id: indexing_task.source_id.clone(),
                };
                if let Some((source_ord, source)) = id_to_ord_map.source(&source_uid) {
                    match &source.source_type {
                        SourceToScheduleType::Sharded { .. } => {
                            indexer_assignment
                                .add_shards(source_ord, indexing_task.shard_ids.len() as u32);
                        }
                        SourceToScheduleType::NonSharded { .. } => {
                            // For non-sharded sources like Kafka, one pipeline = one shard in the
                            // solutions
                            indexer_assignment.add_shards(source_ord, 1);
                        }
                        SourceToScheduleType::IngestV1 => {
                            // Ingest V1 is not part of the logical placement algorithm.
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct IndexerInfo {
    pub cpu_capacity: CpuCapacity,
    pub availability_zone: Option<String>,
    pub eligibility: Eligibility,
}

#[cfg(test)]
impl IndexerInfo {
    pub(crate) fn for_test(cpu_capacity: CpuCapacity) -> IndexerInfo {
        IndexerInfo {
            cpu_capacity,
            availability_zone: None,
            eligibility: Eligibility::Any,
        }
    }
}

#[cfg(test)]
pub(crate) struct IndexerSpec {
    pub node_id: NodeId,
    pub cpu_capacity: CpuCapacity,
    pub availability_zone: Option<String>,
}

#[cfg(test)]
impl IndexerSpec {
    pub(crate) fn new(
        node_id: &str,
        cpu_capacity: CpuCapacity,
        availability_zone: Option<&str>,
    ) -> IndexerSpec {
        IndexerSpec {
            node_id: NodeId::from_str(node_id),
            cpu_capacity,
            availability_zone: availability_zone.map(|az| az.to_string()),
        }
    }

    pub(crate) fn to_indexer_info(&self, eligibility: Eligibility) -> IndexerInfo {
        IndexerInfo {
            cpu_capacity: self.cpu_capacity,
            availability_zone: self.availability_zone.clone(),
            eligibility,
        }
    }
}

#[cfg(test)]
pub(crate) fn shard_ids_of_source(source: &SourceToSchedule) -> &[ShardId] {
    let SourceToScheduleType::Sharded { shard_ids, .. } = &source.source_type else {
        return &[];
    };
    shard_ids
}

#[cfg(test)]
pub(crate) fn total_num_shards(sources: &[SourceToSchedule]) -> usize {
    sources
        .iter()
        .map(|source| shard_ids_of_source(source).len())
        .sum()
}

#[cfg(test)]
pub(crate) fn shard_ids_for_indexer(plan: &PhysicalIndexingPlan, indexer: &str) -> Vec<ShardId> {
    let mut shard_ids: Vec<ShardId> = plan
        .indexer(indexer)
        .unwrap()
        .iter()
        .flat_map(|task| task.shard_ids.iter().cloned())
        .collect();
    shard_ids.sort();
    shard_ids
}

#[derive(Debug)]
pub struct SourceToSchedule {
    pub source_uid: SourceUid,
    pub source_type: SourceToScheduleType,
    pub params_fingerprint: u64,
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

// This converts a scheduling solution for a given node and a given source.
// Major quirk however:
// For sharded function, this function only partially performs this conversion.
// In the resulting function some of the shards may not be allocated.
// The remaining shards will be added in postprocessing pass.
fn convert_scheduling_solution_to_physical_plan_single_node_single_source(
    mut remaining_num_shards_to_schedule_on_node: u32,
    // Specific to the source.
    previous_tasks: &[&IndexingTask],
    source: &SourceToSchedule,
) -> Vec<IndexingTask> {
    match &source.source_type {
        SourceToScheduleType::Sharded {
            shard_ids,
            load_per_shard,
        } => {
            if remaining_num_shards_to_schedule_on_node == 0 {
                return Vec::new();
            }
            // For the moment we do something voluntarily suboptimal.
            let max_num_pipelines = quickwit_common::div_ceil_u32(
                remaining_num_shards_to_schedule_on_node * load_per_shard.get(),
                CPU_PER_PIPELINE_LOAD_LOWER_THRESHOLD.cpu_millis(),
            );
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
                    .filter(|shard_id| shard_ids.contains(shard_id))
                    .take(max_shard_in_pipeline)
                    .cloned()
                    .collect();
                remaining_num_shards_to_schedule_on_node -= shard_ids.len() as u32;
                let pipeline_uid = if previous_task.params_fingerprint == source.params_fingerprint
                {
                    previous_task.pipeline_uid
                } else {
                    Some(PipelineUid::random())
                };
                let new_task = IndexingTask {
                    index_uid: previous_task.index_uid.clone(),
                    source_id: previous_task.source_id.clone(),
                    pipeline_uid,
                    shard_ids,
                    params_fingerprint: source.params_fingerprint,
                };
                new_tasks.push(new_task);
                if new_tasks.len() >= max_num_pipelines as usize {
                    break;
                }
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
            for indexing_task in &mut indexing_tasks {
                if indexing_task.params_fingerprint != source.params_fingerprint {
                    indexing_task.params_fingerprint = source.params_fingerprint;
                    indexing_task.pipeline_uid = Some(PipelineUid::random());
                }
            }
            indexing_tasks.resize_with(remaining_num_shards_to_schedule_on_node as usize, || {
                IndexingTask {
                    index_uid: Some(source.source_uid.index_uid.clone()),
                    source_id: source.source_uid.source_id.clone(),
                    pipeline_uid: Some(PipelineUid::random()),
                    shard_ids: Vec::new(),
                    params_fingerprint: source.params_fingerprint,
                }
            });
            indexing_tasks
        }
        SourceToScheduleType::IngestV1 => {
            // Ingest V1 is simple. One pipeline per indexer node.
            if let Some(indexing_task) = previous_tasks.first() {
                // The pipeline already exists, let's reuse it.
                let mut indexing_task = (*indexing_task).clone();
                if indexing_task.params_fingerprint != source.params_fingerprint {
                    indexing_task.params_fingerprint = source.params_fingerprint;
                    indexing_task.pipeline_uid = Some(PipelineUid::random());
                }
                vec![indexing_task]
            } else {
                // The source is new, we need to create a new task.
                vec![IndexingTask {
                    index_uid: Some(source.source_uid.index_uid.clone()),
                    source_id: source.source_uid.source_id.clone(),
                    pipeline_uid: Some(PipelineUid::random()),
                    shard_ids: Vec::new(),
                    params_fingerprint: source.params_fingerprint,
                }]
            }
        }
    }
}

fn convert_scheduling_solution_to_physical_plan_single_node(
    indexer_assignment: &IndexerAssignment,
    previous_tasks: &[IndexingTask],
    sources: &[SourceToSchedule],
    id_to_ord_map: &IdToOrdMap,
) -> Vec<IndexingTask> {
    let mut tasks = Vec::new();
    for source in sources {
        let source_num_shards =
            if let Some(source_ord) = id_to_ord_map.source_ord(&source.source_uid) {
                indexer_assignment.num_shards(source_ord)
            } else {
                // This can happen for IngestV1
                1u32
            };
        let source_pipelines: Vec<&IndexingTask> = previous_tasks
            .iter()
            .filter(|task| {
                task.index_uid() == &source.source_uid.index_uid
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

/// This function takes a scheduling solution (which abstracts the notion of pipelines,
/// and shard ids) and builds a physical plan, attempting to make as little change as possible
/// to the existing pipelines.
///
/// We do not support moving shard from one pipeline to another, so if required this function may
/// also return instruction about deleting / adding new shards.
fn convert_scheduling_solution_to_physical_plan(
    solution: &SchedulingSolution,
    id_to_ord_map: &IdToOrdMap,
    sources: &[SourceToSchedule],
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> PhysicalIndexingPlan {
    let mut indexer_assignments = solution.indexer_assignments.clone();
    let mut new_physical_plan = PhysicalIndexingPlan::with_indexer_ids(&id_to_ord_map.indexer_ids);
    for (indexer_id, indexer_assignment) in id_to_ord_map
        .indexer_ids
        .iter()
        .zip(&mut indexer_assignments)
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
        let mut remaining_num_shards_per_node: HashMap<String, NonZeroU32> =
            HashMap::with_capacity(new_physical_plan.num_indexers());
        for (indexer, indexing_tasks) in new_physical_plan.indexing_tasks_per_indexer_mut() {
            let indexer_ord = id_to_ord_map.indexer_ord(indexer).unwrap();
            let eligibility = indexer_infos[indexer].eligibility;
            let mut num_shards_for_indexer_source: u32 =
                indexer_assignments[indexer_ord].num_shards(source_ord);
            for indexing_task in indexing_tasks {
                if indexing_task.index_uid() == &source.source_uid.index_uid
                    && indexing_task.source_id == source.source_uid.source_id
                {
                    indexing_task.shard_ids.retain(|shard_id| {
                        if !may_keep_shard_in_previous_pipeline(
                            indexer,
                            eligibility,
                            shard_id,
                            shard_locations,
                            indexer_infos,
                        ) {
                            return false;
                        }
                        let shard_added = scheduled_shards.insert(shard_id.clone());
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
            if let Some(num_shards_for_indexer_source_non_zero) =
                NonZeroU32::new(num_shards_for_indexer_source)
            {
                remaining_num_shards_per_node
                    .insert(indexer.clone(), num_shards_for_indexer_source_non_zero);
            }
        }

        // Missing shards is an iterator over the shards that are not scheduled into a pipeline yet.
        let missing_shards: Vec<ShardId> = shard_ids
            .iter()
            .filter(|shard_id| !scheduled_shards.contains(shard_id))
            .cloned()
            .collect();

        // Let's assign the missing shards.
        let max_shard_per_pipeline = compute_max_num_shards_per_pipeline(&source.source_type);

        let shard_to_indexer: HashMap<ShardId, String> = assign_shards(
            missing_shards,
            remaining_num_shards_per_node,
            shard_locations,
            indexer_infos,
        );
        for (shard_id, indexer) in shard_to_indexer {
            add_shard_to_indexer(
                shard_id,
                indexer,
                &source.source_uid,
                max_shard_per_pipeline,
                &mut new_physical_plan,
                source.params_fingerprint,
            );
        }
    }

    new_physical_plan.normalize();

    new_physical_plan
}

fn is_shard_local(indexer: &str, shard_id: &ShardId, shard_locations: &ShardLocations) -> bool {
    shard_locations
        .get_shard_locations(shard_id)
        .iter()
        .any(|node_id| node_id.as_str() == indexer)
}

fn is_shard_hosted_on_draining_indexer(
    shard_id: &ShardId,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> bool {
    shard_locations
        .get_shard_locations(shard_id)
        .iter()
        .any(|node_id| {
            let Some(indexer_info) = indexer_infos.get(node_id.as_str()) else {
                return false;
            };
            indexer_info.eligibility == Eligibility::SelfHostedOnly
        })
}

fn may_keep_shard_in_previous_pipeline(
    indexer: &str,
    eligibility: Eligibility,
    shard_id: &ShardId,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> bool {
    if is_shard_local(indexer, shard_id, shard_locations) {
        return true;
    }
    if eligibility == Eligibility::SelfHostedOnly {
        return false;
    }
    !is_shard_hosted_on_draining_indexer(shard_id, shard_locations, indexer_infos)
}

fn indexer_availability_zone<'a>(
    node_id: &str,
    indexer_infos: &'a FnvHashMap<String, IndexerInfo>,
) -> Option<&'a str> {
    indexer_infos.get(node_id)?.availability_zone.as_deref()
}

fn shard_availability_zone<'a>(
    shard_id: &ShardId,
    shard_locations: &ShardLocations,
    indexer_infos: &'a FnvHashMap<String, IndexerInfo>,
) -> Option<&'a str> {
    let hosting_node_id = shard_locations.get_shard_locations(shard_id).first()?;
    indexer_availability_zone(hosting_node_id.as_str(), indexer_infos)
}

pub(crate) fn is_shard_nearby(
    indexer: &str,
    shard_id: &ShardId,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> bool {
    let availability_zone =
        shard_availability_zone(shard_id, shard_locations, indexer_infos);
    availability_zone.is_some()
        && availability_zone == indexer_availability_zone(indexer, indexer_infos)
}

fn find_nearby_indexer(
    shard_id: &ShardId,
    remaining_num_shards_per_node: &HashMap<String, NonZeroU32>,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> Option<String> {
    remaining_num_shards_per_node
        .iter()
        .filter(|(node_id, _)| is_shard_nearby(node_id, shard_id, shard_locations, indexer_infos))
        // Fill up the nearly-full indexers first. Ties break on node id, for determinism.
        .min_by_key(|(node_id, num_remaining_shards)| (**num_remaining_shards, *node_id))
        .map(|(node_id, _)| node_id.clone())
}

/// This function is meant to be called after we have solved the scheduling
/// problem, so we already know the number of shards to be assigned on each indexer node.
/// We now need to precisely where each shard should be assigned.
///
/// It assigns the missing shards for a given source to the indexers, given:
/// - the total number of shards that are to be scheduled on each nodes
/// - the shard locations
///
/// This function will assign shards on a node hosting them in priority.
///
/// The current implementation is a heuristic.
/// In the first pass, we attempt to assign as many shards as possible on the
/// node hosting them.
fn assign_shards(
    missing_shards: Vec<ShardId>,
    mut remaining_num_shards_per_node: HashMap<String, NonZeroU32>,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> HashMap<ShardId, String> {
    let mut shard_to_indexer: HashMap<ShardId, String> =
        HashMap::with_capacity(missing_shards.len());

    // In a first pass we first assign as many shards on their hosting nodes as possible.
    let mut remaining_missing_shards: Vec<ShardId> = Vec::new();
    for shard_id in missing_shards {
        // As a heuristic, we pick the first node hosting the shard that is available.
        let indexer_hosting_shard: Option<(NonZeroU32, &str)> = shard_locations
            .get_shard_locations(&shard_id)
            .iter()
            .map(|node_id| node_id.as_str())
            .flat_map(|node_id| {
                let num_shards = remaining_num_shards_per_node.get(node_id)?;
                Some((*num_shards, node_id))
            })
            .min_by_key(|(num_shards, _node_id)| *num_shards);
        if let Some((_num_shards, indexer)) = indexer_hosting_shard {
            decrement_num_shards(indexer, &mut remaining_num_shards_per_node);
            shard_to_indexer.insert(shard_id, indexer.to_string());
        } else {
            remaining_missing_shards.push(shard_id);
        }
    }

    for shard_id in remaining_missing_shards {
        let indexer = find_indexer_for_shard(
            &shard_id,
            &remaining_num_shards_per_node,
            shard_locations,
            indexer_infos,
        );
        decrement_num_shards(&indexer, &mut remaining_num_shards_per_node);
        shard_to_indexer.insert(shard_id, indexer);
    }
    assert!(remaining_num_shards_per_node.is_empty());

    shard_to_indexer
}

// Try to place the shard on an indexer in the same availability zone, if enabled; otherwise, pick
// the next available one.
fn find_indexer_for_shard(
    shard_id: &ShardId,
    remaining_num_shards_per_node: &HashMap<String, NonZeroU32>,
    shard_locations: &ShardLocations,
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
) -> String {
    if let Some(indexer) = find_nearby_indexer(
        shard_id,
        remaining_num_shards_per_node,
        shard_locations,
        indexer_infos,
    ) {
        indexer
    } else {
        remaining_num_shards_per_node
            .keys()
            .next()
            .expect("failed to assign all shards. please report")
            .to_string()
    }
}

fn decrement_num_shards(
    node_id: &str,
    remaining_num_shards_to_schedule_per_indexers: &mut HashMap<String, NonZeroU32>,
) {
    {
        let previous_num_shards = remaining_num_shards_to_schedule_per_indexers
            .get_mut(node_id)
            .unwrap();
        if let Some(new_num_shards) = NonZeroU32::new(previous_num_shards.get() - 1) {
            *previous_num_shards = new_num_shards;
            return;
        }
    }
    remaining_num_shards_to_schedule_per_indexers.remove(node_id);
}

// Checks that's the physical solution indeed matches the scheduling solution.
fn assert_post_condition_physical_plan_match_solution(
    physical_plan: &PhysicalIndexingPlan,
    solution: &SchedulingSolution,
    id_to_ord_map: &IdToOrdMap,
) {
    let num_indexers = physical_plan.indexing_tasks_per_indexer().len();
    assert_eq!(num_indexers, solution.indexer_assignments.len());
    assert_eq!(num_indexers, id_to_ord_map.indexer_ids.len());
    let mut reconstructed_solution = SchedulingSolution::with_num_indexers(num_indexers);
    convert_physical_plan_to_solution(physical_plan, id_to_ord_map, &mut reconstructed_solution);
    assert_eq!(
        solution.indexer_assignments,
        reconstructed_solution.indexer_assignments
    );
}

fn add_shard_to_indexer(
    missing_shard: ShardId,
    indexer: String,
    source_uid: &SourceUid,
    max_shard_per_pipeline: NonZeroU32,
    new_physical_plan: &mut PhysicalIndexingPlan,
    params_fingerprint: u64,
) {
    let indexer_tasks = new_physical_plan
        .indexing_tasks_per_indexer_mut()
        .entry(indexer)
        .or_default();

    let indexing_task_opt = indexer_tasks
        .iter_mut()
        .filter(|indexing_task| {
            indexing_task.index_uid() == &source_uid.index_uid
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
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec![missing_shard],
            params_fingerprint,
        });
    }
}

// If the total node capacities is lower than 120% of the problem load, this
// function scales the load of the indexer to reach this limit.
fn inflate_node_capacities_if_necessary(problem: &mut SchedulingProblem) {
    // First we scale the problem to the point where any indexer can fit the largest shard.
    let Some(largest_shard_load) = problem.sources().map(|source| source.load_per_shard).max()
    else {
        return;
    };

    // We first artificially scale down the node capacities.
    //
    // The node capacity is an estimate of the amount of CPU available on a given indexer node.
    // It has two purpose,
    // - under a lot of load, indexer will receive work proportional to their relative capacity.
    // - under low load, the absolute magnitude will be used by the scheduler, to decide whether
    // to prefer having a balanced workload over other criteria (all pipeline from a same index on
    // the same node, indexing local shards, etc.).
    //
    // The default CPU capacity is detected from the OS. Using these values directly leads
    // a non uniform distribution of the load which is very confusing for users. We artificially
    // scale down the indexer capacities.
    problem.scale_node_capacities(0.3f32);

    let min_indexer_capacity = (0..problem.num_indexers())
        .map(|indexer_ord| problem.indexer_cpu_capacity(indexer_ord))
        .min()
        .expect("At least one indexer is required");

    assert_ne!(min_indexer_capacity.cpu_millis(), 0);
    if min_indexer_capacity.cpu_millis() < largest_shard_load.get() {
        let scaling_factor =
            (largest_shard_load.get() as f32) / (min_indexer_capacity.cpu_millis() as f32);
        problem.scale_node_capacities(scaling_factor);
    }

    let total_node_capacities: f32 = problem.total_node_capacities().cpu_millis() as f32;
    let total_load: f32 = problem.total_load() as f32;
    let inflated_total_load = total_load * 1.2f32;
    if inflated_total_load >= total_node_capacities {
        // We need to inflate our node capacities to match the problem.
        let ratio = inflated_total_load / total_node_capacities;
        problem.scale_node_capacities(ratio);
    }
}

/// Creates a physical plan given the current situation of the cluster and the list of sources
/// to schedule.
///
/// The scheduling problem abstracts all notion of shard ids, source types, and node_ids,
/// to transform scheduling into a math problem.
///
/// This function implementation therefore goes
/// 1) transform our problem into a scheduling problem. Something closer to a well-defined
///    optimization problem. In particular this step removes:
///    - the notion of shard ids, and only considers a number of shards being allocated.
///    - node_ids and shard ids. These are replaced by integers.
/// 2) convert the current situation of the cluster into something a previous scheduling solution.
/// 3) compute the new scheduling solution.
/// 4) convert the new scheduling solution back to the real world by reallocating the shard ids.
///
/// TODO cut into pipelines.
/// Panics if any sources has no shards.
pub fn build_physical_indexing_plan(
    sources: &[SourceToSchedule],
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
    locality_aware: bool,
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
    shard_locations: &ShardLocations,
) -> PhysicalIndexingPlan {
    // Asserts that the source are valid.
    check_sources(sources);

    // We convert our problem into a simplified scheduling problem.
    // In this simplified version, nodes and sources are just ids.
    // Instead of individual shard ids, we just keep count of shards.
    // Similarly, instead of accurate locality, we just keep the number of shards local
    // to an indexer.
    let (id_to_ord_map, problem) =
        convert_to_simplified_problem(indexer_infos, locality_aware, sources, shard_locations);

    // Populate the previous solution, if any.
    let mut previous_solution = problem.new_solution();
    if let Some(previous_plan) = previous_plan_opt {
        convert_physical_plan_to_solution(previous_plan, &id_to_ord_map, &mut previous_solution);
    }

    // Compute the new scheduling solution using a heuristic.
    let new_solution = scheduling_logic::solve(problem, previous_solution);

    // Convert the new scheduling solution back to a physical plan.
    let new_physical_plan = convert_scheduling_solution_to_physical_plan(
        &new_solution,
        &id_to_ord_map,
        sources,
        previous_plan_opt,
        shard_locations,
        indexer_infos,
    );

    assert_post_condition_physical_plan_match_solution(
        &new_physical_plan,
        &new_solution,
        &id_to_ord_map,
    );

    new_physical_plan
}

#[cfg(test)]
pub(crate) fn build_physical_indexing_plan_without_locality(
    sources: &[SourceToSchedule],
    indexer_infos: &FnvHashMap<String, IndexerInfo>,
    previous_plan_opt: Option<&PhysicalIndexingPlan>,
    shard_locations: &ShardLocations,
) -> PhysicalIndexingPlan {
    let locality_aware = false;
    build_physical_indexing_plan(
        sources,
        indexer_infos,
        locality_aware,
        previous_plan_opt,
        shard_locations,
    )
}

/// Makes any checks on the sources.
/// Sharded sources are not allowed to have no shards.
fn check_sources(sources: &[SourceToSchedule]) {
    for source in sources {
        if let SourceToScheduleType::Sharded { shard_ids, .. } = &source.source_type {
            assert!(!shard_ids.is_empty())
        }
    }
}

fn intern_locality_group<'a>(
    availability_zone: &'a Option<String>,
    locality_groups: &mut FnvHashMap<&'a str, LocalityGroup>,
) -> Option<LocalityGroup> {
    let availability_zone = availability_zone.as_deref()?;
    let next_group_ord = locality_groups.len();
    let locality_group = locality_groups
        .entry(availability_zone)
        .or_insert_with(|| LocalityGroup::from_ord(next_group_ord));
    Some(*locality_group)
}

fn convert_to_simplified_problem<'a>(
    indexer_infos: &'a FnvHashMap<String, IndexerInfo>,
    locality_aware: bool,
    sources: &'a [SourceToSchedule],
    shard_locations: &ShardLocations,
) -> (IdToOrdMap<'a>, SchedulingProblem) {
    // Convert our problem to a scheduling problem.
    let mut id_to_ord_map: IdToOrdMap<'a> = IdToOrdMap::default();

    // We use a Vec as a `IndexOrd` -> Max load map.
    let mut indexer_cpu_capacities: Vec<CpuCapacity> = Vec::with_capacity(indexer_infos.len());
    let mut indexer_localities: Vec<IndexerLocality> = Vec::with_capacity(indexer_infos.len());
    let mut locality_groups: FnvHashMap<&str, LocalityGroup> = FnvHashMap::default();
    for (indexer_id, indexer_info) in indexer_infos {
        let indexer_ord = id_to_ord_map.add_indexer_id(indexer_id.clone());
        assert_eq!(indexer_ord, indexer_cpu_capacities.len() as IndexerOrd);
        indexer_cpu_capacities.push(indexer_info.cpu_capacity);
        if !locality_aware {
            continue;
        }
        let locality_group =
            intern_locality_group(&indexer_info.availability_zone, &mut locality_groups);
        indexer_localities.push(IndexerLocality {
            group: locality_group,
            eligibility: indexer_info.eligibility,
        });
    }

    let mut problem = if locality_aware {
        SchedulingProblem::with_indexer_localities(indexer_cpu_capacities, indexer_localities)
    } else {
        SchedulingProblem::with_indexer_cpu_capacities(indexer_cpu_capacities)
    };

    for source in sources {
        if let Some(source_ord) = populate_problem(source, &mut problem) {
            let registered_source_ord = id_to_ord_map.add_source(source);
            if let SourceToScheduleType::Sharded { shard_ids, .. } = &source.source_type {
                for shard_id in shard_ids {
                    for &indexer in shard_locations.get_shard_locations(shard_id) {
                        let Some(indexer_ord) = id_to_ord_map.indexer_ord(indexer.as_str()) else {
                            // This happens if the ingester is unavailable.
                            rate_limited_debug!(
                                limit_per_min = 10,
                                "failed to find indexer ord for indexer {indexer}"
                            );
                            continue;
                        };
                        problem.inc_affinity(source_ord, indexer_ord);
                    }
                }
            }
            assert_eq!(source_ord, registered_source_ord);
        }
    }
    (id_to_ord_map, problem)
}

#[cfg(test)]
mod tests {

    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::num::NonZeroU32;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use fnv::FnvHashMap;
    use itertools::Itertools;
    use quickwit_proto::indexing::{CpuCapacity, IndexingTask, mcpu};
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, ShardId, SourceUid};
    use rand::SeedableRng;
    use rand::prelude::IndexedRandom;
    use rand::rngs::StdRng;

    use super::scheduling_logic::solve;
    use super::{
        Eligibility, IndexerInfo, IndexerSpec, SourceToSchedule, SourceToScheduleType,
        build_physical_indexing_plan, build_physical_indexing_plan_without_locality,
        convert_scheduling_solution_to_physical_plan_single_node_single_source,
        convert_to_simplified_problem, shard_ids_for_indexer,
    };
    use crate::indexing_plan::PhysicalIndexingPlan;
    use crate::indexing_scheduler::get_shard_locality_metrics;
    use crate::indexing_scheduler::scheduling::assign_shards;
    use crate::model::ShardLocations;

    fn indexer_info_in_az(
        cpu_capacity: CpuCapacity,
        availability_zone: &str,
        eligibility: Eligibility,
    ) -> IndexerInfo {
        IndexerInfo {
            cpu_capacity,
            availability_zone: Some(availability_zone.to_string()),
            eligibility,
        }
    }

    fn shard_counts_per_az(
        plan: &PhysicalIndexingPlan,
        indexer_infos: &FnvHashMap<String, IndexerInfo>,
    ) -> BTreeMap<Option<String>, Vec<usize>> {
        let mut counts_per_az: BTreeMap<Option<String>, Vec<usize>> = BTreeMap::default();
        for (indexer, tasks) in plan.indexing_tasks_per_indexer() {
            let num_shards: usize = tasks.iter().map(|task| task.shard_ids.len()).sum();
            if num_shards == 0 {
                continue;
            }
            let availability_zone = indexer_infos[indexer].availability_zone.clone();
            counts_per_az
                .entry(availability_zone)
                .or_default()
                .push(num_shards);
        }
        for counts in counts_per_az.values_mut() {
            counts.sort();
        }
        counts_per_az
    }

    fn source_id() -> SourceUid {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let index = IndexUid::for_test("test_index", 0);
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
                shard_ids: vec![
                    ShardId::from(0),
                    ShardId::from(1),
                    ShardId::from(2),
                    ShardId::from(3),
                    ShardId::from(4),
                    ShardId::from(5),
                    ShardId::from(6),
                    ShardId::from(7),
                ],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        };
        let source_1 = SourceToSchedule {
            source_uid: source_uid1.clone(),
            source_type: SourceToScheduleType::NonSharded {
                num_pipelines: 2,
                load_per_pipeline: NonZeroU32::new(3_200).unwrap(),
            },
            params_fingerprint: 0,
        };
        let source_2 = SourceToSchedule {
            source_uid: source_uid2.clone(),
            source_type: SourceToScheduleType::IngestV1,
            params_fingerprint: 0,
        };
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(indexer1.clone(), IndexerInfo::for_test(mcpu(16_000)));
        indexer_infos.insert(indexer2.clone(), IndexerInfo::for_test(mcpu(16_000)));
        let shard_locations = ShardLocations::default();
        let indexing_plan = build_physical_indexing_plan_without_locality(
            &[source_0, source_1, source_2],
            &indexer_infos,
            None,
            &shard_locations,
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

        let mut shard_ids: HashSet<ShardId> = HashSet::default();
        let mut shard_lens = Vec::new();
        shard_lens.push(node2_plan[0].shard_ids.len());
        shard_ids.extend(node2_plan[0].shard_ids.iter().cloned());
        assert_eq!(&node2_plan[1].source_id, &source_uid0.source_id);
        shard_lens.push(node2_plan[1].shard_ids.len());
        shard_ids.extend(node2_plan[1].shard_ids.iter().cloned());
        assert_eq!(&node2_plan[2].source_id, &source_uid0.source_id);
        shard_lens.push(node2_plan[2].shard_ids.len());
        shard_ids.extend(node2_plan[2].shard_ids.iter().cloned());
        assert_eq!(shard_ids.len(), 8);
        assert_eq!(&node2_plan[3].source_id, &source_uid2.source_id);
        shard_lens.sort();
        assert_eq!(&shard_lens[..], &[2, 3, 3]);
    }

    #[test]
    fn test_build_physical_plan_with_locality() {
        let num_indexers = 10;
        let num_shards: usize = 1000;
        let indexers: Vec<NodeId> = (0..num_indexers)
            .map(|indexer_id| NodeId::from_str(&format!("indexer{indexer_id}")))
            .collect();
        let source_uids: Vec<SourceUid> = std::iter::repeat_with(source_id).take(1_000).collect();
        let shard_ids: Vec<ShardId> = (0..num_shards as u64).map(ShardId::from).collect();
        let sources: Vec<SourceToSchedule> = (0..num_shards)
            .map(|i| SourceToSchedule {
                source_uid: source_uids[i].clone(),
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: vec![shard_ids[i].clone()],
                    load_per_shard: NonZeroU32::new(250).unwrap(),
                },
                params_fingerprint: 0,
            })
            .collect();

        let mut indexer_infos = FnvHashMap::default();
        for indexer in &indexers {
            let indexer_info = IndexerInfo::for_test(mcpu(16_000));
            indexer_infos.insert(indexer.as_str().to_string(), indexer_info);
        }
        let mut rng = StdRng::seed_from_u64(42);

        let mut shard_locations = ShardLocations::default();
        for shard_id in &shard_ids {
            let indexer = indexers[..].choose(&mut rng).unwrap();
            shard_locations.add_location(shard_id, indexer);
        }

        let plan = build_physical_indexing_plan_without_locality(
            &sources,
            &indexer_infos,
            None,
            &shard_locations,
        );
        assert_eq!(plan.indexing_tasks_per_indexer().len(), num_indexers);
        let metrics = get_shard_locality_metrics(&plan, &shard_locations, &indexer_infos);
        assert_eq!(
            metrics.num_remote_shards + metrics.num_local_shards,
            num_shards
        );
        assert!(metrics.num_remote_shards < 10);
    }

    // asserts that if the flag is off, even if az/eligibiltiy are applied, the indexing plan
    // falls back to the old logic.
    #[test]
    fn test_flag_off_ignores_locality_and_eligibility() {
        let shard0 = ShardId::from(0);
        let shard1 = ShardId::from(1);
        let shard2 = ShardId::from(2);
        let sharded_source = SourceToSchedule {
            source_uid: source_id(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![shard0.clone(), shard1.clone(), shard2.clone()],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        };
        let source_without_affinity = SourceToSchedule {
            source_uid: source_id(),
            source_type: SourceToScheduleType::NonSharded {
                num_pipelines: 1,
                load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        };
        let sources = vec![sharded_source, source_without_affinity];

        let indexer1 = NodeId::from_str("indexer1");
        let indexer2 = NodeId::from_str("indexer2");
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard0, &indexer1);
        shard_locations.add_location(&shard1, &indexer1);
        shard_locations.add_location(&shard2, &indexer2);

        let mut plain_indexer_infos = FnvHashMap::default();
        plain_indexer_infos.insert(indexer1.to_string(), IndexerInfo::for_test(mcpu(4_000)));
        plain_indexer_infos.insert(indexer2.to_string(), IndexerInfo::for_test(mcpu(4_000)));

        let indexer1_in_az = indexer_info_in_az(mcpu(4_000), "az-a", Eligibility::SelfHostedOnly);
        let indexer2_in_az = indexer_info_in_az(mcpu(4_000), "az-b", Eligibility::SelfHostedOnly);
        let mut az_indexer_infos = FnvHashMap::default();
        az_indexer_infos.insert(indexer1.to_string(), indexer1_in_az);
        az_indexer_infos.insert(indexer2.to_string(), indexer2_in_az);

        let locality_aware = false;
        let (_, plain_problem) = convert_to_simplified_problem(
            &plain_indexer_infos,
            locality_aware,
            &sources,
            &shard_locations,
        );
        let (_, az_problem) = convert_to_simplified_problem(
            &az_indexer_infos,
            locality_aware,
            &sources,
            &shard_locations,
        );

        assert!(!plain_problem.is_locality_aware());
        assert!(!az_problem.is_locality_aware());
        assert_eq!(plain_problem.num_locality_groups(), 0);
        assert_eq!(az_problem.num_locality_groups(), 0);
        for indexer_ord in 0..az_problem.num_indexers() {
            assert!(az_problem.is_eligible_for_foreign_shards(indexer_ord));
        }

        let plain_previous_solution = plain_problem.new_solution();
        let az_previous_solution = az_problem.new_solution();
        let plain_solution = solve(plain_problem, plain_previous_solution);
        let az_solution = solve(az_problem, az_previous_solution);
        assert_eq!(
            plain_solution.indexer_assignments,
            az_solution.indexer_assignments
        );
    }

    struct TopologyOutcome {
        num_local_shards: usize,
        num_nearby_shards: usize,
        num_remote_shards: usize,
        shard_counts_per_az: BTreeMap<Option<String>, Vec<usize>>,
    }

    fn assert_stable_locality_topology(
        indexer_specs: &[IndexerSpec],
        shard_ids: &[ShardId],
        shard_hosts: &[&NodeId],
    ) -> TopologyOutcome {
        let source = SourceToSchedule {
            source_uid: source_id(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: shard_ids.to_vec(),
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        };
        let sources = vec![source];

        let mut shard_locations = ShardLocations::default();
        for (shard_id, shard_host) in shard_ids.iter().zip(shard_hosts) {
            shard_locations.add_location(shard_id, shard_host);
        }

        let mut indexer_infos = FnvHashMap::default();
        for indexer_spec in indexer_specs {
            let indexer_info = indexer_spec.to_indexer_info(Eligibility::Any);
            indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
        }

        let locality_aware = true;
        let plan = build_physical_indexing_plan(
            &sources,
            &indexer_infos,
            locality_aware,
            None,
            &shard_locations,
        );

        let scheduled_shard_ids: Vec<ShardId> = plan
            .indexing_tasks_per_indexer()
            .values()
            .flatten()
            .flat_map(|task| task.shard_ids.iter().cloned())
            .collect();
        let unique_shard_ids: HashSet<&ShardId> = scheduled_shard_ids.iter().collect();
        assert_eq!(scheduled_shard_ids.len(), shard_ids.len());
        assert_eq!(unique_shard_ids.len(), shard_ids.len());

        let replanned = build_physical_indexing_plan(
            &sources,
            &indexer_infos,
            locality_aware,
            Some(&plan),
            &shard_locations,
        );
        assert_eq!(plan, replanned);

        let mut reversed_indexer_infos = FnvHashMap::default();
        for indexer_spec in indexer_specs.iter().rev() {
            let indexer_info = indexer_spec.to_indexer_info(Eligibility::Any);
            reversed_indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
        }
        let reversed_plan = build_physical_indexing_plan(
            &sources,
            &reversed_indexer_infos,
            locality_aware,
            None,
            &shard_locations,
        );
        let counts_per_az = shard_counts_per_az(&plan, &indexer_infos);
        let reversed_counts_per_az = shard_counts_per_az(&reversed_plan, &reversed_indexer_infos);
        assert_eq!(counts_per_az, reversed_counts_per_az);

        let metrics = get_shard_locality_metrics(&plan, &shard_locations, &indexer_infos);
        TopologyOutcome {
            num_local_shards: metrics.num_local_shards,
            num_nearby_shards: metrics.num_nearby_shards,
            num_remote_shards: metrics.num_remote_shards,
            shard_counts_per_az: counts_per_az,
        }
    }

    #[test]
    fn test_locality_aware_topology_matrix_is_stable() {
        let shard_ids: Vec<ShardId> = (0..5u64).map(ShardId::from).collect();
        let unschedulable_host = NodeId::from_str("unschedulable");
        {
            let indexer_specs = vec![IndexerSpec::new("indexer1", mcpu(4_000), None)];
            let shard_hosts = vec![&indexer_specs[0].node_id, &unschedulable_host];
            let outcome =
                assert_stable_locality_topology(&indexer_specs, &shard_ids[..2], &shard_hosts);
            assert_eq!(outcome.num_local_shards, 1);
            assert_eq!(outcome.num_nearby_shards, 0);
            assert_eq!(outcome.num_remote_shards, 1);
            let expected_counts_per_az = BTreeMap::from_iter([(None, vec![2])]);
            assert_eq!(outcome.shard_counts_per_az, expected_counts_per_az);
        }
        {
            let indexer_specs = vec![
                IndexerSpec::new("indexer1", mcpu(4_000), Some("az-a")),
                IndexerSpec::new("indexer2", mcpu(4_000), Some("az-a")),
            ];
            let shard_hosts = vec![&indexer_specs[0].node_id, &indexer_specs[0].node_id];
            let outcome =
                assert_stable_locality_topology(&indexer_specs, &shard_ids[..2], &shard_hosts);
            assert_eq!(outcome.num_local_shards, 1);
            assert_eq!(outcome.num_nearby_shards, 1);
            assert_eq!(outcome.num_remote_shards, 0);
            let expected_counts_per_az =
                BTreeMap::from_iter([(Some("az-a".to_string()), vec![1, 1])]);
            assert_eq!(outcome.shard_counts_per_az, expected_counts_per_az);
        }
        {
            let indexer_specs = vec![
                IndexerSpec::new("indexer1", mcpu(4_000), Some("az-a")),
                IndexerSpec::new("indexer2", mcpu(4_000), Some("az-b")),
            ];
            let shard_hosts = vec![&indexer_specs[0].node_id, &indexer_specs[0].node_id];
            let outcome =
                assert_stable_locality_topology(&indexer_specs, &shard_ids[..2], &shard_hosts);
            assert_eq!(outcome.num_local_shards, 1);
            assert_eq!(outcome.num_nearby_shards, 0);
            assert_eq!(outcome.num_remote_shards, 1);
            let expected_counts_per_az = BTreeMap::from_iter([
                (Some("az-a".to_string()), vec![1]),
                (Some("az-b".to_string()), vec![1]),
            ]);
            assert_eq!(outcome.shard_counts_per_az, expected_counts_per_az);
        }
        {
            let indexer_specs = vec![
                IndexerSpec::new("indexer1", mcpu(8_000), Some("az-a")),
                IndexerSpec::new("indexer2", mcpu(4_000), Some("az-a")),
                IndexerSpec::new("indexer3", mcpu(4_000), Some("az-b")),
                IndexerSpec::new("indexer4", mcpu(4_000), Some("az-c")),
            ];
            let shard_hosts = vec![&indexer_specs[0].node_id; 5];
            let outcome = assert_stable_locality_topology(&indexer_specs, &shard_ids, &shard_hosts);
            assert_eq!(outcome.num_local_shards, 2);
            assert_eq!(outcome.num_nearby_shards, 1);
            assert_eq!(outcome.num_remote_shards, 2);
            let expected_counts_per_az = BTreeMap::from_iter([
                (Some("az-a".to_string()), vec![1, 2]),
                (Some("az-b".to_string()), vec![1]),
                (Some("az-c".to_string()), vec![1]),
            ]);
            assert_eq!(outcome.shard_counts_per_az, expected_counts_per_az);
        }
    }

    #[test]
    fn test_small_clusters_are_not_unbalanced() {
        const NUM_SHARDS: usize = 12;
        for num_indexers in [2usize, 4, 5] {
            let indexer_specs: Vec<IndexerSpec> = (0..num_indexers)
                .map(|indexer_ord| {
                    let node_id = format!("indexer{indexer_ord}");
                    IndexerSpec::new(&node_id, mcpu(4_000), Some("az-a"))
                })
                .collect();
            let shard_ids: Vec<ShardId> = (0..NUM_SHARDS as u64).map(ShardId::from).collect();
            let source = SourceToSchedule {
                source_uid: source_id(),
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: shard_ids.clone(),
                    load_per_shard: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let sources = vec![source];

            let mut shard_locations = ShardLocations::default();
            for shard_id in &shard_ids {
                shard_locations.add_location(shard_id, &indexer_specs[0].node_id);
            }

            let mut indexer_infos = FnvHashMap::default();
            for indexer_spec in &indexer_specs {
                let indexer_info = indexer_spec.to_indexer_info(Eligibility::Any);
                indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
            }

            let locality_aware = true;
            let plan = build_physical_indexing_plan(
                &sources,
                &indexer_infos,
                locality_aware,
                None,
                &shard_locations,
            );

            let mut legacy_indexer_infos = FnvHashMap::default();
            for indexer_spec in &indexer_specs {
                let indexer_info = IndexerInfo::for_test(mcpu(4_000));
                legacy_indexer_infos.insert(indexer_spec.node_id.to_string(), indexer_info);
            }
            let legacy_plan = build_physical_indexing_plan_without_locality(
                &sources,
                &legacy_indexer_infos,
                None,
                &shard_locations,
            );

            let mut shard_counts: Vec<usize> = indexer_specs
                .iter()
                .map(|indexer_spec| {
                    shard_ids_for_indexer(&plan, indexer_spec.node_id.as_str()).len()
                })
                .collect();
            shard_counts.sort();
            let mut legacy_shard_counts: Vec<usize> = indexer_specs
                .iter()
                .map(|indexer_spec| {
                    shard_ids_for_indexer(&legacy_plan, indexer_spec.node_id.as_str()).len()
                })
                .collect();
            legacy_shard_counts.sort();
            let fair_share = NUM_SHARDS.div_ceil(num_indexers);
            let max_shard_count = *shard_counts.last().unwrap();
            println!(
                "{num_indexers} indexers hold {shard_counts:?}, legacy holds {legacy_shard_counts:?}"
            );
            assert_eq!(shard_counts.iter().sum::<usize>(), NUM_SHARDS);
            assert_eq!(shard_counts, legacy_shard_counts);
            assert!(
                max_shard_count <= fair_share + 1,
                "{num_indexers} indexers hold {shard_counts:?}, fair share is {fair_share}"
            );
        }
    }

    #[test]
    fn test_draining_indexer_keeps_only_hosted_shard_ids() {
        let shard0 = ShardId::from(0);
        let shard1 = ShardId::from(1);
        let source_uid = source_id();
        let source = SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![shard0.clone(), shard1.clone()],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        };
        let sources = vec![source];

        let draining_indexer = NodeId::from_str("indexer1");
        let ready_indexer = NodeId::from_str("indexer2");
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard0, &draining_indexer);
        shard_locations.add_location(&shard1, &ready_indexer);

        let draining_info = indexer_info_in_az(mcpu(4_000), "az-a", Eligibility::SelfHostedOnly);
        let ready_info = indexer_info_in_az(mcpu(4_000), "az-a", Eligibility::Any);
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(draining_indexer.to_string(), draining_info);
        indexer_infos.insert(ready_indexer.to_string(), ready_info);

        let indexer_ids = vec![draining_indexer.to_string(), ready_indexer.to_string()];
        let mut swapped_plan = PhysicalIndexingPlan::with_indexer_ids(&indexer_ids);
        let draining_task = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            shard_ids: vec![shard1.clone()],
            params_fingerprint: 0,
        };
        let ready_task = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.clone(),
            pipeline_uid: Some(PipelineUid::for_test(2u128)),
            shard_ids: vec![shard0.clone()],
            params_fingerprint: 0,
        };
        swapped_plan.add_indexing_task(draining_indexer.as_str(), draining_task);
        swapped_plan.add_indexing_task(ready_indexer.as_str(), ready_task);

        let locality_aware = true;
        let plan = build_physical_indexing_plan(
            &sources,
            &indexer_infos,
            locality_aware,
            Some(&swapped_plan),
            &shard_locations,
        );

        let draining_shard_ids = shard_ids_for_indexer(&plan, draining_indexer.as_str());
        let ready_shard_ids = shard_ids_for_indexer(&plan, ready_indexer.as_str());
        assert_eq!(draining_shard_ids, vec![shard0.clone()]);
        assert_eq!(ready_shard_ids, vec![shard1.clone()]);

        let metrics = get_shard_locality_metrics(&plan, &shard_locations, &indexer_infos);
        assert_eq!(metrics.num_local_shards, 2);
        assert_eq!(metrics.num_nearby_shards, 0);
        assert_eq!(metrics.num_remote_shards, 0);

        let replanned = build_physical_indexing_plan(
            &sources,
            &indexer_infos,
            locality_aware,
            Some(&plan),
            &shard_locations,
        );
        assert_eq!(plan, replanned);
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
            params_fingerprint: 0,
        };
        let sources = vec![source_1];

        let indexer1 = "indexer1".to_string();
        let mut indexer_infos = FnvHashMap::default();
        let shard_locations = ShardLocations::default();
        {
            indexer_infos.insert(indexer1.clone(), IndexerInfo::for_test(mcpu(1_999)));
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan_without_locality(
                &sources,
                &indexer_infos,
                None,
                &shard_locations,
            );
            assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 1);
            let expected_tasks = physical_plan.indexer(&indexer1).unwrap();
            assert_eq!(expected_tasks.len(), 2);
            assert_eq!(&expected_tasks[0].source_id, &source_uid1.source_id);
        }
        {
            indexer_infos.insert(indexer1.clone(), IndexerInfo::for_test(mcpu(2_000)));
            // This test what happens when there isn't enough capacity on the cluster.
            let physical_plan = build_physical_indexing_plan_without_locality(
                &sources,
                &indexer_infos,
                None,
                &shard_locations,
            );
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
                index_uid: Some(source_uid.index_uid.clone()),
                source_id: source_uid.source_id.clone(),
                pipeline_uid: Some(*pipeline_uid),
                shard_ids: shard_ids.to_vec(),
                params_fingerprint: 0,
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
                (
                    PipelineUid::for_test(1u128),
                    &[ShardId::from(1), ShardId::from(2)],
                ),
                (
                    PipelineUid::for_test(2u128),
                    &[ShardId::from(3), ShardId::from(4), ShardId::from(5)],
                ),
            ],
        );
        let sources = vec![SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: vec![
                    ShardId::from(0),
                    ShardId::from(1),
                    ShardId::from(3),
                    ShardId::from(4),
                    ShardId::from(5),
                ],
                load_per_shard: NonZeroU32::new(1_000).unwrap(),
            },
            params_fingerprint: 0,
        }];
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert("node1".to_string(), IndexerInfo::for_test(mcpu(10_000)));
        let mut indexing_plan = PhysicalIndexingPlan::with_indexer_ids(&["node1".to_string()]);
        for indexing_task in indexing_tasks {
            indexing_plan.add_indexing_task("node1", indexing_task);
        }
        let shard_locations = ShardLocations::default();
        let new_plan = build_physical_indexing_plan_without_locality(
            &sources,
            &indexer_infos,
            Some(&indexing_plan),
            &shard_locations,
        );
        let indexing_tasks = new_plan.indexer("node1").unwrap();
        assert_eq!(indexing_tasks.len(), 2);
        assert_eq!(
            &indexing_tasks[0].shard_ids,
            &[ShardId::from(0), ShardId::from(1)]
        );
        assert_eq!(
            &indexing_tasks[1].shard_ids,
            &[ShardId::from(3), ShardId::from(4), ShardId::from(5)]
        );
    }

    fn group_shards_into_pipelines_aux(
        source_uid: &SourceUid,
        shard_ids: &[u64],
        previous_pipeline_shards: &[(PipelineUid, &[ShardId])],
        load_per_shard: CpuCapacity,
    ) -> Vec<IndexingTask> {
        let indexing_tasks = make_indexing_tasks(source_uid, previous_pipeline_shards);
        let sources = vec![SourceToSchedule {
            source_uid: source_uid.clone(),
            source_type: SourceToScheduleType::Sharded {
                shard_ids: shard_ids.iter().copied().map(ShardId::from).collect(),
                load_per_shard: NonZeroU32::new(load_per_shard.cpu_millis()).unwrap(),
            },
            params_fingerprint: 0,
        }];
        const NODE: &str = "node1";
        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(NODE.to_string(), IndexerInfo::for_test(mcpu(10_000)));
        let mut indexing_plan = PhysicalIndexingPlan::with_indexer_ids(&["node1".to_string()]);
        for indexing_task in indexing_tasks {
            indexing_plan.add_indexing_task(NODE, indexing_task);
        }
        let shard_locations = ShardLocations::default();
        let new_plan = build_physical_indexing_plan_without_locality(
            &sources,
            &indexer_infos,
            Some(&indexing_plan),
            &shard_locations,
        );
        let mut indexing_tasks = new_plan.indexer(NODE).unwrap().to_vec();
        for indexing_task in &mut indexing_tasks {
            indexing_task.shard_ids.sort();
        }
        // We sort indexing tasks for normalization purpose
        indexing_tasks.sort_by_key(|task| task.shard_ids[0].clone());
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
        let indexing_tasks_len_1: Vec<usize> = indexing_tasks_1
            .iter()
            .map(|task| task.shard_ids.len())
            .sorted()
            .collect();
        assert_eq!(&indexing_tasks_len_1, &[3, 8]);

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
        let indexing_tasks_len_2: Vec<usize> = indexing_tasks_2
            .iter()
            .map(|task| task.shard_ids.len())
            .sorted()
            .collect();
        assert_eq!(&indexing_tasks_len_2, &[1, 5, 5]);

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
        let indexing_tasks_len_4: Vec<usize> = indexing_tasks_4
            .iter()
            .map(|task| task.shard_ids.len())
            .sorted()
            .collect();
        assert_eq!(&indexing_tasks_len_4, &[5, 6]);
    }

    /// We want to make sure for small pipelines, we still reschedule them with the same
    /// pipeline uid.
    #[test]
    fn test_group_shards_into_pipeline_single_small_pipeline() {
        let source_uid = source_id();
        let pipeline_uid = PipelineUid::for_test(1u128);
        let indexing_tasks = group_shards_into_pipelines_aux(
            &source_uid,
            &[12],
            &[(pipeline_uid, &[ShardId::from(12)])],
            mcpu(100),
        );
        assert_eq!(indexing_tasks.len(), 1);
        let indexing_task = &indexing_tasks[0];
        assert_eq!(&indexing_task.shard_ids, &[ShardId::from(12)]);
        assert_eq!(indexing_task.pipeline_uid.unwrap(), pipeline_uid);
    }

    #[test]
    fn test_assign_missing_shards() {
        let shard0 = ShardId::from(0);
        let shard1 = ShardId::from(1);
        let shard2 = ShardId::from(2);
        let shard3 = ShardId::from(3);

        let missing_shards = vec![
            shard0.clone(),
            shard1.clone(),
            shard2.clone(),
            shard3.clone(),
        ];
        let node1 = NodeId::from_str("node1");
        let node2 = NodeId::from_str("node2");
        // This node is missing from the capacity map.
        // It should not be assigned any task despite being present in shard locations.
        let node_missing = NodeId::from_str("node_missing");
        let mut remaining_num_shards_per_node = HashMap::default();
        remaining_num_shards_per_node
            .insert(node1.as_str().to_string(), NonZeroU32::new(3).unwrap());
        remaining_num_shards_per_node
            .insert(node2.as_str().to_string(), NonZeroU32::new(1).unwrap());

        let mut indexer_infos = FnvHashMap::default();
        indexer_infos.insert(
            node1.as_str().to_string(),
            IndexerInfo::for_test(mcpu(4_000)),
        );
        indexer_infos.insert(
            node2.as_str().to_string(),
            IndexerInfo::for_test(mcpu(4_000)),
        );

        let mut shard_locations: ShardLocations = ShardLocations::default();
        // shard1 on 1
        shard_locations.add_location(&shard1, &node1);
        // shard2 on 2
        shard_locations.add_location(&shard2, &node2);
        // shard3 on both 1 and 2
        shard_locations.add_location(&shard3, &node1);
        shard_locations.add_location(&shard3, &node2);
        shard_locations.add_location(&shard0, &node_missing);

        let shard_to_indexer = assign_shards(
            missing_shards,
            remaining_num_shards_per_node,
            &shard_locations,
            &indexer_infos,
        );
        assert_eq!(shard_to_indexer.len(), 4);
        assert_eq!(shard_to_indexer.get(&shard1).unwrap(), "node1");
        assert_eq!(shard_to_indexer.get(&shard2).unwrap(), "node2");
        assert_eq!(shard_to_indexer.get(&shard3).unwrap(), "node1");
        assert_eq!(shard_to_indexer.get(&shard0).unwrap(), "node1");
    }

    #[test]
    fn test_solution_reconstruction() {
        let sources_to_schedule = vec![
            SourceToSchedule {
                source_uid: SourceUid {
                    index_uid: IndexUid::from_str("otel-logs-v0_6:01HKYD1SE37C90KSH21CD1M11A")
                        .unwrap(),
                    source_id: "_ingest-api-source".to_string(),
                },
                source_type: SourceToScheduleType::IngestV1,
                params_fingerprint: 0,
            },
            SourceToSchedule {
                source_uid: SourceUid {
                    index_uid: IndexUid::from_str(
                        "simian_chico_12856033706389338959:01HKYD414H1WVSASC5YD972P39",
                    )
                    .unwrap(),
                    source_id: "_ingest-source".to_string(),
                },
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: vec![ShardId::from(1)],
                    load_per_shard: NonZeroU32::new(250).unwrap(),
                },
                params_fingerprint: 0,
            },
        ];
        let mut indexer_infos = FnvHashMap::default();
        let indexer_info = IndexerInfo::for_test(CpuCapacity::from_cpu_millis(8000));
        indexer_infos.insert("indexer-1".to_string(), indexer_info);
        let shard_locations = ShardLocations::default();
        build_physical_indexing_plan_without_locality(
            &sources_to_schedule,
            &indexer_infos,
            None,
            &shard_locations,
        );
    }

    #[test]
    fn test_convert_scheduling_solution_to_physical_plan_single_node_single_source_sharded() {
        let source_uid = SourceUid {
            index_uid: IndexUid::new_with_random_ulid("testindex"),
            source_id: "testsource".to_string(),
        };
        let previous_task1 = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec![ShardId::from(1), ShardId::from(4), ShardId::from(5)],
            params_fingerprint: 0,
        };
        let previous_task2 = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec![
                ShardId::from(6),
                ShardId::from(7),
                ShardId::from(8),
                ShardId::from(9),
                ShardId::from(10),
            ],
            params_fingerprint: 0,
        };
        {
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: vec![
                        ShardId::from(1),
                        ShardId::from(2),
                        ShardId::from(4),
                        ShardId::from(6),
                    ],
                    load_per_shard: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                4,
                &[&previous_task1, &previous_task2],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 2);
            assert_eq!(tasks[0].index_uid(), &source_uid.index_uid);
            assert_eq!(tasks[0].shard_ids, [ShardId::from(1), ShardId::from(4)]);
            assert_eq!(tasks[1].index_uid(), &source_uid.index_uid);
            assert_eq!(tasks[1].shard_ids, [ShardId::from(6)]);
        }
        {
            // smaller shards force a merge into a single pipeline
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::Sharded {
                    shard_ids: vec![
                        ShardId::from(1),
                        ShardId::from(2),
                        ShardId::from(4),
                        ShardId::from(6),
                    ],
                    load_per_shard: NonZeroU32::new(250).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                4,
                &[&previous_task1, &previous_task2],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].index_uid(), &source_uid.index_uid);
            assert_eq!(tasks[0].shard_ids, [ShardId::from(1), ShardId::from(4)]);
        }
    }

    #[test]
    fn test_convert_scheduling_solution_to_physical_plan_single_node_single_source_non_sharded() {
        let source_uid = SourceUid {
            index_uid: IndexUid::new_with_random_ulid("testindex"),
            source_id: "testsource".to_string(),
        };
        let pipeline_uid1 = PipelineUid::random();
        let previous_task1 = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.to_string(),
            pipeline_uid: Some(pipeline_uid1),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        let pipeline_uid2 = PipelineUid::random();
        let previous_task2 = IndexingTask {
            index_uid: Some(source_uid.index_uid.clone()),
            source_id: source_uid.source_id.to_string(),
            pipeline_uid: Some(pipeline_uid2),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        {
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 1,
                    load_per_pipeline: NonZeroU32::new(4000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                1,
                &[&previous_task1, &previous_task2],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].index_uid(), &source_uid.index_uid);
            assert!(tasks[0].shard_ids.is_empty());
            assert_eq!(tasks[0].pipeline_uid.as_ref().unwrap(), &pipeline_uid1);
        }
        {
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 0,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                0,
                &[&previous_task1, &previous_task2],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 0);
        }
        {
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 2,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                2,
                &[&previous_task1, &previous_task2],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 2);
            assert_eq!(tasks[0].index_uid(), &source_uid.index_uid);
            assert!(tasks[0].shard_ids.is_empty());
            assert_eq!(tasks[0].pipeline_uid.as_ref().unwrap(), &pipeline_uid1);
            assert_eq!(tasks[1].index_uid(), &source_uid.index_uid);
            assert!(tasks[1].shard_ids.is_empty());
            assert_eq!(tasks[1].pipeline_uid.as_ref().unwrap(), &pipeline_uid2);
        }
        {
            let sharded_source = SourceToSchedule {
                source_uid: source_uid.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 2,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            };
            let tasks = convert_scheduling_solution_to_physical_plan_single_node_single_source(
                2,
                &[&previous_task1],
                &sharded_source,
            );
            assert_eq!(tasks.len(), 2);
            assert_eq!(tasks[0].index_uid(), &source_uid.index_uid);
            assert!(tasks[0].shard_ids.is_empty());
            assert_eq!(tasks[0].pipeline_uid.as_ref().unwrap(), &pipeline_uid1);
            assert_eq!(tasks[1].index_uid(), &source_uid.index_uid);
            assert!(tasks[1].shard_ids.is_empty());
            assert_ne!(tasks[1].pipeline_uid.as_ref().unwrap(), &pipeline_uid1);
        }
    }
}
