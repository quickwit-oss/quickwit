// Copyright (C) 2024 Quickwit, Inc.
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

mod change_tracker;
mod scheduling;

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use quickwit_common::pretty::PrettySample;
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, CpuCapacity, IndexingService, IndexingTask, PIPELINE_FULL_CAPACITY,
    PIPELINE_THROUGHTPUT,
};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::NodeId;
use scheduling::{SourceToSchedule, SourceToScheduleType};
use serde::Serialize;
use tracing::{debug, info, warn};

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::change_tracker::{NotifyChangeOnDrop, RebuildNotifier};
use crate::indexing_scheduler::scheduling::build_physical_indexing_plan;
use crate::metrics::ShardLocalityMetrics;
use crate::model::{ControlPlaneModel, ShardEntry, ShardLocations};
use crate::{IndexerNodeInfo, IndexerPool};

pub(crate) const MIN_DURATION_BETWEEN_SCHEDULING: Duration =
    if cfg!(any(test, feature = "testsuite")) {
        Duration::from_millis(50)
    } else {
        Duration::from_secs(30)
    };

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexingSchedulerState {
    pub num_applied_physical_indexing_plan: usize,
    pub num_schedule_indexing_plan: usize,
    pub last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    #[serde(skip)]
    pub last_applied_plan_timestamp: Option<Instant>,
}

/// The [`IndexingScheduler`] is responsible for listing indexing tasks and assiging them to
/// indexers.
/// We call this duty `scheduling`. Contrary to what the name suggests, most indexing tasks are
/// ever running. We just borrowed the terminology to Kubernetes.
///
/// Scheduling executes the following steps:
/// 1. Builds a [`PhysicalIndexingPlan`] from the list of logical indexing tasks. See
///    [`build_physical_indexing_plan`] for the implementation details.
/// 2. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan. Any
///    errors (network) happening in this step are ignored. The scheduler runs a control loop that
///    regularly checks if indexers are effectively running their plans (more details in the next
///    section).
///
/// All events altering the list of indexes and sources are proxied through
/// through the control plane. The control plane model is therefore guaranteed to be up-to-date
/// (at the cost of making the control plane a single point of failure).
///
/// Each change to the model triggers the production of a new `PhysicalIndexingPlan`.
///
/// A `ControlPlanLoop` event is scheduled every `CONTROL_PLAN_LOOP_INTERVAL` and steers
/// the cluster toward the last applied [`PhysicalIndexingPlan`].
///
/// This physical plan is a desired state. Even after that state is reached, it can be altered due
/// to faulty server for instance.
///
/// We then need to detect deviation, possibly recompute the desired `PhysicalIndexingPlan`
/// and steer back the cluster to the right state.
///
/// First to detect deviation, the control plan gathers an eventually consistent view of what is
/// running on the different nodes of the cluster: the `running plan`. This is done via `chitchat`.
///
/// If the list of node ids has changed, the scheduler will retrigger a scheduling.
/// If the indexing tasks do not match, the scheduler will apply again the last applied plan.
/// Concretely, it will send the faulty nodes of the plan they are supposed to follow.
//
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// plane will wait at least [`MIN_DURATION_BETWEEN_SCHEDULING`] before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster_id: String,
    self_node_id: NodeId,
    indexer_pool: IndexerPool,
    state: IndexingSchedulerState,
    pub(crate) next_rebuild_tracker: RebuildNotifier,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.self_node_id)
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}

fn enable_variable_shard_load() -> bool {
    static IS_SHARD_LOAD_CP_ENABLED: OnceCell<bool> = OnceCell::new();
    *IS_SHARD_LOAD_CP_ENABLED.get_or_init(|| {
        !quickwit_common::get_bool_from_env("QW_DISABLE_VARIABLE_SHARD_LOAD", false)
    })
}

/// Computes the CPU load associated to a single shard of a given index.
///
/// The array passed contains all of data we have about the shard of the index.
/// This function averages their statistics.
///
/// For the moment, this function only takes in account the measured throughput,
/// and assumes a constant CPU usage of 4 vCPU = 20mb/s.
///
/// It does not take in account the variation that could raise from the different
/// doc mapping / nature of the data, etc.
fn compute_load_per_shard(shard_entries: &[&ShardEntry]) -> NonZeroU32 {
    if enable_variable_shard_load() {
        let num_shards = shard_entries.len().max(1) as u64;
        let average_throughput_per_shard_bytes: u64 = shard_entries
            .iter()
            .map(|shard_entry| shard_entry.long_term_ingestion_rate.0 as u64 * bytesize::MIB)
            .sum::<u64>()
            .div_ceil(num_shards)
            // A shard throughput cannot exceed PIPELINE_THROUGHPUT in the long term (this is
            // enforced by the configuration).
            .min(PIPELINE_THROUGHTPUT.as_u64());
        let num_cpu_millis = (PIPELINE_FULL_CAPACITY.cpu_millis() as u64
            * average_throughput_per_shard_bytes)
            / PIPELINE_THROUGHTPUT.as_u64();
        const MIN_CPU_LOAD_PER_SHARD: u32 = 50u32;
        NonZeroU32::new((num_cpu_millis as u32).max(MIN_CPU_LOAD_PER_SHARD)).unwrap()
    } else {
        NonZeroU32::new(PIPELINE_FULL_CAPACITY.cpu_millis() / 4).unwrap()
    }
}

fn get_sources_to_schedule(model: &ControlPlaneModel) -> Vec<SourceToSchedule> {
    let mut sources = Vec::new();

    for (source_uid, source_config) in model.source_configs() {
        if !source_config.enabled {
            continue;
        }
        match source_config.source_type() {
            SourceType::Cli
            | SourceType::File
            | SourceType::Vec
            | SourceType::Void
            | SourceType::Unspecified => {
                // We don't need to schedule those.
            }
            SourceType::IngestV1 => {
                // TODO ingest v1 is scheduled differently
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::IngestV1,
                });
            }
            SourceType::IngestV2 => {
                // Expect: the source should exist since we just read it from `get_source_configs`.
                // Note that we keep all shards, including Closed shards:
                // A closed shards still needs to be indexed.
                let shard_entries: Vec<&ShardEntry> = model
                    .get_shards_for_source(&source_uid)
                    .expect("source should exist")
                    .values()
                    .collect();
                if shard_entries.is_empty() {
                    continue;
                }
                let shard_ids = shard_entries
                    .iter()
                    .map(|shard_entry| shard_entry.shard_id().clone())
                    .collect();
                let load_per_shard = compute_load_per_shard(&shard_entries[..]);
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::Sharded {
                        shard_ids,
                        load_per_shard,
                    },
                });
            }
            SourceType::Kafka
            | SourceType::Kinesis
            | SourceType::PubSub
            | SourceType::Nats
            | SourceType::Pulsar => {
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::NonSharded {
                        num_pipelines: source_config.num_pipelines.get() as u32,
                        // FIXME
                        load_per_pipeline: NonZeroU32::new(PIPELINE_FULL_CAPACITY.cpu_millis())
                            .unwrap(),
                    },
                });
            }
        }
    }
    sources
}

impl IndexingScheduler {
    pub fn new(cluster_id: String, self_node_id: NodeId, indexer_pool: IndexerPool) -> Self {
        IndexingScheduler {
            cluster_id,
            self_node_id,
            indexer_pool,
            state: IndexingSchedulerState::default(),
            next_rebuild_tracker: RebuildNotifier::default(),
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    // Should be called whenever a change in the list of index/shard
    // has happened.
    //
    // Prefer not calling this method directly, and instead call
    // `ControlPlane::rebuild_indexing_plan_debounced`.
    pub(crate) fn rebuild_plan(&mut self, model: &ControlPlaneModel) {
        crate::metrics::CONTROL_PLANE_METRICS.schedule_total.inc();

        let notify_on_drop = self.next_rebuild_tracker.start_rebuild();

        let sources = get_sources_to_schedule(model);

        let indexers: Vec<IndexerNodeInfo> = self.get_indexers_from_indexer_pool();

        let indexer_id_to_cpu_capacities: FnvHashMap<String, CpuCapacity> = indexers
            .iter()
            .filter_map(|indexer| {
                if indexer.indexing_capacity.cpu_millis() > 0 {
                    Some((indexer.node_id.to_string(), indexer.indexing_capacity))
                } else {
                    None
                }
            })
            .collect();

        if indexer_id_to_cpu_capacities.is_empty() {
            if !sources.is_empty() {
                warn!("no indexing capacity available, cannot schedule an indexing plan");
            }
            return;
        };

        let shard_locations = model.shard_locations();
        let new_physical_plan = build_physical_indexing_plan(
            &sources,
            &indexer_id_to_cpu_capacities,
            self.state.last_applied_physical_plan.as_ref(),
            &shard_locations,
        );
        let shard_locality_metrics =
            get_shard_locality_metrics(&new_physical_plan, &shard_locations);
        crate::metrics::CONTROL_PLANE_METRICS.set_shard_locality_metrics(shard_locality_metrics);
        if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
            let plans_diff = get_indexing_plans_diff(
                last_applied_plan.indexing_tasks_per_indexer(),
                new_physical_plan.indexing_tasks_per_indexer(),
            );
            // No need to apply the new plan as it is the same as the old one.
            if plans_diff.is_empty() {
                return;
            }
        }
        self.apply_physical_indexing_plan(&indexers, new_physical_plan, Some(notify_on_drop));
        self.state.num_schedule_indexing_plan += 1;
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) fn control_running_plan(&mut self, model: &ControlPlaneModel) {
        let last_applied_plan =
            if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.rebuild_plan(model);
                return;
            };
        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp {
            if Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
            {
                return;
            }
        }
        let indexers: Vec<IndexerNodeInfo> = self.get_indexers_from_indexer_pool();
        let running_indexing_tasks_by_node_id: FnvHashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.node_id.to_string(), indexer.indexing_tasks.clone()))
            .collect();

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_indexer(),
        );
        if !indexing_plans_diff.has_same_nodes() {
            info!(plans_diff=?indexing_plans_diff, "running plan and last applied plan node IDs differ: schedule an indexing plan");
            self.rebuild_plan(model);
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            info!(plans_diff=?indexing_plans_diff, "running tasks and last applied tasks differ: reapply last plan");
            self.apply_physical_indexing_plan(&indexers, last_applied_plan.clone(), None);
        }
    }

    fn get_indexers_from_indexer_pool(&self) -> Vec<IndexerNodeInfo> {
        self.indexer_pool.values()
    }

    fn apply_physical_indexing_plan(
        &mut self,
        indexers: &[IndexerNodeInfo],
        new_physical_plan: PhysicalIndexingPlan,
        notify_on_drop: Option<Arc<NotifyChangeOnDrop>>,
    ) {
        debug!(new_physical_plan=?new_physical_plan, "apply physical indexing plan");
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_indexer() {
            // We don't want to block on a slow indexer so we apply this change asynchronously
            // TODO not blocking is cool, but we need to make sure there is not accumulation
            // possible here.
            let notify_on_drop = notify_on_drop.clone();
            tokio::spawn({
                let indexer = indexers
                    .iter()
                    .find(|indexer| indexer.node_id == *node_id)
                    .expect("This should never happen as the plan was built from these indexers.")
                    .clone();
                let indexing_tasks = indexing_tasks.clone();
                async move {
                    if let Err(error) = indexer
                        .client
                        .clone()
                        .apply_indexing_plan(ApplyIndexingPlanRequest { indexing_tasks })
                        .await
                    {
                        warn!(
                            error=%error,
                            node_id=%indexer.node_id,
                            generation_id=indexer.generation_id,
                            "failed to apply indexing plan to indexer"
                        );
                    }
                    drop(notify_on_drop);
                }
            });
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }
}

struct IndexingPlansDiff<'a> {
    pub missing_node_ids: FnvHashSet<&'a str>,
    pub unplanned_node_ids: FnvHashSet<&'a str>,
    pub missing_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
    pub unplanned_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
}

impl<'a> IndexingPlansDiff<'a> {
    pub fn has_same_nodes(&self) -> bool {
        self.missing_node_ids.is_empty() && self.unplanned_node_ids.is_empty()
    }

    pub fn has_same_tasks(&self) -> bool {
        self.missing_tasks_by_node_id
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 0
            && self
                .unplanned_tasks_by_node_id
                .values()
                .map(Vec::len)
                .sum::<usize>()
                == 0
    }

    pub fn is_empty(&self) -> bool {
        self.has_same_nodes() && self.has_same_tasks()
    }
}

fn get_shard_locality_metrics(
    physical_plan: &PhysicalIndexingPlan,
    shard_locations: &ShardLocations,
) -> ShardLocalityMetrics {
    let mut num_local_shards = 0;
    let mut num_remote_shards = 0;
    for (indexer, tasks) in physical_plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                if shard_locations
                    .get_shard_locations(shard_id)
                    .iter()
                    .any(|node| node.as_str() == indexer)
                {
                    num_local_shards += 1;
                } else {
                    num_remote_shards += 1;
                }
            }
        }
    }
    ShardLocalityMetrics {
        num_remote_shards,
        num_local_shards,
    }
}

impl<'a> fmt::Debug for IndexingPlansDiff<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_same_nodes() && self.has_same_tasks() {
            return write!(formatter, "EmptyIndexingPlansDiff");
        }
        write!(formatter, "IndexingPlansDiff(")?;
        let mut separator = "";
        if !self.missing_node_ids.is_empty() {
            write!(
                formatter,
                "missing_node_ids={:?}",
                PrettySample::new(&self.missing_node_ids, 10)
            )?;
            separator = ", "
        }
        if !self.unplanned_node_ids.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_node_ids={:?}",
                PrettySample::new(&self.unplanned_node_ids, 10)
            )?;
            separator = ", "
        }
        if !self.missing_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}missing_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.missing_tasks_by_node_id)?;
            separator = ", "
        }
        if !self.unplanned_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}unplanned_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.unplanned_tasks_by_node_id)?;
        }
        write!(formatter, ")")
    }
}

fn format_indexing_task_map(
    formatter: &mut std::fmt::Formatter,
    indexing_tasks: &FnvHashMap<&str, Vec<&IndexingTask>>,
) -> std::fmt::Result {
    // we show at most 5 nodes, and aggregate the results for the other.
    // we show at most 10 indexes, but aggregate results after.
    // we always aggregate shard ids
    // we hide pipeline id and incarnation id, they are not very useful in most case, but take a
    // lot of place
    const MAX_NODE: usize = 5;
    const MAX_INDEXES: usize = 10;
    let mut index_displayed = 0;
    write!(formatter, "{{")?;
    let mut indexer_iter = indexing_tasks.iter().enumerate();
    for (i, (index_name, tasks)) in &mut indexer_iter {
        if i != 0 {
            write!(formatter, ", ")?;
        }
        if index_displayed != MAX_INDEXES - 1 {
            write!(formatter, "{index_name:?}: [")?;
            let mut tasks_iter = tasks.iter().enumerate();
            for (i, task) in &mut tasks_iter {
                if i != 0 {
                    write!(formatter, ", ")?;
                }
                write!(
                    formatter,
                    r#"(index_id: "{}", source_id: "{}", shard_count: {})"#,
                    task.index_uid.as_ref().unwrap().index_id,
                    task.source_id,
                    task.shard_ids.len()
                )?;
                index_displayed += 1;
                if index_displayed == MAX_INDEXES - 1 {
                    let (task_count, shard_count) = tasks_iter.fold((0, 0), |(t, s), (_, task)| {
                        (t + 1, s + task.shard_ids.len())
                    });
                    if task_count > 0 {
                        write!(
                            formatter,
                            " and {task_count} tasks and {shard_count} shards"
                        )?;
                    }
                    break;
                }
            }
            write!(formatter, "]")?;
        } else {
            write!(
                formatter,
                "{index_name:?}: [with {} tasks and {} shards]",
                tasks.len(),
                tasks.iter().map(|task| task.shard_ids.len()).sum::<usize>()
            )?;
        }
        if i == MAX_NODE - 1 {
            break;
        }
    }
    let (indexer, tasks, shards) = indexer_iter.fold((0, 0, 0), |(i, t, s), (_, (_, task))| {
        (
            i + 1,
            t + task.len(),
            s + task.iter().map(|task| task.shard_ids.len()).sum::<usize>(),
        )
    });
    if indexer > 0 {
        write!(
            formatter,
            " and {indexer} more indexers, handling {tasks} tasks and {shards} shards}}"
        )
    } else {
        write!(formatter, "}}")
    }
}

/// Returns the difference between the `running_plan` retrieved from the chitchat state and
/// the last plan applied by the scheduler.
fn get_indexing_plans_diff<'a>(
    running_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
    last_applied_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
) -> IndexingPlansDiff<'a> {
    // Nodes diff.
    let running_node_ids: FnvHashSet<&str> = running_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let planned_node_ids: FnvHashSet<&str> = last_applied_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let missing_node_ids: FnvHashSet<&str> = planned_node_ids
        .difference(&running_node_ids)
        .copied()
        .collect();
    let unplanned_node_ids: FnvHashSet<&str> = running_node_ids
        .difference(&planned_node_ids)
        .copied()
        .collect();
    // Tasks diff.
    let mut missing_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> = FnvHashMap::default();
    let mut unplanned_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> =
        FnvHashMap::default();
    for node_id in running_node_ids.iter().chain(planned_node_ids.iter()) {
        let running_tasks = running_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let last_applied_tasks = last_applied_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let (missing_tasks, unplanned_tasks) =
            get_indexing_tasks_diff(running_tasks, last_applied_tasks);
        missing_tasks_by_node_id.insert(*node_id, missing_tasks);
        unplanned_tasks_by_node_id.insert(*node_id, unplanned_tasks);
    }
    IndexingPlansDiff {
        missing_node_ids,
        unplanned_node_ids,
        missing_tasks_by_node_id,
        unplanned_tasks_by_node_id,
    }
}

/// Computes the difference between `running_tasks` and `last_applied_tasks` and returns a tuple
/// of `missing_tasks` and `unplanned_tasks`.
/// Note: we need to handle duplicate tasks in each array, so we count them and make the diff.
fn get_indexing_tasks_diff<'a>(
    running_tasks: &'a [IndexingTask],
    last_applied_tasks: &'a [IndexingTask],
) -> (Vec<&'a IndexingTask>, Vec<&'a IndexingTask>) {
    let mut missing_tasks: Vec<&IndexingTask> = Vec::new();
    let mut unplanned_tasks: Vec<&IndexingTask> = Vec::new();
    let grouped_running_tasks: FnvHashMap<&IndexingTask, usize> = running_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: FnvHashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let all_tasks: FnvHashSet<&IndexingTask> =
        FnvHashSet::from_iter(running_tasks.iter().chain(last_applied_tasks.iter()));
    for task in all_tasks {
        let running_task_count = grouped_running_tasks.get(task).unwrap_or(&0);
        let desired_task_count = grouped_last_applied_tasks.get(task).unwrap_or(&0);
        match running_task_count.cmp(desired_task_count) {
            Ordering::Greater => {
                unplanned_tasks
                    .extend_from_slice(&vec![task; running_task_count - desired_task_count]);
            }
            Ordering::Less => {
                missing_tasks
                    .extend_from_slice(&vec![task; desired_task_count - running_task_count])
            }
            _ => {}
        }
    }

    (missing_tasks, unplanned_tasks)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use proptest::{prop_compose, proptest};
    use quickwit_config::{IndexConfig, KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};

    use super::*;
    use crate::model::ShardLocations;
    #[test]
    fn test_indexing_plans_diff() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let index_uid2 = IndexUid::from_str("index-2:11111111111111111111111111").unwrap();
        {
            let running_plan = FnvHashMap::default();
            let desired_plan = FnvHashMap::default();
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(20u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1b.clone(), task_2.clone()],
            );
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_2, task_1.clone(), task_1b.clone()],
            );
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_2.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1])])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_2])])
            );
        }
        {
            // Task assigned to indexer-1 in desired plan but another one running.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid2.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-2".to_string(), vec![task_2.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(!indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_node_ids,
                FnvHashSet::from_iter(["indexer-1"])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_node_ids,
                FnvHashSet::from_iter(["indexer-2"])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1]), ("indexer-2", Vec::new())])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-2", vec![&task_2]), ("indexer-1", Vec::new())])
            );
        }
        {
            // Diff with 3 same tasks running but only one on the desired plan.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1a = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1c = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(12u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1a.clone()]);
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_1a.clone(), task_1b.clone(), task_1c.clone()],
            );

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1b, &task_1c])])
            );
        }
    }

    #[test]
    fn test_get_sources_to_schedule() {
        let mut model = ControlPlaneModel::default();
        let kafka_source_params = KafkaSourceParams {
            topic: "kafka-topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({}),
            enable_backfill_mode: false,
        };
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_disabled".to_string(),
                    num_pipelines: NonZeroUsize::new(3).unwrap(),
                    enabled: false,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_enabled".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v1".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestApi,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        // ingest v2 without any open shard is skipped.
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2_without_shard".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_cli".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestCli,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "ingest_v2".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        model.insert_shards(&index_uid, &"ingest_v2".to_string(), vec![shard]);
        let shards: Vec<SourceToSchedule> = get_sources_to_schedule(&model);
        assert_eq!(shards.len(), 3);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() {
        let source_1 = SourceUid {
            index_uid: IndexUid::for_test("index-1", 0),
            source_id: "source1".to_string(),
        };
        let source_2 = SourceUid {
            index_uid: IndexUid::for_test("index-2", 0),
            source_id: "source2".to_string(),
        };
        let sources = vec![
            SourceToSchedule {
                source_uid: source_1.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 3,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
            },
            SourceToSchedule {
                source_uid: source_2.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 2,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
            },
        ];
        let mut indexer_max_loads = FnvHashMap::default();
        indexer_max_loads.insert("indexer1".to_string(), mcpu(3_000));
        indexer_max_loads.insert("indexer2".to_string(), mcpu(3_000));
        let shard_locations = ShardLocations::default();
        let physical_plan =
            build_physical_indexing_plan(&sources[..], &indexer_max_loads, None, &shard_locations);
        assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 2);
        let indexing_tasks_1 = physical_plan.indexer("indexer1").unwrap();
        assert_eq!(indexing_tasks_1.len(), 2);
        let indexer_2_tasks = physical_plan.indexer("indexer2").unwrap();
        assert_eq!(indexer_2_tasks.len(), 3);
    }

    #[test]
    fn test_debug_indexing_task_map() {
        let mut map = FnvHashMap::default();
        let task1 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index1", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard1".into()],
        };
        let task2 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index2", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard2".into(), "shard3".into()],
        };
        let task3 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index3", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard6".into()],
        };
        // order made to map with the debug for lisibility
        map.insert("indexer5", vec![&task2]);
        map.insert("indexer4", vec![&task1]);
        map.insert("indexer3", vec![&task1, &task3]);
        map.insert("indexer2", vec![&task2, &task3, &task1, &task2]);
        map.insert("indexer1", vec![&task1, &task2, &task3, &task1]);
        map.insert("indexer6", vec![&task1, &task2, &task3]);
        let plan = IndexingPlansDiff {
            missing_node_ids: FnvHashSet::default(),
            unplanned_node_ids: FnvHashSet::default(),
            missing_tasks_by_node_id: map,
            unplanned_tasks_by_node_id: FnvHashMap::default(),
        };

        let debug = format!("{plan:?}");
        assert_eq!(
            debug,
            r#"IndexingPlansDiff(missing_tasks_by_node_id={"indexer5": [(index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer4": [(index_id: "index1", source_id: "my-source", shard_count: 1)], "indexer3": [(index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index3", source_id: "my-source", shard_count: 1)], "indexer2": [(index_id: "index2", source_id: "my-source", shard_count: 2), (index_id: "index3", source_id: "my-source", shard_count: 1), (index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer1": [(index_id: "index1", source_id: "my-source", shard_count: 1) and 3 tasks and 4 shards] and 1 more indexers, handling 3 tasks and 4 shards})"#
        );
    }

    proptest! {
        #[test]
        fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
            let index_uids: fnv::FnvHashSet<IndexUid> =
                index_id_sources.iter()
                    .map(|(index_uid, _)| index_uid.clone())
                    .collect();
            let mut model = ControlPlaneModel::default();
            for index_uid in index_uids {
                let index_config = IndexConfig::for_test(&index_uid.index_id, &format!("ram://test/{index_uid}"));
                model.add_index(IndexMetadata::new_with_index_uid(index_uid, index_config));
            }
            for (index_uid, source_config) in &index_id_sources {
                model.add_source(index_uid, source_config.clone()).unwrap();
            }

            let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model);
            let mut indexer_max_loads = FnvHashMap::default();
            for i in 0..num_indexers {
                let indexer_id = format!("indexer-{i}");
                indexer_max_loads.insert(indexer_id, mcpu(4_000));
            }
            let shard_locations = ShardLocations::default();
            let _physical_indexing_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None, &shard_locations);
        }
    }

    use quickwit_config::SourceInputFormat;
    use quickwit_proto::indexing::mcpu;
    use quickwit_proto::ingest::{Shard, ShardState};

    fn kafka_source_params_for_test() -> SourceParams {
        SourceParams::Kafka(KafkaSourceParams {
            topic: "topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({
                "bootstrap.servers": "localhost:9092",
            }),
            enable_backfill_mode: true,
        })
    }

    prop_compose! {
      fn gen_kafka_source()
        (index_idx in 0usize..100usize, num_pipelines in 1usize..51usize) -> (IndexUid, SourceConfig) {
          let index_uid = IndexUid::for_test(&format!("index-id-{index_idx}"), 0 /* this is the index uid */);
          let source_id = quickwit_common::rand::append_random_suffix("kafka-source");
          (index_uid, SourceConfig {
              source_id,
              num_pipelines: NonZeroUsize::new(num_pipelines).unwrap(),
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
              input_format: SourceInputFormat::Json,
          })
      }
    }
}
