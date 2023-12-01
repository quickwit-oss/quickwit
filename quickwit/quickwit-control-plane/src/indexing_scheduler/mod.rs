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

mod scheduling;

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, CpuCapacity, IndexingService, IndexingTask, PIPELINE_FULL_CAPACITY,
};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{NodeId, ShardId};
use scheduling::{SourceToSchedule, SourceToScheduleType};
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling::build_physical_indexing_plan;
use crate::model::ControlPlaneModel;
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

fn get_sources_to_schedule(model: &ControlPlaneModel) -> Vec<SourceToSchedule> {
    let mut sources = Vec::new();

    for (source_uid, source_config) in model.get_source_configs() {
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
                let shard_ids: Vec<ShardId> = model
                    .list_shards(&source_uid)
                    .expect("source should exist")
                    .map(|shard| shard.shard_id)
                    .collect();

                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::Sharded {
                        shard_ids,
                        // FIXME
                        load_per_shard: NonZeroU32::new(250u32).unwrap(),
                    },
                });
            }
            SourceType::Kafka
            | SourceType::Kinesis
            | SourceType::GcpPubsub
            | SourceType::Nats
            | SourceType::Pulsar => {
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::NonSharded {
                        num_pipelines: source_config.desired_num_pipelines.get() as u32,
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
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    // Should be called whenever a change in the list of index/shard
    // has happened.
    pub(crate) fn schedule_indexing_plan_if_needed(&mut self, model: &ControlPlaneModel) {
        crate::metrics::CONTROL_PLANE_METRICS.schedule_total.inc();
        let mut indexers: Vec<(String, IndexerNodeInfo)> = self.get_indexers_from_indexer_pool();
        if indexers.is_empty() {
            warn!("no indexer available, cannot schedule an indexing plan");
            return;
        };

        let sources = get_sources_to_schedule(model);

        let indexer_id_to_cpu_capacities: FnvHashMap<String, CpuCapacity> = indexers
            .iter()
            .map(|(indexer_id, indexer_node_info)| {
                (indexer_id.to_string(), indexer_node_info.indexing_capacity)
            })
            .collect();

        let new_physical_plan = build_physical_indexing_plan(
            &sources,
            &indexer_id_to_cpu_capacities,
            self.state.last_applied_physical_plan.as_ref(),
        );
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
        self.apply_physical_indexing_plan(&mut indexers, new_physical_plan);
        self.state.num_schedule_indexing_plan += 1;
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) fn control_running_plan(&mut self, model: &ControlPlaneModel) {
        let last_applied_plan =
            if let Some(last_applied_plan) = self.state.last_applied_physical_plan.as_ref() {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.schedule_indexing_plan_if_needed(model);
                return;
            };

        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp {
            if Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
            {
                return;
            }
        }

        let mut indexers = self.get_indexers_from_indexer_pool();
        let running_indexing_tasks_by_node_id: FnvHashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.0.clone(), indexer.1.indexing_tasks.clone()))
            .collect();

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_indexer(),
        );
        if !indexing_plans_diff.has_same_nodes() {
            info!(plans_diff=?indexing_plans_diff, "running plan and last applied plan node IDs differ: schedule an indexing plan");
            self.schedule_indexing_plan_if_needed(model);
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            info!(plans_diff=?indexing_plans_diff, "running tasks and last applied tasks differ: reapply last plan");
            self.apply_physical_indexing_plan(&mut indexers, last_applied_plan.clone());
        }
    }

    fn get_indexers_from_indexer_pool(&self) -> Vec<(String, IndexerNodeInfo)> {
        self.indexer_pool.pairs()
    }

    fn apply_physical_indexing_plan(
        &mut self,
        indexers: &mut [(String, IndexerNodeInfo)],
        new_physical_plan: PhysicalIndexingPlan,
    ) {
        debug!(new_physical_plan=?new_physical_plan, "apply physical indexing plan");
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_indexer() {
            // We don't want to block on a slow indexer so we apply this change asynchronously
            // TODO not blocking is cool, but we need to make sure there is not accumulation
            // possible here.
            tokio::spawn({
                let indexer = indexers
                    .iter()
                    .find(|indexer| &indexer.0 == node_id)
                    .expect("This should never happen as the plan was built from these indexers.")
                    .clone();
                let indexing_tasks = indexing_tasks.clone();
                async move {
                    if let Err(error) = indexer
                        .1
                        .client
                        .clone()
                        .apply_indexing_plan(ApplyIndexingPlanRequest { indexing_tasks })
                        .await
                    {
                        error!(indexer_node_id=%indexer.0, err=?error, "error occurred when applying indexing plan to indexer");
                    }
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

impl<'a> fmt::Debug for IndexingPlansDiff<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_same_nodes() && self.has_same_tasks() {
            return write!(formatter, "EmptyIndexingPlanDiff");
        }
        write!(formatter, "IndexingPlanDiff(")?;
        let mut separator = "";
        if !self.missing_node_ids.is_empty() {
            write!(formatter, "missing_node_ids={:?}, ", self.missing_node_ids)?;
            separator = ", "
        }
        if !self.unplanned_node_ids.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_node_ids={:?}",
                self.unplanned_node_ids
            )?;
            separator = ", "
        }
        if !self.missing_tasks_by_node_id.is_empty() {
            write!(
                formatter,
                "{separator}missing_tasks_by_node_id={:?}, ",
                self.missing_tasks_by_node_id
            )?;
            separator = ", "
        }
        if !self.unplanned_tasks_by_node_id.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_tasks_by_node_id={:?}",
                self.unplanned_tasks_by_node_id
            )?;
        }
        write!(formatter, ")")
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
        .group_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: FnvHashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .group_by(|&task| task)
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

    use proptest::{prop_compose, proptest};
    use quickwit_config::{IndexConfig, KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::types::{IndexUid, PipelineUid, SourceUid};

    use super::*;
    #[test]
    fn test_indexing_plans_diff() {
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
                pipeline_uid: Some(PipelineUid::from_u128(10u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(11u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(20u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
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
                pipeline_uid: Some(PipelineUid::from_u128(1u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(2u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
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
                pipeline_uid: Some(PipelineUid::from_u128(1u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(2u128)),
                index_uid: "index-2:11111111111111111111111111".to_string(),
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
                pipeline_uid: Some(PipelineUid::from_u128(10u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(11u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_1c = IndexingTask {
                pipeline_uid: Some(PipelineUid::from_u128(12u128)),
                index_uid: "index-1:11111111111111111111111111".to_string(),
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
                    max_num_pipelines_per_indexer: NonZeroUsize::new(3).unwrap(),
                    desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
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
                    max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
                    desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
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
                    max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
                    desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
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
                    max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
                    desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
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
                    max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
                    desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestCli,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        let shards: Vec<SourceToSchedule> = get_sources_to_schedule(&model);
        assert_eq!(shards.len(), 3);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() {
        let source_1 = SourceUid {
            index_uid: IndexUid::from_parts("index-1", "000"),
            source_id: "source1".to_string(),
        };
        let source_2 = SourceUid {
            index_uid: IndexUid::from_parts("index-2", "000"),
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
        let physical_plan = build_physical_indexing_plan(&sources[..], &indexer_max_loads, None);
        assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 2);
        let indexing_tasks_1 = physical_plan.indexer("indexer1").unwrap();
        assert_eq!(indexing_tasks_1.len(), 2);
        let indexer_2_tasks = physical_plan.indexer("indexer2").unwrap();
        assert_eq!(indexer_2_tasks.len(), 3);
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
                let index_config = IndexConfig::for_test(index_uid.index_id(), &format!("ram://test/{index_uid}"));
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
            let physical_indexing_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None);
            let source_map: FnvHashMap<&SourceUid, &SourceToSchedule> = sources
                .iter()
                .map(|source| (&source.source_uid, source))
                .collect();
            for (node_id, tasks) in physical_indexing_plan.indexing_tasks_per_indexer() {
                let mut load_in_node = 0u32;
                for task in tasks {
                    let source_uid = SourceUid {
                        index_uid: IndexUid::from(task.index_uid.clone()),
                        source_id: task.source_id.clone(),
                    };
                    let source_to_schedule = source_map.get(&source_uid).unwrap();
                    match &source_to_schedule.source_type {
                        SourceToScheduleType::IngestV1 => {}
                        SourceToScheduleType::Sharded {
                            load_per_shard,
                            ..
                        } => {
                            load_in_node += load_per_shard.get() * task.shard_ids.len() as u32;
                        }
                        SourceToScheduleType::NonSharded {
                            num_pipelines: _ ,
                            load_per_pipeline,
                        } => {
                            load_in_node += load_per_pipeline.get();
                        }
                    }
                }
                assert!(load_in_node <= indexer_max_loads.get(node_id).unwrap().cpu_millis());
            }
        }
    }

    use quickwit_config::SourceInputFormat;
    use quickwit_proto::indexing::mcpu;

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
        (index_idx in 0usize..100usize, desired_num_pipelines in 1usize..51usize, max_num_pipelines_per_indexer in 1usize..5usize) -> (IndexUid, SourceConfig) {
          let index_uid = IndexUid::from_parts(&format!("index-id-{index_idx}"), "" /* this is the index uid */);
          let source_id = quickwit_common::rand::append_random_suffix("kafka-source");
          (index_uid, SourceConfig {
              source_id,
              desired_num_pipelines: NonZeroUsize::new(desired_num_pipelines).unwrap(),
              max_num_pipelines_per_indexer: NonZeroUsize::new(max_num_pipelines_per_indexer).unwrap(),
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
              input_format: SourceInputFormat::Json,
          })
      }
    }
}
