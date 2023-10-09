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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use itertools::Itertools;
use quickwit_config::{SourceConfig, INGEST_SOURCE_ID};
use quickwit_metastore::{ListIndexesQuery, Metastore};
use quickwit_proto::indexing::{ApplyIndexingPlanRequest, IndexingService, IndexingTask};
use quickwit_proto::metastore::{ListShardsRequest, ListShardsSubrequest};
use quickwit_proto::{NodeId, ShardId};
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::indexing_plan::{
    build_indexing_plan, build_physical_indexing_plan, PhysicalIndexingPlan, SourceUid,
};
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

/// The [`IndexingScheduler`] is responsible for scheduling indexing tasks to indexers.
/// The scheduling executes the following steps:
/// 1. Fetches all indexes metadata.
/// 2. Builds an indexing plan = `[Vec<IndexingTask>]`, from the indexes metadatas. See
///    [`build_indexing_plan`] for the implementation details.
/// 3. Builds a [`PhysicalIndexingPlan`] from the list of indexing tasks. See
///    [`build_physical_indexing_plan`] for the implementation details.
/// 4. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan. Any
///    errors (network) happening in this step are ignored. The scheduler runs a control loop that
///    regularly checks if indexers are effectively running their plans (more details in the next
///    section).
///
/// All events altering the list of indexes and sources are proxied through
/// through the control plane. The control plane state is therefore guaranteed to be up-to-date
/// (at the cost of making the control plane a single point of failure).
///
/// They then trigger the production of a new `PhysicalIndexingPlan`.
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
/// plase will wait at least [`MIN_DURATION_BETWEEN_SCHEDULING`] before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster_id: String,
    self_node_id: NodeId,
    metastore: Arc<dyn Metastore>,
    indexer_pool: IndexerPool,
    state: IndexingSchedulerState,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.self_node_id)
            .field("metastore_uri", &self.metastore.uri())
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}

impl IndexingScheduler {
    pub fn new(
        cluster_id: String,
        self_node_id: NodeId,
        metastore: Arc<dyn Metastore>,
        indexer_pool: IndexerPool,
    ) -> Self {
        Self {
            cluster_id,
            self_node_id,
            metastore,
            indexer_pool,
            state: IndexingSchedulerState::default(),
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    pub(crate) async fn schedule_indexing_plan_if_needed(&mut self) -> anyhow::Result<()> {
        let mut indexers = self.get_indexers_from_indexer_pool();
        if indexers.is_empty() {
            warn!("No indexer available, cannot schedule an indexing plan.");
            return Ok(());
        };
        let source_configs: HashMap<SourceUid, SourceConfig> = self.fetch_source_configs().await?;
        let mut indexing_tasks = build_indexing_plan(&source_configs, indexers.len());

        // TODO: This is a temporary hack to always create an indexing pipeline for the shards that
        // exist.
        let shards = self.fetch_shards(&source_configs).await?;
        for (source_uid, shard_ids) in shards {
            indexing_tasks.push(IndexingTask {
                index_uid: source_uid.index_uid.into(),
                source_id: source_uid.source_id.clone(),
                shard_ids,
            });
        }
        let new_physical_plan =
            build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks);
        if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
            let plans_diff = get_indexing_plans_diff(
                last_applied_plan.indexing_tasks_per_node(),
                new_physical_plan.indexing_tasks_per_node(),
            );
            // No need to apply the new plan as it is the same as the old one.
            if plans_diff.is_empty() {
                return Ok(());
            }
        }
        self.apply_physical_indexing_plan(&mut indexers, new_physical_plan)
            .await;
        self.state.num_schedule_indexing_plan += 1;
        Ok(())
    }

    async fn fetch_source_configs(&self) -> anyhow::Result<HashMap<SourceUid, SourceConfig>> {
        let indexes_metadatas = self
            .metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await?;
        let source_configs: HashMap<SourceUid, SourceConfig> = indexes_metadatas
            .into_iter()
            .flat_map(|index_metadata| {
                index_metadata
                    .sources
                    .into_iter()
                    .map(move |(source_id, source_config)| {
                        (
                            SourceUid {
                                index_uid: index_metadata.index_uid.clone(),
                                source_id,
                            },
                            source_config,
                        )
                    })
            })
            .collect();
        Ok(source_configs)
    }

    async fn fetch_shards(
        &self,
        source_configs: &HashMap<SourceUid, SourceConfig>,
    ) -> anyhow::Result<HashMap<SourceUid, Vec<ShardId>>> {
        let mut list_shards_subrequests = Vec::new();

        for (source_uid, source_config) in source_configs {
            if source_uid.source_id != INGEST_SOURCE_ID || !source_config.enabled {
                continue;
            }
            // TODO: More precisely, we want the shards that are open or closing or such that
            // `shard.publish_position_inclusive`
            // < `shard.replication_position_inclusive`.
            let list_shard_subrequest = ListShardsSubrequest {
                index_uid: source_uid.index_uid.clone().into(),
                source_id: source_uid.source_id.clone(),
                shard_state: None,
            };
            list_shards_subrequests.push(list_shard_subrequest);
        }
        let list_shards_request = ListShardsRequest {
            subrequests: list_shards_subrequests,
        };
        let list_shards_response = self
            .metastore
            .list_shards(list_shards_request)
            .await
            .context("failed to list shards from metastore")?;

        let mut shards = HashMap::new();

        for list_shards_subresponse in list_shards_response.subresponses {
            let source_uid = SourceUid {
                index_uid: list_shards_subresponse.index_uid.into(),
                source_id: list_shards_subresponse.source_id,
            };
            if list_shards_subresponse.shards.is_empty() {
                continue;
            }
            let shard_ids = list_shards_subresponse
                .shards
                .into_iter()
                .filter(|shard| shard.is_indexable())
                .map(|shard| shard.shard_id)
                .collect();
            shards.insert(source_uid, shard_ids);
        }
        Ok(shards)
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) async fn control_running_plan(&mut self) -> anyhow::Result<()> {
        let last_applied_plan =
            if let Some(last_applied_plan) = self.state.last_applied_physical_plan.as_ref() {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.schedule_indexing_plan_if_needed().await?;
                return Ok(());
            };

        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp {
            if Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
            {
                return Ok(());
            }
        }

        let mut indexers = self.get_indexers_from_indexer_pool();
        let running_indexing_tasks_by_node_id: HashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.0.clone(), indexer.1.indexing_tasks.clone()))
            .collect();

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_node(),
        );
        if !indexing_plans_diff.has_same_nodes() {
            info!(plans_diff=?indexing_plans_diff, "Running plan and last applied plan node IDs differ: schedule an indexing plan.");
            self.schedule_indexing_plan_if_needed().await?;
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            info!(plans_diff=?indexing_plans_diff, "Running tasks and last applied tasks differ: reapply last plan.");
            self.apply_physical_indexing_plan(&mut indexers, last_applied_plan.clone())
                .await;
        }
        Ok(())
    }

    fn get_indexers_from_indexer_pool(&self) -> Vec<(String, IndexerNodeInfo)> {
        self.indexer_pool.pairs()
    }

    async fn apply_physical_indexing_plan(
        &mut self,
        indexers: &mut [(String, IndexerNodeInfo)],
        new_physical_plan: PhysicalIndexingPlan,
    ) {
        debug!("Apply physical indexing plan: {:?}", new_physical_plan);
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_node() {
            // We don't want to block on a slow indexer so we apply this change asynchronously
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
                        error!(indexer_node_id=%indexer.0, err=?error, "Error occurred when applying indexing plan to indexer.");
                    }
                }
            });
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }
}

impl IndexingScheduler {
    // Should be called whenever a change in the list of index/shard
    // has happened
    pub(crate) async fn on_index_change(&mut self) -> anyhow::Result<()> {
        self.schedule_indexing_plan_if_needed()
            .await
            .context("error when scheduling indexing plan")?;
        Ok(())
    }
}

struct IndexingPlansDiff<'a> {
    pub missing_node_ids: HashSet<&'a str>,
    pub unplanned_node_ids: HashSet<&'a str>,
    pub missing_tasks_by_node_id: HashMap<&'a str, Vec<&'a IndexingTask>>,
    pub unplanned_tasks_by_node_id: HashMap<&'a str, Vec<&'a IndexingTask>>,
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
    running_plan: &'a HashMap<String, Vec<IndexingTask>>,
    last_applied_plan: &'a HashMap<String, Vec<IndexingTask>>,
) -> IndexingPlansDiff<'a> {
    // Nodes diff.
    let running_node_ids: HashSet<&str> = running_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let planned_node_ids: HashSet<&str> = last_applied_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let missing_node_ids: HashSet<&str> = planned_node_ids
        .difference(&running_node_ids)
        .copied()
        .collect();
    let unplanned_node_ids: HashSet<&str> = running_node_ids
        .difference(&planned_node_ids)
        .copied()
        .collect();
    // Tasks diff.
    let mut missing_tasks_by_node_id: HashMap<&str, Vec<&IndexingTask>> = HashMap::new();
    let mut unplanned_tasks_by_node_id: HashMap<&str, Vec<&IndexingTask>> = HashMap::new();
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
    let grouped_running_tasks: HashMap<&IndexingTask, usize> = running_tasks
        .iter()
        .group_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: HashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .group_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let all_tasks: HashSet<&IndexingTask> =
        HashSet::from_iter(running_tasks.iter().chain(last_applied_tasks.iter()));
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
    use super::*;

    #[test]
    fn test_indexing_plans_diff() {
        {
            let running_plan = HashMap::new();
            let desired_plan = HashMap::new();
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_2.clone()],
            );
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_2, task_1.clone(), task_1],
            );
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
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
                HashMap::from_iter([("indexer-1", vec![&task_1])])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                HashMap::from_iter([("indexer-1", vec![&task_2])])
            );
        }
        {
            // Task assigned to indexer-1 in desired plan but another one running.
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
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
                HashSet::from_iter(["indexer-1"])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_node_ids,
                HashSet::from_iter(["indexer-2"])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                HashMap::from_iter([("indexer-1", vec![&task_1]), ("indexer-2", Vec::new())])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                HashMap::from_iter([("indexer-2", vec![&task_2]), ("indexer-1", Vec::new())])
            );
        }
        {
            // Diff with 3 same tasks running but only one on the desired plan.
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_1.clone()],
            );

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                HashMap::from_iter([("indexer-1", vec![&task_1, &task_1])])
            );
        }
        {
            // Diff with 3 same tasks on desired plan but only one running.
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_1.clone()],
            );
            desired_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                HashMap::from_iter([("indexer-1", vec![&task_1, &task_1])])
            );
        }
    }
}
