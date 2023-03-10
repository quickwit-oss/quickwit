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
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, HEARTBEAT};
use quickwit_cluster::{Cluster, ClusterMember};
use quickwit_config::service::QuickwitService;
use quickwit_config::SourceConfig;
use quickwit_grpc_clients::service_client_pool::ServiceClientPool;
use quickwit_indexing::indexing_client::IndexingServiceClient;
use quickwit_metastore::Metastore;
use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, IndexingTask};
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::indexing_plan::{
    build_indexing_plan, build_physical_indexing_plan, IndexSourceId, PhysicalIndexingPlan,
};
use crate::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};

const REFRESH_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_secs(3)
} else {
    Duration::from_secs(60 * 5)
};

const MIN_DURATION_BETWEEN_SCHEDULING: Duration = if cfg!(any(test, feature = "testsuite")) {
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
/// 2. Builds an indexing plan = `[Vec<IndexingTask>]`, from the indexes metadatas.
///    See [`build_indexing_plan`] for the implementation details.
/// 3. Builds a [`PhysicalIndexingPlan`] from the list of indexing tasks.
///    See [`build_physical_indexing_plan`] for the implementation details.
/// 4. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan.
///    Any errors (network) happening in this step are ignored. The scheduler runs a control loop
///    that regularly checks if indexers are effectively running their plans (more details in the
///    next section).
///
/// The scheduling is executed when the scheduler receives external or internal events and on
/// certains conditions. The following events possibly trigger a scheduling:
/// - [`NotifyIndexChangeRequest`]: this gRPC event is sent by a metastore node and will trigger a
///   scheduling on each event. TODO(fmassot): this can be refined by adding some relevant info to
///   the event, example: the creation of a source of type `void` should not trigger a scheduling.
/// - [`RefreshPlanLoop`]: this event is scheduled every [`REFRESH_PLAN_LOOP_INTERVAL`] and triggers
///   a scheduling. Due to network issues, a control plane will not always receive the gRPC events
///   [`NotifyIndexChangeRequest`] and thus will not be aware of index changes in the metastore.
///   TODO(fmassot): to avoid a scheduling on each [`RefreshPlanLoop`], we can store in the
///   scheduler state a metastore version number that will be compared to the number stored in the
///   metastore itself.
/// - [`ControlPlanLoop`]: this event is scheduled every [`HEARTBEAT`] and control if the `desired
///   plan`, that is the last applied [`PhysicalIndexingPlan`] by the scheduler, and the `running
///   plan`, that is the indexing tasks running on all indexers and retrieved from the chitchat
///   state, are the same:
///   - if node IDs are different, the scheduler will trigger a scheduling.
///   - if indexing tasks are different, the scheduler will apply again the last applied plan.
///
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// phase will wait at least [`MIN_DURATION_BETWEEN_SCHEDULING`] before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
    indexing_client_pool: ServiceClientPool<IndexingServiceClient>,
    state: IndexingSchedulerState,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster.cluster_id)
            .field("metastore_uri", &self.metastore.uri())
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}

#[async_trait]
impl Actor for IndexingScheduler {
    type ObservableState = IndexingSchedulerState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    fn name(&self) -> String {
        "IndexingScheduler".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(RefreshPlanLoop, ctx).await?;
        ctx.schedule_self_msg(HEARTBEAT, ControlPlanLoop).await;
        Ok(())
    }
}

impl IndexingScheduler {
    pub fn new(
        cluster: Arc<Cluster>,
        metastore: Arc<dyn Metastore>,
        indexing_client_pool: ServiceClientPool<IndexingServiceClient>,
    ) -> Self {
        Self {
            cluster,
            metastore,
            indexing_client_pool,
            state: IndexingSchedulerState::default(),
        }
    }

    async fn schedule_indexing_plan_if_needed(&mut self) -> anyhow::Result<()> {
        let indexers: Vec<ClusterMember> = self.get_indexers_from_cluster_state().await;
        if indexers.is_empty() {
            warn!("No indexer available, cannot schedule an indexing plan.");
            return Ok(());
        };
        let source_configs: HashMap<IndexSourceId, SourceConfig> =
            self.fetch_source_configs().await?;
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs);
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
        self.apply_physical_indexing_plan(&indexers, new_physical_plan)
            .await;
        self.state.num_schedule_indexing_plan += 1;
        Ok(())
    }

    async fn fetch_source_configs(&self) -> anyhow::Result<HashMap<IndexSourceId, SourceConfig>> {
        let indexes_metadatas = self.metastore.list_indexes_metadatas().await?;
        let source_configs: HashMap<IndexSourceId, SourceConfig> = indexes_metadatas
            .into_iter()
            .flat_map(|index_metadata| {
                index_metadata
                    .sources
                    .into_iter()
                    .map(move |(source_id, source_config)| {
                        (
                            IndexSourceId {
                                index_id: index_metadata.index_config.index_id.to_string(),
                                source_id,
                            },
                            source_config,
                        )
                    })
            })
            .collect();
        Ok(source_configs)
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    async fn control_running_plan(&mut self) -> anyhow::Result<()> {
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

        let indexers = self.get_indexers_from_cluster_state().await;
        let running_indexing_tasks_by_node_id: HashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|cluster_member| {
                (
                    cluster_member.node_id.clone(),
                    cluster_member.indexing_tasks.clone(),
                )
            })
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
            self.apply_physical_indexing_plan(&indexers, last_applied_plan.clone())
                .await;
        }
        Ok(())
    }

    async fn get_indexers_from_cluster_state(&self) -> Vec<ClusterMember> {
        self.cluster
            .ready_members_from_chitchat_state()
            .await
            .into_iter()
            .filter(|member| member.enabled_services.contains(&QuickwitService::Indexer))
            .collect_vec()
    }

    async fn apply_physical_indexing_plan(
        &mut self,
        indexers: &[ClusterMember],
        new_physical_plan: PhysicalIndexingPlan,
    ) {
        debug!("Apply physical indexing plan: {:?}", new_physical_plan);
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_node() {
            let indexer = indexers
                .iter()
                .find(|indexer| &indexer.node_id == node_id)
                .expect("This should never happen as the plan was built from these indexers.");
            match self.indexing_client_pool.get(indexer.grpc_advertise_addr) {
                Some(mut indexing_client) => {
                    if let Err(error) = indexing_client
                        .apply_indexing_plan(ApplyIndexingPlanRequest {
                            indexing_tasks: indexing_tasks.clone(),
                        })
                        .await
                    {
                        error!(indexer_node_id=%indexer.node_id, err=?error, "Error occurred when appling indexing plan to indexer.");
                    }
                }
                None => {
                    error!(indexer_node_id=%indexer.node_id,
                        "Indexing service client not found in pool for indexer, it should never happened, skip indexing plan.",
                    );
                }
            }
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }
}

#[async_trait]
impl Handler<NotifyIndexChangeRequest> for IndexingScheduler {
    type Reply = crate::Result<NotifyIndexChangeResponse>;

    async fn handle(
        &mut self,
        _: NotifyIndexChangeRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Index change notification: shedule indexing plan.");
        self.schedule_indexing_plan_if_needed()
            .await
            .context("Error when scheduling indexing plan")?;
        Ok(Ok(NotifyIndexChangeResponse {}))
    }
}

#[derive(Debug)]
struct ControlPlanLoop;

#[async_trait]
impl Handler<ControlPlanLoop> for IndexingScheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: ControlPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(error) = self.control_running_plan().await {
            error!("Error when controlling the running plan: `{}`.", error);
        }
        ctx.schedule_self_msg(HEARTBEAT, ControlPlanLoop).await;
        Ok(())
    }
}

#[derive(Debug)]
struct RefreshPlanLoop;

#[async_trait]
impl Handler<RefreshPlanLoop> for IndexingScheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RefreshPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(error) = self.schedule_indexing_plan_if_needed().await {
            error!("Error when scheduling indexing plan: `{}`.", error);
        }
        ctx.schedule_self_msg(REFRESH_PLAN_LOOP_INTERVAL, RefreshPlanLoop)
            .await;
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
    use std::collections::{HashMap, HashSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::{ActorHandle, Inbox, Universe, HEARTBEAT};
    use quickwit_cluster::{create_cluster_for_test, grpc_addr_from_listen_addr_for_test, Cluster};
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_config::{KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_grpc_clients::service_client_pool::ServiceClientPool;
    use quickwit_indexing::indexing_client::IndexingServiceClient;
    use quickwit_indexing::IndexingService;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, IndexingTask};
    use serde_json::json;

    use super::IndexingScheduler;
    use crate::scheduler::{
        get_indexing_plans_diff, MIN_DURATION_BETWEEN_SCHEDULING, REFRESH_PLAN_LOOP_INTERVAL,
    };

    fn index_metadata_for_test(
        index_id: &str,
        source_id: &str,
        desired_num_pipelines: usize,
        max_num_pipelines_per_indexer: usize,
    ) -> IndexMetadata {
        let mut index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/test-index");
        let source_config = SourceConfig {
            enabled: true,
            source_id: source_id.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(max_num_pipelines_per_indexer)
                .unwrap(),
            desired_num_pipelines: NonZeroUsize::new(desired_num_pipelines).unwrap(),
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "topic".to_string(),
                client_log_level: None,
                client_params: json!({
                    "bootstrap.servers": "localhost:9092",
                }),
                enable_backfill_mode: true,
            }),
            transform_config: None,
        };
        index_metadata
            .sources
            .insert(source_id.to_string(), source_config);
        index_metadata
    }

    async fn start_scheduler(
        cluster: Arc<Cluster>,
        indexers: &[&Cluster],
        universe: &Universe,
    ) -> (Vec<Inbox<IndexingService>>, ActorHandle<IndexingScheduler>) {
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let index_metadata_1 = index_metadata_for_test(index_1, source_1, 2, 2);
        let mut index_metadata_2 = index_metadata_for_test(index_2, source_2, 1, 1);
        index_metadata_2.create_timestamp = index_metadata_1.create_timestamp + 1;
        let mut metastore = MockMetastore::default();
        metastore
            .expect_list_indexes_metadatas()
            .returning(move || Ok(vec![index_metadata_2.clone(), index_metadata_1.clone()]));
        let mut indexer_inboxes = Vec::new();
        let mut indexing_clients = Vec::new();
        for indexer in indexers {
            let (indexing_service_mailbox, indexing_service_inbox) = universe.create_test_mailbox();
            let client_grpc_addr = grpc_addr_from_listen_addr_for_test(indexer.gossip_listen_addr);
            let indexing_client =
                IndexingServiceClient::from_service(indexing_service_mailbox, client_grpc_addr);
            indexing_clients.push(indexing_client);
            indexer_inboxes.push(indexing_service_inbox);
        }
        let indexing_client_pool = ServiceClientPool::for_clients_list(indexing_clients);
        let indexing_scheduler =
            IndexingScheduler::new(cluster, Arc::new(metastore), indexing_client_pool);
        let (_, scheduler_handler) = universe.spawn_builder().spawn(indexing_scheduler);
        (indexer_inboxes, scheduler_handler)
    }

    #[tokio::test]
    async fn test_scheduler_scheduling_and_control_loop_apply_plan_again() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["indexer", "control_plane"], &transport, true)
                .await
                .unwrap(),
        );
        let universe = Universe::with_accelerated_time();
        let (indexing_service_inboxes, scheduler_handler) =
            start_scheduler(cluster.clone(), &[&cluster.clone()], &universe).await;
        let indexing_service_inbox = indexing_service_inboxes[0].clone();
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
        assert!(scheduler_state.last_applied_physical_plan.is_some());
        assert_eq!(indexing_service_inbox_messages.len(), 1);

        // After a HEARTBEAT, the control loop will check if the desired plan is running on the
        // indexer. As chitchat state of the indexer is not updated (we did not
        // instantiate a indexing service for that), the control loop will apply again the
        // same plan.
        // Check first the plan is not updated before `MIN_DURATION_BETWEEN_SCHEDULING`.
        tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(0.5)).await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);

        // After `MIN_DURATION_BETWEEN_SCHEDULING`, we should see a plan update.
        tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(0.7)).await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 2);
        assert_eq!(indexing_service_inbox_messages.len(), 1);
        let indexing_tasks = indexing_service_inbox_messages
            .first()
            .unwrap()
            .indexing_tasks
            .clone();

        // Update the indexer state and check that the indexer does not receive any new
        // `ApplyIndexingPlanRequest`.
        cluster
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await
            .unwrap();
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 2);
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(indexing_service_inbox_messages.len(), 0);

        // Update the indexer state with a different plan and check that the indexer does now
        // receive a new `ApplyIndexingPlanRequest`.
        cluster
            .update_self_node_indexing_tasks(&[indexing_tasks[0].clone()])
            .await
            .unwrap();
        tokio::time::sleep(MIN_DURATION_BETWEEN_SCHEDULING.mul_f32(1.2)).await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 3);
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(indexing_service_inbox_messages.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_scheduler_scheduling_no_indexer() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["control_plane"], &transport, true)
                .await
                .unwrap(),
        );
        let universe = Universe::with_accelerated_time();
        let (indexing_service_inboxes, scheduler_handler) =
            start_scheduler(cluster.clone(), &[&cluster.clone()], &universe).await;
        let indexing_service_inbox = indexing_service_inboxes[0].clone();

        // No indexer.
        universe.sleep(HEARTBEAT).await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
        assert!(scheduler_state.last_applied_physical_plan.is_none());
        assert_eq!(indexing_service_inbox_messages.len(), 0);

        // Wait REFRESH_PLAN_LOOP_INTERVAL * 2, as there is no indexer, we should observe no
        // scheduling.
        universe.sleep(REFRESH_PLAN_LOOP_INTERVAL * 2).await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
        assert!(scheduler_state.last_applied_physical_plan.is_none());
        assert_eq!(indexing_service_inbox_messages.len(), 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_scheduler_scheduling_multiple_indexers() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["control_plane"], &transport, true)
                .await
                .unwrap(),
        );
        let cluster_indexer_1 = create_cluster_for_test(
            vec![cluster.node_id.gossip_public_address.to_string()],
            &["indexer"],
            &transport,
            true,
        )
        .await
        .unwrap();
        let cluster_indexer_2 = create_cluster_for_test(
            vec![cluster.node_id.gossip_public_address.to_string()],
            &["indexer"],
            &transport,
            true,
        )
        .await
        .unwrap();
        let universe = Universe::new();
        let (indexing_service_inboxes, scheduler_handler) = start_scheduler(
            cluster.clone(),
            &[&cluster_indexer_1, &cluster_indexer_2],
            &universe,
        )
        .await;
        let indexing_service_inbox_1 = indexing_service_inboxes[0].clone();
        let indexing_service_inbox_2 = indexing_service_inboxes[1].clone();
        let scheduler_handler_arc = Arc::new(scheduler_handler);

        // No indexer.
        let scheduler_state = scheduler_handler_arc.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox_1.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 0);
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 0);
        assert!(scheduler_state.last_applied_physical_plan.is_none());
        assert_eq!(indexing_service_inbox_messages.len(), 0);

        // Wait for chitchat update, sheduler will detect new indexers and schedule a plan.
        wait_until_predicate(
            || {
                let scheduler_handler_arc_clone = scheduler_handler_arc.clone();
                async move {
                    let scheduler_state = scheduler_handler_arc_clone
                        .process_pending_and_observe()
                        .await;
                    scheduler_state.num_schedule_indexing_plan == 1
                }
            },
            HEARTBEAT * 4,
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let scheduler_state = scheduler_handler_arc.process_pending_and_observe().await;
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);
        let indexing_service_inbox_messages_1 =
            indexing_service_inbox_1.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        let indexing_service_inbox_messages_2 =
            indexing_service_inbox_2.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(indexing_service_inbox_messages_1.len(), 1);
        assert_eq!(indexing_service_inbox_messages_2.len(), 1);
        cluster_indexer_1
            .update_self_node_indexing_tasks(&indexing_service_inbox_messages_1[0].indexing_tasks)
            .await
            .unwrap();
        cluster_indexer_2
            .update_self_node_indexing_tasks(&indexing_service_inbox_messages_2[0].indexing_tasks)
            .await
            .unwrap();

        // Wait 2 heartbeats again and check the scheduler will not apply the plan several times.
        universe.sleep(HEARTBEAT * 2).await;
        let scheduler_state = scheduler_handler_arc.process_pending_and_observe().await;
        assert!(scheduler_state.num_applied_physical_indexing_plan < 3);
        assert_eq!(scheduler_state.num_schedule_indexing_plan, 1);

        // Shutdown cluster and wait until the new scheduling.
        cluster_indexer_2.shutdown().await;
        wait_until_predicate(
            || {
                let scheduler_handler_arc_clone = scheduler_handler_arc.clone();
                async move {
                    let scheduler_state = scheduler_handler_arc_clone
                        .process_pending_and_observe()
                        .await;
                    scheduler_state.num_schedule_indexing_plan == 2
                        && scheduler_state.num_applied_physical_indexing_plan < 4
                }
            },
            HEARTBEAT * 10,
            Duration::from_millis(100),
        )
        .await
        .unwrap();

        universe.assert_quit().await;
    }

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
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
            };
            let task_2 = IndexingTask {
                index_id: "index-1".to_string(),
                source_id: "source-2".to_string(),
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
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
            };
            let task_2 = IndexingTask {
                index_id: "index-1".to_string(),
                source_id: "source-2".to_string(),
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
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
            };
            let task_2 = IndexingTask {
                index_id: "index-2".to_string(),
                source_id: "source-2".to_string(),
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
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
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
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
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
