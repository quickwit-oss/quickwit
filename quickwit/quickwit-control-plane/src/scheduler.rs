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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};

use crate::indexing_plan::{
    build_indexing_plan, build_physical_indexing_plan, IndexSourceId, PhysicalIndexingPlan,
};
use crate::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};

const REFRESH_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_secs(3)
} else {
    Duration::from_secs(60)
};

// TODO: replace the value 0 by 1 once time warping is correctly handle by the actor framework.
const MIN_DURATION_BETWEEN_SCHEDULING_SECS: u64 = if cfg!(any(test, feature = "testsuite")) {
    0
} else {
    30
};

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexingSchedulerState {
    pub num_applied_physical_indexing_plan: usize,
    pub last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    pub last_applied_plan_timestamp: Option<i64>,
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
///   state, are the same. If not, the scheduler will trigger a scheduling.
///
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// phase will wait at least [`MIN_DURATION_BETWEEN_SCHEDULING_SECS`] before comparing the desired
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

    async fn schedule_indexing_plan(&mut self) -> anyhow::Result<()> {
        let indexers: Vec<ClusterMember> = self.get_indexers_from_cluster_state().await;
        if indexers.is_empty() {
            warn!("No indexer available, set an empty physical indexing plan.");
            self.state.last_applied_physical_plan = Some(PhysicalIndexingPlan::default());
            self.state.last_applied_plan_timestamp =
                Some(OffsetDateTime::now_utc().unix_timestamp());
            return Ok(());
        };
        let source_configs: HashMap<IndexSourceId, SourceConfig> =
            self.fetch_source_configs().await?;
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs);
        let new_physical_plan =
            build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks);
        self.apply_physical_indexing_plan(&indexers, new_physical_plan)
            .await;
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
    /// If false, schedule a new indexing plan.
    async fn control_running_plan(&mut self) -> anyhow::Result<()> {
        let last_applied_plan =
            if let Some(last_applied_plan) = self.state.last_applied_physical_plan.as_ref() {
                last_applied_plan
            } else {
                return Ok(());
            };

        // TODO(fmassot): once the actor framekwork can correctly simulate time advance, change
        // `MIN_DURATION_BETWEEN_SCHEDULING_SECS` value for test and correctly test it. Currently, I
        // put its value 0 to make test simple.
        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp.as_ref() {
            // If the last applied plan is empty, the node is probably starting and did not find
            // indexers yet. In this case, we dont want to wait
            // `MIN_DURATION_BETWEEN_SCHEDULING_SECS` for checking if new indexers are
            // there.
            if !last_applied_plan.is_empty()
                && (OffsetDateTime::now_utc().unix_timestamp() - last_applied_plan_timestamp)
                    < MIN_DURATION_BETWEEN_SCHEDULING_SECS as i64
            {
                return Ok(());
            }
        }
        let indexers = self.get_indexers_from_cluster_state().await;
        let running_indexing_tasks_by_node_id: HashMap<String, Vec<IndexingTask>> = indexers
            .into_iter()
            .map(|cluster_member| (cluster_member.node_id, cluster_member.indexing_tasks))
            .collect();
        // TODO(fmassot): in the case where we have the same set of nodes in the two plans, a
        // rescheduling is not needed, we probably should just reapply the same plan on the
        // indexers.
        if !are_indexing_plans_equal(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_node(),
        ) {
            info!("Running plan and last applied plan differs: schedule an indexing plan.");
            self.schedule_indexing_plan().await?;
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
        self.state.last_applied_plan_timestamp = Some(OffsetDateTime::now_utc().unix_timestamp());
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
        info!("Index change notification: schedule indexing plan.");
        self.schedule_indexing_plan()
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
        if let Err(error) = self.schedule_indexing_plan().await {
            error!("Error when scheduling indexing plan: `{}`.", error);
        }
        ctx.schedule_self_msg(REFRESH_PLAN_LOOP_INTERVAL, RefreshPlanLoop)
            .await;
        Ok(())
    }
}

/// Compares the `running_plan` retrieved from the chitchat state and
/// the last plan applied by the scheduler and returns
/// - true if plans are equal. Plans are considered equal if they have the same node IDs and if each
///   node has the same running tasks.
/// - false if not and logs the difference between plans.
fn are_indexing_plans_equal(
    running_plan: &HashMap<String, Vec<IndexingTask>>,
    last_applied_plan: &HashMap<String, Vec<IndexingTask>>,
) -> bool {
    let mut plans_are_equal = true;
    // Compare node IDs with at least one task.
    let running_node_ids: HashSet<&str> = running_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let planned_node_ids: HashSet<&str> = last_applied_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let common_node_ids = running_node_ids.intersection(&planned_node_ids);
    let missing_planned_node_ids = planned_node_ids.difference(&running_node_ids).collect_vec();
    let unplanned_node_ids = running_node_ids.difference(&planned_node_ids);

    for unplanned_node_id in unplanned_node_ids {
        plans_are_equal = false;
        let running_tasks = running_plan
            .get(*unplanned_node_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if running_tasks.is_empty() {
            // An new indexer may have joined the cluster.
            info!("The indexer `{unplanned_node_id}` is not part of the last applied plan.")
        } else {
            info!(
                "The indexer `{unplanned_node_id}`, which is not part of the last applied plan, \
                 has some running tasks: {running_tasks:?}."
            );
        }
    }

    for missing_planned_node_id in missing_planned_node_ids {
        // An indexer may have left the cluster.
        plans_are_equal = false;
        let planned_tasks = last_applied_plan
            .get(*missing_planned_node_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        info!(
            "The indexer `{missing_planned_node_id}` is not part of the the running plan, none of \
             his tasks are running: {planned_tasks:?}."
        );
    }

    // Compare indexing tasks for each common node ID.
    for common_node_id in common_node_ids {
        let running_tasks: HashSet<_> = running_plan
            .get(*common_node_id)
            .map(|tasks| HashSet::from_iter(tasks.iter()))
            .unwrap_or_default();
        let desired_tasks: HashSet<_> = last_applied_plan
            .get(*common_node_id)
            .map(|tasks| HashSet::from_iter(tasks.iter()))
            .unwrap_or_default();
        let missing_desired_tasks = desired_tasks.difference(&running_tasks).collect_vec();
        let non_desired_tasks = running_tasks.difference(&desired_tasks).collect_vec();

        if !missing_desired_tasks.is_empty() {
            plans_are_equal = false;
            info!(
                "Indexer `{common_node_id}` does not run the desired indexing tasks: \
                 `{missing_desired_tasks:?}`"
            );
        }
        if !non_desired_tasks.is_empty() {
            plans_are_equal = false;
            info!(
                "Indexer `{common_node_id}` is running non desired indexing tasks: \
                 `{non_desired_tasks:?}`"
            );
        }
    }

    plans_are_equal
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::Universe;
    use quickwit_cluster::{create_cluster_for_test, grpc_addr_from_listen_addr_for_test};
    use quickwit_config::{KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_grpc_clients::service_client_pool::ServiceClientPool;
    use quickwit_indexing::indexing_client::IndexingServiceClient;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, IndexingTask};
    use serde_json::json;

    use super::{are_indexing_plans_equal, IndexingScheduler};

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
            max_num_pipelines_per_indexer,
            desired_num_pipelines,
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

    #[tokio::test]
    async fn test_scheduler_scheduling_and_control_loop() {
        quickwit_common::setup_logging_for_tests();
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
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["indexer", "control_plane"], &transport, true)
                .await
                .unwrap(),
        );
        let universe = Universe::with_accelerated_time();
        let (indexing_service_mailbox, indexing_service_inbox) = universe.create_test_mailbox();
        let client_grpc_addr = grpc_addr_from_listen_addr_for_test(cluster.gossip_listen_addr);
        let indexing_client =
            IndexingServiceClient::from_service(indexing_service_mailbox, client_grpc_addr);
        let indexing_client_pool = ServiceClientPool::for_clients_list(vec![indexing_client]);
        let indexing_scheduler =
            IndexingScheduler::new(cluster.clone(), Arc::new(metastore), indexing_client_pool);
        let (_, scheduler_handler) = universe.spawn_builder().spawn(indexing_scheduler);

        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 1);
        assert!(scheduler_state.last_applied_physical_plan.is_some());
        assert_eq!(indexing_service_inbox_messages.len(), 1);

        // After a HEARTBEAT, the control loop will check if the desired plan is running on the
        // indexer. As the chitchat state of the indexer is not updated (we did not instantiate a
        // indexing service for that), the control loop will trigger a new scheduling.
        scheduler_handler.process_pending_and_observe().await;
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
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
        let scheduler_state = scheduler_handler.process_pending_and_observe().await;
        assert_eq!(scheduler_state.num_applied_physical_indexing_plan, 3);
        let indexing_service_inbox_messages =
            indexing_service_inbox.drain_for_test_typed::<ApplyIndexingPlanRequest>();
        assert_eq!(indexing_service_inbox_messages.len(), 1);
        universe.assert_quit().await;
    }

    #[test]
    fn test_are_indexing_plans_equal() {
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

            assert!(are_indexing_plans_equal(&running_plan, &desired_plan));
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
            running_plan.insert("indexer-1".to_string(), vec![task_1]);
            desired_plan.insert("indexer-1".to_string(), vec![]);

            assert!(!are_indexing_plans_equal(&running_plan, &desired_plan));

            desired_plan.insert("indexer-1".to_string(), vec![task_2]);

            assert!(!are_indexing_plans_equal(&running_plan, &desired_plan));
        }
        {
            let mut running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            let task_1 = IndexingTask {
                index_id: "index-1".to_string(),
                source_id: "source-1".to_string(),
            };
            running_plan.insert("indexer-2".to_string(), vec![]);
            desired_plan.insert("indexer-1".to_string(), vec![task_1]);

            assert!(!are_indexing_plans_equal(&running_plan, &desired_plan));
        }
        {
            let mut running_plan = HashMap::new();
            let desired_plan = HashMap::new();
            running_plan.insert("indexer-1".to_string(), Vec::new());

            assert!(!are_indexing_plans_equal(&running_plan, &desired_plan));
        }
        {
            let running_plan = HashMap::new();
            let mut desired_plan = HashMap::new();
            desired_plan.insert("indexer-1".to_string(), Vec::new());

            assert!(!are_indexing_plans_equal(&running_plan, &desired_plan));
        }
    }
}
