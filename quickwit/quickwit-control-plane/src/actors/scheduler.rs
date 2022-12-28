// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, HEARTBEAT};
use quickwit_cluster::Cluster;
use quickwit_common::service::QuickwitService;
use quickwit_config::SourceConfig;
use quickwit_metastore::Metastore;
use quickwit_proto::control_plane_api::RefreshIndexingPlanEventRequest;
use quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient;
use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, IndexingTask};
use quickwit_proto::tonic::transport::{Channel, Endpoint, Uri};
use quickwit_proto::ClusterMember;
use time::OffsetDateTime;
use tracing::info;
use tracing::log::{error, warn};

use crate::indexing_plan::{
    build_indexing_plan, build_physical_indexing_plan, IndexSourceId, PhysicalIndexingPlan,
};

const REFRESH_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(60)
};

const MIN_DURATION_BETWEEN_SCHEDULING_SECS: usize = 30;

pub struct IndexingScheduler {
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
    last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    last_applied_plan_timestamp: Option<i64>,
}

#[async_trait]
impl Actor for IndexingScheduler {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexerScheduler".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(RefreshPlanLoop, ctx).await?;
        self.handle(ControlPlanLoop, ctx).await?;
        Ok(())
    }
}

impl IndexingScheduler {
    pub fn new(cluster: Arc<Cluster>, metastore: Arc<dyn Metastore>) -> Self {
        Self {
            cluster,
            metastore,
            last_applied_physical_plan: None,
            last_applied_plan_timestamp: None,
        }
    }

    async fn schedule_indexing_plan(&mut self) -> anyhow::Result<()> {
        let indexers = self.get_indexers_from_cluster_state().await;
        if indexers.is_empty() {
            warn!("No indexer available, cannot build a physical indexing plan.");
            return Ok(());
        };
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
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs);
        let new_physical_plan =
            build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks);
        self.apply_physical_indexing_plan(&indexers, new_physical_plan)
            .await?;
        Ok(())
    }

    /// Check if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// If false, schedule a new indexing plan.
    async fn control_running_plan(&mut self) -> anyhow::Result<()> {
        let last_applied_plan =
            if let Some(last_applied_plan) = self.last_applied_physical_plan.as_ref() {
                last_applied_plan
            } else {
                return Ok(());
            };
        if let Some(last_applied_plan_timestamp) = self.last_applied_plan_timestamp.as_ref() {
            if OffsetDateTime::now_utc().unix_timestamp() - last_applied_plan_timestamp
                < MIN_DURATION_BETWEEN_SCHEDULING_SECS as i64
            {
                return Ok(());
            }
        }

        let indexers = self.get_indexers_from_cluster_state().await;
        let running_indexing_tasks_by_node_id: HashMap<String, Vec<IndexingTask>> = indexers
            .into_iter()
            .map(|cluster_member| {
                (
                    cluster_member.node_id,
                    cluster_member.running_indexing_tasks,
                )
            })
            .collect();
        if &running_indexing_tasks_by_node_id != last_applied_plan.indexing_tasks_per_indexer() {
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
            // Sort by node_id to assign indexing task always in the same order.
            .sorted_by_key(|indexer| indexer.node_id.clone())
            .collect_vec()
    }

    async fn apply_physical_indexing_plan(
        &mut self,
        indexers: &[ClusterMember],
        new_physical_plan: PhysicalIndexingPlan,
    ) -> anyhow::Result<()> {
        info!("Apply physical indexing plan: {:?}", new_physical_plan);
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_indexer().iter() {
            // TODO: use an indexing service client pool.
            let indexer = indexers
                .iter().find(|indexer| &indexer.node_id == node_id)
                .ok_or_else(|| anyhow::anyhow!("Indexer of node ID {node_id} not found"))?;
            match create_indexing_service_client(indexer.grpc_advertise_addr).await {
                Ok(mut indexing_client) => {
                    if let Err(error) = indexing_client
                        .apply_indexing_plan(ApplyIndexingPlanRequest {
                            indexing_tasks: indexing_tasks.clone(),
                        })
                        .await
                    {
                        error!(
                            "Cannot apply indexing plan to indexer `{}`: `{}`.",
                            indexer.node_id, error
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "Cannot create indexing service client for indexer `{}`: `{}`.",
                        indexer.node_id, error
                    );
                }
            }
        }

        // TODO: we should report errors in the scheduler state.
        self.last_applied_plan_timestamp = Some(OffsetDateTime::now_utc().unix_timestamp());
        self.last_applied_physical_plan = Some(new_physical_plan);

        Ok(())
    }
}

#[async_trait]
impl Handler<RefreshIndexingPlanEventRequest> for IndexingScheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        _: RefreshIndexingPlanEventRequest,
        _: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(error) = self.schedule_indexing_plan().await {
            error!("Error when scheduling indexing tasks: `{}`.", error);
        }
        Ok(())
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
            error!("Error when scheduling indexing tasks: `{}`.", error);
        }
        ctx.schedule_self_msg(REFRESH_PLAN_LOOP_INTERVAL, RefreshPlanLoop)
            .await;
        Ok(())
    }
}

pub async fn create_indexing_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<IndexingServiceClient<Channel>> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    let channel = Endpoint::from(uri).connect().await?;
    let client = IndexingServiceClient::new(channel);
    Ok(client)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::Universe;
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_common::service::QuickwitService;
    use quickwit_config::{KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use serde_json::json;

    use super::IndexingScheduler;

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
        };
        index_metadata
            .sources
            .insert(source_id.to_string(), source_config);
        index_metadata
    }

    #[tokio::test]
    async fn test_scheduler_simple() {
        quickwit_common::setup_logging_for_tests();
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let index_metadata_1 = index_metadata_for_test(index_1, source_1, 2, 2);
        let mut index_metadata_2 = index_metadata_for_test(index_2, source_2, 1, 1);
        index_metadata_2.create_timestamp = index_metadata_1.create_timestamp + 1;
        let mut metastore = MockMetastore::default();
        metastore.expect_list_indexes_metadatas().return_once(|| {
            // We voluntary the second index metastore first to check if sorting is done
            // when building the plan.
            Ok(vec![index_metadata_2, index_metadata_1])
        });
        let transport = ChannelTransport::default();
        // One indexer.
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &["indexer", "control_plane"], &transport, true)
                .await
                .unwrap(),
        );
        let universe = Universe::new();
        let indexing_scheduler = IndexingScheduler::new(cluster.clone(), Arc::new(metastore));
        let (_, scheduler_handler) = universe.spawn_builder().spawn(indexing_scheduler);

        // Insure that the message sent by initialize method is processed.
        // TODO: fix the test. Currently, it fails when we try to apply the plan, we need to mock
        // some clients.
        scheduler_handler.process_pending_and_observe().await;

        let cluster_members = cluster.ready_members_from_chitchat_state().await;
        let indexer = cluster_members
            .iter().find(|member| member.enabled_services.contains(&QuickwitService::Indexer))
            .unwrap();

        assert_eq!(indexer.running_indexing_tasks.len(), 2);
    }
}
