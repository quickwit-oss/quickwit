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
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_cluster::Cluster;
use quickwit_metastore::Metastore;
use quickwit_proto::control_plane_api::RefreshIndexingPlanEventRequest;
use quickwit_proto::indexing_api::indexing_service_client::IndexingServiceClient;
use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, IndexingTask};
use quickwit_proto::tonic::transport::{Channel, Endpoint, Uri};
use quickwit_proto::{ClusterMember, QuickwitService};
use tracing::info;
use tracing::log::{error, warn};

type PhysicalIndexingPlan = HashMap<SocketAddr, Vec<IndexingTask>>;

const REFRESH_INDEXING_PLAN_LOOP_DURATION: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(60)
};

/// TODO
pub struct IndexingScheduler {
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
}

#[async_trait]
impl Actor for IndexingScheduler {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexerScheduler".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(UpdatePhysicalIndexingPlanLoop, ctx).await?;
        Ok(())
    }
}

impl IndexingScheduler {
    pub fn new(cluster: Arc<Cluster>, metastore: Arc<dyn Metastore>) -> Self {
        Self { cluster, metastore }
    }

    async fn build_indexing_plan(&mut self) -> anyhow::Result<Vec<IndexingTask>> {
        let mut indexing_tasks: Vec<IndexingTask> = Vec::new();
        let mut indexes_metadatas = self.metastore.list_indexes_metadatas().await?;
        indexes_metadatas.sort_by_key(|index_metadata| index_metadata.create_timestamp);
        for index_metadata in indexes_metadatas {
            for source_config in index_metadata.sources.into_values() {
                for pipeline_ord in 0..source_config.num_pipelines().unwrap_or(1) {
                    indexing_tasks.push(IndexingTask {
                        index_id: index_metadata.index_id.clone(),
                        source_id: source_config.source_id.clone(),
                        pipeline_ord: pipeline_ord as u64,
                    });
                }
            }
        }
        Ok(indexing_tasks)
    }

    async fn build_physical_indexing_plan(
        &self,
        indexers: &[ClusterMember],
        indexing_tasks: Vec<IndexingTask>,
    ) -> anyhow::Result<PhysicalIndexingPlan> {
        assert!(indexers.iter().all(|indexer| indexer
            .available_services
            .contains(&QuickwitService::Indexer)));
        assert!(!indexers.is_empty());
        let mut new_physical_plan: HashMap<SocketAddr, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.grpc_advertise_addr, Vec::new()))
            .collect();
        for (indexing_task_counter, indexing_task) in indexing_tasks.into_iter().enumerate() {
            let indexer = &indexers[indexing_task_counter % indexers.len()];
            if let Some(running_tasks) = new_physical_plan.get_mut(&indexer.grpc_advertise_addr) {
                running_tasks.push(indexing_task);
            }
        }

        Ok(new_physical_plan)
    }

    async fn apply_physical_indexing_plan(&mut self) -> anyhow::Result<()> {
        let indexing_tasks = self.build_indexing_plan().await?;
        let indexers = self
            .cluster
            .ready_members_from_chitchat_state()
            .await
            .into_iter()
            .filter(|member| {
                member
                    .available_services
                    .contains(&QuickwitService::Indexer)
            })
            // Sort by node_id to assign indexing task always in the same order.
            .sorted_by_key(|indexer| indexer.node_unique_id.clone())
            .collect_vec();
        if indexers.is_empty() {
            warn!("No indexer available to run indexing tasks.");
            return Ok(());
        };

        let new_physical_plan = self
            .build_physical_indexing_plan(&indexers, indexing_tasks)
            .await?;
        let current_physical_plan: HashMap<SocketAddr, Vec<IndexingTask>> = indexers
            .into_iter()
            .map(|indexer| (indexer.grpc_advertise_addr, indexer.running_indexing_tasks))
            .collect();

        // If current and new physical plan are equal, there is nothing to do.
        if current_physical_plan == new_physical_plan {
            return Ok(());
        }

        info!("Apply physical indexing plan: {:?}", new_physical_plan);
        for (grpc_addr, indexing_tasks) in new_physical_plan.into_iter() {
            // TODO: use an indexing service client pool.
            match create_indexing_service_client(grpc_addr).await {
                Ok(mut indexing_client) => {
                    if let Err(error) = indexing_client
                        .apply_indexing_plan(ApplyIndexingPlanRequest { indexing_tasks })
                        .await
                    {
                        error!(
                            "Cannot apply indexing plan to indexer with grpc address `{}`: `{}`.",
                            grpc_addr, error
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "Cannot create indexing service client for indexer with grpc address \
                         `{}`: `{}`.",
                        grpc_addr, error
                    );
                }
            }
        }

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
        if let Err(error) = self.apply_physical_indexing_plan().await {
            error!("Error when applying physical indexing plan: `{}`.", error);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct UpdatePhysicalIndexingPlanLoop;

#[async_trait]
impl Handler<UpdatePhysicalIndexingPlanLoop> for IndexingScheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: UpdatePhysicalIndexingPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.handle(RefreshIndexingPlanEventRequest {}, ctx).await?;
        ctx.schedule_self_msg(
            REFRESH_INDEXING_PLAN_LOOP_DURATION,
            UpdatePhysicalIndexingPlanLoop,
        )
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
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use itertools::Itertools;
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_config::{KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::indexing_api::IndexingTask;
    use serde_json::json;

    use super::IndexingScheduler;

    fn index_metadata_for_test(
        index_id: &str,
        source_id: &str,
        num_pipelines: usize,
    ) -> IndexMetadata {
        let mut index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/test-index");
        let source_config = SourceConfig {
            source_id: source_id.to_string(),
            num_pipelines,
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
    async fn test_build_indexing_plan() -> anyhow::Result<()> {
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let index_metadata_1 = index_metadata_for_test(index_1, source_1, 2);
        let mut index_metadata_2 = index_metadata_for_test(index_2, source_2, 1);
        index_metadata_2.create_timestamp = index_metadata_1.create_timestamp + 1;
        let mut metastore = MockMetastore::default();
        metastore.expect_list_indexes_metadatas().return_once(|| {
            // We voluntary the second index metastore first to check if sorting is done
            // when building the plan.
            Ok(vec![index_metadata_2, index_metadata_1])
        });
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["control_plane"], &transport, true)
            .await
            .unwrap();
        let mut indexing_scheduler = IndexingScheduler::new(Arc::new(cluster), Arc::new(metastore));

        let indexing_tasks = indexing_scheduler.build_indexing_plan().await.unwrap();

        assert_eq!(indexing_tasks.len(), 3);
        assert_eq!(
            &indexing_tasks[0],
            &IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
                pipeline_ord: 0
            }
        );
        assert_eq!(
            &indexing_tasks[1],
            &IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
                pipeline_ord: 1
            }
        );
        assert_eq!(
            &indexing_tasks[2],
            &IndexingTask {
                index_id: index_2.to_string(),
                source_id: source_2.to_string(),
                pipeline_ord: 0
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_build_physical_indexing_plan() -> anyhow::Result<()> {
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let mut indexing_tasks = Vec::new();
        for pipeline_ord in 0..10 {
            indexing_tasks.push(IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
                pipeline_ord,
            });
        }
        for pipeline_ord in 0..5 {
            indexing_tasks.push(IndexingTask {
                index_id: index_2.to_string(),
                source_id: source_2.to_string(),
                pipeline_ord,
            });
        }

        let transport = ChannelTransport::default();
        let cluster1 = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let node_1 = cluster1.gossip_listen_addr.to_string();
        let _cluster2 =
            create_cluster_for_test(vec![node_1.clone()], &["indexer"], &transport, true).await?;
        cluster1
            .wait_for_members(|members| members.len() == 2, Duration::from_secs(10))
            .await
            .unwrap();
        let members = cluster1.ready_members_from_chitchat_state().await;
        let metastore = MockMetastore::default();
        let indexing_scheduler = IndexingScheduler::new(Arc::new(cluster1), Arc::new(metastore));

        let physical_plan = indexing_scheduler
            .build_physical_indexing_plan(&members, indexing_tasks.clone())
            .await
            .unwrap();

        assert_eq!(physical_plan.len(), 2);
        let indexer_1_tasks = physical_plan.get(&members[0].grpc_advertise_addr).unwrap();
        let indexer_2_tasks = physical_plan.get(&members[1].grpc_advertise_addr).unwrap();
        let expected_indexer_1_tasks = indexing_tasks
            .iter()
            .cloned()
            .enumerate()
            .filter(|(idx, _)| idx % 2 == 0)
            .map(|(_, task)| task)
            .collect_vec();
        assert_eq!(indexer_1_tasks, &expected_indexer_1_tasks);
        let expected_indexer_2_tasks = indexing_tasks
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| idx % 2 != 0)
            .map(|(_, task)| task)
            .collect_vec();
        assert_eq!(indexer_2_tasks, &expected_indexer_2_tasks);
        Ok(())
    }
}
