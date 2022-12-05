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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, HEARTBEAT};
use quickwit_config::IndexConfig;
use quickwit_metastore::Metastore;
use quickwit_search::SearchClientPool;
use quickwit_storage::StorageUriResolver;
use serde::Serialize;
use tracing::{error, info, warn};

use super::delete_task_pipeline::DeleteTaskPipeline;

pub const DELETE_SERVICE_TASK_DIR_NAME: &str = "delete_task_service";

#[derive(Debug, Clone, Serialize)]
pub struct DeleteTaskServiceState {
    pub num_running_pipelines: usize,
}

pub struct DeleteTaskService {
    metastore: Arc<dyn Metastore>,
    search_client_pool: SearchClientPool,
    storage_resolver: StorageUriResolver,
    data_dir_path: PathBuf,
    pipeline_handles_by_index_id: HashMap<String, ActorHandle<DeleteTaskPipeline>>,
    max_concurrent_split_uploads: usize,
}

impl DeleteTaskService {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        search_client_pool: SearchClientPool,
        storage_resolver: StorageUriResolver,
        data_dir_path: PathBuf,
        max_concurrent_split_uploads: usize,
    ) -> Self {
        Self {
            metastore,
            search_client_pool,
            storage_resolver,
            data_dir_path,
            pipeline_handles_by_index_id: Default::default(),
            max_concurrent_split_uploads,
        }
    }
}

#[async_trait]
impl Actor for DeleteTaskService {
    type ObservableState = DeleteTaskServiceState;

    fn observable_state(&self) -> Self::ObservableState {
        DeleteTaskServiceState {
            num_running_pipelines: self.pipeline_handles_by_index_id.len(),
        }
    }

    fn name(&self) -> String {
        "DeleteTaskService".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(SuperviseLoop, ctx).await?;
        Ok(())
    }
}

impl DeleteTaskService {
    pub async fn update_pipeline_handles(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let mut index_config_by_index_id: HashMap<String, IndexConfig> = self
            .metastore
            .list_indexes_metadatas()
            .await?
            .into_iter()
            .map(|index_metadata| {
                (
                    index_metadata.index_id().to_string(),
                    index_metadata.into_index_config(),
                )
            })
            .collect();
        let index_ids: HashSet<String> = index_config_by_index_id.keys().cloned().collect();
        let pipeline_index_ids: HashSet<String> =
            self.pipeline_handles_by_index_id.keys().cloned().collect();

        // Remove pipelines on deleted indexes.
        for deleted_index_id in pipeline_index_ids.difference(&index_ids) {
            info!(
                deleted_index_id = deleted_index_id,
                "Remove deleted index from delete task pipelines."
            );
            let pipeline_handle = self
                .pipeline_handles_by_index_id
                .remove(deleted_index_id)
                .expect("Handle must be present.");
            // Kill the pipeline, this avoids to wait a long time for a delete operation to finish.
            pipeline_handle.kill().await;
        }

        // Start new pipelines and add them to the handles hashmap.
        for index_id in index_ids.difference(&pipeline_index_ids) {
            let index_config = index_config_by_index_id
                .remove(index_id)
                .expect("Index metadata must be present.");
            if self.spawn_pipeline(index_config, ctx).await.is_err() {
                warn!("Failed to spawn delete pipeline for {index_id}");
            }
        }

        Ok(())
    }

    pub async fn spawn_pipeline(
        &mut self,
        index_config: IndexConfig,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let delete_task_service_dir = self.data_dir_path.join(DELETE_SERVICE_TASK_DIR_NAME);
        let index_uri = index_config.index_uri.clone();
        let index_storage = self.storage_resolver.resolve(&index_uri)?;
        let pipeline = DeleteTaskPipeline::new(
            index_config.index_id.clone(),
            self.metastore.clone(),
            self.search_client_pool.clone(),
            index_config.indexing_settings,
            index_storage,
            delete_task_service_dir,
            self.max_concurrent_split_uploads,
        );
        let (_pipeline_mailbox, pipeline_handler) = ctx.spawn_actor().spawn(pipeline);
        self.pipeline_handles_by_index_id
            .insert(index_config.index_id, pipeline_handler);
        Ok(())
    }
}

#[derive(Debug)]
struct SuperviseLoop;

#[async_trait]
impl Handler<SuperviseLoop> for DeleteTaskService {
    type Reply = ();

    async fn handle(
        &mut self,
        _: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let result = self.update_pipeline_handles(ctx).await;
        if let Err(error) = result {
            error!("Delete task pipelines udpate failed: {}", error);
        }
        ctx.schedule_self_msg(HEARTBEAT, SuperviseLoop).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::{Universe, HEARTBEAT};
    use quickwit_indexing::TestSandbox;
    use quickwit_proto::metastore_api::DeleteQuery;
    use quickwit_search::{MockSearchService, SearchClientPool};
    use quickwit_storage::StorageUriResolver;

    use super::DeleteTaskService;

    #[tokio::test]
    async fn test_delete_task_service() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-task-service-index";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
        let metastore = test_sandbox.metastore();
        let mock_search_service = MockSearchService::new();
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?;
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let delete_task_service = DeleteTaskService::new(
            metastore.clone(),
            client_pool,
            StorageUriResolver::for_test(),
            data_dir_path,
            4,
        );
        let universe = Universe::new();
        let (_delete_task_service_mailbox, delete_task_service_handler) =
            universe.spawn_builder().spawn(delete_task_service);
        let state = delete_task_service_handler
            .process_pending_and_observe()
            .await;
        assert_eq!(state.num_running_pipelines, 1);
        let delete_query = DeleteQuery {
            index_id: index_id.to_string(),
            start_timestamp: None,
            end_timestamp: None,
            query: "*".to_string(),
            search_fields: Vec::new(),
        };
        metastore.create_delete_task(delete_query).await.unwrap();
        // Just test creation of delete query.
        assert_eq!(
            metastore
                .list_delete_tasks(index_id, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        metastore.delete_index(index_id).await.unwrap();
        tokio::time::sleep(HEARTBEAT * 2).await;
        let state_after_deletion = delete_task_service_handler
            .process_pending_and_observe()
            .await;
        assert_eq!(state_after_deletion.num_running_pipelines, 0);
        assert!(universe.get_one::<DeleteTaskService>().is_some());
        let actors_observations = universe.observe(HEARTBEAT).await;
        // Once the pipeline is properly shut down, the only remaining actors are the scheduler and
        // the delete service.
        assert_eq!(actors_observations.len(), 2);
        assert!(universe.get_one::<DeleteTaskService>().is_some());
        Ok(())
    }
}
