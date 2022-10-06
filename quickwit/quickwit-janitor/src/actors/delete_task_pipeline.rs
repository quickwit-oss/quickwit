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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, KillSwitch, Supervisable,
    HEARTBEAT,
};
use quickwit_config::{build_doc_mapper, IndexingSettings};
use quickwit_indexing::actors::{
    MergeExecutor, MergeSplitDownloader, Packager, Publisher, Uploader,
};
use quickwit_indexing::merge_policy::merge_policy_from_settings;
use quickwit_indexing::models::{IndexingDirectory, IndexingPipelineId};
use quickwit_indexing::{IndexingSplitStore, PublisherType, SplitsUpdateMailbox};
use quickwit_metastore::Metastore;
use quickwit_search::SearchClientPool;
use quickwit_storage::Storage;
use serde::Serialize;
use tracing::{debug, error, info};

use super::delete_task_planner::DeleteTaskPlanner;

const SUPERVISE_DELAY: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    HEARTBEAT
};

struct DeletePipelineHandle {
    pub delete_task_planner: ActorHandle<DeleteTaskPlanner>,
    pub downloader: ActorHandle<MergeSplitDownloader>,
    pub delete_task_executor: ActorHandle<MergeExecutor>,
    pub packager: ActorHandle<Packager>,
    pub uploader: ActorHandle<Uploader>,
    pub publisher: ActorHandle<Publisher>,
}

/// A Struct to hold all statistical data about deletes.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DeletePipelineState {
    /// Actors health.
    pub actors_health: HashMap<String, Health>,
}

pub struct DeleteTaskPipeline {
    index_id: String,
    metastore: Arc<dyn Metastore>,
    search_client_pool: SearchClientPool,
    indexing_settings: IndexingSettings,
    index_storage: Arc<dyn Storage>,
    delete_service_dir_path: PathBuf,
    handles: Option<DeletePipelineHandle>,
}

#[async_trait]
impl Actor for DeleteTaskPipeline {
    type ObservableState = DeletePipelineState;

    fn observable_state(&self) -> Self::ObservableState {
        let actors_health: HashMap<String, Health> = self
            .supervisables()
            .into_iter()
            .map(|actor| (actor.name().to_string(), actor.health()))
            .collect();
        Self::ObservableState { actors_health }
    }

    fn name(&self) -> String {
        "DeleteTaskPipeline".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.spawn_pipeline(ctx).await?;
        self.handle(DeletePipelineSuperviseLoop, ctx).await?;
        Ok(())
    }
}

impl DeleteTaskPipeline {
    pub fn new(
        index_id: String,
        metastore: Arc<dyn Metastore>,
        search_client_pool: SearchClientPool,
        indexing_settings: IndexingSettings,
        index_storage: Arc<dyn Storage>,
        delete_service_dir_path: PathBuf,
    ) -> Self {
        Self {
            index_id,
            metastore,
            search_client_pool,
            indexing_settings,
            index_storage,
            delete_service_dir_path,
            handles: Default::default(),
        }
    }

    pub async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        info!(
            index_id=%self.index_id,
            root_dir=%self.delete_service_dir_path.display(),
            "Spawning delete tasks pipeline.",
        );
        let index_metadata = self.metastore.index_metadata(&self.index_id).await?;
        let publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.metastore.clone(),
            None,
            None,
        );
        let (publisher_mailbox, publisher_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(publisher);
        let split_store =
            IndexingSplitStore::create_without_local_store(self.index_storage.clone());
        let uploader = Uploader::new(
            "MergeUploader",
            self.metastore.clone(),
            split_store.clone(),
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
        );
        let (uploader_mailbox, uploader_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(uploader);

        let doc_mapper = build_doc_mapper(
            &index_metadata.doc_mapping,
            &index_metadata.search_settings,
            &index_metadata.indexing_settings,
        )?;
        let tag_fields = doc_mapper.tag_named_fields()?;
        let packager = Packager::new("MergePackager", tag_fields, uploader_mailbox);
        let (packager_mailbox, packager_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(packager);
        let index_pipeline_id = IndexingPipelineId {
            index_id: self.index_id.to_string(),
            node_id: "unknown".to_string(),
            pipeline_ord: 0,
            source_id: "unknown".to_string(),
        };
        let delete_executor =
            MergeExecutor::new(index_pipeline_id, self.metastore.clone(), packager_mailbox);
        let (delete_executor_mailbox, task_executor_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(delete_executor);
        let indexing_directory_path = self.delete_service_dir_path.join(&self.index_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: indexing_directory.scratch_directory().clone(),
            split_store: split_store.clone(),
            executor_mailbox: delete_executor_mailbox,
        };
        let (downloader_mailbox, downloader_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(merge_split_downloader);
        let merge_policy = merge_policy_from_settings(&self.indexing_settings);
        let doc_mapper_str = serde_json::to_string(&doc_mapper)?;
        let task_planner = DeleteTaskPlanner::new(
            self.index_id.clone(),
            index_metadata.index_uri,
            doc_mapper_str,
            self.metastore.clone(),
            self.search_client_pool.clone(),
            merge_policy,
            downloader_mailbox,
        );
        let (_, task_planner_handler) = ctx
            .spawn_actor()
            .set_kill_switch(KillSwitch::default())
            .spawn(task_planner);
        self.handles = Some(DeletePipelineHandle {
            delete_task_planner: task_planner_handler,
            downloader: downloader_handler,
            delete_task_executor: task_executor_handler,
            packager: packager_handler,
            uploader: uploader_handler,
            publisher: publisher_handler,
        });
        Ok(())
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        if let Some(handles) = &self.handles {
            let supervisables: Vec<&dyn Supervisable> = vec![
                &handles.packager,
                &handles.uploader,
                &handles.publisher,
                &handles.delete_task_planner,
                &handles.downloader,
                &handles.delete_task_executor,
            ];
            supervisables
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug)]
struct DeletePipelineSuperviseLoop;

#[async_trait]
impl Handler<DeletePipelineSuperviseLoop> for DeleteTaskPipeline {
    type Reply = ();

    async fn handle(
        &mut self,
        _: DeletePipelineSuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.handles.is_some() {
            let actors_by_health: HashMap<Health, Vec<&str>> = self
                .supervisables()
                .into_iter()
                .group_by(|supervisable| supervisable.health())
                .into_iter()
                .map(|(health, group)| (health, group.map(|actor| actor.name()).collect()))
                .collect();

            if actors_by_health.contains_key(&Health::FailureOrUnhealthy) {
                error!(
                    index_id=?self.index_id,
                    healthy_actors=?actors_by_health.get(&Health::Healthy),
                    failed_or_unhealthy_actors=?actors_by_health.get(&Health::FailureOrUnhealthy),
                    success_actors=?actors_by_health.get(&Health::Success),
                    "Delete task pipeline failure."
                );
            } else {
                debug!(
                    index_id=?self.index_id,
                    healthy_actors=?actors_by_health.get(&Health::Healthy),
                    failed_or_unhealthy_actors=?actors_by_health.get(&Health::FailureOrUnhealthy),
                    success_actors=?actors_by_health.get(&Health::Success),
                    "Delete task pipeline running."
                );
            }
        }
        ctx.schedule_self_msg(SUPERVISE_DELAY, DeletePipelineSuperviseLoop)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::{Health, Universe};
    use quickwit_config::IndexingSettings;
    use quickwit_indexing::TestSandbox;
    use quickwit_metastore::SplitState;
    use quickwit_proto::metastore_api::DeleteQuery;
    use quickwit_proto::{LeafSearchRequest, LeafSearchResponse};
    use quickwit_search::{MockSearchService, SearchClientPool};

    use super::DeleteTaskPipeline;

    #[tokio::test]
    async fn test_delete_pipeline_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-pipeline-simple";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let metastore_uri = "ram:///delete-pipeline";
        let test_sandbox = TestSandbox::create(
            index_id,
            doc_mapping_yaml,
            "{}",
            &["body"],
            Some(metastore_uri),
        )
        .await?;
        let docs = vec![
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "delete", "ts": 0 }),
        ];
        test_sandbox.add_documents(docs).await?;
        let metastore = test_sandbox.metastore();
        metastore
            .create_delete_task(DeleteQuery {
                index_id: index_id.to_string(),
                start_timestamp: None,
                end_timestamp: None,
                query: "body:delete".to_string(),
                search_fields: Vec::new(),
            })
            .await?;
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request.search_request.as_ref().unwrap().index_id
                    == "test-delete-pipeline-simple"
            })
            .times(1)
            .returning(move |_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    ..Default::default()
                })
            });
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?;

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let mut indexing_settings = IndexingSettings::for_test();
        // Ensures that all split will be mature and thus be candidates for deletion.
        indexing_settings.split_num_docs_target = 1;
        let pipeline = DeleteTaskPipeline::new(
            index_id.to_string(),
            metastore.clone(),
            client_pool,
            indexing_settings,
            test_sandbox.storage(),
            data_dir_path,
        );
        let universe = Universe::new();

        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let state = pipeline_handler.process_pending_and_observe().await;
        let all_healthy = state
            .actors_health
            .values()
            .all(|health| health == &Health::Healthy);
        assert!(all_healthy);
        let splits = metastore.list_all_splits(index_id).await?;
        assert_eq!(splits.len(), 2);
        let published_split = splits
            .iter()
            .find(|split| split.split_state == SplitState::Published)
            .unwrap();
        assert_eq!(published_split.split_metadata.delete_opstamp, 1);
        Ok(())
    }
}
