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

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Supervisor, SupervisorState,
    HEARTBEAT,
};
use quickwit_common::io::IoControls;
use quickwit_config::{build_doc_mapper, IndexingSettings};
use quickwit_indexing::actors::{
    MergeExecutor, MergeSplitDownloader, Packager, Publisher, Uploader, UploaderType,
};
use quickwit_indexing::merge_policy::merge_policy_from_settings;
use quickwit_indexing::models::{IndexingDirectory, IndexingPipelineId};
use quickwit_indexing::{IndexingSplitStore, PublisherType, SplitsUpdateMailbox};
use quickwit_metastore::Metastore;
use quickwit_search::SearchClientPool;
use quickwit_storage::Storage;
use serde::Serialize;
use tokio::join;
use tracing::info;

use super::delete_task_planner::DeleteTaskPlanner;

struct DeletePipelineHandle {
    pub delete_task_planner: ActorHandle<Supervisor<DeleteTaskPlanner>>,
    pub downloader: ActorHandle<Supervisor<MergeSplitDownloader>>,
    pub delete_task_executor: ActorHandle<Supervisor<MergeExecutor>>,
    pub packager: ActorHandle<Supervisor<Packager>>,
    pub uploader: ActorHandle<Supervisor<Uploader>>,
    pub publisher: ActorHandle<Supervisor<Publisher>>,
}

/// A Struct to hold all statistical data about deletes.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DeleteTaskPipelineState {
    pub delete_task_planner: SupervisorState,
    pub downloader: SupervisorState,
    pub delete_task_executor: SupervisorState,
    pub packager: SupervisorState,
    pub uploader: SupervisorState,
    pub publisher: SupervisorState,
}

pub struct DeleteTaskPipeline {
    index_id: String,
    metastore: Arc<dyn Metastore>,
    search_client_pool: SearchClientPool,
    indexing_settings: IndexingSettings,
    index_storage: Arc<dyn Storage>,
    delete_service_dir_path: PathBuf,
    handles: Option<DeletePipelineHandle>,
    max_concurrent_split_uploads: usize,
    state: DeleteTaskPipelineState,
}

#[async_trait]
impl Actor for DeleteTaskPipeline {
    type ObservableState = DeleteTaskPipelineState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    fn name(&self) -> String {
        "DeleteTaskPipeline".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.spawn_pipeline(ctx).await?;
        self.handle(Observe, ctx).await?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        if let Some(handles) = self.handles.take() {
            join!(
                handles.delete_task_planner.quit(),
                handles.downloader.quit(),
                handles.delete_task_executor.quit(),
                handles.packager.quit(),
                handles.uploader.quit(),
                handles.publisher.quit(),
            );
        };
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
        max_concurrent_split_uploads: usize,
    ) -> Self {
        Self {
            index_id,
            metastore,
            search_client_pool,
            indexing_settings,
            index_storage,
            delete_service_dir_path,
            handles: Default::default(),
            max_concurrent_split_uploads,
            state: DeleteTaskPipelineState::default(),
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
        let (publisher_mailbox, publisher_supervisor_handler) =
            ctx.spawn_actor().supervise(publisher);
        let split_store =
            IndexingSplitStore::create_without_local_store(self.index_storage.clone());
        let uploader = Uploader::new(
            UploaderType::DeleteUploader,
            self.metastore.clone(),
            split_store.clone(),
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            self.max_concurrent_split_uploads,
        );
        let (uploader_mailbox, uploader_supervisor_handler) = ctx.spawn_actor().supervise(uploader);

        let doc_mapper = build_doc_mapper(
            &index_metadata.doc_mapping,
            &index_metadata.search_settings,
            &index_metadata.indexing_settings,
        )?;
        let tag_fields = doc_mapper.tag_named_fields()?;
        let packager = Packager::new("MergePackager", tag_fields, uploader_mailbox);
        let (packager_mailbox, packager_supervisor_handler) = ctx.spawn_actor().supervise(packager);
        let index_pipeline_id = IndexingPipelineId {
            index_id: self.index_id.to_string(),
            node_id: "unknown".to_string(),
            pipeline_ord: 0,
            source_id: "unknown".to_string(),
        };
        let throughput_limit: f64 = index_metadata
            .indexing_settings
            .resources
            .max_janitor_write_throughput
            .as_ref()
            .map(|bytes_per_sec| bytes_per_sec.get_bytes() as f64)
            .unwrap_or(f64::INFINITY);
        let delete_executor_io_controls = IoControls::default()
            .set_throughput_limit(throughput_limit)
            .set_index_and_component(self.index_id.as_str(), "deleter");
        let split_download_io_controls = delete_executor_io_controls
            .clone()
            .set_index_and_component(self.index_id.as_str(), "split_downloader_delete");
        let delete_executor = MergeExecutor::new(
            index_pipeline_id,
            self.metastore.clone(),
            doc_mapper.clone(),
            delete_executor_io_controls,
            packager_mailbox,
        );
        let (delete_executor_mailbox, task_executor_supervisor_handler) =
            ctx.spawn_actor().supervise(delete_executor);
        let indexing_directory_path = self.delete_service_dir_path.join(&self.index_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: indexing_directory.scratch_directory().clone(),
            split_store: split_store.clone(),
            executor_mailbox: delete_executor_mailbox,
            io_controls: split_download_io_controls,
        };
        let (downloader_mailbox, downloader_supervisor_handler) =
            ctx.spawn_actor().supervise(merge_split_downloader);
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
        let (_, task_planner_supervisor_handler) = ctx.spawn_actor().supervise(task_planner);
        self.handles = Some(DeletePipelineHandle {
            delete_task_planner: task_planner_supervisor_handler,
            downloader: downloader_supervisor_handler,
            delete_task_executor: task_executor_supervisor_handler,
            packager: packager_supervisor_handler,
            uploader: uploader_supervisor_handler,
            publisher: publisher_supervisor_handler,
        });
        Ok(())
    }
}

#[derive(Debug)]
struct Observe;

#[async_trait]
impl Handler<Observe> for DeleteTaskPipeline {
    type Reply = ();
    async fn handle(
        &mut self,
        _: Observe,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(handles) = &self.handles {
            let (
                delete_task_planner,
                downloader,
                delete_task_executor,
                packager,
                uploader,
                publisher,
            ) = join!(
                handles.delete_task_planner.observe(),
                handles.downloader.observe(),
                handles.delete_task_executor.observe(),
                handles.packager.observe(),
                handles.uploader.observe(),
                handles.publisher.observe(),
            );
            self.state = DeleteTaskPipelineState {
                delete_task_planner: delete_task_planner.state,
                downloader: downloader.state,
                delete_task_executor: delete_task_executor.state,
                packager: packager.state,
                uploader: uploader.state,
                publisher: publisher.state,
            }
        }
        // Supervisors supervise every `HEARTBEAT`. We can wait a bit more to observe supervisors.
        ctx.schedule_self_msg(HEARTBEAT, Observe).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use quickwit_actors::{Handler, Universe, HEARTBEAT};
    use quickwit_config::merge_policy_config::MergePolicyConfig;
    use quickwit_config::IndexingSettings;
    use quickwit_indexing::TestSandbox;
    use quickwit_metastore::SplitState;
    use quickwit_proto::metastore_api::DeleteQuery;
    use quickwit_proto::{LeafSearchRequest, LeafSearchResponse};
    use quickwit_search::{MockSearchService, SearchClientPool, SearchError};

    use super::{ActorContext, ActorExitStatus, DeleteTaskPipeline};

    #[derive(Debug)]
    struct GracefulShutdown;

    #[async_trait]
    impl Handler<GracefulShutdown> for DeleteTaskPipeline {
        type Reply = ();
        async fn handle(
            &mut self,
            _: GracefulShutdown,
            _: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            if let Some(handles) = self.handles.take() {
                handles.delete_task_planner.quit().await;
                handles.publisher.join().await;
            }
            // Nothing to do.
            Err(ActorExitStatus::Success)
        }
    }

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
        .await
        .unwrap();
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
            .await
            .unwrap();
        let mut mock_search_service = MockSearchService::new();
        let mut leaf_search_num_failures = 1;
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request.search_request.as_ref().unwrap().index_id
                    == "test-delete-pipeline-simple"
            })
            .times(2)
            .returning(move |_: LeafSearchRequest| {
                if leaf_search_num_failures > 0 {
                    leaf_search_num_failures -= 1;
                    return Err(SearchError::InternalError("leaf search error".to_string()));
                }
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    ..Default::default()
                })
            });
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?;

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.merge_policy = MergePolicyConfig::Nop;
        let pipeline = DeleteTaskPipeline::new(
            index_id.to_string(),
            metastore.clone(),
            client_pool,
            indexing_settings,
            test_sandbox.storage(),
            data_dir_path,
            4,
        );
        let universe = Universe::new();

        let (pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        // Insure that the message sent by initialize method is processed.
        let _ = pipeline_handler.process_pending_and_observe().await.state;
        // Pipeline will first fail and we need to wait a HEARTBEAT * 2 for the pipeline state to be
        // updated.
        universe.simulate_time_shift(HEARTBEAT * 2).await;
        let pipeline_state = pipeline_handler.process_pending_and_observe().await.state;
        assert_eq!(pipeline_state.delete_task_planner.num_errors, 1);
        assert_eq!(pipeline_state.downloader.num_errors, 0);
        assert_eq!(pipeline_state.delete_task_executor.num_errors, 0);
        assert_eq!(pipeline_state.packager.num_errors, 0);
        assert_eq!(pipeline_state.uploader.num_errors, 0);
        assert_eq!(pipeline_state.publisher.num_errors, 0);
        let _ = pipeline_mailbox.send_message(GracefulShutdown).await;
        // Time shifting to speed up the test.
        universe.simulate_time_shift(HEARTBEAT * 10).await;
        let splits = metastore.list_all_splits(index_id).await?;
        assert_eq!(splits.len(), 2);
        let published_split = splits
            .iter()
            .find(|split| split.split_state == SplitState::Published)
            .unwrap();
        assert_eq!(published_split.split_metadata.delete_opstamp, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_pipeline_shut_down() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-pipeline-shut-down";
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
        .await
        .unwrap();
        let metastore = test_sandbox.metastore();
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request.search_request.as_ref().unwrap().index_id
                    == "test-delete-pipeline-shut-down"
            })
            .returning(move |_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 0,
                    ..Default::default()
                })
            });
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?;

        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexing_settings = IndexingSettings::for_test();
        let pipeline = DeleteTaskPipeline::new(
            index_id.to_string(),
            metastore.clone(),
            client_pool,
            indexing_settings,
            test_sandbox.storage(),
            data_dir_path,
            4,
        );
        let universe = Universe::new();

        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        pipeline_handler.quit().await;
        let observations = universe.observe(HEARTBEAT).await;
        // Once the pipeline is properly shut down, the only remaining actor is the scheduler.
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].type_name,
            "quickwit_actors::scheduler::Scheduler"
        );
        Ok(())
    }
}
