// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Mailbox, Supervisor,
    SupervisorState,
};
use quickwit_common::io::IoControls;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::{self};
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_indexing::actors::{
    MergeExecutor, MergeSchedulerService, MergeSplitDownloader, Packager, Publisher,
    PublisherCounters, Uploader, UploaderCounters, UploaderType,
};
use quickwit_indexing::merge_policy::merge_policy_from_settings;
use quickwit_indexing::{IndexingSplitStore, PublisherType, SplitsUpdateMailbox};
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId};
use quickwit_search::SearchJobPlacer;
use quickwit_storage::Storage;
use serde::Serialize;
use tokio::join;
use tracing::info;

use super::delete_task_planner::DeleteTaskPlanner;
use crate::actors::delete_task_planner::DeleteTaskPlannerState;

const OBSERVE_PIPELINE_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(500)
} else {
    // 1 minute.
    // This is only for observation purpose, not supervision.
    Duration::from_secs(60)
};

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
    pub delete_task_planner: SupervisorState<DeleteTaskPlannerState>,
    pub downloader: SupervisorState<()>,
    pub delete_task_executor: SupervisorState<()>,
    pub packager: SupervisorState<()>,
    pub uploader: SupervisorState<UploaderCounters>,
    pub publisher: SupervisorState<PublisherCounters>,
}

pub struct DeleteTaskPipeline {
    index_uid: IndexUid,
    metastore: MetastoreServiceClient,
    search_job_placer: SearchJobPlacer,
    index_storage: Arc<dyn Storage>,
    delete_service_task_dir: PathBuf,
    handles: Option<DeletePipelineHandle>,
    max_concurrent_split_uploads: usize,
    state: DeleteTaskPipelineState,
    merge_scheduler_service: Mailbox<MergeSchedulerService>,
    event_broker: EventBroker,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index_uid: IndexUid,
        metastore: MetastoreServiceClient,
        search_job_placer: SearchJobPlacer,
        index_storage: Arc<dyn Storage>,
        delete_service_task_dir: PathBuf,
        max_concurrent_split_uploads: usize,
        merge_scheduler_service: Mailbox<MergeSchedulerService>,
        event_broker: EventBroker,
    ) -> Self {
        Self {
            index_uid,
            metastore,
            search_job_placer,
            index_storage,
            delete_service_task_dir,
            handles: Default::default(),
            max_concurrent_split_uploads,
            state: DeleteTaskPipelineState::default(),
            merge_scheduler_service,
            event_broker,
        }
    }

    pub async fn spawn_pipeline(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        info!(
            index_uid=%self.index_uid,
            root_dir=%self.delete_service_task_dir.to_str().unwrap(),
            "spawning delete tasks pipeline",
        );
        let index_config = self
            .metastore
            .index_metadata(IndexMetadataRequest::for_index_uid(self.index_uid.clone()))
            .await?
            .deserialize_index_metadata()?
            .into_index_config();
        let publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.metastore.clone(),
            None,
            None,
        );
        let (publisher_mailbox, publisher_supervisor_handler) =
            ctx.spawn_actor().supervise(publisher);
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(self.index_storage.clone());
        let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
        let uploader = Uploader::new(
            UploaderType::DeleteUploader,
            self.metastore.clone(),
            merge_policy,
            index_config.retention_policy_opt.clone(),
            split_store.clone(),
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            self.max_concurrent_split_uploads,
            self.event_broker.clone(),
        );
        let (uploader_mailbox, uploader_supervisor_handler) = ctx.spawn_actor().supervise(uploader);

        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
        let tag_fields = doc_mapper.tag_named_fields()?;
        let packager = Packager::new("MergePackager", tag_fields, uploader_mailbox);
        let (packager_mailbox, packager_supervisor_handler) = ctx.spawn_actor().supervise(packager);
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from("unknown"),
            index_uid: self.index_uid.clone(),
            source_id: "unknown".to_string(),
        };

        let delete_executor_io_controls = IoControls::default().set_component("deleter");

        let split_download_io_controls = delete_executor_io_controls
            .clone()
            .set_component("split_downloader_delete");
        let delete_executor = MergeExecutor::new(
            pipeline_id,
            self.metastore.clone(),
            doc_mapper.clone(),
            delete_executor_io_controls,
            packager_mailbox,
        );
        let (delete_executor_mailbox, task_executor_supervisor_handler) =
            ctx.spawn_actor().supervise(delete_executor);
        let scratch_directory = temp_dir::Builder::default()
            .join(&self.index_uid.index_id)
            .join(&self.index_uid.incarnation_id.to_string())
            .tempdir_in(&self.delete_service_task_dir)?;
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory,
            split_store,
            executor_mailbox: delete_executor_mailbox,
            io_controls: split_download_io_controls,
        };
        let (downloader_mailbox, downloader_supervisor_handler) =
            ctx.spawn_actor().supervise(merge_split_downloader);
        let doc_mapper_str = serde_json::to_string(&doc_mapper)?;
        let index_uri: &Uri = &index_config.index_uri;
        let task_planner = DeleteTaskPlanner::new(
            self.index_uid.clone(),
            index_uri.clone(),
            doc_mapper_str,
            self.metastore.clone(),
            self.search_job_placer.clone(),
            downloader_mailbox,
            self.merge_scheduler_service.clone(),
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
            handles.delete_task_planner.refresh_observe();
            handles.downloader.refresh_observe();
            handles.delete_task_executor.refresh_observe();
            handles.packager.refresh_observe();
            handles.uploader.refresh_observe();
            handles.publisher.refresh_observe();
            self.state = DeleteTaskPipelineState {
                delete_task_planner: handles.delete_task_planner.last_observation().clone(),
                downloader: handles.downloader.last_observation().clone(),
                delete_task_executor: handles.delete_task_executor.last_observation().clone(),
                packager: handles.packager.last_observation().clone(),
                uploader: handles.uploader.last_observation().clone(),
                publisher: handles.publisher.last_observation().clone(),
            }
        }
        ctx.schedule_self_msg(OBSERVE_PIPELINE_INTERVAL, Observe);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use quickwit_actors::{Handler, Universe};
    use quickwit_common::pubsub::EventBroker;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_indexing::TestSandbox;
    use quickwit_indexing::actors::MergeSchedulerService;
    use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitState};
    use quickwit_proto::metastore::{DeleteQuery, ListSplitsRequest, MetastoreService};
    use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};
    use quickwit_search::{
        MockSearchService, SearchError, SearchJobPlacer, searcher_pool_for_test,
    };

    use super::{ActorContext, ActorExitStatus, DeleteTaskPipeline, OBSERVE_PIPELINE_INTERVAL};

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
        let indexing_settings_yaml = r#"
            merge_policy:
                type: no_merge
        "#;
        let test_sandbox = TestSandbox::create(
            index_id,
            doc_mapping_yaml,
            indexing_settings_yaml,
            &["body"],
        )
        .await
        .unwrap();
        let universe: &Universe = test_sandbox.universe();
        let merge_scheduler_service = universe.get_or_spawn_one::<MergeSchedulerService>();
        let index_uid = test_sandbox.index_uid();
        let docs = vec![
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "delete", "ts": 0 }),
        ];
        test_sandbox.add_documents(docs).await?;
        let metastore = test_sandbox.metastore();
        metastore
            .create_delete_task(DeleteQuery {
                index_uid: Some(index_uid.clone()),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: quickwit_query::query_ast::qast_json_helper("body:delete", &[]),
            })
            .await
            .unwrap();
        let mut mock_search_service = MockSearchService::new();
        let mut leaf_search_num_failures = 1;
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request
                    .search_request
                    .as_ref()
                    .unwrap()
                    .index_id_patterns
                    == vec!["test-delete-pipeline-simple".to_string()]
            })
            .times(2)
            .returning(move |_: LeafSearchRequest| {
                if leaf_search_num_failures > 0 {
                    leaf_search_num_failures -= 1;
                    return Err(SearchError::Internal("leaf search error".to_string()));
                }
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let delete_service_task_dir = TempDirectory::for_test();
        let pipeline = DeleteTaskPipeline::new(
            test_sandbox.index_uid(),
            metastore.clone(),
            search_job_placer,
            test_sandbox.storage(),
            delete_service_task_dir.path().into(),
            4,
            merge_scheduler_service,
            EventBroker::default(),
        );

        let (pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        // Ensure that the message sent by initialize method is processed.
        let _ = pipeline_handler.process_pending_and_observe().await.state;
        // Pipeline will first fail and we need to wait a OBSERVE_PIPELINE_INTERVAL * some number
        // for the pipeline state to be updated.
        universe.sleep(OBSERVE_PIPELINE_INTERVAL * 5).await;
        let pipeline_state = pipeline_handler.process_pending_and_observe().await.state;
        assert_eq!(pipeline_state.delete_task_planner.metrics.num_errors, 1);
        assert_eq!(pipeline_state.downloader.metrics.num_errors, 0);
        assert_eq!(pipeline_state.delete_task_executor.metrics.num_errors, 0);
        assert_eq!(pipeline_state.packager.metrics.num_errors, 0);
        assert_eq!(pipeline_state.uploader.metrics.num_errors, 0);
        assert_eq!(pipeline_state.publisher.metrics.num_errors, 0);
        let _ = pipeline_mailbox.ask(GracefulShutdown).await;

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 2);
        let published_split = splits
            .iter()
            .find(|split| split.split_state == SplitState::Published)
            .unwrap();
        assert_eq!(published_split.split_metadata.delete_opstamp, 1);
        test_sandbox.assert_quit().await;
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
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"])
            .await
            .unwrap();
        let universe: &Universe = test_sandbox.universe();
        let merge_scheduler_mailbox = universe.get_or_spawn_one::<MergeSchedulerService>();
        let metastore = test_sandbox.metastore();
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request
                    .search_request
                    .as_ref()
                    .unwrap()
                    .index_id_patterns
                    == vec!["test-delete-pipeline-shut-down".to_string()]
            })
            .returning(move |_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 0,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let delete_service_task_dir = TempDirectory::for_test();
        let pipeline = DeleteTaskPipeline::new(
            test_sandbox.index_uid(),
            metastore.clone(),
            search_job_placer,
            test_sandbox.storage(),
            delete_service_task_dir.path().into(),
            4,
            merge_scheduler_mailbox,
            EventBroker::default(),
        );

        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_builder().spawn(pipeline);
        pipeline_handler.quit().await;
        let observations = universe.observe(OBSERVE_PIPELINE_INTERVAL).await;
        assert!(observations.into_iter().all(
            |observation| observation.type_name != std::any::type_name::<DeleteTaskPipeline>()
        ));
        test_sandbox.assert_quit().await;
        Ok(())
    }
}
