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

use std::sync::Arc;

use quickwit_actors::{ActorHandle, Health, SpawnContext, Supervisable};
use quickwit_common::KillSwitch;
use quickwit_common::io::{IoControls, Limiter};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::RetentionPolicy;
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::actors::{
    MergeExecutor, MergeSplitDownloader, Packager, Publisher, Uploader, UploaderType,
};
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_indexing::{IndexingSplitStore, PublisherType, SplitsUpdateMailbox};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, SourceId, SplitId};
use tracing::{debug, error, info};

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineStatus {
    InProgress { retry_count: usize },
    Completed,
    Failed { error: String },
}

pub struct PipelineStatusUpdate {
    pub task_id: String,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub split_ids: Vec<SplitId>,
    pub merged_split_id: SplitId,
    pub status: PipelineStatus,
}

struct CompactionPipelineHandles {
    merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    merge_executor: ActorHandle<MergeExecutor>,
    merge_packager: ActorHandle<Packager>,
    merge_uploader: ActorHandle<Uploader>,
    merge_publisher: ActorHandle<Publisher>,
}

/// A single-use compaction pipeline. Processes one merge task and terminates.
///
/// Owned by the `CompactorSupervisor`, which periodically calls
/// `check_actor_health()` to collect status updates. The pipeline manages
/// its own retry logic internally.
pub struct CompactionPipeline {
    task_id: String,
    merge_operation: MergeOperation,
    pipeline_id: MergePipelineId,
    status: PipelineStatus,
    max_retries: usize,
    kill_switch: KillSwitch,
    scratch_directory: TempDirectory,
    handles: Option<CompactionPipelineHandles>,
    doc_mapper: Arc<DocMapper>,
    merge_policy: Arc<dyn quickwit_indexing::merge_policy::MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    metastore: MetastoreServiceClient,
    split_store: IndexingSplitStore,
    io_throughput_limiter: Option<Limiter>,
    max_concurrent_split_uploads: usize,
    event_broker: EventBroker,
}

impl CompactionPipeline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: String,
        scratch_directory: TempDirectory,
        merge_operation: MergeOperation,
        pipeline_id: MergePipelineId,
        doc_mapper: Arc<DocMapper>,
        merge_policy: Arc<dyn quickwit_indexing::merge_policy::MergePolicy>,
        retention_policy: Option<RetentionPolicy>,
        metastore: MetastoreServiceClient,
        split_store: IndexingSplitStore,
        io_throughput_limiter: Option<Limiter>,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
        max_retries: usize,
    ) -> Self {
        CompactionPipeline {
            task_id,
            status: PipelineStatus::InProgress { retry_count: 0 },
            max_retries,
            kill_switch: KillSwitch::default(),
            scratch_directory,
            handles: None,
            merge_operation,
            pipeline_id,
            doc_mapper,
            merge_policy,
            retention_policy,
            metastore,
            split_store,
            io_throughput_limiter,
            max_concurrent_split_uploads,
            event_broker,
        }
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        let Some(handles) = &self.handles else {
            return Vec::new();
        };
        vec![
            &handles.merge_split_downloader,
            &handles.merge_executor,
            &handles.merge_packager,
            &handles.merge_uploader,
            &handles.merge_publisher,
        ]
    }

    /// Returns pipeline status update by checking the health of individual pipeline actors.
    ///
    /// If the pipeline is already completed or failed (terminal status), returns the status
    /// without re-checking actors. Otherwise checks actor health and:
    /// - Restarts the pipeline on failure if retries remain.
    /// - Marks the pipeline as completed or failed when appropriate.
    pub fn pipeline_status_update(
        &mut self,
        spawn_ctx: &SpawnContext,
    ) -> PipelineStatusUpdate {
        self.update_status(spawn_ctx);
        self.build_status_update()
    }

    fn update_status(&mut self, spawn_ctx: &SpawnContext) {
        // Pipeline is finished but yet to be cleaned up.
        if matches!(
            self.status,
            PipelineStatus::Completed | PipelineStatus::Failed { .. }
        ) {
            return;
        }
        // Pipeline is not initialized yet.
        if self.handles.is_none() {
            return;
        }

        let mut healthy_actors: Vec<String> = Vec::new();
        let mut failure_or_unhealthy_actors: Vec<String> = Vec::new();
        let mut success_actors: Vec<String> = Vec::new();

        for supervisable in self.supervisables() {
            match supervisable.check_health(true) {
                Health::Healthy => {
                    healthy_actors.push(supervisable.name().to_string());
                }
                Health::FailureOrUnhealthy => {
                    failure_or_unhealthy_actors.push(supervisable.name().to_string());
                }
                Health::Success => {
                    success_actors.push(supervisable.name().to_string());
                }
            }
        }

        if !failure_or_unhealthy_actors.is_empty() {
            let PipelineStatus::InProgress { retry_count } = self.status else {
                return;
            };
            if retry_count < self.max_retries {
                let new_retry_count = retry_count + 1;
                info!(
                    task_id=%self.task_id,
                    retry_count=%new_retry_count,
                    failed_actors=?failure_or_unhealthy_actors,
                    "retrying compaction pipeline"
                );
                self.restart(spawn_ctx);
                self.status = PipelineStatus::InProgress {
                    retry_count: new_retry_count,
                };
                return;
            }
            let error_msg = format!(
                "exhausted retries, failed actors: {:?}",
                failure_or_unhealthy_actors
            );
            error!(task_id=%self.task_id, retry_count=%retry_count, "{error_msg}");
            self.status = PipelineStatus::Failed { error: error_msg };
            return;
        }
        if healthy_actors.is_empty() {
            debug!(task_id=%self.task_id, "all compaction pipeline actors completed");
            self.status = PipelineStatus::Completed;
        }
    }

    fn build_status_update(&self) -> PipelineStatusUpdate {
        PipelineStatusUpdate {
            task_id: self.task_id.clone(),
            index_uid: self.pipeline_id.index_uid.clone(),
            source_id: self.pipeline_id.source_id.clone(),
            split_ids: self
                .merge_operation
                .splits_as_slice()
                .iter()
                .map(|split| split.split_id().to_string())
                .collect(),
            merged_split_id: self.merge_operation.merge_split_id.clone(),
            status: self.status.clone(),
        }
    }

    /// Terminates the current actor chain and re-spawns with a fresh kill
    /// switch. Downloaded splits remain on disk in the scratch directory.
    fn restart(&mut self, spawn_ctx: &SpawnContext) {
        self.kill_switch.kill();
        self.handles = None;
        self.kill_switch = KillSwitch::default();
        // TODO: handle spawn failure gracefully instead of panicking.
        self.spawn_pipeline(spawn_ctx)
            .expect("failed to respawn compaction pipeline");
    }

    /// Spawns the 5-actor merge execution chain and sends the `MergeOperation`
    /// to the downloader to kick off execution.
    pub(crate) fn spawn_pipeline(&mut self, spawn_ctx: &SpawnContext) -> anyhow::Result<()> {
        info!(
            task_id=%self.task_id,
            pipeline_id=%self.pipeline_id,
            "spawning compaction pipeline"
        );

        // Publisher (no merge planner feedback, no source)
        let merge_publisher = Publisher::new(
            PublisherType::MergePublisher,
            self.metastore.clone(),
            None,
            None,
        );
        let (merge_publisher_mailbox, merge_publisher_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(merge_publisher);

        // Uploader
        let merge_uploader = Uploader::new(
            UploaderType::MergeUploader,
            self.metastore.clone(),
            self.merge_policy.clone(),
            self.retention_policy.clone(),
            self.split_store.clone(),
            SplitsUpdateMailbox::from(merge_publisher_mailbox),
            self.max_concurrent_split_uploads,
            self.event_broker.clone(),
        );
        let (merge_uploader_mailbox, merge_uploader_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(merge_uploader);

        // Packager
        let tag_fields = self.doc_mapper.tag_named_fields()?;
        let merge_packager = Packager::new("MergePackager", tag_fields, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(merge_packager);

        // MergeExecutor
        let split_downloader_io_controls = IoControls::default()
            .set_throughput_limiter_opt(self.io_throughput_limiter.clone())
            .set_component("split_downloader_merge");
        let merge_executor_io_controls =
            split_downloader_io_controls.clone().set_component("merger");

        let merge_executor = MergeExecutor::new(
            self.pipeline_id.clone(),
            self.metastore.clone(),
            self.doc_mapper.clone(),
            merge_executor_io_controls,
            merge_packager_mailbox,
        );
        let (merge_executor_mailbox, merge_executor_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(merge_executor);

        // MergeSplitDownloader
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.scratch_directory.clone(),
            split_store: self.split_store.clone(),
            executor_mailbox: merge_executor_mailbox,
            io_controls: split_downloader_io_controls,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handle) = spawn_ctx
            .spawn_builder()
            .set_kill_switch(self.kill_switch.child())
            .spawn(merge_split_downloader);

        // Kick off the pipeline.
        merge_split_downloader_mailbox
            .try_send_message(self.merge_operation.clone())
            .map_err(|err| {
                anyhow::anyhow!("failed to send merge operation to downloader: {err:?}")
            })?;

        self.handles = Some(CompactionPipelineHandles {
            merge_split_downloader: merge_split_downloader_handle,
            merge_executor: merge_executor_handle,
            merge_packager: merge_packager_handle,
            merge_uploader: merge_uploader_handle,
            merge_publisher: merge_publisher_handle,
        });

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use quickwit_common::pubsub::EventBroker;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_indexing::IndexingSplitStore;
    use quickwit_indexing::merge_policy::{MergeOperation, default_merge_policy};
    use quickwit_metastore::SplitMetadata;
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, NodeId};
    use quickwit_storage::RamStorage;

    use quickwit_actors::Universe;

    use super::{CompactionPipeline, PipelineStatus};

    pub fn test_pipeline(
        task_id: &str,
        split_ids: &[&str],
        max_retries: usize,
    ) -> CompactionPipeline {
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let splits: Vec<SplitMetadata> = split_ids
            .iter()
            .map(|id| SplitMetadata::for_test(id.to_string()))
            .collect();
        let merge_operation = MergeOperation::new_merge_operation(splits);
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        CompactionPipeline::new(
            task_id.to_string(),
            TempDirectory::for_test(),
            merge_operation,
            pipeline_id,
            Arc::new(default_doc_mapper_for_test()),
            default_merge_policy(),
            None,
            metastore,
            split_store,
            None,
            2,
            EventBroker::default(),
            max_retries,
        )
    }

    #[tokio::test]
    async fn test_spawn_pipeline_creates_handles() {
        let universe = Universe::new();
        let mut pipeline = test_pipeline("task-1", &["split-1", "split-2"], 2);
        assert!(pipeline.handles.is_none());
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        assert!(pipeline.handles.is_some());
    }

    #[tokio::test]
    async fn test_status_update_unspawned_pipeline() {
        let universe = Universe::new();
        let mut pipeline = test_pipeline("task-1", &["split-1"], 2);
        let update = pipeline.pipeline_status_update(universe.spawn_ctx());
        assert_eq!(update.status, PipelineStatus::InProgress { retry_count: 0 });
        assert_eq!(update.task_id, "task-1");
        assert_eq!(update.split_ids, vec!["split-1"]);
        assert_eq!(update.source_id, "test-source");
        assert_eq!(update.index_uid, IndexUid::for_test("test-index", 0));
    }

    #[tokio::test]
    async fn test_status_update_healthy_pipeline() {
        let universe = Universe::new();
        let mut pipeline = test_pipeline("task-1", &["split-1"], 2);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        let update = pipeline.pipeline_status_update(universe.spawn_ctx());
        assert_eq!(update.status, PipelineStatus::InProgress { retry_count: 0 });
    }

    #[tokio::test]
    async fn test_killed_pipeline_with_retries_restarts() {
        let universe = Universe::new();
        let mut pipeline = test_pipeline("task-1", &["split-1"], 2);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();

        pipeline.kill_switch.kill();
        // Let actors process the kill signal.
        tokio::task::yield_now().await;
        let update = pipeline.pipeline_status_update(universe.spawn_ctx());
        assert_eq!(update.status, PipelineStatus::InProgress { retry_count: 1 });
        assert!(pipeline.handles.is_some());
    }

    #[tokio::test]
    async fn test_killed_pipeline_exhausted_retries_fails() {
        let universe = Universe::new();
        let mut pipeline = test_pipeline("task-1", &["split-1"], 0);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();

        pipeline.kill_switch.kill();
        // Let actors process the kill signal.
        tokio::task::yield_now().await;
        let update = pipeline.pipeline_status_update(universe.spawn_ctx());
        assert!(matches!(update.status, PipelineStatus::Failed { .. }));

        // Calling again still returns Failed (sticky).
        let update = pipeline.pipeline_status_update(universe.spawn_ctx());
        assert!(matches!(update.status, PipelineStatus::Failed { .. }));
    }
}
