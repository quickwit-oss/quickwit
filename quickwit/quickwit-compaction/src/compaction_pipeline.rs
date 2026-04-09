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

use quickwit_actors::{ActorContext, ActorHandle, Health, Supervisable};
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
use tracing::{debug, error, info};

use crate::CompactorSupervisor;

pub struct CompactionPipelineHandles {
    pub merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    pub merge_executor: ActorHandle<MergeExecutor>,
    pub merge_packager: ActorHandle<Packager>,
    pub merge_uploader: ActorHandle<Uploader>,
    pub merge_publisher: ActorHandle<Publisher>,
}

/// A single-use merge execution pipeline. Processes one merge task and
/// terminates.
///
/// Owned by the `CompactorSupervisor`, which periodically calls
/// `check_actor_health()` and acts on the result (retry, reap, etc.).
pub struct CompactionPipeline {
    pub task_id: String,
    pub retry_count: usize,
    pub kill_switch: KillSwitch,
    pub scratch_directory: TempDirectory,
    pub handles: Option<CompactionPipelineHandles>,

    // Per-task parameters.
    pub merge_operation: MergeOperation,
    pub pipeline_id: MergePipelineId,
    pub doc_mapper: Arc<DocMapper>,
    pub merge_policy: Arc<dyn quickwit_indexing::merge_policy::MergePolicy>,
    pub retention_policy: Option<RetentionPolicy>,

    // Shared resources (cloned from CompactorSupervisor).
    pub metastore: MetastoreServiceClient,
    pub split_store: IndexingSplitStore,
    pub io_throughput_limiter: Option<Limiter>,
    pub max_concurrent_split_uploads: usize,
    pub event_broker: EventBroker,
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
    ) -> Self {
        CompactionPipeline {
            task_id,
            retry_count: 0,
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

    /// Checks child actor health.
    ///
    /// `check_for_progress` controls whether stall detection is performed
    /// (actors that are alive but haven't recorded progress since last check).
    /// The supervisor controls the cadence of progress checks.
    ///
    /// Returns:
    /// - `Success` when all actors have completed (merge published).
    /// - `FailureOrUnhealthy` when any actor has died or stalled.
    /// - `Healthy` when actors are running and making progress.
    pub fn check_actor_health(&self) -> Health {
        if self.handles.is_none() {
            return Health::Healthy;
        }

        let mut healthy_actors: Vec<&str> = Vec::new();
        let mut failure_or_unhealthy_actors: Vec<&str> = Vec::new();
        let mut success_actors: Vec<&str> = Vec::new();

        for supervisable in self.supervisables() {
            match supervisable.check_health(true) {
                Health::Healthy => {
                    healthy_actors.push(supervisable.name());
                }
                Health::FailureOrUnhealthy => {
                    failure_or_unhealthy_actors.push(supervisable.name());
                }
                Health::Success => {
                    success_actors.push(supervisable.name());
                }
            }
        }

        if !failure_or_unhealthy_actors.is_empty() {
            error!(
                task_id=%self.task_id,
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "compaction pipeline actor failure detected"
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            debug!(task_id=%self.task_id, "all compaction pipeline actors completed");
            return Health::Success;
        }
        Health::Healthy
    }

    pub async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handles) = self.handles.take() {
            tokio::join!(
                handles.merge_split_downloader.kill(),
                handles.merge_executor.kill(),
                handles.merge_packager.kill(),
                handles.merge_uploader.kill(),
                handles.merge_publisher.kill(),
            );
        }
    }

    /// Terminates the current actor chain, increments retry count, and
    /// re-spawns. Downloaded splits remain on disk in the scratch directory.
    pub async fn restart(&mut self, ctx: &ActorContext<CompactorSupervisor>) {
        self.terminate().await;
        self.retry_count += 1;
        if let Err(err) = self.spawn_pipeline(ctx) {
            error!(task_id=%self.task_id, error=?err, "failed to respawn compaction pipeline");
        }
    }

    /// Spawns the 5-actor merge execution chain and sends the `MergeOperation`
    /// to the downloader to kick off execution.
    fn spawn_pipeline(&mut self, ctx: &ActorContext<CompactorSupervisor>) -> anyhow::Result<()> {
        self.kill_switch = ctx.kill_switch().child();

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
        let (merge_publisher_mailbox, merge_publisher_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
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
        let (merge_uploader_mailbox, merge_uploader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_uploader);

        // Packager
        let tag_fields = self.doc_mapper.tag_named_fields()?;
        let merge_packager = Packager::new("MergePackager", tag_fields, merge_uploader_mailbox);
        let (merge_packager_mailbox, merge_packager_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
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
        let (merge_executor_mailbox, merge_executor_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
            .spawn(merge_executor);

        // MergeSplitDownloader
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory: self.scratch_directory.clone(),
            split_store: self.split_store.clone(),
            executor_mailbox: merge_executor_mailbox,
            io_controls: split_downloader_io_controls,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handle) = ctx
            .spawn_actor()
            .set_kill_switch(self.kill_switch.clone())
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
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Health;
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

    use super::CompactionPipeline;

    fn test_pipeline() -> CompactionPipeline {
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let splits = vec![SplitMetadata::for_test("split-1".to_string())];
        let merge_operation = MergeOperation::new_merge_operation(splits);
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        CompactionPipeline::new(
            "test-task".to_string(),
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
        )
    }

    #[test]
    fn test_pipeline_no_handles_is_healthy() {
        let pipeline = test_pipeline();
        assert!(pipeline.handles.is_none());
        assert_eq!(pipeline.check_actor_health(), Health::Healthy);
    }

    #[tokio::test]
    async fn test_pipeline_terminate_without_handles() {
        let mut pipeline = test_pipeline();
        pipeline.terminate().await;
        assert!(pipeline.handles.is_none());
    }
}
