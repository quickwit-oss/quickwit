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

use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_common::io::Limiter;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_indexing::IndexingSplitStore;
use quickwit_proto::compaction::{
    CompactionServiceClient, CompletedCompaction, FailedCompaction, InProgressCompaction,
    WorkerStatusUpdateRequest,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use tracing::info;

use crate::compaction_pipeline::{CompactionPipeline, PipelineStatus, PipelineStatusUpdate};

const CHECK_PIPELINE_STATUSES_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct CheckPipelineStatuses;

/// Manages a pool of `CompactionPipeline`s, each executing a single merge task.
///
/// Periodically collects pipeline status updates and forwards them to the
/// compaction planner. Pipelines manage their own retry logic internally.
pub struct CompactorSupervisor {
    node_id: NodeId,
    compaction_client: CompactionServiceClient,
    pipelines: Vec<Option<CompactionPipeline>>,

    // Shared resources distributed to pipelines when spawning actor chains.
    io_throughput_limiter: Option<Limiter>,
    split_store: IndexingSplitStore,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    max_concurrent_split_uploads: usize,
    event_broker: EventBroker,

    // Scratch directory root (<data_dir>/compaction/).
    compaction_root_directory: TempDirectory,
}

impl CompactorSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        compaction_client: CompactionServiceClient,
        num_pipeline_slots: usize,
        io_throughput_limiter: Option<Limiter>,
        split_store: IndexingSplitStore,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
        compaction_root_directory: TempDirectory,
    ) -> Self {
        let pipelines = (0..num_pipeline_slots).map(|_| None).collect();
        CompactorSupervisor {
            node_id,
            compaction_client,
            pipelines,
            io_throughput_limiter,
            split_store,
            metastore,
            storage_resolver,
            max_concurrent_split_uploads,
            event_broker,
            compaction_root_directory,
        }
    }

    fn check_pipeline_statuses(&mut self) -> Vec<PipelineStatusUpdate> {
        let mut statuses = Vec::new();
        for slot in &mut self.pipelines {
            let Some(pipeline) = slot else {
                continue;
            };
            statuses.push(pipeline.pipeline_status_update());
        }
        statuses
    }

    fn build_worker_status_update(
        &self,
        statuses: &[PipelineStatusUpdate],
    ) -> WorkerStatusUpdateRequest {
        let in_progress_count = statuses
            .iter()
            .filter(|s| matches!(s.status, PipelineStatus::InProgress))
            .count();
        let available_slots = (self.pipelines.len() - in_progress_count) as u32;

        let mut in_progress_compactions = Vec::new();
        let mut completed_compactions = Vec::new();
        let mut failed_compactions = Vec::new();

        for update in statuses {
            match &update.status {
                PipelineStatus::InProgress => {
                    in_progress_compactions.push(InProgressCompaction {
                        task_id: update.task_id.clone(),
                        index_uid: Some(update.index_uid.clone()),
                        source_id: update.source_id.clone(),
                        split_ids: update.split_ids.clone(),
                    });
                }
                PipelineStatus::Completed => {
                    completed_compactions.push(CompletedCompaction {
                        task_id: update.task_id.clone(),
                        merged_split_id: update.merged_split_id.clone(),
                    });
                }
                PipelineStatus::Failed { error } => {
                    failed_compactions.push(FailedCompaction {
                        task_id: update.task_id.clone(),
                        error_message: error.clone(),
                    });
                }
            }
        }

        WorkerStatusUpdateRequest {
            worker_id: self.node_id.to_string(),
            available_slots,
            in_progress_compactions,
            completed_compactions,
            failed_compactions,
        }
    }
}

#[async_trait]
impl Actor for CompactorSupervisor {
    type ObservableState = ();

    fn name(&self) -> String {
        "CompactorSupervisor".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        info!(
            num_pipeline_slots=%self.pipelines.len(),
            "compactor supervisor started"
        );
        ctx.schedule_self_msg(CHECK_PIPELINE_STATUSES_INTERVAL, CheckPipelineStatuses);
        Ok(())
    }
}

#[async_trait]
impl Handler<CheckPipelineStatuses> for CompactorSupervisor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: CheckPipelineStatuses,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let statuses = self.check_pipeline_statuses();
        let _request = self.build_worker_status_update(&statuses);
        // TODO: send request to planner via gRPC, clear completed/failed slots on success.
        ctx.schedule_self_msg(CHECK_PIPELINE_STATUSES_INTERVAL, CheckPipelineStatuses);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_proto::compaction::CompactionServiceClient;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::NodeId;
    use quickwit_storage::{RamStorage, StorageResolver};

    use super::*;
    use crate::compaction_pipeline::tests::test_pipeline;
    use crate::planner::StubCompactionService;

    fn test_supervisor(num_slots: usize) -> CompactorSupervisor {
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let compaction_client = CompactionServiceClient::new(StubCompactionService);
        CompactorSupervisor::new(
            NodeId::from("test-node"),
            compaction_client,
            num_slots,
            None,
            split_store,
            metastore,
            StorageResolver::for_test(),
            2,
            EventBroker::default(),
            TempDirectory::for_test(),
        )
    }

    #[test]
    fn test_check_pipeline_statuses_empty_slots() {
        let mut supervisor = test_supervisor(4);
        let statuses = supervisor.check_pipeline_statuses();
        assert!(statuses.is_empty());
    }

    #[tokio::test]
    async fn test_check_pipeline_statuses_with_pipelines() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(4);

        let mut pipeline = test_pipeline("task-1", &["split-a", "split-b"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[0] = Some(pipeline);

        let mut pipeline = test_pipeline("task-2", &["split-c"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[2] = Some(pipeline);

        let statuses = supervisor.check_pipeline_statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].task_id, "task-1");
        assert_eq!(statuses[0].split_ids, vec!["split-a", "split-b"]);
        assert_eq!(statuses[1].task_id, "task-2");
        assert_eq!(statuses[1].split_ids, vec!["split-c"]);
    }

    #[tokio::test]
    async fn test_end_to_end_statuses_to_proto() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(3);

        let mut pipeline = test_pipeline("task-1", &["s1", "s2"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[0] = Some(pipeline);

        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_worker_status_update(&statuses);

        assert_eq!(request.worker_id, "test-node");
        // 3 slots, 1 in-progress = 2 available
        assert_eq!(request.available_slots, 2);
        assert_eq!(request.in_progress_compactions.len(), 1);
        assert_eq!(request.in_progress_compactions[0].task_id, "task-1");
        assert_eq!(
            request.in_progress_compactions[0].split_ids,
            vec!["s1", "s2"]
        );
        assert!(request.completed_compactions.is_empty());
        assert!(request.failed_compactions.is_empty());
    }

    #[test]
    fn test_build_worker_status_update_empty() {
        let supervisor = test_supervisor(4);
        let request = supervisor.build_worker_status_update(&[]);
        assert_eq!(request.worker_id, "test-node");
        assert_eq!(request.available_slots, 4);
        assert!(request.in_progress_compactions.is_empty());
        assert!(request.completed_compactions.is_empty());
        assert!(request.failed_compactions.is_empty());
    }

    #[test]
    fn test_build_worker_status_update_mixed_statuses() {
        let supervisor = test_supervisor(4);
        let statuses = vec![
            PipelineStatusUpdate {
                task_id: "task-1".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s1".to_string(), "s2".to_string()],
                merged_split_id: "merged-1".to_string(),
                status: PipelineStatus::InProgress,
            },
            PipelineStatusUpdate {
                task_id: "task-2".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s3".to_string()],
                merged_split_id: "merged-2".to_string(),
                status: PipelineStatus::Completed,
            },
            PipelineStatusUpdate {
                task_id: "task-3".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s4".to_string()],
                merged_split_id: "merged-3".to_string(),
                status: PipelineStatus::Failed {
                    error: "boom".to_string(),
                },
            },
        ];

        let request = supervisor.build_worker_status_update(&statuses);

        // 4 slots, 1 in-progress = 3 available
        assert_eq!(request.available_slots, 3);
        assert_eq!(request.in_progress_compactions.len(), 1);
        assert_eq!(request.in_progress_compactions[0].task_id, "task-1");
        assert_eq!(
            request.in_progress_compactions[0].split_ids,
            vec!["s1", "s2"]
        );
        assert_eq!(request.completed_compactions.len(), 1);
        assert_eq!(request.completed_compactions[0].task_id, "task-2");
        assert_eq!(request.completed_compactions[0].merged_split_id, "merged-2");
        assert_eq!(request.failed_compactions.len(), 1);
        assert_eq!(request.failed_compactions[0].task_id, "task-3");
        assert_eq!(request.failed_compactions[0].error_message, "boom");
    }
}
