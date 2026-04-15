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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, SpawnContext};
use quickwit_common::io::Limiter;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexingSettings, RetentionPolicy, SearchSettings, build_doc_mapper};
use quickwit_doc_mapper::DocMapping;
use quickwit_indexing::merge_policy::{MergeOperation, merge_policy_from_settings};
use quickwit_indexing::{IndexingSplitCache, IndexingSplitStore};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::compaction::{
    CompactionFailure, CompactionInProgress, CompactionPlannerService,
    CompactionPlannerServiceClient, CompactionSuccess, MergeTaskAssignment, ReportStatusRequest,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use tracing::{error, info, warn};

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
    planner_client: CompactionPlannerServiceClient,
    pipelines: Vec<Option<CompactionPipeline>>,

    // Shared resources distributed to pipelines when spawning actor chains.
    io_throughput_limiter: Option<Limiter>,
    split_cache: Arc<IndexingSplitCache>,
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
        planner_client: CompactionPlannerServiceClient,
        num_pipeline_slots: usize,
        io_throughput_limiter: Option<Limiter>,
        split_cache: Arc<IndexingSplitCache>,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
        compaction_root_directory: TempDirectory,
    ) -> Self {
        let pipelines = (0..num_pipeline_slots).map(|_| None).collect();
        CompactorSupervisor {
            node_id,
            planner_client,
            pipelines,
            io_throughput_limiter,
            split_cache,
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

    async fn process_new_tasks(
        &mut self,
        assignments: Vec<MergeTaskAssignment>,
        spawn_ctx: &SpawnContext,
    ) {
        for assignment in assignments {
            let task_id = assignment.task_id.clone();
            if let Err(err) = self.spawn_task(assignment, spawn_ctx).await {
                error!(task_id=%task_id, error=%err, "failed to spawn compaction task");
            }
        }
    }

    async fn spawn_task(
        &mut self,
        assignment: MergeTaskAssignment,
        spawn_ctx: &SpawnContext,
    ) -> anyhow::Result<()> {
        let slot_idx = self
            .pipelines
            .iter()
            .position(|slot| match slot {
                None => true,
                Some(p) => matches!(
                    p.status(),
                    PipelineStatus::Completed | PipelineStatus::Failed { .. }
                ),
            })
            .ok_or_else(|| anyhow::anyhow!("no free pipeline slot"))?;
        let scratch_directory = self
            .compaction_root_directory
            .named_temp_child(&assignment.task_id)?;
        let mut pipeline = self
            .build_compaction_pipeline(assignment, scratch_directory)
            .await?;
        pipeline.spawn_pipeline(spawn_ctx)?;
        self.pipelines[slot_idx] = Some(pipeline);
        Ok(())
    }

    async fn build_compaction_pipeline(
        &self,
        assignment: MergeTaskAssignment,
        scratch_directory: TempDirectory,
    ) -> anyhow::Result<CompactionPipeline> {
        let splits: Vec<SplitMetadata> = assignment
            .splits_metadata_json
            .iter()
            .map(|json| serde_json::from_str(json))
            .collect::<Result<Vec<SplitMetadata>, serde_json::Error>>()?;
        let doc_mapping: DocMapping = serde_json::from_str(&assignment.doc_mapping_json)?;
        let search_settings: SearchSettings =
            serde_json::from_str(&assignment.search_settings_json)?;
        let indexing_settings: IndexingSettings =
            serde_json::from_str(&assignment.indexing_settings_json)?;
        let retention_policy: Option<RetentionPolicy> =
            if assignment.retention_policy_json.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&assignment.retention_policy_json)?)
            };
        let index_uid = assignment
            .index_uid
            .ok_or_else(|| anyhow::anyhow!("missing index_uid in MergeTaskAssignment"))?;

        let index_storage_uri = Uri::from_str(&assignment.index_storage_uri)?;
        let index_storage = self.storage_resolver.resolve(&index_storage_uri).await?;
        let split_store = IndexingSplitStore::new(index_storage, self.split_cache.clone());

        let doc_mapper = build_doc_mapper(&doc_mapping, &search_settings)?;
        let merge_policy = merge_policy_from_settings(&indexing_settings);
        let merge_operation = MergeOperation::new_merge_operation(splits);
        let pipeline_id = MergePipelineId {
            node_id: self.node_id.clone(),
            index_uid,
            source_id: assignment.source_id,
        };

        Ok(CompactionPipeline::new(
            assignment.task_id,
            scratch_directory,
            merge_operation,
            pipeline_id,
            doc_mapper,
            merge_policy,
            retention_policy,
            self.metastore.clone(),
            split_store,
            self.io_throughput_limiter.clone(),
            self.max_concurrent_split_uploads,
            self.event_broker.clone(),
        ))
    }

    fn build_report_status_request(
        &self,
        statuses: &[PipelineStatusUpdate],
    ) -> ReportStatusRequest {
        let in_progress_count = statuses
            .iter()
            .filter(|s| matches!(s.status, PipelineStatus::InProgress))
            .count();
        let available_slots = (self.pipelines.len() - in_progress_count) as u32;

        let mut in_progress = Vec::new();
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for update in statuses {
            match &update.status {
                PipelineStatus::InProgress => {
                    in_progress.push(CompactionInProgress {
                        task_id: update.task_id.clone(),
                        index_uid: Some(update.index_uid.clone()),
                        source_id: update.source_id.clone(),
                        split_ids: update.split_ids.clone(),
                    });
                }
                PipelineStatus::Completed => {
                    successes.push(CompactionSuccess {
                        task_id: update.task_id.clone(),
                        merged_split_id: update.merged_split_id.clone(),
                    });
                }
                PipelineStatus::Failed { error } => {
                    failures.push(CompactionFailure {
                        task_id: update.task_id.clone(),
                        error_message: error.clone(),
                    });
                }
            }
        }

        ReportStatusRequest {
            node_id: self.node_id.to_string(),
            available_slots,
            in_progress,
            successes,
            failures,
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
        let request = self.build_report_status_request(&statuses);
        match self.planner_client.report_status(request).await {
            Ok(response) => {
                self.process_new_tasks(response.new_tasks, ctx.spawn_ctx())
                    .await;
            }
            Err(err) => {
                warn!(error=%err, "failed to report status to compaction planner");
            }
        }
        ctx.schedule_self_msg(CHECK_PIPELINE_STATUSES_INTERVAL, CheckPipelineStatuses);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_proto::compaction::{
        CompactionPlannerServiceClient, MockCompactionPlannerService,
    };
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::NodeId;
    use quickwit_storage::StorageResolver;

    use super::*;
    use crate::compaction_pipeline::tests::test_pipeline;

    fn test_supervisor(num_slots: usize) -> CompactorSupervisor {
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let compaction_client =
            CompactionPlannerServiceClient::from_mock(MockCompactionPlannerService::new());
        CompactorSupervisor::new(
            NodeId::from("test-node"),
            compaction_client,
            num_slots,
            None,
            Arc::new(IndexingSplitCache::no_caching()),
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
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_end_to_end_statuses_to_proto() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(3);

        let mut pipeline = test_pipeline("task-1", &["s1", "s2"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[0] = Some(pipeline);

        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_report_status_request(&statuses);

        assert_eq!(request.node_id, "test-node");
        // 3 slots, 1 in-progress = 2 available
        assert_eq!(request.available_slots, 2);
        assert_eq!(request.in_progress.len(), 1);
        assert_eq!(request.in_progress[0].task_id, "task-1");
        assert_eq!(request.in_progress[0].split_ids, vec!["s1", "s2"]);
        assert!(request.successes.is_empty());
        assert!(request.failures.is_empty());
        universe.assert_quit().await;
    }

    #[test]
    fn test_build_report_status_request_empty() {
        let supervisor = test_supervisor(4);
        let request = supervisor.build_report_status_request(&[]);
        assert_eq!(request.node_id, "test-node");
        assert_eq!(request.available_slots, 4);
        assert!(request.in_progress.is_empty());
        assert!(request.successes.is_empty());
        assert!(request.failures.is_empty());
    }

    fn test_assignment(task_id: &str, split_ids: &[&str]) -> MergeTaskAssignment {
        let index_metadata =
            quickwit_metastore::IndexMetadata::for_test("test-index", "ram:///test-index");
        let config = &index_metadata.index_config;
        let splits: Vec<quickwit_metastore::SplitMetadata> = split_ids
            .iter()
            .map(|id| quickwit_metastore::SplitMetadata::for_test(id.to_string()))
            .collect();
        MergeTaskAssignment {
            task_id: task_id.to_string(),
            splits_metadata_json: splits
                .iter()
                .map(|s| serde_json::to_string(s).unwrap())
                .collect(),
            doc_mapping_json: serde_json::to_string(&config.doc_mapping).unwrap(),
            search_settings_json: serde_json::to_string(&config.search_settings).unwrap(),
            indexing_settings_json: serde_json::to_string(&config.indexing_settings).unwrap(),
            retention_policy_json: String::new(),
            index_uid: Some(index_metadata.index_uid.clone()),
            source_id: "test-source".to_string(),
            index_storage_uri: config.index_uri.to_string(),
        }
    }

    #[tokio::test]
    async fn test_build_compaction_pipeline_deserialization_errors() {
        let supervisor = test_supervisor(4);
        let scratch = TempDirectory::for_test;

        // Bad splits JSON.
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.splits_metadata_json = vec!["not json".to_string()];
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );

        // Bad doc mapping JSON.
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.doc_mapping_json = "not json".to_string();
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );

        // Bad search settings JSON.
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.search_settings_json = "not json".to_string();
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );

        // Bad indexing settings JSON.
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.indexing_settings_json = "not json".to_string();
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );

        // Bad retention policy JSON (non-empty but invalid).
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.retention_policy_json = "not json".to_string();
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );

        // Missing index_uid.
        let mut assignment = test_assignment("t", &["s1"]);
        assignment.index_uid = None;
        assert!(
            supervisor
                .build_compaction_pipeline(assignment, scratch())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_spawn_task_fails_when_all_slots_occupied() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(2);

        supervisor
            .spawn_task(test_assignment("task-1", &["s1"]), universe.spawn_ctx())
            .await
            .unwrap();
        supervisor
            .spawn_task(test_assignment("task-2", &["s2"]), universe.spawn_ctx())
            .await
            .unwrap();

        // Both slots InProgress — no room.
        assert!(
            supervisor
                .spawn_task(test_assignment("task-3", &["s3"]), universe.spawn_ctx())
                .await
                .is_err()
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_end_to_end_report_status_and_spawn() {
        let universe = Universe::new();

        // Mock planner that returns one assignment on the first call.
        let assignment = test_assignment("planner-task-1", &["s1", "s2"]);
        let assignments = vec![assignment];
        let assignments_clone = assignments.clone();
        let mut mock = quickwit_proto::compaction::MockCompactionPlannerService::new();
        mock.expect_report_status().times(1).returning(move |_req| {
            Ok(quickwit_proto::compaction::ReportStatusResponse {
                new_tasks: assignments_clone.clone(),
            })
        });

        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let client = CompactionPlannerServiceClient::from_mock(mock);
        let mut supervisor = CompactorSupervisor::new(
            NodeId::from("test-node"),
            client,
            3,
            None,
            Arc::new(IndexingSplitCache::no_caching()),
            metastore,
            StorageResolver::for_test(),
            2,
            EventBroker::default(),
            TempDirectory::for_test(),
        );

        // Simulate what the handler does: collect statuses, report, process response.
        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_report_status_request(&statuses);
        assert_eq!(request.available_slots, 3);

        let response = supervisor
            .planner_client
            .report_status(request)
            .await
            .unwrap();
        supervisor
            .process_new_tasks(response.new_tasks, universe.spawn_ctx())
            .await;

        // Verify the pipeline was spawned.
        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_report_status_request(&statuses);
        assert_eq!(request.in_progress.len(), 1);
        assert_eq!(request.in_progress[0].task_id, "planner-task-1");
        assert_eq!(request.in_progress[0].split_ids.len(), 2);
        assert_eq!(request.available_slots, 2);

        universe.assert_quit().await;
    }

    #[test]
    fn test_build_report_status_request_mixed_statuses() {
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

        let request = supervisor.build_report_status_request(&statuses);

        assert_eq!(request.available_slots, 3);
        assert_eq!(request.in_progress.len(), 1);
        assert_eq!(request.in_progress[0].task_id, "task-1");
        assert_eq!(request.in_progress[0].split_ids, vec!["s1", "s2"]);
        assert_eq!(request.successes.len(), 1);
        assert_eq!(request.successes[0].task_id, "task-2");
        assert_eq!(request.successes[0].merged_split_id, "merged-2");
        assert_eq!(request.failures.len(), 1);
        assert_eq!(request.failures[0].task_id, "task-3");
        assert_eq!(request.failures[0].error_message, "boom");
    }
}
