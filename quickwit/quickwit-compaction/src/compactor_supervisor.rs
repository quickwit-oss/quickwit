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

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, SpawnContext};
use quickwit_common::io::{self, Limiter};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::uri::Uri;
use quickwit_config::{
    CompactorConfig, IndexingSettings, RetentionPolicy, SearchSettings, build_doc_mapper,
};
use quickwit_doc_mapper::DocMapping;
use quickwit_indexing::merge_policy::{MergeOperation, merge_policy_from_settings};
use quickwit_indexing::{IndexingSplitCache, IndexingSplitStore};
use quickwit_metastore::SplitMetadata;
use quickwit_metrics::{gauge, label_values};
use quickwit_proto::compaction::{
    CompactionFailure, CompactionInProgress, CompactionPlannerService,
    CompactionPlannerServiceClient, CompactionSuccess, MergeTaskAssignment, ReportStatusRequest,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use tokio::sync::{Semaphore, watch};
use tracing::{error, info};

use crate::compaction_pipeline::{CompactionPipeline, PipelineStatus, PipelineStatusUpdate};
use crate::metrics::{AVAILABLE_SLOTS, COMPACTIONS_IN_PROGRESS, SOURCE_UID_MERGE_LEVEL};
use crate::source_uid_metrics_label;

const CHECK_PIPELINE_STATUSES_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct CheckPipelineStatuses;

#[derive(Debug)]
pub struct Decommission;

/// Lifecycle state of a `CompactorSupervisor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactorStatus {
    /// Normal operation: accepts and spawns new merge tasks.
    Ready,
    /// Draining: rejects new tasks (reports zero available slots) while in-flight merges finish.
    Decommissioning,
    /// All in-flight merges have completed; the supervisor can be torn down.
    Finished,
}

/// Manages a pool of `CompactionPipeline`s, each executing a single merge task.
///
/// Periodically collects pipeline status updates and forwards them to the
/// compaction planner. Pipelines manage their own retry logic internally.
pub struct CompactorSupervisor {
    node_id: NodeId,
    planner_client: CompactionPlannerServiceClient,
    status: CompactorStatus,
    status_tx: watch::Sender<CompactorStatus>,
    pipelines: Vec<Option<CompactionPipeline>>,
    // Bounds the number of pipelines running Tantivy merge step concurrently. There are 2x as many
    // pipeline slots as permits, so half the pipelines can be doing IO while the other half hold a
    // permit.
    merge_execution_semaphore: Arc<Semaphore>,
    // Shared resources distributed to pipelines when spawning actor chains.
    io_throughput_limiter: Option<Limiter>,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    split_cache: Arc<IndexingSplitCache>,
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
        compactor_config: &CompactorConfig,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        split_cache: Arc<IndexingSplitCache>,
        event_broker: EventBroker,
        compaction_root_directory: TempDirectory,
    ) -> Self {
        let &CompactorConfig {
            max_concurrent_merge_executions,
            pipeline_slots_per_merge_execution,
            max_concurrent_split_uploads,
            max_merge_write_throughput,
        } = compactor_config;
        let num_pipeline_slots =
            max_concurrent_merge_executions.get() * pipeline_slots_per_merge_execution.get();
        let pipelines = (0..num_pipeline_slots).map(|_| None).collect();
        let merge_execution_semaphore =
            Arc::new(Semaphore::new(max_concurrent_merge_executions.get()));
        let io_throughput_limiter = max_merge_write_throughput.map(io::limiter);
        let (status_tx, _status_rx) = watch::channel(CompactorStatus::Ready);
        CompactorSupervisor {
            node_id,
            planner_client,
            status: CompactorStatus::Ready,
            status_tx,
            pipelines,
            merge_execution_semaphore,
            io_throughput_limiter,
            metastore,
            storage_resolver,
            split_cache,
            max_concurrent_split_uploads,
            event_broker,
            compaction_root_directory,
        }
    }

    pub fn status_rx(&self) -> watch::Receiver<CompactorStatus> {
        self.status_tx.subscribe()
    }

    fn set_status(&mut self, status: CompactorStatus) {
        self.status = status;
        self.status_tx.send_replace(status);
    }

    fn check_decommissioning_status(&mut self) {
        if self.status != CompactorStatus::Decommissioning {
            return;
        }
        let any_in_progress = self
            .pipelines
            .iter()
            .flatten()
            .any(|pipeline| matches!(pipeline.status(), PipelineStatus::InProgress));
        if !any_in_progress {
            self.set_status(CompactorStatus::Finished);
        }
    }

    async fn report_status_and_maybe_finish(&mut self, ctx: &ActorContext<Self>) {
        let statuses = self.check_pipeline_statuses();
        let request = self.build_report_status_request(&statuses);
        match ctx
            .protect_future(self.planner_client.report_status(request))
            .await
        {
            Ok(response) => {
                ctx.protect_future(self.process_new_tasks(response.new_tasks, ctx.spawn_ctx()))
                    .await;
                self.check_decommissioning_status();
            }
            Err(error) => {
                error!(%error, "failed to report status to compaction planner");
            }
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
        // The planner has acknowledged the statuses we just sent — drop any
        // Completed/Failed pipelines so their scratch directories and actor
        // handles are released.
        for slot in &mut self.pipelines {
            slot.take_if(|p| p.status().is_terminal());
        }
        for assignment in assignments {
            let task_id = assignment.task_id.clone();
            if let Err(error) = self.spawn_task(assignment, spawn_ctx).await {
                error!(%task_id, %error, "failed to spawn compaction task");
            }
        }
        let available_slots = self
            .pipelines
            .iter()
            .filter(|pipeline_opt| pipeline_opt.is_none())
            .count();
        AVAILABLE_SLOTS.set(available_slots as f64);
    }

    async fn spawn_task(
        &mut self,
        assignment: MergeTaskAssignment,
        spawn_ctx: &SpawnContext,
    ) -> anyhow::Result<()> {
        if self.status != CompactorStatus::Ready {
            info!(
                task_id = %assignment.task_id,
                "compactor is decommissioning; dropping assigned merge task, the merge planner will reschedule it"
            );
            return Ok(());
        }
        let source_uid_label = source_uid_metrics_label(
            assignment.index_uid.as_ref().unwrap(),
            &assignment.source_id,
        );
        let merge_level = assignment.merge_level.to_string();
        let labels = label_values!(SOURCE_UID_MERGE_LEVEL => source_uid_label, merge_level);

        let slot_idx = self
            .pipelines
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| anyhow::anyhow!("no free pipeline slot"))?;
        let scratch_directory = self
            .compaction_root_directory
            .named_temp_child(&assignment.task_id)?;
        let mut pipeline = self
            .build_compaction_pipeline(assignment, scratch_directory)
            .await?;
        pipeline.spawn_pipeline(spawn_ctx)?;
        self.pipelines[slot_idx] = Some(pipeline);
        gauge!(parent: COMPACTIONS_IN_PROGRESS, labels: [labels]).inc();
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
            self.merge_execution_semaphore.clone(),
        ))
    }

    fn available_slots(&self, in_progress_count: usize) -> u32 {
        if self.status != CompactorStatus::Ready {
            return 0;
        }
        (self.pipelines.len() - in_progress_count) as u32
    }

    fn build_report_status_request(
        &self,
        statuses: &[PipelineStatusUpdate],
    ) -> ReportStatusRequest {
        let in_progress_count = statuses
            .iter()
            .filter(|s| matches!(s.status, PipelineStatus::InProgress))
            .count();
        let available_slots = self.available_slots(in_progress_count);

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
        // Once finished decommissioning we stop the loop; the node is about to be torn down.
        if self.status == CompactorStatus::Finished {
            return Ok(());
        }
        self.report_status_and_maybe_finish(ctx).await;
        if self.status == CompactorStatus::Finished {
            info!("compactor finished draining in-flight merges");
            return Ok(());
        }
        ctx.schedule_self_msg(CHECK_PIPELINE_STATUSES_INTERVAL, CheckPipelineStatuses);
        Ok(())
    }
}

#[async_trait]
impl Handler<Decommission> for CompactorSupervisor {
    type Reply = watch::Receiver<CompactorStatus>;

    async fn handle(
        &mut self,
        _msg: Decommission,
        _ctx: &ActorContext<Self>,
    ) -> Result<watch::Receiver<CompactorStatus>, ActorExitStatus> {
        if self.status == CompactorStatus::Ready {
            info!("decommissioning compactor");
            self.set_status(CompactorStatus::Decommissioning);
        }
        Ok(self.status_rx())
    }
}

pub async fn wait_for_compactor_decommission(
    compactor_mailbox: &Mailbox<CompactorSupervisor>,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let mut status_rx = compactor_mailbox
        .send_message_with_high_priority(Decommission)
        .context("failed to initiate compactor decommission")?
        .await
        .context("compactor dropped decommission reply")?;
    tokio::time::timeout(
        timeout_after,
        status_rx.wait_for(|status| *status == CompactorStatus::Finished),
    )
    .await
    .context("timed out waiting for compactor to finish decommissioning")?
    .context("compactor status channel closed")?;
    info!("compactor decommissioned successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

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

    /// Builds a test supervisor with `max_concurrent_merge_executions` permits
    /// and `2 * max_concurrent_merge_executions` pipeline slots.
    fn test_supervisor(max_concurrent_merge_executions: usize) -> CompactorSupervisor {
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let compaction_client =
            CompactionPlannerServiceClient::from_mock(MockCompactionPlannerService::new());
        let compactor_config = CompactorConfig {
            max_concurrent_merge_executions: NonZeroUsize::new(max_concurrent_merge_executions)
                .expect("max_concurrent_merge_executions must be non-zero"),
            ..CompactorConfig::for_test()
        };
        CompactorSupervisor::new(
            NodeId::from_str("test-node"),
            compaction_client,
            &compactor_config,
            metastore,
            StorageResolver::for_test(),
            Arc::new(IndexingSplitCache::no_caching()),
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
        // 3 merge-executions → 6 pipeline slots
        let mut supervisor = test_supervisor(3);

        let mut pipeline = test_pipeline("task-1", &["s1", "s2"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[0] = Some(pipeline);

        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_report_status_request(&statuses);

        assert_eq!(request.node_id, "test-node");
        // 6 slots, 1 in-progress = 5 available
        assert_eq!(request.available_slots, 5);
        assert_eq!(request.in_progress.len(), 1);
        assert_eq!(request.in_progress[0].task_id, "task-1");
        assert_eq!(request.in_progress[0].split_ids, vec!["s1", "s2"]);
        assert!(request.successes.is_empty());
        assert!(request.failures.is_empty());
        universe.assert_quit().await;
    }

    #[test]
    fn test_build_report_status_request_empty() {
        // 4 merge-executions → 8 pipeline slots
        let supervisor = test_supervisor(4);
        let request = supervisor.build_report_status_request(&[]);
        assert_eq!(request.node_id, "test-node");
        assert_eq!(request.available_slots, 8);
        assert!(request.in_progress.is_empty());
        assert!(request.successes.is_empty());
        assert!(request.failures.is_empty());
    }

    fn test_assignment(task_id: &str, split_ids: &[&str]) -> MergeTaskAssignment {
        let index_metadata =
            quickwit_metastore::IndexMetadata::for_test("test-index", "ram:///test-index");
        let config = &index_metadata.index_config;
        let splits: Vec<SplitMetadata> = split_ids
            .iter()
            .map(|id| SplitMetadata::for_test(id.to_string()))
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
            merge_level: 1,
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
        // 1 merge-execution → 2 pipeline slots
        let mut supervisor = test_supervisor(1);

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
        let mut mock = MockCompactionPlannerService::new();
        mock.expect_report_status().times(1).returning(move |_req| {
            Ok(quickwit_proto::compaction::ReportStatusResponse {
                new_tasks: assignments_clone.clone(),
            })
        });

        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let client = CompactionPlannerServiceClient::from_mock(mock);
        // 3 merge-executions → 6 pipeline slots
        let compactor_config = CompactorConfig {
            max_concurrent_merge_executions: NonZeroUsize::new(3).unwrap(),
            ..CompactorConfig::for_test()
        };
        let mut supervisor = CompactorSupervisor::new(
            NodeId::from_str("test-node"),
            client,
            &compactor_config,
            metastore,
            StorageResolver::for_test(),
            Arc::new(IndexingSplitCache::no_caching()),
            EventBroker::default(),
            TempDirectory::for_test(),
        );

        // Simulate what the handler does: collect statuses, report, process response.
        let statuses = supervisor.check_pipeline_statuses();
        let request = supervisor.build_report_status_request(&statuses);
        assert_eq!(request.available_slots, 6);

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
        assert_eq!(request.available_slots, 5);

        universe.assert_quit().await;
    }

    #[test]
    fn test_build_report_status_request_mixed_statuses() {
        // 4 merge-executions → 8 pipeline slots
        let supervisor = test_supervisor(4);
        let statuses = vec![
            PipelineStatusUpdate {
                task_id: "task-1".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s1".to_string(), "s2".to_string()],
                status: PipelineStatus::InProgress,
            },
            PipelineStatusUpdate {
                task_id: "task-2".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s3".to_string()],
                status: PipelineStatus::Completed,
            },
            PipelineStatusUpdate {
                task_id: "task-3".to_string(),
                index_uid: quickwit_proto::types::IndexUid::for_test("test-index", 0),
                source_id: "src".to_string(),
                split_ids: vec!["s4".to_string()],
                status: PipelineStatus::Failed {
                    error: "boom".to_string(),
                },
            },
        ];

        let request = supervisor.build_report_status_request(&statuses);

        // 8 slots, 1 in-progress = 7 available
        assert_eq!(request.available_slots, 7);
        assert_eq!(request.in_progress.len(), 1);
        assert_eq!(request.in_progress[0].task_id, "task-1");
        assert_eq!(request.in_progress[0].split_ids, vec!["s1", "s2"]);
        assert_eq!(request.successes.len(), 1);
        assert_eq!(request.successes[0].task_id, "task-2");
        assert_eq!(request.failures.len(), 1);
        assert_eq!(request.failures[0].task_id, "task-3");
        assert_eq!(request.failures[0].error_message, "boom");
    }

    #[test]
    fn test_check_decommissioning_status_finishes_when_idle() {
        let mut supervisor = test_supervisor(2);
        supervisor.set_status(CompactorStatus::Decommissioning);
        supervisor.check_decommissioning_status();
        assert_eq!(supervisor.status, CompactorStatus::Finished);
    }

    #[test]
    fn test_check_decommissioning_status_noop_when_ready() {
        let mut supervisor = test_supervisor(2);
        supervisor.check_decommissioning_status();
        assert_eq!(supervisor.status, CompactorStatus::Ready);
    }

    #[test]
    fn test_decommissioning_reports_zero_available_slots() {
        // 4 merge-executions → 8 free pipeline slots, but decommissioning advertises none.
        let mut supervisor = test_supervisor(4);
        supervisor.set_status(CompactorStatus::Decommissioning);
        let request = supervisor.build_report_status_request(&[]);
        assert_eq!(request.available_slots, 0);
    }

    #[tokio::test]
    async fn test_decommissioning_waits_for_in_flight_merge() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(4);

        let mut pipeline = test_pipeline("task-1", &["s1"]);
        pipeline.spawn_pipeline(universe.spawn_ctx()).unwrap();
        supervisor.pipelines[0] = Some(pipeline);

        supervisor.set_status(CompactorStatus::Decommissioning);
        let statuses = supervisor.check_pipeline_statuses();
        supervisor.check_decommissioning_status();

        // The in-flight pipeline keeps the supervisor draining and advertising no capacity.
        assert_eq!(supervisor.status, CompactorStatus::Decommissioning);
        let request = supervisor.build_report_status_request(&statuses);
        assert_eq!(request.available_slots, 0);
        assert_eq!(request.in_progress.len(), 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_spawn_task_dropped_while_decommissioning() {
        let universe = Universe::new();
        let mut supervisor = test_supervisor(2);
        supervisor.set_status(CompactorStatus::Decommissioning);

        supervisor
            .spawn_task(test_assignment("task-1", &["s1"]), universe.spawn_ctx())
            .await
            .unwrap();

        assert!(supervisor.pipelines.iter().all(|slot| slot.is_none()));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_wait_for_compactor_decommission_drains_when_idle() {
        let universe = Universe::new();

        let mut mock = MockCompactionPlannerService::new();
        mock.expect_report_status().returning(|_req| {
            Ok(quickwit_proto::compaction::ReportStatusResponse {
                new_tasks: Vec::new(),
            })
        });
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let client = CompactionPlannerServiceClient::from_mock(mock);
        let compactor_config = CompactorConfig {
            max_concurrent_merge_executions: NonZeroUsize::new(2).unwrap(),
            ..CompactorConfig::for_test()
        };
        let supervisor = CompactorSupervisor::new(
            NodeId::from_str("test-node"),
            client,
            &compactor_config,
            metastore,
            StorageResolver::for_test(),
            Arc::new(IndexingSplitCache::no_caching()),
            EventBroker::default(),
            TempDirectory::for_test(),
        );
        let status_rx = supervisor.status_rx();
        let (mailbox, _handle) = universe.spawn_builder().spawn(supervisor);

        wait_for_compactor_decommission(&mailbox, Duration::from_secs(10))
            .await
            .unwrap();

        assert_eq!(*status_rx.borrow(), CompactorStatus::Finished);
        universe.assert_quit().await;
    }
}
