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

use std::fmt::Debug;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, Split, SplitState,
};
use quickwit_proto::compaction::{
    CompactionResult, MergeTaskAssignment, PingRequest, PingResponse, ReportStatusRequest,
    ReportStatusResponse,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::NodeId;
use time::OffsetDateTime;
use tracing::error;
use ulid::Ulid;

use super::compaction_service::build_task_assignment;
use super::compaction_state::CompactionState;
use super::index_config_store::IndexConfigStore;

pub struct CompactionPlanner {
    state: CompactionState,
    index_config_store: IndexConfigStore,
    cursor: i64,
    metastore: MetastoreServiceClient,
}

impl Debug for CompactionPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionPlanner")
            .field("cursor", &self.cursor)
            .finish()
    }
}

const STARTUP_LOOKBACK: Duration = Duration::from_secs(24 * 60 * 60);

impl CompactionPlanner {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        let cursor = OffsetDateTime::now_utc().unix_timestamp() - STARTUP_LOOKBACK.as_secs() as i64;
        CompactionPlanner {
            state: CompactionState::new(),
            index_config_store: IndexConfigStore::new(metastore.clone()),
            cursor,
            metastore,
        }
    }

    async fn ingest_splits(&mut self, splits: Vec<Split>) {
        for split in splits {
            if self.state.is_split_known(&split.split_metadata.split_id) {
                continue;
            }
            let Ok(index_entry) = self
                .index_config_store
                .get_for_split(&split.split_metadata)
                .await
            else {
                error!(split_id=%split.split_metadata.split_id, "failed to load index config, skipping split");
                continue;
            };
            if index_entry.is_split_mature(&split.split_metadata) {
                continue;
            }
            self.cursor = self.cursor.max(split.update_timestamp);
            self.state.track_split(split.split_metadata);
        }
    }

    async fn scan_metastore(&self) -> Result<Vec<Split>> {
        let query = ListSplitsQuery::for_all_indexes()
            .with_split_state(SplitState::Published)
            .retain_immature(OffsetDateTime::now_utc())
            .with_update_timestamp_gte(self.cursor);
        let request = ListSplitsRequest::try_from_list_splits_query(&query)?;
        let splits = self
            .metastore
            .list_splits(request)
            .await?
            .collect_splits()
            .await?;
        Ok(splits)
    }

    async fn scan_and_plan(&mut self) -> Result<()> {
        let splits = self.scan_metastore().await?;
        self.ingest_splits(splits).await;
        self.run_merge_policies();
        Ok(())
    }

    fn run_merge_policies(&mut self) {
        for partition_key in self.state.partition_keys() {
            if let Some(index_entry) = self.index_config_store.get(&partition_key.index_uid) {
                self.state
                    .plan_partition(&partition_key, index_entry.merge_policy());
            }
        }
    }

    fn assign_tasks(&mut self, node_id: &NodeId, available_slots: u32) -> Vec<MergeTaskAssignment> {
        let pending = self.state.pop_pending(available_slots as usize);
        let mut assignments = Vec::with_capacity(pending.len());

        for (partition_key, operation) in pending {
            let task_id = Ulid::new().to_string();
            let Some(index_entry) = self.index_config_store.get(&partition_key.index_uid) else {
                error!(index_uid=%partition_key.index_uid, "index config not found for pending operation, skipping");
                continue;
            };
            let assignment = build_task_assignment(
                &task_id,
                index_entry,
                &operation,
                &partition_key.index_uid,
                &partition_key.source_id,
            );

            let split_ids = operation
                .splits_as_slice()
                .iter()
                .map(|s| s.split_id().to_string())
                .collect();
            self.state
                .record_assignment(task_id, split_ids, node_id.clone());

            assignments.push(assignment);
        }
        assignments
    }
}

const SCAN_AND_PLAN_INTERVAL: Duration = Duration::from_secs(5);
/// On initialization, we want to wait for two intervals to allow any in-progress workers to report
/// their progress, preventing us from frivolously rescheduling work.
const INITIAL_SCAN_AND_PLAN_INTERVAL: Duration = SCAN_AND_PLAN_INTERVAL.saturating_mul(2);

#[derive(Debug)]
struct ScanAndPlan;

#[async_trait]
impl Actor for CompactionPlanner {
    type ObservableState = ();

    fn name(&self) -> String {
        "CompactionPlanner".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        tracing::info!("compaction planner starting, scanning metastore for immature splits");
        ctx.schedule_self_msg(INITIAL_SCAN_AND_PLAN_INTERVAL, ScanAndPlan);
        Ok(())
    }
}

#[async_trait]
impl Handler<ScanAndPlan> for CompactionPlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: ScanAndPlan,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(err) = self.scan_and_plan().await {
            error!(error=%err, "error scanning metastore and planning merges");
        }
        self.state.check_heartbeat_timeouts();
        ctx.schedule_self_msg(SCAN_AND_PLAN_INTERVAL, ScanAndPlan);
        Ok(())
    }
}

#[async_trait]
impl Handler<PingRequest> for CompactionPlanner {
    type Reply = CompactionResult<PingResponse>;

    async fn handle(
        &mut self,
        _msg: PingRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<CompactionResult<PingResponse>, ActorExitStatus> {
        Ok(Ok(PingResponse {}))
    }
}

#[async_trait]
impl Handler<ReportStatusRequest> for CompactionPlanner {
    type Reply = CompactionResult<ReportStatusResponse>;

    async fn handle(
        &mut self,
        msg: ReportStatusRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<CompactionResult<ReportStatusResponse>, ActorExitStatus> {
        let node_id = NodeId::from(msg.node_id);
        self.state.process_successes(&msg.successes);
        self.state.process_failures(&msg.failures);
        self.state.update_heartbeats(&node_id, &msg.in_progress);
        let new_tasks = self.assign_tasks(&node_id, msg.available_slots);
        Ok(Ok(ReportStatusResponse { new_tasks }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_common::ServiceStream;
    use quickwit_config::IndexingSettings;
    use quickwit_config::merge_policy_config::{
        ConstWriteAmplificationMergePolicyConfig, MergePolicyConfig,
    };
    use quickwit_metastore::{
        IndexMetadata, IndexMetadataResponseExt, ListSplitsResponseExt, Split, SplitMaturity,
        SplitMetadata, SplitState,
    };
    use quickwit_proto::compaction::CompactionSuccess;
    use quickwit_proto::metastore::{
        IndexMetadataResponse, ListSplitsResponse, MetastoreError, MockMetastoreService,
    };
    use quickwit_proto::types::IndexUid;

    use super::*;

    fn test_split(split_id: &str, index_uid: &IndexUid, update_timestamp: i64) -> Split {
        Split {
            split_state: SplitState::Published,
            update_timestamp,
            publish_timestamp: Some(update_timestamp),
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
                node_id: "test-node".to_string(),
                num_docs: 100,
                create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
                maturity: SplitMaturity::Immature {
                    maturation_period: Duration::from_secs(3600),
                },
                ..Default::default()
            },
        }
    }

    fn test_index_metadata() -> IndexMetadata {
        IndexMetadata::for_test("test-index", "ram:///test-index")
    }

    /// Returns an IndexMetadata with merge_factor=2 so two splits trigger a merge.
    fn test_index_metadata_with_merge_factor_2() -> IndexMetadata {
        let mut metadata = test_index_metadata();
        metadata.index_config.indexing_settings = IndexingSettings {
            merge_policy: MergePolicyConfig::ConstWriteAmplification(
                ConstWriteAmplificationMergePolicyConfig {
                    merge_factor: 2,
                    max_merge_factor: 2,
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        metadata
    }

    fn test_index_metadata_response(index_metadata: &IndexMetadata) -> IndexMetadataResponse {
        IndexMetadataResponse::try_from_index_metadata(index_metadata).unwrap()
    }

    #[tokio::test]
    async fn test_scan_metastore() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let splits = vec![
            test_split("split-1", &index_uid, 1000),
            test_split("split-2", &index_uid, 2000),
        ];
        let splits_clone = splits.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_list_splits().returning(move |_| {
            let response = ListSplitsResponse::try_from_splits(splits_clone.clone()).unwrap();
            Ok(ServiceStream::from(vec![Ok(response)]))
        });

        let planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        let result = planner.scan_metastore().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].split_metadata.split_id, "split-1");
        assert_eq!(result[1].split_metadata.split_id, "split-2");
    }

    #[tokio::test]
    async fn test_ingest_splits_dedup_maturity_and_cursor() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));

        let mut planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        planner.cursor = 0;

        // Pre-populate: "in-flight" is already being compacted.
        planner.state.track_split(SplitMetadata {
            split_id: "in-flight".to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        });

        let mut mature_split = test_split("mature", &index_uid, 4000);
        mature_split.split_metadata.num_docs = 20_000_000;

        let splits = vec![
            test_split("in-flight", &index_uid, 1000),
            test_split("fresh", &index_uid, 3000),
            mature_split,
        ];

        planner.ingest_splits(splits).await;

        assert!(planner.state.is_split_known("fresh"));
        assert!(planner.state.is_split_known("in-flight"));
        assert!(!planner.state.is_split_known("mature"));
        assert_eq!(planner.cursor, 3000);
    }

    #[tokio::test]
    async fn test_scan_and_plan_metastore_error() {
        let mut mock = MockMetastoreService::new();
        mock.expect_list_splits().returning(|_| {
            Err(MetastoreError::Internal {
                message: "test error".to_string(),
                cause: String::new(),
            })
        });

        let mut planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        let original_cursor = planner.cursor;

        let result = planner.scan_and_plan().await;
        assert!(result.is_err());
        assert_eq!(planner.cursor, original_cursor);
    }

    #[tokio::test]
    async fn test_ingest_splits_skips_on_config_error() {
        let index_uid = IndexUid::for_test("missing-index", 0);
        let splits = vec![test_split("orphan", &index_uid, 1000)];

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata().returning(|_| {
            Err(MetastoreError::Internal {
                message: "test error".to_string(),
                cause: String::new(),
            })
        });

        let mut planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        planner.cursor = 0;
        planner.ingest_splits(splits).await;

        assert!(!planner.state.is_split_known("orphan"));
    }

    #[tokio::test]
    async fn test_scan_and_plan_happy_path() {
        let index_metadata = test_index_metadata();
        let index_metadata_response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let splits = vec![
            test_split("s1", &index_uid, 5000),
            test_split("s2", &index_uid, 6000),
        ];
        let splits_clone = splits.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_list_splits().returning(move |_| {
            let response = ListSplitsResponse::try_from_splits(splits_clone.clone()).unwrap();
            Ok(ServiceStream::from(vec![Ok(response)]))
        });
        mock.expect_index_metadata()
            .returning(move |_| Ok(index_metadata_response.clone()));

        let mut planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        planner.cursor = 0;
        planner.scan_and_plan().await.unwrap();

        assert!(planner.state.is_split_known("s1"));
        assert!(planner.state.is_split_known("s2"));
        assert_eq!(planner.cursor, 6000);
    }

    /// Helper: creates a planner with merge_factor=2, ingests the given splits,
    /// and runs merge policies. Returns the planner ready for `assign_tasks`.
    async fn planner_with_pending_merges(split_ids: &[&str]) -> (CompactionPlanner, IndexUid) {
        let index_metadata = test_index_metadata_with_merge_factor_2();
        let index_uid = index_metadata.index_uid.clone();
        let response = test_index_metadata_response(&index_metadata);

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));
        mock.expect_list_splits().returning(|_| {
            Ok(ServiceStream::from(vec![Ok(
                ListSplitsResponse::try_from_splits(Vec::new()).unwrap(),
            )]))
        });

        let mut planner = CompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        planner.cursor = 0;

        let splits: Vec<Split> = split_ids
            .iter()
            .enumerate()
            .map(|(i, id)| test_split(id, &index_uid, (i + 1) as i64 * 1000))
            .collect();
        planner.ingest_splits(splits).await;
        planner.run_merge_policies();
        (planner, index_uid)
    }

    #[tokio::test]
    async fn test_assign_tasks_returns_assignments_and_drains_queue() {
        let (mut planner, index_uid) = planner_with_pending_merges(&["s1", "s2"]).await;
        let node_id = NodeId::from("worker-1");

        // First call: get the assignment.
        let assignments = planner.assign_tasks(&node_id, 10);
        assert_eq!(assignments.len(), 1);

        let assignment = &assignments[0];
        assert!(!assignment.task_id.is_empty());
        assert_eq!(assignment.splits_metadata_json.len(), 2);
        assert_eq!(assignment.index_uid, Some(index_uid));
        assert_eq!(assignment.source_id, "test-source");
        assert!(!assignment.doc_mapping_json.is_empty());
        assert!(!assignment.index_storage_uri.is_empty());

        // Second call: queue is drained, no more assignments.
        let assignments = planner.assign_tasks(&node_id, 10);
        assert!(assignments.is_empty());
    }

    #[tokio::test]
    async fn test_assign_tasks_respects_available_slots() {
        // 4 splits with merge_factor=2 produces 2 merge operations.
        let (mut planner, _) = planner_with_pending_merges(&["s1", "s2", "s3", "s4"]).await;
        let node_id = NodeId::from("worker-1");

        // Request only 1 slot.
        let assignments = planner.assign_tasks(&node_id, 1);
        assert_eq!(assignments.len(), 1);

        // The remaining operation is still pending.
        let assignments = planner.assign_tasks(&node_id, 10);
        assert_eq!(assignments.len(), 1);
    }

    #[tokio::test]
    async fn test_report_status_success_frees_splits_for_future_merges() {
        let (mut planner, index_uid) = planner_with_pending_merges(&["s1", "s2"]).await;
        let node_id = NodeId::from("worker-1");

        let assignments = planner.assign_tasks(&node_id, 10);
        assert_eq!(assignments.len(), 1);
        let task_id = assignments[0].task_id.clone();

        // Report success for the task.
        planner.state.process_successes(&[CompactionSuccess {
            task_id,
            merged_split_id: "merged-1".to_string(),
        }]);

        // The original splits are no longer tracked. Re-ingesting them
        // (simulating the merged output being immature) creates new work.
        let new_splits = vec![
            test_split("s5", &index_uid, 5000),
            test_split("s6", &index_uid, 6000),
        ];
        planner.ingest_splits(new_splits).await;
        planner.run_merge_policies();

        let assignments = planner.assign_tasks(&node_id, 10);
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].splits_metadata_json.len(), 2);
    }
}
