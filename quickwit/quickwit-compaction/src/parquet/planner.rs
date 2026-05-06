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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use quickwit_common::{is_parquet_pipeline_index, is_sketches_index};
use quickwit_indexing::merge_policy::parquet_merge_policy_from_settings;
use quickwit_metastore::{
    IndexMetadata, ListIndexesMetadataResponseExt, ListParquetSplitsQuery,
    ListParquetSplitsRequestExt, ListParquetSplitsResponseExt, ParquetSplitRecord,
};
use quickwit_parquet_engine::merge::policy::{
    ParquetMergeOperation, ParquetMergePolicy, ParquetSplitMaturity,
};
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_proto::compaction::{
    CompactionFailure, CompactionInProgress, CompactionSuccess, CompactionTaskKind,
    MergeTaskAssignment,
};
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, ListMetricsSplitsRequest, ListSketchSplitsRequest,
    MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{IndexUid, NodeId};
use time::OffsetDateTime;
use tracing::{error, info};
use ulid::Ulid;

use super::state::ParquetCompactionState;

#[derive(Clone)]
struct ParquetIndexEntry {
    index_metadata: IndexMetadata,
    merge_policy: Arc<dyn ParquetMergePolicy>,
}

impl ParquetIndexEntry {
    fn new(index_metadata: IndexMetadata) -> Self {
        let merge_policy =
            parquet_merge_policy_from_settings(&index_metadata.index_config.indexing_settings);
        Self {
            index_metadata,
            merge_policy,
        }
    }

    fn is_split_mature(&self, split: &ParquetSplitMetadata) -> bool {
        match self
            .merge_policy
            .split_maturity(split.size_bytes, split.num_merge_ops)
        {
            ParquetSplitMaturity::Mature => true,
            ParquetSplitMaturity::Immature {
                maturation_period, ..
            } => split.created_at + maturation_period <= SystemTime::now(),
        }
    }

    fn doc_mapping_json(&self) -> String {
        serde_json::to_string(&self.index_metadata.index_config.doc_mapping)
            .expect("doc mapping serialization should not fail")
    }

    fn search_settings_json(&self) -> String {
        serde_json::to_string(&self.index_metadata.index_config.search_settings)
            .expect("search settings serialization should not fail")
    }

    fn indexing_settings_json(&self) -> String {
        serde_json::to_string(&self.index_metadata.index_config.indexing_settings)
            .expect("indexing settings serialization should not fail")
    }

    fn retention_policy_json(&self) -> String {
        match &self.index_metadata.index_config.retention_policy_opt {
            Some(policy) => serde_json::to_string(policy)
                .expect("retention policy serialization should not fail"),
            None => String::new(),
        }
    }

    fn index_storage_uri(&self) -> String {
        self.index_metadata.index_config.index_uri.to_string()
    }
}

pub(crate) struct ParquetCompactionPlanner {
    state: ParquetCompactionState,
    indexes: HashMap<IndexUid, ParquetIndexEntry>,
    cursor: i64,
    metastore: MetastoreServiceClient,
}

const STARTUP_LOOKBACK: Duration = Duration::from_secs(24 * 60 * 60);

impl ParquetCompactionPlanner {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        let cursor = OffsetDateTime::now_utc().unix_timestamp() - STARTUP_LOOKBACK.as_secs() as i64;
        Self {
            state: ParquetCompactionState::new(),
            indexes: HashMap::new(),
            cursor,
            metastore,
        }
    }

    pub async fn scan_and_plan(&mut self) -> Result<()> {
        self.refresh_indexes().await?;
        let splits = self.scan_metastore().await?;
        self.ingest_splits(splits);
        self.run_merge_policies();
        Ok(())
    }

    pub fn process_successes(&mut self, successes: &[CompactionSuccess]) {
        self.state.process_successes(successes);
    }

    pub fn process_failures(&mut self, failures: &[CompactionFailure]) {
        self.state.process_failures(failures);
    }

    pub fn update_heartbeats(&mut self, node_id: &NodeId, in_progress: &[CompactionInProgress]) {
        self.state.update_heartbeats(node_id, in_progress);
    }

    pub fn check_heartbeat_timeouts(&mut self) {
        self.state.check_heartbeat_timeouts();
    }

    pub fn assign_tasks(
        &mut self,
        node_id: &NodeId,
        available_slots: u32,
    ) -> Vec<MergeTaskAssignment> {
        let pending = self.state.pop_pending(available_slots as usize);
        let mut assignments = Vec::with_capacity(pending.len());

        for (scope, operation) in pending {
            let Ok(index_uid) = IndexUid::from_str(&scope.index_uid) else {
                error!(index_uid=%scope.index_uid, "invalid parquet index uid in pending operation, skipping");
                continue;
            };
            let Some(index_entry) = self.indexes.get(&index_uid) else {
                error!(index_uid=%index_uid, "parquet index config not found for pending operation, skipping");
                continue;
            };

            let task_id = Ulid::new().to_string();
            let assignment = build_task_assignment(&task_id, index_entry, &operation, &index_uid);
            let split_ids = operation
                .splits_as_slice()
                .iter()
                .map(|split| split.split_id.as_str().to_string())
                .collect();
            self.state
                .record_assignment(task_id, split_ids, node_id.clone());
            assignments.push(assignment);
        }
        assignments
    }

    async fn refresh_indexes(&mut self) -> Result<()> {
        let index_metadata_list = self
            .metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await?
            .deserialize_indexes_metadata()
            .await?;

        self.indexes.clear();
        for index_metadata in index_metadata_list {
            if !is_parquet_pipeline_index(&index_metadata.index_uid.index_id) {
                continue;
            }
            self.indexes.insert(
                index_metadata.index_uid.clone(),
                ParquetIndexEntry::new(index_metadata),
            );
        }
        Ok(())
    }

    async fn scan_metastore(&self) -> Result<Vec<ParquetSplitRecord>> {
        let mut splits = Vec::new();
        for index_uid in self.indexes.keys() {
            let query = ListParquetSplitsQuery::for_index(index_uid.clone())
                .retain_immature(OffsetDateTime::now_utc())
                .with_update_timestamp_gte(self.cursor);
            let mut index_splits = self.list_index_splits(index_uid, &query).await?;
            splits.append(&mut index_splits);
        }
        if !splits.is_empty() {
            info!(
                num_splits = splits.len(),
                "fetched published parquet splits for compaction planning"
            );
        }
        Ok(splits)
    }

    async fn list_index_splits(
        &self,
        index_uid: &IndexUid,
        query: &ListParquetSplitsQuery,
    ) -> Result<Vec<ParquetSplitRecord>> {
        let records = if is_sketches_index(&index_uid.index_id) {
            let request = ListSketchSplitsRequest::try_from_query(index_uid.clone(), query)?;
            self.metastore
                .list_sketch_splits(request)
                .await?
                .deserialize_splits()?
        } else {
            let request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), query)?;
            self.metastore
                .list_metrics_splits(request)
                .await?
                .deserialize_splits()?
        };
        Ok(records)
    }

    fn ingest_splits(&mut self, splits: Vec<ParquetSplitRecord>) {
        for split_record in splits {
            let split = split_record.metadata;
            if self.state.is_split_known(split.split_id.as_str()) {
                continue;
            }
            let Ok(index_uid) = IndexUid::from_str(&split.index_uid) else {
                error!(index_uid=%split.index_uid, split_id=%split.split_id, "invalid index uid on parquet split, skipping split");
                continue;
            };
            let Some(index_entry) = self.indexes.get(&index_uid) else {
                error!(index_uid=%index_uid, split_id=%split.split_id, "parquet index config not found, skipping split");
                continue;
            };
            if index_entry.is_split_mature(&split) {
                continue;
            }
            self.cursor = self.cursor.max(split_record.update_timestamp);
            self.state.track_split(split);
        }
    }

    fn run_merge_policies(&mut self) {
        for scope in self.state.scope_keys() {
            let Ok(index_uid) = IndexUid::from_str(&scope.index_uid) else {
                error!(index_uid=%scope.index_uid, "invalid parquet index uid in compaction scope, skipping");
                continue;
            };
            if let Some(index_entry) = self.indexes.get(&index_uid) {
                self.state.plan_scope(&scope, &index_entry.merge_policy);
            }
        }
    }
}

fn build_task_assignment(
    task_id: &str,
    index_entry: &ParquetIndexEntry,
    operation: &ParquetMergeOperation,
    index_uid: &IndexUid,
) -> MergeTaskAssignment {
    MergeTaskAssignment {
        task_id: task_id.to_string(),
        splits_metadata_json: operation
            .splits_as_slice()
            .iter()
            .map(|split| {
                serde_json::to_string(split)
                    .expect("parquet split metadata serialization should not fail")
            })
            .collect(),
        doc_mapping_json: index_entry.doc_mapping_json(),
        search_settings_json: index_entry.search_settings_json(),
        indexing_settings_json: index_entry.indexing_settings_json(),
        retention_policy_json: index_entry.retention_policy_json(),
        index_uid: Some(index_uid.clone()),
        source_id: String::new(),
        index_storage_uri: index_entry.index_storage_uri(),
        task_kind: CompactionTaskKind::Parquet as i32,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ops::Bound;
    use std::time::SystemTime;

    use quickwit_metastore::{IndexMetadata, SplitState};
    use quickwit_parquet_engine::split::{
        ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange,
    };
    use quickwit_proto::metastore::{
        ListMetricsSplitsResponse, MetastoreServiceClient, MockMetastoreService,
    };
    use quickwit_proto::types::{IndexUid, NodeId};

    use super::*;

    fn test_index_entry() -> ParquetIndexEntry {
        let mut index_metadata =
            IndexMetadata::for_test("datadog-metrics", "ram:///datadog-metrics");
        index_metadata
            .index_config
            .indexing_settings
            .parquet_merge_policy = Some(
            quickwit_config::merge_policy_config::ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                maturation_period: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
        );
        ParquetIndexEntry::new(index_metadata)
    }

    fn test_split(split_id: &str, index_uid: &IndexUid) -> ParquetSplitMetadata {
        ParquetSplitMetadata {
            kind: ParquetSplitKind::Metrics,
            split_id: ParquetSplitId::new(split_id),
            index_uid: index_uid.to_string(),
            partition_id: 0,
            time_range: TimeRange::new(1000, 2000),
            num_rows: 100,
            size_bytes: 1_000_000,
            metric_names: HashSet::new(),
            low_cardinality_tags: Default::default(),
            high_cardinality_tag_keys: Default::default(),
            created_at: SystemTime::now(),
            parquet_file: format!("{split_id}.parquet"),
            window: Some(0..3600),
            sort_fields: "metric_name|host|timestamp_secs/V2".to_string(),
            num_merge_ops: 0,
            row_keys_proto: None,
            zonemap_regexes: Default::default(),
        }
    }

    fn test_split_record(
        split_id: &str,
        index_uid: &IndexUid,
        update_timestamp: i64,
    ) -> ParquetSplitRecord {
        ParquetSplitRecord {
            state: SplitState::Published,
            update_timestamp,
            metadata: test_split(split_id, index_uid),
        }
    }

    #[test]
    fn test_new_starts_cursor_with_24h_lookback() {
        let before = OffsetDateTime::now_utc().unix_timestamp();
        let planner = ParquetCompactionPlanner::new(MetastoreServiceClient::mocked());
        let after = OffsetDateTime::now_utc().unix_timestamp();

        assert!(planner.cursor >= before - STARTUP_LOOKBACK.as_secs() as i64);
        assert!(planner.cursor <= after - STARTUP_LOOKBACK.as_secs() as i64);
    }

    #[tokio::test]
    async fn test_scan_metastore_uses_cursor() {
        let expected_cursor = 12_345;

        let mut mock = MockMetastoreService::new();
        mock.expect_list_metrics_splits().returning(move |request| {
            let query = request.deserialize_query().unwrap();
            assert_eq!(
                query.update_timestamp.start,
                Bound::Included(expected_cursor)
            );
            assert!(matches!(query.mature, Bound::Excluded(_)));
            assert_eq!(query.split_states, vec![SplitState::Published]);
            Ok(ListMetricsSplitsResponse::try_from_splits(&[]).unwrap())
        });

        let mut planner = ParquetCompactionPlanner::new(MetastoreServiceClient::from_mock(mock));
        planner.cursor = expected_cursor;
        let index_entry = test_index_entry();
        let index_uid = index_entry.index_metadata.index_uid.clone();
        planner.indexes.insert(index_uid, index_entry);

        let splits = planner.scan_metastore().await.unwrap();
        assert!(splits.is_empty());
    }

    #[test]
    fn test_ingest_splits_advances_cursor_for_tracked_splits() {
        let index_entry = test_index_entry();
        let index_uid = index_entry.index_metadata.index_uid.clone();

        let mut planner = ParquetCompactionPlanner::new(MetastoreServiceClient::mocked());
        planner.cursor = 0;
        planner.indexes.insert(index_uid.clone(), index_entry);

        planner.state.track_split(test_split("known", &index_uid));
        planner.ingest_splits(vec![
            test_split_record("known", &index_uid, 1000),
            test_split_record("fresh", &index_uid, 3000),
        ]);

        assert!(planner.state.is_split_known("known"));
        assert!(planner.state.is_split_known("fresh"));
        assert_eq!(planner.cursor, 3000);
    }

    #[test]
    fn test_build_task_assignment_marks_parquet_kind() {
        let index_entry = test_index_entry();
        let index_uid = index_entry.index_metadata.index_uid.clone();
        let operation = ParquetMergeOperation::new(vec![
            test_split("s1", &index_uid),
            test_split("s2", &index_uid),
        ]);

        let assignment = build_task_assignment("task-1", &index_entry, &operation, &index_uid);

        assert_eq!(assignment.task_kind, CompactionTaskKind::Parquet as i32);
        assert_eq!(assignment.splits_metadata_json.len(), 2);
        assert_eq!(assignment.index_uid, Some(index_uid));
        assert_eq!(assignment.source_id, String::new());
        assert_eq!(assignment.doc_mapping_json, index_entry.doc_mapping_json());
        assert_eq!(
            assignment.search_settings_json,
            index_entry.search_settings_json()
        );
        assert_eq!(
            assignment.retention_policy_json,
            index_entry.retention_policy_json()
        );
        assert!(!assignment.indexing_settings_json.is_empty());
    }

    #[test]
    fn test_assign_tasks_sets_parquet_kind() {
        let index_entry = test_index_entry();
        let index_uid = index_entry.index_metadata.index_uid.clone();

        let mut planner = ParquetCompactionPlanner::new(MetastoreServiceClient::mocked());
        planner.indexes.insert(index_uid.clone(), index_entry);
        planner.ingest_splits(vec![
            test_split_record("s1", &index_uid, 1000),
            test_split_record("s2", &index_uid, 2000),
        ]);
        planner.run_merge_policies();

        let assignments = planner.assign_tasks(&NodeId::from("test-node"), 1);
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].task_kind, CompactionTaskKind::Parquet as i32);
        assert_eq!(assignments[0].splits_metadata_json.len(), 2);
    }
}
