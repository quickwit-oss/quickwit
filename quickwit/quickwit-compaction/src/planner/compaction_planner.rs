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

use anyhow::Result;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, Split, SplitState,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService, MetastoreServiceClient};
use time::OffsetDateTime;
use tracing::error;

use super::compaction_state::CompactionState;
use super::index_config_store::IndexConfigStore;

pub struct CompactionPlanner {
    state: CompactionState,
    index_config_store: IndexConfigStore,
    cursor: i64,
    metastore: MetastoreServiceClient,
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
}

const SCAN_AND_PLAN_INTERVAL: Duration = Duration::from_secs(5);

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
        if let Err(err) = self.scan_and_plan().await {
            error!(error=%err, "error scanning metastore and planning merges");
        }
        ctx.schedule_self_msg(SCAN_AND_PLAN_INTERVAL, ScanAndPlan);
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

#[cfg(test)]
mod tests {
    use quickwit_common::ServiceStream;
    use quickwit_metastore::{
        IndexMetadata, IndexMetadataResponseExt, ListSplitsResponseExt, Split, SplitMetadata,
        SplitState,
    };
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
                ..Default::default()
            },
        }
    }

    fn test_index_metadata() -> IndexMetadata {
        IndexMetadata::for_test("test-index", "ram:///test-index")
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
}
