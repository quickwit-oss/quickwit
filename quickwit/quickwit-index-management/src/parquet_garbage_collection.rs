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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use quickwit_common::Progress;
use quickwit_metastore::{
    ListMetricsSplitsQuery, ListMetricsSplitsRequestExt, ListMetricsSplitsResponseExt,
};
use quickwit_parquet_engine::split::{MetricsSplitRecord, MetricsSplitState};
use quickwit_proto::metastore::{
    DeleteMetricsSplitsRequest, ListMetricsSplitsRequest, MarkMetricsSplitsForDeletionRequest,
    MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::Storage;
use time::OffsetDateTime;
use tracing::{error, info, instrument, warn};

use crate::garbage_collection::{
    GcMetrics, RecordGcMetrics, SplitToDelete, delete_split_files,
    get_maximum_split_deletion_rate_per_sec, protect_future,
};

/// Detail about a single parquet split affected by GC.
#[derive(Debug)]
pub struct ParquetSplitInfo {
    pub split_id: String,
    pub file_size_bytes: u64,
}

/// Information on what parquet splits have and have not been cleaned up by the GC.
#[derive(Debug, Default)]
pub struct ParquetSplitRemovalInfo {
    pub removed_parquet_splits_entries: Vec<ParquetSplitInfo>,
    pub failed_parquet_splits: Vec<ParquetSplitInfo>,
}

impl ParquetSplitRemovalInfo {
    pub fn removed_split_count(&self) -> usize {
        self.removed_parquet_splits_entries.len()
    }

    pub fn removed_bytes(&self) -> u64 {
        self.removed_parquet_splits_entries
            .iter()
            .map(|s| s.file_size_bytes)
            .sum()
    }

    pub fn failed_split_count(&self) -> usize {
        self.failed_parquet_splits.len()
    }
}

/// Maximum number of parquet splits to process per paginated query.
const DELETE_PARQUET_SPLITS_BATCH_SIZE: usize = 10_000;

/// Runs garbage collection for parquet splits.
#[instrument(skip_all, fields(num_indexes=%indexes.len()))]
pub async fn run_parquet_garbage_collect(
    indexes: HashMap<IndexUid, Arc<dyn Storage>>,
    metastore: MetastoreServiceClient,
    staged_grace_period: Duration,
    deletion_grace_period: Duration,
    dry_run: bool,
    progress_opt: Option<&Progress>,
    metrics: Option<GcMetrics>,
) -> anyhow::Result<ParquetSplitRemovalInfo> {
    let mut removal_info = ParquetSplitRemovalInfo::default();

    // Phase 1: List stale Staged splits
    let staged_cutoff =
        OffsetDateTime::now_utc().unix_timestamp() - staged_grace_period.as_secs() as i64;

    let mut deletable_staged_splits = Vec::new();
    for index_uid in indexes.keys() {
        match list_stale_staged_splits(&metastore, index_uid, staged_cutoff, progress_opt).await {
            Ok(splits) => deletable_staged_splits.extend(splits),
            Err(err) => {
                error!(index_uid=%index_uid, error=?err, "failed to list stale staged parquet splits");
            }
        }
    }

    if dry_run {
        let deletion_cutoff =
            OffsetDateTime::now_utc().unix_timestamp() - deletion_grace_period.as_secs() as i64;

        let mut splits_marked_for_deletion = Vec::new();
        for index_uid in indexes.keys() {
            match list_marked_splits(&metastore, index_uid, deletion_cutoff, progress_opt).await {
                Ok(splits) => splits_marked_for_deletion.extend(splits),
                Err(err) => {
                    error!(index_uid=%index_uid, error=?err, "failed to list marked parquet splits");
                }
            }
        }
        splits_marked_for_deletion.extend(deletable_staged_splits);

        let candidate_entries: Vec<ParquetSplitInfo> = splits_marked_for_deletion
            .into_iter()
            .map(|s| ParquetSplitInfo {
                split_id: s.metadata.split_id.to_string(),
                file_size_bytes: s.metadata.size_bytes,
            })
            .collect();
        return Ok(ParquetSplitRemovalInfo {
            removed_parquet_splits_entries: candidate_entries,
            failed_parquet_splits: Vec::new(),
        });
    }

    // Schedule all eligible staged splits for delete
    if let Err(err) =
        mark_splits_for_deletion(&metastore, &deletable_staged_splits, progress_opt).await
    {
        error!(error=?err, "failed to mark stale staged parquet splits");
    }

    // Phase 2: Delete splits marked for deletion past the grace period
    let deletion_cutoff =
        OffsetDateTime::now_utc().unix_timestamp() - deletion_grace_period.as_secs() as i64;

    for (index_uid, storage) in &indexes {
        match delete_marked_parquet_splits(
            &metastore,
            index_uid,
            storage.clone(),
            deletion_cutoff,
            progress_opt,
        )
        .await
        {
            Ok(batch_info) => {
                removal_info
                    .removed_parquet_splits_entries
                    .extend(batch_info.removed_parquet_splits_entries);
                removal_info
                    .failed_parquet_splits
                    .extend(batch_info.failed_parquet_splits);
            }
            Err(err) => {
                error!(index_uid=%index_uid, error=?err, "failed to delete marked parquet splits");
            }
        }
    }

    metrics.record(
        removal_info.removed_split_count(),
        removal_info.removed_bytes(),
        removal_info.failed_split_count(),
    );

    Ok(removal_info)
}

/// Lists Staged parquet splits older than the cutoff.
async fn list_stale_staged_splits(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    staged_cutoff: i64,
    progress_opt: Option<&Progress>,
) -> anyhow::Result<Vec<MetricsSplitRecord>> {
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec![MetricsSplitState::Staged.to_string()])
        .with_update_timestamp_lte(staged_cutoff);

    let request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query)
        .context("failed to build list metrics splits request")?;

    let response = protect_future(progress_opt, metastore.list_metrics_splits(request)).await?;
    response
        .deserialize_splits()
        .context("failed to deserialize metrics splits")
}

/// Lists MarkedForDeletion parquet splits older than the cutoff.
async fn list_marked_splits(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    deletion_cutoff: i64,
    progress_opt: Option<&Progress>,
) -> anyhow::Result<Vec<MetricsSplitRecord>> {
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec![MetricsSplitState::MarkedForDeletion.to_string()])
        .with_update_timestamp_lte(deletion_cutoff);

    let request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query)
        .context("failed to build list metrics splits request")?;

    let response = protect_future(progress_opt, metastore.list_metrics_splits(request)).await?;
    response
        .deserialize_splits()
        .context("failed to deserialize metrics splits")
}

/// Marks the given splits for deletion in the metastore, grouped by index.
async fn mark_splits_for_deletion(
    metastore: &MetastoreServiceClient,
    splits: &[MetricsSplitRecord],
    progress_opt: Option<&Progress>,
) -> anyhow::Result<()> {
    if splits.is_empty() {
        return Ok(());
    }

    // Group split IDs by index_uid string, then resolve to IndexUid for the request.
    let mut splits_by_index: HashMap<String, Vec<String>> = HashMap::new();
    for split in splits {
        splits_by_index
            .entry(split.metadata.index_uid.clone())
            .or_default()
            .push(split.metadata.split_id.to_string());
    }

    for (index_uid_str, split_ids) in splits_by_index {
        let index_uid: IndexUid = index_uid_str.parse()?;
        info!(index_uid=%index_uid, count=%split_ids.len(), "marking stale staged parquet splits for deletion");

        protect_future(
            progress_opt,
            metastore.mark_metrics_splits_for_deletion(MarkMetricsSplitsForDeletionRequest {
                index_uid: Some(index_uid),
                split_ids,
            }),
        )
        .await?;
    }

    Ok(())
}

/// Phase 2: Find MarkedForDeletion parquet splits older than the cutoff,
/// delete their storage files, then delete the metastore entries.
async fn delete_marked_parquet_splits(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    storage: Arc<dyn Storage>,
    deletion_cutoff: i64,
    progress_opt: Option<&Progress>,
) -> anyhow::Result<ParquetSplitRemovalInfo> {
    let mut removal_info = ParquetSplitRemovalInfo::default();

    let mut query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec![MetricsSplitState::MarkedForDeletion.to_string()])
        .with_update_timestamp_lte(deletion_cutoff)
        .with_limit(DELETE_PARQUET_SPLITS_BATCH_SIZE);

    loop {
        let sleep_duration = if let Some(max_rate) = get_maximum_split_deletion_rate_per_sec() {
            Duration::from_secs(DELETE_PARQUET_SPLITS_BATCH_SIZE.div_ceil(max_rate) as u64)
        } else {
            Duration::default()
        };
        let sleep_future = tokio::time::sleep(sleep_duration);

        let request = match ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query) {
            Ok(req) => req,
            Err(err) => {
                error!(index_uid=%index_uid, error=?err, "failed to build list metrics splits request");
                break;
            }
        };

        let splits: Vec<MetricsSplitRecord> = match protect_future(
            progress_opt,
            metastore.list_metrics_splits(request),
        )
        .await
        {
            Ok(resp) => match resp.deserialize_splits() {
                Ok(splits) => splits,
                Err(err) => {
                    error!(index_uid=%index_uid, error=?err, "failed to deserialize metrics splits");
                    break;
                }
            },
            Err(err) => {
                error!(index_uid=%index_uid, error=?err, "failed to list metrics splits");
                break;
            }
        };

        // We page through the list of splits to delete using a limit and a `search_after` trick.
        // To detect if this is the last page, we check if the number of splits is less than the
        // limit.
        assert!(splits.len() <= DELETE_PARQUET_SPLITS_BATCH_SIZE);
        let splits_to_delete_possibly_remaining = splits.len() == DELETE_PARQUET_SPLITS_BATCH_SIZE;

        // Set split after which to search for the next loop.
        let Some(last_split) = splits.last() else {
            break;
        };
        query = query.with_after_split_id(last_split.metadata.split_id.to_string());

        let (batch_succeeded, batch_failed) = delete_parquet_splits_from_storage_and_metastore(
            metastore,
            index_uid,
            storage.as_ref(),
            &splits,
            progress_opt,
        )
        .await;
        removal_info
            .removed_parquet_splits_entries
            .extend(batch_succeeded);
        removal_info.failed_parquet_splits.extend(batch_failed);

        if splits_to_delete_possibly_remaining {
            sleep_future.await;
        } else {
            // Stop the GC if this was the last batch.
            // We are guaranteed to make progress due to .with_after_split_id().
            break;
        }
    }

    Ok(removal_info)
}

/// Deletes a single batch of parquet splits from storage and metastore.
/// Returns (succeeded, failed).
async fn delete_parquet_splits_from_storage_and_metastore(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    storage: &dyn Storage,
    splits: &[MetricsSplitRecord],
    progress_opt: Option<&Progress>,
) -> (Vec<ParquetSplitInfo>, Vec<ParquetSplitInfo>) {
    let splits_to_delete: Vec<SplitToDelete> = splits
        .iter()
        .map(|s| SplitToDelete {
            split_id: s.metadata.split_id.to_string(),
            path: PathBuf::from(s.metadata.parquet_filename()),
            size_bytes: s.metadata.size_bytes,
        })
        .collect();

    let (succeeded_stds, failed_stds, storage_err) =
        delete_split_files(storage, splits_to_delete, progress_opt).await;

    if let Some(bulk_err) = storage_err {
        warn!(
            index_id=%index_uid,
            num_failed=%failed_stds.len(),
            num_succeeded=%succeeded_stds.len(),
            error=?bulk_err,
            "partial failure deleting parquet files from storage"
        );
    }

    let storage_failed: Vec<ParquetSplitInfo> = failed_stds
        .into_iter()
        .map(|s| ParquetSplitInfo {
            split_id: s.split_id,
            file_size_bytes: s.size_bytes,
        })
        .collect();

    if succeeded_stds.is_empty() {
        return (Vec::new(), storage_failed);
    }

    let batch_len = succeeded_stds.len();
    let ids_to_delete: Vec<String> = succeeded_stds.iter().map(|s| s.split_id.clone()).collect();
    let delete_request = DeleteMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        split_ids: ids_to_delete,
    };
    let metastore_result = protect_future(
        progress_opt,
        metastore.delete_metrics_splits(delete_request),
    )
    .await;

    if let Some(progress) = progress_opt {
        progress.record_progress();
    }

    let succeeded: Vec<ParquetSplitInfo> = succeeded_stds
        .into_iter()
        .map(|s| ParquetSplitInfo {
            split_id: s.split_id,
            file_size_bytes: s.size_bytes,
        })
        .collect();

    match metastore_result {
        Ok(_) => {
            let bytes_deleted: u64 = succeeded.iter().map(|s| s.file_size_bytes).sum();
            info!(index_uid=%index_uid, count=%batch_len, bytes=%bytes_deleted, "deleted parquet splits");
            (succeeded, storage_failed)
        }
        Err(err) => {
            error!(index_uid=%index_uid, count=%batch_len, error=?err, "failed to delete parquet splits from metastore");
            let mut failed = storage_failed;
            failed.extend(succeeded);
            (Vec::new(), failed)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_metastore::ListMetricsSplitsResponseExt;
    use quickwit_parquet_engine::split::{
        MetricsSplitMetadata, MetricsSplitRecord, MetricsSplitState, SplitId, TimeRange,
    };
    use quickwit_proto::metastore::{
        EmptyResponse, ListMetricsSplitsResponse, MetastoreServiceClient, MockMetastoreService,
    };
    use quickwit_storage::{DeleteFailure, MockStorage};

    use super::*;

    const TEST_INDEX: &str = "otel-metrics-v0_9";

    fn test_index_uid() -> IndexUid {
        IndexUid::for_test(TEST_INDEX, 0)
    }

    fn make_split(split_id: &str, state: MetricsSplitState) -> MetricsSplitRecord {
        MetricsSplitRecord {
            state,
            update_timestamp: 0,
            metadata: MetricsSplitMetadata::builder()
                .split_id(SplitId::new(split_id))
                .index_uid(test_index_uid().to_string())
                .time_range(TimeRange::new(1000, 2000))
                .num_rows(100)
                .size_bytes(1024)
                .build(),
        }
    }

    fn list_response(splits: &[MetricsSplitRecord]) -> ListMetricsSplitsResponse {
        ListMetricsSplitsResponse::try_from_splits(splits).unwrap()
    }

    fn test_indexes(storage: Arc<dyn Storage>) -> HashMap<IndexUid, Arc<dyn Storage>> {
        HashMap::from([(test_index_uid(), storage)])
    }

    #[tokio::test]
    async fn test_parquet_gc_marks_stale_staged_splits() {
        let mut mock = MockMetastoreService::new();

        let staged = vec![
            make_split("staged-1", MetricsSplitState::Staged),
            make_split("staged-2", MetricsSplitState::Staged),
        ];
        let resp = list_response(&staged);
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(move |_| Ok(resp.clone()));
        mock.expect_mark_metrics_splits_for_deletion()
            .times(1)
            .returning(|req| {
                assert_eq!(req.index_uid().index_id, TEST_INDEX);
                assert_eq!(req.split_ids.len(), 2);
                Ok(EmptyResponse {})
            });
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(|_| Ok(ListMetricsSplitsResponse::empty()));

        let result = run_parquet_garbage_collect(
            test_indexes(Arc::new(MockStorage::new())),
            MetastoreServiceClient::from_mock(mock),
            Duration::from_secs(0),
            Duration::from_secs(30),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.removed_split_count(), 0);
        assert_eq!(result.failed_split_count(), 0);
    }

    #[tokio::test]
    async fn test_parquet_gc_deletes_marked_splits() {
        let mut mock = MockMetastoreService::new();

        mock.expect_list_metrics_splits()
            .times(1)
            .returning(|_| Ok(ListMetricsSplitsResponse::empty()));

        let marked = vec![
            make_split("marked-1", MetricsSplitState::MarkedForDeletion),
            make_split("marked-2", MetricsSplitState::MarkedForDeletion),
        ];
        let resp = list_response(&marked);
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(move |_| Ok(resp.clone()));
        mock.expect_delete_metrics_splits()
            .times(1)
            .returning(|req| {
                assert_eq!(req.index_uid().index_id, TEST_INDEX);
                assert_eq!(req.split_ids.len(), 2);
                Ok(EmptyResponse {})
            });

        let mut storage = MockStorage::new();
        storage.expect_bulk_delete().times(1).returning(|paths| {
            assert_eq!(paths.len(), 2);
            Ok(())
        });

        let result = run_parquet_garbage_collect(
            test_indexes(Arc::new(storage)),
            MetastoreServiceClient::from_mock(mock),
            Duration::from_secs(30),
            Duration::from_secs(0),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.removed_split_count(), 2);
        assert_eq!(result.removed_bytes(), 2048);
        assert_eq!(result.failed_split_count(), 0);
    }

    #[tokio::test]
    async fn test_parquet_gc_handles_partial_storage_failure() {
        let mut mock = MockMetastoreService::new();

        mock.expect_list_metrics_splits()
            .times(1)
            .returning(|_| Ok(ListMetricsSplitsResponse::empty()));

        let marked = vec![
            make_split("ok-split", MetricsSplitState::MarkedForDeletion),
            make_split("fail-split", MetricsSplitState::MarkedForDeletion),
        ];
        let resp = list_response(&marked);
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(move |_| Ok(resp.clone()));
        // Only the successful split should be deleted from metastore
        mock.expect_delete_metrics_splits()
            .times(1)
            .returning(|req| {
                assert_eq!(req.split_ids.len(), 1);
                assert_eq!(req.split_ids[0], "ok-split");
                Ok(EmptyResponse {})
            });

        let mut storage = MockStorage::new();
        storage.expect_bulk_delete().times(1).returning(|_paths| {
            let successes = vec![PathBuf::from("ok-split.parquet")];
            let failures = HashMap::from([(
                PathBuf::from("fail-split.parquet"),
                DeleteFailure {
                    code: Some("AccessDenied".to_string()),
                    ..Default::default()
                },
            )]);
            Err(quickwit_storage::BulkDeleteError {
                successes,
                failures,
                ..Default::default()
            })
        });

        let result = run_parquet_garbage_collect(
            test_indexes(Arc::new(storage)),
            MetastoreServiceClient::from_mock(mock),
            Duration::from_secs(30),
            Duration::from_secs(0),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.removed_split_count(), 1);
        assert_eq!(result.removed_bytes(), 1024);
        assert_eq!(result.failed_split_count(), 1);
    }

    #[tokio::test]
    async fn test_parquet_gc_dry_run() {
        let mut mock = MockMetastoreService::new();

        // Phase 1: list staged splits
        let staged = vec![make_split("staged-1", MetricsSplitState::Staged)];
        let resp = list_response(&staged);
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(move |_| Ok(resp.clone()));
        // mark_metrics_splits_for_deletion should NOT be called in dry_run
        mock.expect_mark_metrics_splits_for_deletion().times(0);

        // Phase 2: list marked splits
        let marked = vec![
            make_split("marked-1", MetricsSplitState::MarkedForDeletion),
            make_split("marked-2", MetricsSplitState::MarkedForDeletion),
        ];
        let resp = list_response(&marked);
        mock.expect_list_metrics_splits()
            .times(1)
            .returning(move |_| Ok(resp.clone()));
        // delete should NOT be called in dry_run
        mock.expect_delete_metrics_splits().times(0);

        let storage = MockStorage::new();
        // bulk_delete should NOT be called in dry_run

        let result = run_parquet_garbage_collect(
            test_indexes(Arc::new(storage)),
            MetastoreServiceClient::from_mock(mock),
            Duration::from_secs(0),
            Duration::from_secs(0),
            true,
            None,
            None,
        )
        .await
        .unwrap();

        // Dry run reports candidates as "removed" (would be removed):
        // 1 stale staged + 2 marked for deletion = 3 candidates
        assert_eq!(result.removed_split_count(), 3);
        assert_eq!(result.removed_bytes(), 3072);
        assert_eq!(result.failed_split_count(), 0);
    }
}
