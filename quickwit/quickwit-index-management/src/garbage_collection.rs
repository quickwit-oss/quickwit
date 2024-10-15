// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{Future, StreamExt};
use itertools::Itertools;
use quickwit_common::metrics::IntCounter;
use quickwit_common::pretty::PrettySample;
use quickwit_common::Progress;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitInfo,
    SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{
    DeleteSplitsRequest, ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError,
    MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{IndexUid, SplitId};
use quickwit_storage::{BulkDeleteError, Storage};
use thiserror::Error;
use time::OffsetDateTime;
use tracing::{error, instrument};

/// The maximum number of splits that the GC should delete per attempt.
const DELETE_SPLITS_BATCH_SIZE: usize = 10_000;

pub struct GcMetrics {
    pub deleted_splits: IntCounter,
    pub deleted_bytes: IntCounter,
    pub failed_splits: IntCounter,
}

trait RecordGcMetrics {
    fn record(&self, num_delete_splits: usize, num_deleted_bytes: u64, num_failed_splits: usize);
}

impl RecordGcMetrics for Option<GcMetrics> {
    fn record(&self, num_deleted_splits: usize, num_deleted_bytes: u64, num_failed_splits: usize) {
        if let Some(metrics) = self {
            metrics.deleted_splits.inc_by(num_deleted_splits as u64);
            metrics.deleted_bytes.inc_by(num_deleted_bytes);
            metrics.failed_splits.inc_by(num_failed_splits as u64);
        }
    }
}

/// [`DeleteSplitsError`] describes the errors that occurred during the deletion of splits from
/// storage and metastore.
#[derive(Error, Debug)]
#[error("failed to delete splits from storage and/or metastore")]
pub struct DeleteSplitsError {
    successes: Vec<SplitInfo>,
    storage_error: Option<BulkDeleteError>,
    storage_failures: Vec<SplitInfo>,
    metastore_error: Option<MetastoreError>,
    metastore_failures: Vec<SplitInfo>,
}

async fn protect_future<Fut, T>(progress: Option<&Progress>, future: Fut) -> T
where Fut: Future<Output = T> {
    match progress {
        None => future.await,
        Some(progress) => {
            let _guard = progress.protect_zone();
            future.await
        }
    }
}

/// Information on what splits have and have not been cleaned up by the GC.
#[derive(Debug, Default)]
pub struct SplitRemovalInfo {
    /// The set of splits that have been removed.
    pub removed_split_entries: Vec<SplitInfo>,
    /// The set of split ids that were attempted to be removed, but were unsuccessful.
    pub failed_splits: Vec<SplitInfo>,
}

/// Detect all dangling splits and associated files from the index and removes them.
///
/// * `indexes` - The target index uids and storages.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `staged_grace_period` -  Threshold period after which a staged split can be safely garbage
///   collected.
/// * `deletion_grace_period` -  Threshold period after which a marked as deleted split can be
///   safely deleted.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
/// * `progress` - For reporting progress (useful when called from within a quickwit actor).
pub async fn run_garbage_collect(
    indexes: HashMap<IndexUid, Arc<dyn Storage>>,
    metastore: MetastoreServiceClient,
    staged_grace_period: Duration,
    deletion_grace_period: Duration,
    dry_run: bool,
    progress_opt: Option<&Progress>,
    metrics: Option<GcMetrics>,
) -> anyhow::Result<SplitRemovalInfo> {
    let grace_period_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - staged_grace_period.as_secs() as i64;

    let index_uids: Vec<IndexUid> = indexes.keys().cloned().collect();

    // TODO maybe we want to do a ListSplitsQuery::for_all_indexes and post-filter ourselves here
    let Some(list_splits_query_for_index_uids) =
        ListSplitsQuery::try_from_index_uids(index_uids.clone())
    else {
        return Ok(SplitRemovalInfo::default());
    };
    let list_splits_query = list_splits_query_for_index_uids
        .clone()
        .with_split_state(SplitState::Staged)
        .with_update_timestamp_lte(grace_period_timestamp);

    let list_deletable_staged_request =
        ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
    let deletable_staged_splits: Vec<SplitMetadata> = protect_future(
        progress_opt,
        metastore.list_splits(list_deletable_staged_request),
    )
    .await?
    .collect_splits_metadata()
    .await?;

    if dry_run {
        let marked_for_deletion_query =
            list_splits_query_for_index_uids.with_split_state(SplitState::MarkedForDeletion);
        let marked_for_deletion_request =
            ListSplitsRequest::try_from_list_splits_query(&marked_for_deletion_query)?;
        let mut splits_marked_for_deletion: Vec<SplitMetadata> = protect_future(
            progress_opt,
            metastore.list_splits(marked_for_deletion_request),
        )
        .await?
        .collect_splits_metadata()
        .await?;
        splits_marked_for_deletion.extend(deletable_staged_splits);

        let candidate_entries: Vec<SplitInfo> = splits_marked_for_deletion
            .into_iter()
            .map(|split| split.as_split_info())
            .collect();
        return Ok(SplitRemovalInfo {
            removed_split_entries: candidate_entries,
            failed_splits: Vec::new(),
        });
    }

    // Schedule all eligible staged splits for delete
    let split_ids: HashMap<IndexUid, Vec<SplitId>> = deletable_staged_splits
        .into_iter()
        .map(|split| (split.index_uid, split.split_id))
        .into_group_map();
    for (index_uid, split_ids) in split_ids {
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid, split_ids);
        protect_future(
            progress_opt,
            metastore.mark_splits_for_deletion(mark_splits_for_deletion_request),
        )
        .await?;
    }

    // We delete splits marked for deletion that have an update timestamp anterior
    // to `now - deletion_grace_period`.
    let updated_before_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - deletion_grace_period.as_secs() as i64;

    let index_uid_set = index_uids.into_iter().collect();

    Ok(delete_splits_marked_for_deletion_several_indexes(
        index_uid_set,
        updated_before_timestamp,
        metastore,
        indexes,
        progress_opt,
        metrics,
    )
    .await)
}

async fn delete_splits(
    splits_metadata_to_delete_per_index: HashMap<IndexUid, Vec<SplitMetadata>>,
    storages: &HashMap<IndexUid, Arc<dyn Storage>>,
    metastore: MetastoreServiceClient,
    progress_opt: Option<&Progress>,
    metrics: &Option<GcMetrics>,
    split_removal_info: &mut SplitRemovalInfo,
) -> Result<(), ()> {
    let mut delete_split_from_index_res_stream =
        futures::stream::iter(splits_metadata_to_delete_per_index)
            .map(|(index_uid, splits_metadata_to_delete)| {
                let storage = storages.get(&index_uid).cloned();
                let metastore = metastore.clone();
                async move {
                    if let Some(storage) = storage {
                        delete_splits_from_storage_and_metastore(
                            index_uid,
                            storage,
                            metastore,
                            splits_metadata_to_delete,
                            progress_opt,
                        )
                        .await
                    } else {
                        // in practice this can happen if the index was created between the start of
                        // the run and now, and one of its splits has already expired, which likely
                        // means a very long gc run, or if we run gc on a single index from the cli.
                        quickwit_common::rate_limited_warn!(
                            limit_per_min = 2,
                            index_uid=%index_uid,
                            "we are trying to GC without knowing the storage",
                        );
                        Ok(Vec::new())
                    }
                }
            })
            .buffer_unordered(10);
    let mut error_encountered = false;
    while let Some(delete_split_result) = delete_split_from_index_res_stream.next().await {
        match delete_split_result {
            Ok(entries) => {
                let deleted_bytes = entries
                    .iter()
                    .map(|entry| entry.file_size_bytes.as_u64())
                    .sum::<u64>();
                let deleted_splits_count = entries.len();

                metrics.record(deleted_splits_count, deleted_bytes, 0);
                split_removal_info.removed_split_entries.extend(entries);
            }
            Err(delete_split_error) => {
                let deleted_bytes = delete_split_error
                    .successes
                    .iter()
                    .map(|entry| entry.file_size_bytes.as_u64())
                    .sum::<u64>();
                let deleted_splits_count = delete_split_error.successes.len();
                let failed_splits_count = delete_split_error.storage_failures.len()
                    + delete_split_error.metastore_failures.len();

                metrics.record(deleted_splits_count, deleted_bytes, failed_splits_count);
                split_removal_info
                    .removed_split_entries
                    .extend(delete_split_error.successes);
                split_removal_info
                    .failed_splits
                    .extend(delete_split_error.storage_failures);
                split_removal_info
                    .failed_splits
                    .extend(delete_split_error.metastore_failures);
                error_encountered = true;
            }
        }
    }
    if error_encountered {
        Err(())
    } else {
        Ok(())
    }
}

/// Fetch the list metadata from the metastore and returns them as a Vec.
async fn list_splits_metadata(
    metastore: &MetastoreServiceClient,
    query: &ListSplitsQuery,
) -> anyhow::Result<Vec<SplitMetadata>> {
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(query)
        .context("failed to build list splits request")?;
    let splits_to_delete_stream = metastore
        .list_splits(list_splits_request)
        .await
        .context("failed to fetch stream splits")?;
    let splits = splits_to_delete_stream
        .collect_splits_metadata()
        .await
        .context("failed to collect splits")?;
    Ok(splits)
}

/// Removes any splits marked for deletion which haven't been
/// updated after `updated_before_timestamp` in batches of 1000 splits.
///
/// The aim of this is to spread the load out across a longer period
/// rather than short, heavy bursts on the metastore and storage system itself.
#[instrument(skip(index_uids, storages, metastore, progress_opt, metrics), fields(num_indexes=%index_uids.len()))]
async fn delete_splits_marked_for_deletion_several_indexes(
    index_uids: HashSet<IndexUid>,
    updated_before_timestamp: i64,
    metastore: MetastoreServiceClient,
    storages: HashMap<IndexUid, Arc<dyn Storage>>,
    progress_opt: Option<&Progress>,
    metrics: Option<GcMetrics>,
) -> SplitRemovalInfo {
    let mut split_removal_info = SplitRemovalInfo::default();

    // we ask for all indexes because the query is more efficient and we almost always want all
    // indexes anyway. The exception is when garbage collecting a single index from the commandline.
    // In this case, we will log a bunch of warn. i (trinity) consider it worth the more generic
    // code which needs fewer special case while testing, but we could check index_uids len if we
    // think it's a better idea.
    let list_splits_query = ListSplitsQuery::for_all_indexes();

    let mut list_splits_query = list_splits_query
        .with_split_state(SplitState::MarkedForDeletion)
        .with_update_timestamp_lte(updated_before_timestamp)
        .with_limit(DELETE_SPLITS_BATCH_SIZE)
        .sort_by_index_uid();

    loop {
        let splits_metadata_to_delete: Vec<SplitMetadata> = match protect_future(
            progress_opt,
            list_splits_metadata(&metastore, &list_splits_query),
        )
        .await
        {
            Ok(splits) => splits,
            Err(list_splits_err) => {
                error!(error=?list_splits_err, "failed to list splits");
                break;
            }
        };

        // set split after which to search for the next loop
        let Some(last_split_metadata) = splits_metadata_to_delete.last() else {
            break;
        };
        list_splits_query = list_splits_query.after_split(last_split_metadata);

        let num_splits_to_delete = splits_metadata_to_delete.len();

        let splits_metadata_to_delete_per_index: HashMap<IndexUid, Vec<SplitMetadata>> =
            splits_metadata_to_delete
                .into_iter()
                .filter(|meta| index_uids.contains(&meta.index_uid))
                .map(|meta| (meta.index_uid.clone(), meta))
                .into_group_map();

        // ignore return we continue either way
        let _: Result<(), ()> = delete_splits(
            splits_metadata_to_delete_per_index,
            &storages,
            metastore.clone(),
            progress_opt,
            &metrics,
            &mut split_removal_info,
        )
        .await;

        if num_splits_to_delete < DELETE_SPLITS_BATCH_SIZE {
            // stop the gc if this was the last batch
            // we are guaranteed to make progress due to .after_split()
            break;
        }
    }

    split_removal_info
}

/// Delete a list of splits from the storage and the metastore.
/// It should leave the index and the metastore in good state.
///
/// * `index_id` - The target index id.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `splits`  - The list of splits to delete.
/// * `progress` - For reporting progress (useful when called from within a quickwit actor).
pub async fn delete_splits_from_storage_and_metastore(
    index_uid: IndexUid,
    storage: Arc<dyn Storage>,
    metastore: MetastoreServiceClient,
    splits: Vec<SplitMetadata>,
    progress_opt: Option<&Progress>,
) -> Result<Vec<SplitInfo>, DeleteSplitsError> {
    let mut split_infos: HashMap<PathBuf, SplitInfo> = HashMap::with_capacity(splits.len());

    for split in splits {
        let split_info = split.as_split_info();
        split_infos.insert(split_info.file_name.clone(), split_info);
    }
    let split_paths = split_infos
        .keys()
        .map(|split_path_buf| split_path_buf.as_path())
        .collect::<Vec<&Path>>();
    let delete_result = protect_future(progress_opt, storage.bulk_delete(&split_paths)).await;

    if let Some(progress) = progress_opt {
        progress.record_progress();
    }
    let mut successes = Vec::with_capacity(split_infos.len());
    let mut storage_error: Option<BulkDeleteError> = None;
    let mut storage_failures = Vec::new();

    match delete_result {
        Ok(_) => successes.extend(split_infos.into_values()),
        Err(bulk_delete_error) => {
            let success_split_paths: HashSet<&PathBuf> =
                bulk_delete_error.successes.iter().collect();
            for (split_path, split_info) in split_infos {
                if success_split_paths.contains(&split_path) {
                    successes.push(split_info);
                } else {
                    storage_failures.push(split_info);
                }
            }
            let failed_split_paths = storage_failures
                .iter()
                .map(|split_info| split_info.file_name.as_path())
                .collect::<Vec<_>>();
            error!(
                error=?bulk_delete_error.error,
                index_id=index_uid.index_id,
                "Failed to delete split file(s) {:?} from storage.",
                PrettySample::new(&failed_split_paths, 5),
            );
            storage_error = Some(bulk_delete_error);
        }
    };
    if !successes.is_empty() {
        let split_ids: Vec<SplitId> = successes
            .iter()
            .map(|split_info| split_info.split_id.to_string())
            .collect();
        let delete_splits_request = DeleteSplitsRequest {
            index_uid: Some(index_uid.clone()),
            split_ids: split_ids.clone(),
        };
        let metastore_result =
            protect_future(progress_opt, metastore.delete_splits(delete_splits_request)).await;

        if let Err(metastore_error) = metastore_result {
            error!(
                error=?metastore_error,
                index_id=index_uid.index_id,
                "failed to delete split(s) {:?} from metastore",
                PrettySample::new(&split_ids, 5),
            );
            let delete_splits_error = DeleteSplitsError {
                successes: Vec::new(),
                storage_error,
                storage_failures,
                metastore_error: Some(metastore_error),
                metastore_failures: successes,
            };
            return Err(delete_splits_error);
        }
    }
    if !storage_failures.is_empty() {
        let delete_splits_error = DeleteSplitsError {
            successes,
            storage_error,
            storage_failures,
            metastore_error: None,
            metastore_failures: Vec::new(),
        };
        return Err(delete_splits_error);
    }
    Ok(successes)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;
    use quickwit_common::ServiceStream;
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        metastore_for_test, CreateIndexRequestExt, ListSplitsQuery,
        MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState, StageSplitsRequestExt,
    };
    use quickwit_proto::metastore::{
        CreateIndexRequest, EntityKind, MockMetastoreService, StageSplitsRequest,
    };
    use quickwit_proto::types::IndexUid;
    use quickwit_storage::{
        storage_for_test, BulkDeleteError, DeleteFailure, MockStorage, PutPayload,
    };

    use super::*;
    use crate::run_garbage_collect;

    fn hashmap<K: Eq + std::hash::Hash, V>(key: K, value: V) -> HashMap<K, V> {
        let mut map = HashMap::new();
        map.insert(key, value);
        map
    }

    #[tokio::test]
    async fn test_run_gc_marks_stale_staged_splits_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            1
        );

        // The staging grace period hasn't passed yet so the split remains staged.
        run_garbage_collect(
            hashmap(index_uid.clone(), storage.clone()),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            1
        );

        // The staging grace period has passed so the split is marked for deletion.
        run_garbage_collect(
            hashmap(index_uid.clone(), storage.clone()),
            metastore.clone(),
            Duration::from_secs(0),
            Duration::from_secs(30),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::MarkedForDeletion);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_run_gc_deletes_splits_marked_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id.to_string()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            1
        );

        // The delete grace period hasn't passed yet so the split remains marked for deletion.
        run_garbage_collect(
            hashmap(index_uid.clone(), storage.clone()),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            1
        );

        // The delete grace period has passed so the split is deleted.
        run_garbage_collect(
            hashmap(index_uid.clone(), storage.clone()),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(0),
            false,
            None,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_uid);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
        assert_eq!(
            metastore
                .list_splits(list_splits_request)
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_run_gc_deletes_splits_with_no_split() {
        // Test that we make only 2 calls to the metastore.
        let storage = storage_for_test();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_splits()
            .times(2)
            .returning(|_| Ok(ServiceStream::empty()));
        run_garbage_collect(
            hashmap(
                IndexUid::new_with_random_ulid("index-test-gc-deletes"),
                storage.clone(),
            ),
            MetastoreServiceClient::from_mock(mock_metastore),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_delete_splits_from_storage_and_metastore_happy_path() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-delete-splits-happy--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let split_id = "test-delete-splits-happy--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: IndexUid::new_with_random_ulid(index_id),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata.clone())
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let mark_splits_for_deletion =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), vec![split_id.to_string()]);
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion)
            .await
            .unwrap();

        let split_path_str = format!("{}.split", split_id);
        let split_path = Path::new(&split_path_str);
        let payload: Box<dyn PutPayload> = Box::new(vec![0]);
        storage.put(split_path, payload).await.unwrap();
        assert!(storage.exists(split_path).await.unwrap());

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);

        let deleted_split_infos = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            vec![split_metadata],
            None,
        )
        .await
        .unwrap();

        assert_eq!(deleted_split_infos.len(), 1);
        assert_eq!(deleted_split_infos[0].split_id, split_id,);
        assert_eq!(
            deleted_split_infos[0].file_name,
            Path::new(&format!("{split_id}.split"))
        );
        assert!(!storage.exists(split_path).await.unwrap());
        assert!(metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_delete_splits_from_storage_and_metastore_storage_error() {
        let mut mock_storage = MockStorage::new();
        mock_storage
            .expect_bulk_delete()
            .return_once(|split_paths| {
                assert_eq!(split_paths.len(), 2);

                let split_paths: Vec<PathBuf> = split_paths
                    .iter()
                    .map(|split_path| split_path.to_path_buf())
                    .sorted()
                    .collect();
                let split_path = split_paths[0].to_path_buf();
                let successes = vec![split_path];

                let split_path = split_paths[1].to_path_buf();
                let delete_failure = DeleteFailure {
                    code: Some("AccessDenied".to_string()),
                    ..Default::default()
                };
                let failures = HashMap::from_iter([(split_path, delete_failure)]);
                let bulk_delete_error = BulkDeleteError {
                    successes,
                    failures,
                    ..Default::default()
                };
                Err(bulk_delete_error)
            });
        let storage = Arc::new(mock_storage);
        let metastore = metastore_for_test();

        let index_id = "test-delete-splits-storage-error--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let split_id_0 = "test-delete-splits-storage-error--split-0";
        let split_metadata_0 = SplitMetadata {
            split_id: split_id_0.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let split_id_1 = "test-delete-splits-storage-error--split-1";
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            [split_metadata_0.clone(), split_metadata_1.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let mark_splits_for_deletion_request = MarkSplitsForDeletionRequest::new(
            index_uid.clone(),
            vec![split_id_0.to_string(), split_id_1.to_string()],
        );
        metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await
            .unwrap();

        let error = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            vec![split_metadata_0, split_metadata_1],
            None,
        )
        .await
        .unwrap_err();

        assert_eq!(error.successes.len(), 1);
        assert_eq!(error.storage_failures.len(), 1);
        assert_eq!(error.metastore_failures.len(), 0);

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id(), split_id_1);
    }

    #[tokio::test]
    async fn test_delete_splits_from_storage_and_metastore_metastore_error() {
        let mut mock_storage = MockStorage::new();
        mock_storage
            .expect_bulk_delete()
            .return_once(|split_paths| {
                assert_eq!(split_paths.len(), 2);

                let split_path = split_paths[0].to_path_buf();
                let successes = vec![split_path];

                let split_path = split_paths[1].to_path_buf();
                let delete_failure = DeleteFailure {
                    code: Some("AccessDenied".to_string()),
                    ..Default::default()
                };
                let failures = HashMap::from_iter([(split_path, delete_failure)]);
                let bulk_delete_error = BulkDeleteError {
                    successes,
                    failures,
                    ..Default::default()
                };
                Err(bulk_delete_error)
            });
        let storage = Arc::new(mock_storage);

        let index_id = "test-delete-splits-storage-error--index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_delete_splits().return_once(|_| {
            Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }))
        });

        let split_id_0 = "test-delete-splits-storage-error--split-0";
        let split_metadata_0 = SplitMetadata {
            split_id: split_id_0.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let split_id_1 = "test-delete-splits-storage-error--split-1";
        let split_metadata_1 = SplitMetadata {
            split_id: split_id_1.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let error = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage.clone(),
            MetastoreServiceClient::from_mock(mock_metastore),
            vec![split_metadata_0, split_metadata_1],
            None,
        )
        .await
        .unwrap_err();

        assert!(error.successes.is_empty());
        assert_eq!(error.storage_failures.len(), 1);
        assert_eq!(error.metastore_failures.len(), 1);
    }
}
