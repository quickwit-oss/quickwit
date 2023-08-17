// Copyright (C) 2023 Quickwit, Inc.
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

use futures::Future;
use quickwit_common::{PrettySample, Progress};
use quickwit_metastore::{
    ListSplitsQuery, Metastore, MetastoreError, SplitInfo, SplitMetadata, SplitState,
};
use quickwit_proto::IndexUid;
use quickwit_storage::{BulkDeleteError, Storage};
use thiserror::Error;
use time::OffsetDateTime;
use tracing::{error, instrument};

/// The maximum number of splits that the GC should delete per attempt.
const DELETE_SPLITS_BATCH_SIZE: usize = 1000;

/// [`DeleteSplitsError`] describes the errors that occurred during the deletion of splits from
/// storage and metastore.
#[derive(Error, Debug)]
#[error("Failed to delete splits from storage and/or metastore.")]
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
pub struct SplitRemovalInfo {
    /// The set of splits that have been removed.
    pub removed_split_entries: Vec<SplitInfo>,
    /// The set of split ids that were attempted to be removed, but were unsuccessful.
    pub failed_splits: Vec<SplitInfo>,
}

/// Detect all dangling splits and associated files from the index and removes them.
///
/// * `index_id` - The target index id.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `staged_grace_period` -  Threshold period after which a staged split can be safely garbage
///   collected.
/// * `deletion_grace_period` -  Threshold period after which a marked as deleted split can be
///   safely deleted.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
/// * `progress` - For reporting progress (useful when called from within a quickwit actor).
pub async fn run_garbage_collect(
    index_uid: IndexUid,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    staged_grace_period: Duration,
    deletion_grace_period: Duration,
    dry_run: bool,
    progress_opt: Option<&Progress>,
) -> anyhow::Result<SplitRemovalInfo> {
    // Select staged splits with staging timestamp older than grace period timestamp.
    let grace_period_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - staged_grace_period.as_secs() as i64;

    let query = ListSplitsQuery::for_index(index_uid.clone())
        .with_split_state(SplitState::Staged)
        .with_update_timestamp_lte(grace_period_timestamp);

    let deletable_staged_splits: Vec<SplitMetadata> =
        protect_future(progress_opt, metastore.list_splits(query))
            .await?
            .into_iter()
            .map(|meta| meta.split_metadata)
            .collect();

    if dry_run {
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);

        let mut splits_marked_for_deletion =
            protect_future(progress_opt, metastore.list_splits(query))
                .await?
                .into_iter()
                .map(|split| split.split_metadata)
                .collect::<Vec<_>>();
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
    let split_ids: Vec<&str> = deletable_staged_splits
        .iter()
        .map(|split| split.split_id())
        .collect();
    if !split_ids.is_empty() {
        protect_future(
            progress_opt,
            metastore.mark_splits_for_deletion(index_uid.clone(), &split_ids),
        )
        .await?;
    }

    // We delete splits marked for deletion that have an update timestamp anterior
    // to `now - deletion_grace_period`.
    let updated_before_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - deletion_grace_period.as_secs() as i64;

    let deleted_splits = delete_splits_marked_for_deletion(
        index_uid,
        updated_before_timestamp,
        storage,
        metastore,
        progress_opt,
    )
    .await;

    Ok(deleted_splits)
}
#[instrument(skip(storage, metastore, progress_opt))]
/// Removes any splits marked for deletion which haven't been
/// updated after `updated_before_timestamp` in batches of 1000 splits.
///
/// The aim of this is to spread the load out across a longer period
/// rather than short, heavy bursts on the metastore and storage system itself.
async fn delete_splits_marked_for_deletion(
    index_uid: IndexUid,
    updated_before_timestamp: i64,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    progress_opt: Option<&Progress>,
) -> SplitRemovalInfo {
    let mut removed_splits = Vec::new();
    let mut failed_splits = Vec::new();

    loop {
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion)
            .with_update_timestamp_lte(updated_before_timestamp)
            .with_limit(DELETE_SPLITS_BATCH_SIZE);

        let list_splits_result = protect_future(progress_opt, metastore.list_splits(query)).await;

        let splits_to_delete = match list_splits_result {
            Ok(splits) => splits,
            Err(error) => {
                error!(error = ?error, "Failed to fetch deletable splits.");
                break;
            }
        };
        let splits_to_delete = splits_to_delete
            .into_iter()
            .map(|split| split.split_metadata)
            .collect::<Vec<_>>();

        let num_splits_to_delete = splits_to_delete.len();

        if num_splits_to_delete == 0 {
            break;
        }
        let delete_splits_result = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            splits_to_delete,
            progress_opt,
        )
        .await;

        match delete_splits_result {
            Ok(entries) => removed_splits.extend(entries),
            Err(delete_splits_error) => {
                failed_splits.extend(delete_splits_error.storage_failures);
                failed_splits.extend(delete_splits_error.metastore_failures);
                break;
            }
        }
        if num_splits_to_delete < DELETE_SPLITS_BATCH_SIZE {
            break;
        }
    }
    SplitRemovalInfo {
        removed_split_entries: removed_splits,
        failed_splits,
    }
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
    metastore: Arc<dyn Metastore>,
    splits: Vec<SplitMetadata>,
    progress_opt: Option<&Progress>,
) -> anyhow::Result<Vec<SplitInfo>, DeleteSplitsError> {
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
                index_id=index_uid.index_id(),
                "Failed to delete split file(s) {:?} from storage.",
                PrettySample::new(&failed_split_paths, 5),
            );
            storage_error = Some(bulk_delete_error);
        }
    };
    if !successes.is_empty() {
        let split_ids: Vec<&str> = successes
            .iter()
            .map(|split_info| split_info.split_id.as_str())
            .collect();
        let metastore_result = protect_future(
            progress_opt,
            metastore.delete_splits(index_uid.clone(), &split_ids),
        )
        .await;

        if let Err(metastore_error) = metastore_result {
            error!(
                error=?metastore_error,
                index_id=index_uid.index_id(),
                "Failed to delete split(s) {:?} from metastore.",
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
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        metastore_for_test, ListSplitsQuery, MockMetastore, SplitMetadata, SplitState,
    };
    use quickwit_proto::IndexUid;
    use quickwit_storage::{
        storage_for_test, BulkDeleteError, DeleteFailure, MockStorage, PutPayload,
    };

    use super::*;
    use crate::run_garbage_collect;

    #[tokio::test]
    async fn test_run_gc_marks_stale_staged_splits_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let index_uid = metastore.create_index(index_config).await.unwrap();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata])
            .await
            .unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The staging grace period hasn't passed yet so the split remains staged.
        run_garbage_collect(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
        )
        .await
        .unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The staging grace period has passed so the split is marked for deletion.
        run_garbage_collect(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(0),
            Duration::from_secs(30),
            false,
            None,
        )
        .await
        .unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_run_gc_deletes_splits_marked_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let index_uid = metastore.create_index(index_config).await.unwrap();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: IndexUid::new(index_id),
            ..Default::default()
        };
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata])
            .await
            .unwrap();
        metastore
            .mark_splits_for_deletion(index_uid.clone(), &[split_id])
            .await
            .unwrap();

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The delete grace period hasn't passed yet so the split remains marked for deletion.
        run_garbage_collect(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The delete grace period has passed so the split is deleted.
        run_garbage_collect(
            index_uid.clone(),
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(0),
            false,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_uid);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_run_gc_deletes_splits_with_no_split() {
        // Test that we make only 2 calls to the metastore.
        let storage = storage_for_test();
        let mut metastore = MockMetastore::new();
        metastore
            .expect_list_splits()
            .times(2)
            .returning(|_| Ok(Vec::new()));
        run_garbage_collect(
            IndexUid::new("index-test-gc-deletes"),
            storage.clone(),
            Arc::new(metastore),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
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
        let index_uid = metastore.create_index(index_config).await.unwrap();

        let split_id = "test-delete-splits-happy--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: IndexUid::new(index_id),
            ..Default::default()
        };
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata.clone()])
            .await
            .unwrap();
        metastore
            .mark_splits_for_deletion(index_uid.clone(), &[split_id])
            .await
            .unwrap();

        let split_path_str = format!("{}.split", split_id);
        let split_path = Path::new(&split_path_str);
        let payload: Box<dyn PutPayload> = Box::new(vec![0]);
        storage.put(split_path, payload).await.unwrap();
        assert!(storage.exists(split_path).await.unwrap());

        let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
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
            .list_all_splits(index_uid)
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
        let index_uid = metastore.create_index(index_config).await.unwrap();

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
        metastore
            .stage_splits(
                index_uid.clone(),
                vec![split_metadata_0.clone(), split_metadata_1.clone()],
            )
            .await
            .unwrap();
        metastore
            .mark_splits_for_deletion(index_uid.clone(), &[split_id_0, split_id_1])
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

        let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
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
        let index_uid = IndexUid::new(index_id.to_string());

        let mut mock_metastore = MockMetastore::new();
        mock_metastore.expect_delete_splits().return_once(|_, _| {
            Err(MetastoreError::IndexesDoNotExist {
                index_ids: vec![index_id.to_string()],
            })
        });
        let metastore = Arc::new(mock_metastore);

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
            metastore.clone(),
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
