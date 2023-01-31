// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use quickwit_actors::ActorContext;
use quickwit_common::{FileEntry, PrettySample};
use quickwit_metastore::{ListSplitsQuery, Metastore, MetastoreError, SplitMetadata, SplitState};
use quickwit_storage::Storage;
use thiserror::Error;
use time::OffsetDateTime;
use tracing::{error, instrument};

use crate::actors::GarbageCollector;

/// The maximum number of splits that should be deleted in one go by the GC.
const DELETE_SPLITS_BATCH_SIZE: usize = 1000;

/// SplitDeletionError denotes error that can happen when deleting split
/// during garbage collection.
#[derive(Error, Debug)]
pub enum SplitDeletionError {
    #[error("Failed to delete splits from metastore: '{error:?}'.")]
    MetastoreFailure {
        error: MetastoreError,
        failed_split_ids: Vec<String>,
    },
}

async fn protect_future<Fut, T>(
    ctx_opt: Option<&ActorContext<GarbageCollector>>,
    future: Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    if let Some(ctx) = ctx_opt {
        ctx.protect_future(future).await
    } else {
        future.await
    }
}

/// Information on what splits have and have not been cleaned up by the GC.
pub struct SplitRemovalInfo {
    /// The set of splits that have been removed.
    pub removed_split_entries: Vec<FileEntry>,
    /// The set of split ids that were attempted to be removed, but were unsuccessful.
    pub failed_split_ids: Vec<String>,
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
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_garbage_collect(
    index_id: &str,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    staged_grace_period: Duration,
    deletion_grace_period: Duration,
    dry_run: bool,
    ctx_opt: Option<&ActorContext<GarbageCollector>>,
) -> anyhow::Result<SplitRemovalInfo> {
    // Select staged splits with staging timestamp older than grace period timestamp.
    let grace_period_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - staged_grace_period.as_secs() as i64;

    let query = ListSplitsQuery::for_index(index_id)
        .with_split_state(SplitState::Staged)
        .with_update_timestamp_lte(grace_period_timestamp);

    let deletable_staged_splits: Vec<SplitMetadata> =
        protect_future(ctx_opt, metastore.list_splits(query))
            .await?
            .into_iter()
            .map(|meta| meta.split_metadata)
            .collect();

    if dry_run {
        let query =
            ListSplitsQuery::for_index(index_id).with_split_state(SplitState::MarkedForDeletion);

        let mut splits_marked_for_deletion = protect_future(ctx_opt, metastore.list_splits(query))
            .await?
            .into_iter()
            .map(|meta| meta.split_metadata)
            .collect::<Vec<_>>();
        splits_marked_for_deletion.extend(deletable_staged_splits);

        let candidate_entries: Vec<FileEntry> = splits_marked_for_deletion
            .iter()
            .map(FileEntry::from)
            .collect();
        return Ok(SplitRemovalInfo {
            removed_split_entries: candidate_entries,
            failed_split_ids: Vec::new(),
        });
    }

    // Schedule all eligible staged splits for delete
    let split_ids: Vec<&str> = deletable_staged_splits
        .iter()
        .map(|meta| meta.split_id())
        .collect();
    protect_future(
        ctx_opt,
        metastore.mark_splits_for_deletion(index_id, &split_ids),
    )
    .await?;

    // We wait another 2 minutes until the split is actually deleted.
    let updated_before_timestamp =
        OffsetDateTime::now_utc().unix_timestamp() - deletion_grace_period.as_secs() as i64;

    let deleted_files = incrementally_remove_marked_splits(
        index_id,
        updated_before_timestamp,
        storage,
        metastore,
        ctx_opt,
    )
    .await;

    Ok(deleted_files)
}

#[instrument(skip(storage, metastore, ctx_opt))]
/// Incrementally removes any splits marked for deletion which haven't been
/// updated after the given grace period in batches of 1000 splits.
///
/// The aim of this is to spread the load out across a longer period
/// rather than short, heavy bursts on the metastore and storage system itself.
async fn incrementally_remove_marked_splits(
    index_id: &str,
    updated_before_timestamp: i64,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    ctx_opt: Option<&ActorContext<GarbageCollector>>,
) -> SplitRemovalInfo {
    let mut failed_split_ids = Vec::new();
    let mut removed_split_files = Vec::new();
    loop {
        let query = ListSplitsQuery::for_index(index_id)
            .with_split_state(SplitState::MarkedForDeletion)
            .with_update_timestamp_lte(updated_before_timestamp)
            .with_limit(DELETE_SPLITS_BATCH_SIZE);

        let list_splits_result = protect_future(ctx_opt, metastore.list_splits(query)).await;

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

        let delete_splits_result = delete_splits_with_files(
            index_id,
            storage.clone(),
            metastore.clone(),
            splits_to_delete,
            ctx_opt,
        )
        .await;

        match delete_splits_result {
            Ok(entries) => removed_split_files.extend(entries),
            Err(SplitDeletionError::MetastoreFailure {
                error,
                failed_split_ids: failed_split_ids_inner,
            }) => {
                error!(
                    error=?error,
                    index_id=%index_id,
                    split_ids=?PrettySample::new(&failed_split_ids_inner, 5),
                    "Failed to delete {} splits.",
                    failed_split_ids_inner.len()
                );
                failed_split_ids.extend(failed_split_ids_inner);
                break;
            }
        }

        if num_splits_to_delete < DELETE_SPLITS_BATCH_SIZE {
            break;
        }
    }

    SplitRemovalInfo {
        removed_split_entries: removed_split_files,
        failed_split_ids,
    }
}

/// Delete a list of splits from the storage and the metastore.
/// It should leave the index and the metastore in good state.
///
/// * `index_id` - The target index id.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `splits`  - The list of splits to delete.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn delete_splits_with_files(
    index_id: &str,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    splits: Vec<SplitMetadata>,
    ctx_opt: Option<&ActorContext<GarbageCollector>>,
) -> anyhow::Result<Vec<FileEntry>, SplitDeletionError> {
    let mut paths_to_splits = HashMap::with_capacity(splits.len());

    for split in splits {
        let file_entry = FileEntry::from(&split);
        let split_filename = quickwit_common::split_file(split.split_id());
        let split_path = Path::new(&split_filename);

        paths_to_splits.insert(
            split_path.to_path_buf(),
            (split.split_id().to_string(), file_entry),
        );
    }

    let paths = paths_to_splits
        .keys()
        .map(|key| key.as_path())
        .collect::<Vec<&Path>>();
    let delete_result = storage.bulk_delete(&paths).await;

    if let Some(ctx) = ctx_opt {
        ctx.record_progress();
    }

    let mut deleted_split_ids = Vec::new();
    let mut deleted_file_entries = Vec::new();

    match delete_result {
        Ok(()) => {
            for (split_id, entry) in paths_to_splits.into_values() {
                deleted_split_ids.push(split_id);
                deleted_file_entries.push(entry);
            }
        }
        Err(bulk_delete_error) => {
            let num_failed_splits =
                bulk_delete_error.failures.len() + bulk_delete_error.unattempted.len();
            let truncated_split_ids = bulk_delete_error
                .failures
                .keys()
                .chain(bulk_delete_error.unattempted.iter())
                .take(5)
                .collect::<Vec<_>>();

            error!(
                error = ?bulk_delete_error.error,
                index_id = ?index_id,
                num_failed_splits = num_failed_splits,
                "Failed to delete {:?} and {} other splits.",
                truncated_split_ids, num_failed_splits,
            );

            for split_path in bulk_delete_error.successes {
                let (split_id, entry) = paths_to_splits
                    .remove(&split_path)
                    .expect("The successful split path should be present within the lookup table.");

                deleted_split_ids.push(split_id);
                deleted_file_entries.push(entry);
            }
        }
    };

    if !deleted_split_ids.is_empty() {
        let split_ids: Vec<&str> = deleted_split_ids.iter().map(String::as_str).collect();
        protect_future(ctx_opt, metastore.delete_splits(index_id, &split_ids))
            .await
            .map_err(|error| SplitDeletionError::MetastoreFailure {
                error,
                failed_split_ids: split_ids.into_iter().map(str::to_string).collect(),
            })?;
    }

    Ok(deleted_file_entries)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_config::IndexConfig;
    use quickwit_metastore::{metastore_for_test, ListSplitsQuery, SplitMetadata, SplitState};
    use quickwit_storage::storage_for_test;

    use crate::run_garbage_collect;

    #[tokio::test]
    async fn test_run_gc_marks_stale_staged_splits_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        metastore.create_index(index_config).await.unwrap();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_id: index_id.to_string(),
            ..Default::default()
        };
        metastore
            .stage_splits(index_id, vec![split_metadata])
            .await
            .unwrap();

        let query = ListSplitsQuery::for_index(index_id).with_split_state(SplitState::Staged);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The staging grace period hasn't passed yet so the split remains staged.
        run_garbage_collect(
            index_id,
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(30),
            false,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_id).with_split_state(SplitState::Staged);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The staging grace period has passed so the split is marked for deletion.
        run_garbage_collect(
            index_id,
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
            ListSplitsQuery::for_index(index_id).with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_run_gc_deletes_splits_marked_for_deletion_after_grace_period() {
        let storage = storage_for_test();
        let metastore = metastore_for_test();

        let index_id = "test-run-gc--index";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        metastore.create_index(index_config).await.unwrap();

        let split_id = "test-run-gc--split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_id: index_id.to_string(),
            ..Default::default()
        };
        metastore
            .stage_splits(index_id, vec![split_metadata])
            .await
            .unwrap();
        metastore
            .mark_splits_for_deletion(index_id, &[split_id])
            .await
            .unwrap();

        let query =
            ListSplitsQuery::for_index(index_id).with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The delete grace period hasn't passed yet so the split remains marked for deletion.
        run_garbage_collect(
            index_id,
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
            ListSplitsQuery::for_index(index_id).with_split_state(SplitState::MarkedForDeletion);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 1);

        // The delete grace period has passed so the split is deleted.
        run_garbage_collect(
            index_id,
            storage.clone(),
            metastore.clone(),
            Duration::from_secs(30),
            Duration::from_secs(0),
            false,
            None,
        )
        .await
        .unwrap();

        let query = ListSplitsQuery::for_index(index_id);
        assert_eq!(metastore.list_splits(query).await.unwrap().len(), 0);
    }
}
