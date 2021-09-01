/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crate::FileEntry;
use futures::StreamExt;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreUriResolver};
use quickwit_metastore::{SplitMetadataAndFooterOffsets, SplitState};
use quickwit_storage::{quickwit_storage_uri_resolver, StorageUriResolver};
use std::path::Path;
use std::time::Duration;
use tantivy::chrono::Utc;
use tracing::warn;

pub const MAX_CONCURRENT_SPLIT_TASKS: usize = if cfg!(test) { 2 } else { 10 };

/// Creates an index at `index-path` extracted from `metastore_uri`. The command fails if an index
/// already exists at `index-path`.
///
/// * `metastore_uri` - The metastore URI for accessing the metastore.
/// * `index_metadata` - The metadata used to create the target index.
///
pub async fn create_index(
    metastore_uri: &str,
    index_metadata: IndexMetadata,
) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    metastore.create_index(index_metadata).await?;
    Ok(())
}

/// Deletes the index specified with `index_id`.
/// This is equivalent to running `rm -rf <index path>` for a local index or
/// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
///
/// * `metastore_uri` - The metastore URI for accessing the metastore.
/// * `index_id` - The target index Id.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
pub async fn delete_index(
    metastore_uri: &str,
    index_id: &str,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = quickwit_storage_uri_resolver();

    if dry_run {
        let all_splits = metastore.list_all_splits(index_id).await?;
        return Ok(list_split_files(&all_splits));
    }

    // Schedule staged and published splits for deletion.
    let staged_splits = metastore
        .list_splits(index_id, SplitState::Staged, None, &[])
        .await?;
    let published_splits = metastore
        .list_splits(index_id, SplitState::Published, None, &[])
        .await?;
    let split_ids = staged_splits
        .iter()
        .chain(published_splits.iter())
        .map(|meta| meta.split_metadata.split_id.as_ref())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;

    let file_entries =
        delete_garbage_files(metastore.as_ref(), index_id, storage_resolver.clone()).await?;
    metastore.delete_index(index_id).await?;
    Ok(file_entries)
}

/// Detect all dangling splits and associated files from the index and removes them.
///
/// * `metastore_uri` - The metastore URI for accessing the metastore.
/// * `index_id` - The target index Id.
/// * `grace_period` -  Threshold period after which a staged split can be garbage collected.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
///
pub async fn garbage_collect_index(
    metastore_uri: &str,
    index_id: &str,
    grace_period: Duration,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = quickwit_storage_uri_resolver();

    // Prune staged splits that are not older than the `grace_period`
    let grace_period_timestamp = Utc::now().timestamp() - grace_period.as_secs() as i64;
    let staged_splits = metastore
        .list_splits(index_id, SplitState::Staged, None, &[])
        .await?
        .into_iter()
        // TODO: Update metastore API and push this filter down.
        .filter(|meta| meta.split_metadata.update_timestamp < grace_period_timestamp)
        .collect::<Vec<_>>();

    if dry_run {
        let mut scheduled_for_delete_splits = metastore
            .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
            .await?;

        scheduled_for_delete_splits.extend(staged_splits);
        return Ok(list_split_files(&scheduled_for_delete_splits));
    }

    // schedule all staged splits for delete
    let split_ids = staged_splits
        .iter()
        .map(|meta| meta.split_metadata.split_id.as_str())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;

    let file_entries =
        delete_garbage_files(metastore.as_ref(), index_id, storage_resolver.clone()).await?;
    Ok(file_entries)
}

/// Remove the list of split from the storage.
/// It should leave the index and its metastore in good state.
///
/// * `metastore_uri` - The target index metastore uri.
/// * `index_id` - The target index id.
/// * `storage_resolver` - The storage resolver object.
///
pub async fn delete_garbage_files(
    metastore: &dyn Metastore,
    index_id: &str,
    storage_resolver: StorageUriResolver,
) -> anyhow::Result<Vec<FileEntry>> {
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
        .await?;

    let index_uri = metastore.index_metadata(index_id).await?.index_uri;
    let storage = storage_resolver.resolve(&index_uri)?;

    let mut success_deletes_split_ids: Vec<&str> = Vec::new();
    let mut success_deletes_file_entries: Vec<FileEntry> = Vec::new();
    let mut failure_deletes: Vec<&str> = Vec::new();

    let mut split_delete_results_stream = tokio_stream::iter(splits_to_delete.iter())
        .map(|meta| {
            let moved_storage = storage.clone();
            async move {
                let file_entry = FileEntry::from(meta);
                let delete_result = moved_storage.delete(Path::new(&file_entry.file_name)).await;
                (
                    &meta.split_metadata.split_id,
                    file_entry,
                    delete_result.is_ok(),
                )
            }
        })
        .buffer_unordered(MAX_CONCURRENT_SPLIT_TASKS);

    while let Some((split_id, file_entry, is_success)) = split_delete_results_stream.next().await {
        if is_success {
            success_deletes_split_ids.push(split_id);
            success_deletes_file_entries.push(file_entry);
        } else {
            failure_deletes.push(split_id);
        }
    }

    if !failure_deletes.is_empty() {
        warn!(splits=?failure_deletes, "Some splits were not deleted");
    }

    if !success_deletes_split_ids.is_empty() {
        metastore
            .delete_splits(index_id, &success_deletes_split_ids)
            .await?;
    }
    Ok(success_deletes_file_entries)
}

/// Clears the index by applying the following actions:
/// - mark all splits as deleted in the metastore.
/// - delete the files of all splits marked as deleted using garbage collection.
/// - delete the splits from the metastore.
///
/// * `index_uri` - The target index Uri.
/// * `index_id` - The target index Id.
/// * `storage_resolver` - A storage resolver object to access the storage.
/// * `metastore` - A metastore object for interacting with the metastore.
///
pub async fn reset_index(
    metastore: &dyn Metastore,
    index_id: &str,
    storage_resolver: StorageUriResolver,
) -> anyhow::Result<()> {
    let splits = metastore.list_all_splits(index_id).await?;
    let split_ids = splits
        .iter()
        .map(|meta| meta.split_metadata.split_id.as_str())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;
    let garbage_removal_result = delete_garbage_files(metastore, index_id, storage_resolver).await;
    if garbage_removal_result.is_err() {
        warn!(metastore_uri = %metastore.uri(), "All split files could not be removed during garbage collection.");
    }
    Ok(())
}

/// Lists the files for a list of split files.
///
/// Some of these file may actually not exist. (It happens in the middle of a delete operation
/// for instance, or right after the failure of a delete operation.
fn list_split_files(splits: &[SplitMetadataAndFooterOffsets]) -> Vec<FileEntry> {
    splits.iter().map(FileEntry::from).collect()
}
