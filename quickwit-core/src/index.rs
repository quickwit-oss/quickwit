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
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreUriResolver, SplitState};
use quickwit_storage::{Storage, StorageErrorKind, StorageUriResolver};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tantivy::chrono::Utc;
use tracing::warn;

pub const MAX_CONCURRENT_SPLIT_TASKS: usize = if cfg!(test) { 2 } else { 10 };

// use crate::indexing::FileEntry;

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
///
pub async fn delete_index(
    metastore_uri: &str,
    index_id: &str,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
    /*
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = StorageUriResolver::default();

    let all_splits_ids = metastore
        .list_all_splits(index_id)
        .await?
        .into_iter()
        .map(|split| split.split_metadata.split_id)
        .collect();
    let index_uri = metastore.index_metadata(index_id).await?.index_uri;
    let index_storage = storage_resolver.resolve(&index_uri)?;

    if dry_run {
        return list_splits_files(all_splits_ids, &*index_storage).await;
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

    let file_entries = delete_garbage_files(metastore.as_ref(), index_id, storage_resolver).await?;
    //TODO: discuss & fix possible data race
    metastore.delete_index(index_id).await?;
    */
    todo!()
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
    todo!()
    /*
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = StorageUriResolver::default();

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

        let index_uri = metastore.index_metadata(index_id).await?.index_uri;
        scheduled_for_delete_splits.extend(staged_splits);
        return list_splits_files(scheduled_for_delete_splits, index_uri, storage_resolver).await;
    }

    // schedule all staged splits for delete
    let split_ids = staged_splits
        .iter()
        .map(|meta| meta.split_metadata.split_id.as_str())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;

    let file_entries = delete_garbage_files(metastore.as_ref(), index_id, storage_resolver).await?;
    */
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
) -> anyhow::Result<()> {
    /*
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
        .await?
        .into_iter()
        .map(|split| split.split_metadata.split_id );
    let index_uri = metastore.index_metadata(index_id).await?.index_uri;
    let index_storage = storage_resolver.resolve(&index_uri)?;
    let mut delete_stream = tokio_stream::iter(splits_to_delete.clone())
        .map(|split_id| {
            let split_filename = format!("{}.split", split_id);
            let moved_index_storage = index_storage.clone();
            async move {
                moved_index_storage.delete(&Path::new(&split_filename)).await
            }
        })
        .buffer_unordered(crate::indexing::MAX_CONCURRENT_SPLIT_TASKS);
    while let Some(delete_result) = delete_stream.next().await {
        let deleted_files = delete_result.map_err(|error| {
            warn!("Some split files were not deleted.");
            error
        })?;
    }

    let split_ids = splits_to_delete
        .iter()
        .map(|meta| meta.split_metadata.split_id.as_str())
        .collect::<Vec<_>>();
    metastore.delete_splits(index_id, &split_ids).await?;
    */
    Ok(())
}

/// Clears the index by applying the following actions:
/// - mark all splits as deleted.
/// - delete the files of all splits marked as deleted using garbage collection.
/// - delete the splits from the metastore.
///
/// * `index_uri` - The target index Uri.
/// * `index_id` - The target index Id.
/// * `storage_resolver` - A storage resolver object to access the storage.
/// * `metastore` - A emtastore object for interracting with the metastore.
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
    metastore.delete_splits(index_id, &split_ids).await?;
    Ok(())
}

/// list the files for a list of split.
async fn list_splits_files(
    split_ids: Vec<String>,
    index_storage: &dyn Storage,
) -> anyhow::Result<Vec<FileEntry>> {
    let mut file_entries_res_stream = futures::stream::iter(split_ids)
        .map(|split_id| {
            let split_path = format!("{}.split", split_id);
            async move {
                let file_num_bytes_res = index_storage.file_num_bytes(Path::new(&split_path)).await;
                file_num_bytes_res.map(|file_num_bytes| FileEntry {
                    file_name: split_path,
                    file_size_in_bytes: file_num_bytes,
                })
            }
        })
        .buffer_unordered(MAX_CONCURRENT_SPLIT_TASKS);
    let mut file_entries = Vec::new();
    for file_entry_res in file_entries_res_stream.next().await {
        match file_entry_res {
            Ok(file_entry) => {
                file_entries.push(file_entry);
            }
            Err(storage_error) if storage_error.kind() == StorageErrorKind::DoesNotExist => {}
            Err(storage_err) => {
                anyhow::bail!(storage_err);
            }
        }
    }
    Ok(file_entries)
}

/// list the files for a list of split.
async fn remove_splits_files(
    split_ids: Vec<String>,
    index_uri: String,
    storage_resolver: StorageUriResolver,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
    todo!();
    // let mut list_stream = tokio_stream::iter(split_ids)
    //     .map(|split_id| {
    //         let moved_storage_resolver = storage_resolver.clone();
    //         let split_uri = format!("{}/{}", index_uri, split_id);
    //         async move {
    //             remove_split_files_from_storage(&split_uri, moved_storage_resolver, true).await
    //         }
    //     })
    //     .buffer_unordered(crate::indexing::MAX_CONCURRENT_SPLIT_TASKS);

    // let mut file_entries = vec![];
    // while let Some(list_result) = list_stream.next().await {
    //     file_entries.extend(list_result?);
    // }
}
