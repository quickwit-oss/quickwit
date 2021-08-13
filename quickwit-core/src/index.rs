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

use std::time::Duration;

use futures::StreamExt;
use quickwit_metastore::{
    IndexMetadata, Metastore, MetastoreUriResolver, SplitMetadata, SplitState,
};
use quickwit_storage::StorageUriResolver;
use tantivy::chrono::Utc;
use tracing::warn;

use crate::indexing::{remove_split_files_from_storage, FileEntry};

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
) -> anyhow::Result<Vec<SplitMetadata>> {
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = StorageUriResolver::default();

    if dry_run {
        let all_splits = metastore.list_all_splits(index_id).await?;
        return Ok(all_splits);
    }

    // Schedule staged and published splits for deletion.
    let staged_splits = metastore
        .list_splits(index_id, SplitState::Staged, None)
        .await?;
    let published_splits = metastore
        .list_splits(index_id, SplitState::Published, None)
        .await?;
    let split_ids = staged_splits
        .iter()
        .chain(published_splits.iter())
        .map(|split_meta| split_meta.split_id.as_ref())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;

    let splits = delete_garbage_files(metastore.as_ref(), index_id, storage_resolver).await?;
    //TODO: discuss & fix possible data race
    metastore.delete_index(index_id).await?;

    Ok(splits)
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
) -> anyhow::Result<Vec<SplitMetadata>> {
    let metastore = MetastoreUriResolver::default()
        .resolve(metastore_uri)
        .await?;
    let storage_resolver = StorageUriResolver::default();

    // Prune staged splits that are not older than the `grace_period`
    let grace_period_timestamp = Utc::now().timestamp() - grace_period.as_secs() as i64;
    let staged_splits = metastore
        .list_splits(index_id, SplitState::Staged, None)
        .await?
        .into_iter()
        // TODO: Update metastore API and push this filter down.
        .filter(|split_meta| split_meta.update_timestamp < grace_period_timestamp)
        .collect::<Vec<_>>();

    if dry_run {
        let mut scheduled_for_delete_splits = metastore
            .list_splits(index_id, SplitState::ScheduledForDeletion, None)
            .await?;

        let index_uri = metastore.index_metadata(index_id).await?.index_uri;
        scheduled_for_delete_splits.extend(staged_splits);
        return Ok(scheduled_for_delete_splits);
    }

    // schedule all staged splits for delete
    let split_ids = staged_splits
        .iter()
        .map(|split_meta| split_meta.split_id.as_str())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, &split_ids)
        .await?;

    let splits = delete_garbage_files(metastore.as_ref(), index_id, storage_resolver).await?;
    Ok(splits)
}

/// Get the list of files logically deleted from the index and removes them from the storage.
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
) -> anyhow::Result<Vec<SplitMetadata>> {
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::ScheduledForDeletion, None)
        .await?;

    let index_uri = metastore.index_metadata(index_id).await?.index_uri;

    let mut delete_stream = tokio_stream::iter(splits_to_delete.clone())
        .map(|split_meta| {
            let moved_storage_resolver = storage_resolver.clone();
            let split_uri = format!("{}/{}", index_uri, split_meta.split_id);
            async move {
                remove_split_files_from_storage(&split_uri, moved_storage_resolver, false).await
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
        .map(|split_meta| split_meta.split_id.as_str())
        .collect::<Vec<_>>();
    metastore.delete_splits(index_id, &split_ids).await?;

    Ok(splits_to_delete)
}
