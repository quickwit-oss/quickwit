// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::Arc;
use std::time::Duration;

use quickwit_indexing::{
    delete_splits_with_files, run_garbage_collect, FileEntry, IndexingSplitStore,
};
use quickwit_metastore::{
    IndexMetadata, Metastore, MetastoreUriResolver, SplitMetadata, SplitState,
};
use quickwit_storage::{quickwit_storage_uri_resolver, Storage};
use tracing::error;

/// Creates an index at `index-path` extracted from `metastore_uri`. The command fails if an index
/// already exists at `index-path`.
///
/// * `metastore_uri` - The metastore URI for accessing the metastore.
/// * `index_metadata` - The metadata used to create the target index.
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
    let index_uri = metastore.index_metadata(index_id).await?.index_uri;
    let storage = storage_resolver.resolve(&index_uri)?;

    if dry_run {
        let all_splits = metastore
            .list_all_splits(index_id)
            .await?
            .into_iter()
            .map(|metadata| metadata.split_metadata)
            .collect::<Vec<_>>();

        let file_entries_to_delete: Vec<FileEntry> =
            all_splits.iter().map(FileEntry::from).collect();
        return Ok(file_entries_to_delete);
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
        .map(|meta| meta.split_id())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;

    // Select split to delete
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
        .await?
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>();

    let split_store = IndexingSplitStore::create_with_no_local_store(storage);
    let deleted_entries = delete_splits_with_files(
        index_id,
        split_store,
        metastore.clone(),
        splits_to_delete,
        None,
    )
    .await?;
    metastore.delete_index(index_id).await?;
    Ok(deleted_entries)
}

/// Detect all dangling splits and associated files from the index and removes them.
///
/// * `metastore_uri` - The metastore URI for accessing the metastore.
/// * `index_id` - The target index Id.
/// * `grace_period` -  Threshold period after which a staged split can be garbage collected.
/// * `dry_run` - Should this only return a list of affected files without performing deletion.
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

    let index_uri = metastore.index_metadata(index_id).await?.index_uri;
    let storage = storage_resolver.resolve(&index_uri)?;
    let split_store = IndexingSplitStore::create_with_no_local_store(storage);

    let deleted_entries = run_garbage_collect(
        index_id,
        split_store,
        metastore,
        grace_period,
        // deletion_grace_period of zero, so that a cli call directly deletes splits after marking
        // to be deleted.
        Duration::ZERO,
        dry_run,
        None,
    )
    .await?;

    Ok(deleted_entries)
}

/// Clears the index by applying the following actions:
/// - mark all splits for deletion in the metastore.
/// - delete the files of all splits marked for deletion using garbage collection.
/// - delete the splits from the metastore.
///
/// * `metastore` - A metastore object for interacting with the metastore.
/// * `index_id` - The target index Id.
/// * `storage_resolver` - A storage resolver object to access the storage.
pub async fn reset_index(
    index_metadata: &IndexMetadata,
    metastore: Arc<dyn Metastore>,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<()> {
    let index_id = index_metadata.index_id.as_ref();
    let splits = metastore.list_all_splits(index_id).await?;
    let split_ids: Vec<&str> = splits.iter().map(|split| split.split_id()).collect();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;
    let split_metas: Vec<SplitMetadata> = splits
        .into_iter()
        .map(|split| split.split_metadata)
        .collect();
    let split_store = IndexingSplitStore::create_with_no_local_store(storage);
    // FIXME: return an error.
    if let Err(err) =
        delete_splits_with_files(index_id, split_store, metastore.clone(), split_metas, None).await
    {
        error!(metastore_uri = %metastore.uri(), index_id = %index_id, error = %err, "Not all split files could be deleted during garbage collection.");
    }
    Ok(())
}
