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

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};
use quickwit_storage::{Storage, StorageError, StorageUriResolver};
use serde::Serialize;
use thiserror::Error;
use time::OffsetDateTime;
use tracing::{error, info};

pub const MAX_CONCURRENT_STORAGE_REQUESTS: usize = if cfg!(test) { 2 } else { 10 };

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    /// The file_name is a file name, within an index directory.
    pub file_name: String,
    /// File size in bytes.
    pub file_size_in_bytes: u64, //< TODO switch to `byte_unit::Byte`.
}

impl From<&SplitMetadata> for FileEntry {
    fn from(split: &SplitMetadata) -> Self {
        FileEntry {
            file_name: quickwit_common::split_file(split.split_id()),
            file_size_in_bytes: split.footer_offsets.end,
        }
    }
}

/// SplitDeletionError denotes error that can happen when deleting split
/// during garbage collection.
#[derive(Error, Debug)]
pub enum SplitDeletionError {
    #[error("Failed to delete splits from storage: '{0:?}'.")]
    StorageFailure(Vec<(String, StorageError)>),

    #[error("Failed to delete splits from metastore: '{0:?}'.")]
    MetastoreFailure(MetastoreError),
}

/// Creates an index from `IndexConfig`.
pub async fn create_index(
    metastore: Arc<dyn Metastore>,
    index_config: IndexConfig,
    default_index_root_uri: Uri,
    overwrite: bool,
) -> Result<IndexMetadata, MetastoreError> {
    // Delete existing index if it exists.
    if overwrite {
        match metastore.delete_index(&index_config.index_id).await {
            Ok(_) | Err(MetastoreError::IndexDoesNotExist { index_id: _ }) => {
                // Ignore IndexDoesNotExist error.
            }
            Err(error) => {
                return Err(error);
            }
        }
    }
    index_config
        .validate()
        .map_err(|error| MetastoreError::InternalError {
            message: error.to_string(),
            cause: "".to_string(),
        })?;
    let index_id = index_config.index_id.clone();
    let index_uri = if let Some(index_uri) = &index_config.index_uri {
        index_uri.clone()
    } else {
        let index_uri = default_index_root_uri.join(&index_id).expect(
            "Failed to create default index URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.",
        );
        info!(
            index_id = %index_id,
            index_uri = %index_uri,
            "Index config does not specify `index_uri`, falling back to default value.",
        );
        index_uri
    };
    let index_metadata = IndexMetadata {
        index_id,
        index_uri,
        checkpoint: Default::default(),
        sources: index_config.sources(),
        doc_mapping: index_config.doc_mapping,
        indexing_settings: index_config.indexing_settings,
        search_settings: index_config.search_settings,
        create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
        update_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
    };
    metastore.create_index(index_metadata).await?;
    let index_metadata = metastore.index_metadata(&index_config.index_id).await?;
    Ok(index_metadata)
}

pub async fn delete_index(
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    index_id: &str,
    dry_run: bool,
) -> anyhow::Result<Vec<FileEntry>> {
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
        .list_splits(index_id, SplitState::Staged, None, None)
        .await?;
    let published_splits = metastore
        .list_splits(index_id, SplitState::Published, None, None)
        .await?;
    let split_ids = staged_splits
        .iter()
        .chain(published_splits.iter())
        .map(|meta| meta.split_id())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;

    // Select splits to delete
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::MarkedForDeletion, None, None)
        .await?
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>();

    let deleted_entries = delete_splits_with_files(
        index_id,
        storage.clone(),
        metastore.clone(),
        splits_to_delete,
    )
    .await?;

    metastore.delete_index(index_id).await?;

    Ok(deleted_entries)
}

// TODO: It's a copy/paste from quickwit-indexing where currently
// garbage collection is done. We do not want a dependency
// quickwit-indexing -> quickwit-control-plane but the reverse.
/// Deletes a list of splits from the storage and the metastore.
///
/// It should leave the index and the metastore in good state.
///
/// * `index_id` - The target index id.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `splits`  - The list of splits to delete.
pub async fn delete_splits_with_files(
    index_id: &str,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    splits: Vec<SplitMetadata>,
) -> anyhow::Result<Vec<FileEntry>, SplitDeletionError> {
    let mut deleted_file_entries = Vec::new();
    let mut deleted_split_ids = Vec::new();
    let mut failed_split_ids_to_error = Vec::new();

    let mut delete_splits_results_stream = tokio_stream::iter(splits.into_iter())
        .map(|split| {
            let storage_clone = storage.clone();
            async move {
                let file_entry = FileEntry::from(&split);
                let split_filename = quickwit_common::split_file(split.split_id());
                let split_path = Path::new(&split_filename);
                let delete_result = storage_clone.delete(split_path).await;
                (split.split_id().to_string(), file_entry, delete_result)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_STORAGE_REQUESTS);

    while let Some((split_id, file_entry, delete_split_res)) =
        delete_splits_results_stream.next().await
    {
        if let Err(error) = delete_split_res {
            error!(error = ?error, index_id = ?index_id, split_id = ?split_id, "Failed to delete split.");
            failed_split_ids_to_error.push((split_id, error));
        } else {
            deleted_split_ids.push(split_id);
            deleted_file_entries.push(file_entry);
        };
    }

    if !failed_split_ids_to_error.is_empty() {
        error!(index_id = ?index_id, failed_split_ids_to_error = ?failed_split_ids_to_error, "Failed to delete splits.");
        return Err(SplitDeletionError::StorageFailure(
            failed_split_ids_to_error,
        ));
    }

    if !deleted_split_ids.is_empty() {
        let split_ids: Vec<&str> = deleted_split_ids.iter().map(String::as_str).collect();
        metastore
            .delete_splits(index_id, &split_ids)
            .await
            .map_err(SplitDeletionError::MetastoreFailure)?;
    }

    Ok(deleted_file_entries)
}
