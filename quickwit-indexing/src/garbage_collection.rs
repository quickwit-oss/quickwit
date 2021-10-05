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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use quickwit_actors::ActorContext;
use quickwit_metastore::{Metastore, SplitMetadataAndFooterOffsets, SplitState};
use quickwit_storage::Storage;
use tantivy::chrono::Utc;
use tracing::{error, warn};

const MAX_CONCURRENT_STORAGE_REQUESTS: usize = if cfg!(test) { 2 } else { 10 };

/// A struct holding splits deletion statistics.
#[derive(Default)]
pub struct SplitDeletionStats {
    /// Entries subject to be deleted.
    pub candidate_entries: Vec<FileEntry>,
    /// Entries that were successfully deleted.
    pub deleted_entries: Vec<FileEntry>,
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// The file_name is a file name, within an index directory.
    pub file_name: String,
    /// File size in bytes.
    pub file_size_in_bytes: u64, //< TODO switch to `byte_unit::Byte`.
}

impl From<&SplitMetadataAndFooterOffsets> for FileEntry {
    fn from(split: &SplitMetadataAndFooterOffsets) -> Self {
        FileEntry {
            file_name: quickwit_common::split_file(&split.split_metadata.split_id),
            file_size_in_bytes: split.footer_offsets.end,
        }
    }
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
    ctx_opt: Option<&ActorContext<()>>,
) -> anyhow::Result<SplitDeletionStats> {
    // Select staged splits with staging timestamp older than grace period timestamp.
    let grace_period_timestamp = Utc::now().timestamp() - staged_grace_period.as_secs() as i64;

    let deletable_staged_splits: Vec<SplitMetadataAndFooterOffsets> = metastore
        .list_splits(index_id, SplitState::Staged, None, &[])
        .await?
        .into_iter()
        // TODO: Update metastore API and push this filter down.
        .filter(|meta| meta.split_metadata.update_timestamp < grace_period_timestamp)
        .collect();
    if let Some(ctx) = ctx_opt {
        ctx.record_progress();
    }

    if dry_run {
        let mut scheduled_for_delete_splits = metastore
            .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
            .await?;
        scheduled_for_delete_splits.extend(deletable_staged_splits);

        let candidate_entries: Vec<FileEntry> = scheduled_for_delete_splits
            .iter()
            .map(FileEntry::from)
            .collect();
        return Ok(SplitDeletionStats {
            candidate_entries,
            ..Default::default()
        });
    }

    // Schedule all eligible staged splits for delete
    let split_ids: Vec<&str> = deletable_staged_splits
        .iter()
        .map(|meta| meta.split_metadata.split_id.as_str())
        .collect();
    metastore
        .mark_splits_for_deletion(index_id, &split_ids)
        .await?;

    // We wait another 2 minutes until the split is actually deleted.
    let grace_period_deletion = Utc::now().timestamp() - deletion_grace_period.as_secs() as i64;
    let splits_to_delete = metastore
        .list_splits(index_id, SplitState::ScheduledForDeletion, None, &[])
        .await?
        .into_iter()
        // TODO: Update metastore API and push this filter down.
        .filter(|meta| meta.split_metadata.update_timestamp <= grace_period_deletion)
        .collect();

    let deleted_files = delete_splits_with_files(
        index_id,
        storage.clone(),
        metastore.clone(),
        splits_to_delete,
        ctx_opt,
    )
    .await?;

    Ok(deleted_files)
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
    splits: Vec<SplitMetadataAndFooterOffsets>,
    ctx_opt: Option<&ActorContext<()>>,
) -> anyhow::Result<SplitDeletionStats> {
    let mut deletion_stats = SplitDeletionStats::default();
    let mut deleted_split_ids: Vec<String> = Vec::new();
    let mut failed_split_ids: Vec<String> = Vec::new();

    let mut delete_splits_results_stream = tokio_stream::iter(splits)
        .map(|meta| {
            let storage_clone = storage.clone();
            async move {
                let file_entry = FileEntry::from(&meta);
                let delete_split_res = storage_clone.delete(Path::new(&file_entry.file_name)).await;
                if let Some(ctx) = ctx_opt {
                    ctx.record_progress();
                }
                (meta.split_metadata.split_id, file_entry, delete_split_res)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_STORAGE_REQUESTS);

    while let Some((split_id, file_entry, delete_split_res)) =
        delete_splits_results_stream.next().await
    {
        deletion_stats.candidate_entries.push(file_entry.clone());
        if let Err(error) = delete_split_res {
            error!(error = ?error, index_id = ?index_id, split_id = ?split_id, "Failed to delete split.");
            failed_split_ids.push(split_id);
        } else {
            deleted_split_ids.push(split_id);
            deletion_stats.deleted_entries.push(file_entry);
        };
    }
    if !failed_split_ids.is_empty() {
        warn!(index_id = ?index_id, split_ids = ?failed_split_ids, "Failed to delete splits.");
    }
    if !deleted_split_ids.is_empty() {
        let split_ids: Vec<&str> = deleted_split_ids.iter().map(String::as_str).collect();
        metastore.delete_splits(index_id, &split_ids).await?;
    }
    Ok(deletion_stats)
}
