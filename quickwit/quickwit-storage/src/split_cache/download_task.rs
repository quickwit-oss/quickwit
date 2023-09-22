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

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quickwit_common::split_file;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, instrument};
use ulid::Ulid;

use crate::split_cache::split_table::{CandidateSplit, DownloadOpportunity, SplitTable};
use crate::StorageResolver;

/// Removes the evicted split files from the file system.
/// This function just logs errors, and swallows them.
///
/// At this point, the disk space is already accounted as released,
/// so the error could result in a "disk space leak".
#[instrument]
pub(crate) fn delete_evicted_splits(root_path: &Path, splits_to_delete: &[Ulid]) {
    for &split_to_delete in splits_to_delete {
        let split_file_path = root_path.join(split_file(split_to_delete));
        if let Err(_io_err) = std::fs::remove_file(&split_file_path) {
            // This is an pretty critical error. The split size is not tracked anymore at this
            // point.
            error!(path=%split_file_path.display(), "Failed to remove split file from cache directory. This is critical as the file is now not taken in account in the cache size limits.");
        }
    }
}

async fn download_split(
    root_path: &Path,
    candidate_split: &CandidateSplit,
    storage_resolver: StorageResolver,
) -> anyhow::Result<u64> {
    let CandidateSplit {
        split_ulid,
        storage_uri,
        living_token: _,
    } = candidate_split;
    let split_filename = split_file(*split_ulid);
    let target_filepath = root_path.join(&split_filename);
    let storage = storage_resolver.resolve(storage_uri).await?;
    let num_bytes = storage
        .copy_to_file(Path::new(&split_filename), &target_filepath)
        .await?;
    Ok(num_bytes)
}

async fn perform_eviction_and_download(
    download_opportunity: DownloadOpportunity,
    root_path: PathBuf,
    storage_resolver: StorageResolver,
    shared_split_table: Arc<Mutex<SplitTable>>,
    _download_permit: OwnedSemaphorePermit,
) -> anyhow::Result<()> {
    let DownloadOpportunity {
        splits_to_delete,
        split_to_download,
    } = download_opportunity;
    let split_ulid = split_to_download.split_ulid;
    // tokio io runs on `spawn_blocking` threads anyway.
    let root_path_clone = root_path.clone();
    let _ = tokio::task::spawn_blocking(move || {
        delete_evicted_splits(&root_path_clone, &splits_to_delete[..]);
    })
    .await;
    let num_bytes = download_split(&root_path, &split_to_download, storage_resolver).await?;
    let mut shared_split_table_lock = shared_split_table.lock().unwrap();
    shared_split_table_lock.register_as_downloaded(split_ulid, num_bytes);
    Ok(())
}

pub(crate) fn spawn_download_task(
    root_path: PathBuf,
    shared_split_table: Arc<Mutex<SplitTable>>,
    storage_resolver: StorageResolver,
    num_concurrent_downloads: NonZeroU32,
) {
    let semaphore = Arc::new(Semaphore::new(num_concurrent_downloads.get() as usize));
    tokio::task::spawn(async move {
        loop {
            let download_permit = Semaphore::acquire_owned(semaphore.clone()).await.unwrap();
            let download_opportunity_opt = shared_split_table
                .lock()
                .unwrap()
                .find_download_opportunity();
            if let Some(download_opportunity) = download_opportunity_opt {
                tokio::task::spawn(perform_eviction_and_download(
                    download_opportunity,
                    root_path.clone(),
                    storage_resolver.clone(),
                    shared_split_table.clone(),
                    download_permit,
                ));
            } else {
                // We wait 1 sec before retrying, to avoid wasting CPU.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });
}
