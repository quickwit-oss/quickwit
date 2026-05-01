// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actor that downloads Parquet files from object storage for merge.
//!
//! For each `ParquetMergeTask`, downloads all input split files to a local
//! temp directory, then forwards a `ParquetMergeScratch` to the
//! `ParquetMergeExecutor`.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::temp_dir::TempDirectory;
use quickwit_storage::Storage;
use tracing::{debug, info, warn};

use super::parquet_merge_executor::ParquetMergeExecutor;
use super::parquet_merge_messages::{ParquetMergeScratch, ParquetMergeTask};

/// Downloads Parquet split files from object storage for merge execution.
///
/// Downloads are isolated in a separate actor so that I/O latency doesn't
/// block the CPU-intensive merge executor. Much simpler than the Tantivy
/// `MergeSplitDownloader`: Parquet splits are single files (not bundles),
/// so downloading is a straightforward `storage.copy_to_file()` per split.
pub struct ParquetMergeSplitDownloader {
    /// Parent directory for creating per-merge temp directories.
    scratch_directory: TempDirectory,
    /// Object storage for downloading split files.
    storage: Arc<dyn Storage>,
    /// Downstream executor to forward downloaded files to.
    executor_mailbox: Mailbox<ParquetMergeExecutor>,
}

impl ParquetMergeSplitDownloader {
    pub fn new(
        scratch_directory: TempDirectory,
        storage: Arc<dyn Storage>,
        executor_mailbox: Mailbox<ParquetMergeExecutor>,
    ) -> Self {
        Self {
            scratch_directory,
            storage,
            executor_mailbox,
        }
    }
}

#[async_trait]
impl Actor for ParquetMergeSplitDownloader {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn name(&self) -> String {
        "ParquetMergeSplitDownloader".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }
}

#[async_trait]
impl Handler<ParquetMergeTask> for ParquetMergeSplitDownloader {
    type Reply = ();

    async fn handle(
        &mut self,
        task: ParquetMergeTask,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let merge_split_id = task.merge_operation.merge_split_id.to_string();
        let num_inputs = task.merge_operation.splits.len();

        info!(
            merge_split_id = %merge_split_id,
            num_inputs,
            "downloading parquet files for merge"
        );

        // Each merge gets its own temp directory so partial downloads from a
        // failed merge don't interfere with other merges. TempDirectory's Drop
        // impl cleans up automatically on error paths.
        let download_dir = self
            .scratch_directory
            .named_temp_child("parquet-merge-")
            .context("failed to create merge download directory")
            .map_err(|e| ActorExitStatus::from(anyhow::anyhow!(e)))?;

        // Download each input split's Parquet file.
        // protect_zone() tells the actor framework this I/O should not count
        // toward the heartbeat timeout, and the kill switch check lets us
        // abort early if the pipeline is shutting down.
        let mut downloaded_paths = Vec::with_capacity(num_inputs);
        for split in &task.merge_operation.splits {
            if ctx.kill_switch().is_dead() {
                debug!(
                    split_id = %split.split_id,
                    "kill switch activated, cancelling download"
                );
                return Err(ActorExitStatus::Killed);
            }

            let parquet_filename = split.parquet_filename();
            let local_path = download_dir.path().join(&parquet_filename);

            debug!(
                split_id = %split.split_id,
                parquet_file = %parquet_filename,
                "downloading parquet file"
            );

            let _protect_guard = ctx.protect_zone();
            self.storage
                .copy_to_file(Path::new(&parquet_filename), &local_path)
                .await
                .map_err(|e| {
                    warn!(
                        error = %e,
                        split_id = %split.split_id,
                        "failed to download parquet file for merge"
                    );
                    ActorExitStatus::from(anyhow::anyhow!(e))
                })?;

            downloaded_paths.push(local_path);
            ctx.record_progress();
        }

        info!(
            merge_split_id = %merge_split_id,
            num_files = downloaded_paths.len(),
            "all parquet files downloaded for merge"
        );

        let scratch = ParquetMergeScratch {
            merge_operation: task.merge_operation,
            downloaded_parquet_files: downloaded_paths,
            scratch_directory: download_dir,
            merge_permit: task.merge_permit,
        };

        ctx.send_message(&self.executor_mailbox, scratch).await?;
        Ok(())
    }
}
