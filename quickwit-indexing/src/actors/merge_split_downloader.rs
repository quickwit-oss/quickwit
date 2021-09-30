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
use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, AsyncActor, Mailbox};
use quickwit_common::split_file;
use quickwit_metastore::SplitMetadata;
use quickwit_storage::Storage;
use tracing::{debug, info};

use crate::merge_policy::MergeOperation;
use crate::models::{MergeScratch, ScratchDirectory};

pub struct MergeSplitDownloader {
    pub scratch_directory: ScratchDirectory,
    pub storage: Arc<dyn Storage>,
    pub merge_executor_mailbox: Mailbox<MergeScratch>,
}

impl Actor for MergeSplitDownloader {
    type Message = MergeOperation;
    type ObservableState = ();
    fn observable_state(&self) -> Self::ObservableState {}
}

#[async_trait]
impl AsyncActor for MergeSplitDownloader {
    async fn process_message(
        &mut self,
        merge_operation: MergeOperation,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.process_merge_operation(merge_operation, ctx).await?;
        Ok(())
    }
}

impl MergeSplitDownloader {
    async fn process_merge_operation(
        &self,
        merge_operation: MergeOperation,
        ctx: &ActorContext<MergeOperation>,
    ) -> anyhow::Result<()> {
        info!(merge_operation=?merge_operation, "download-merge-splits");
        let merge_scratch_directory = self.scratch_directory.temp_child()?;
        let downloaded_splits_directory = merge_scratch_directory.temp_child()?;
        self.download_splits(
            merge_operation.splits(),
            downloaded_splits_directory.path(),
            ctx,
        )
        .await?;
        let msg = MergeScratch {
            merge_operation,
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        ctx.send_message(&self.merge_executor_mailbox, msg).await?;
        Ok(())
    }

    async fn download_splits(
        &self,
        splits: &[SplitMetadata],
        download_directory: &Path,
        ctx: &ActorContext<MergeOperation>,
    ) -> anyhow::Result<()> {
        // we download all of the split files in the scratch directory.
        for split in splits {
            let split_filename = split_file(&split.split_id);
            let split_file = Path::new(&split_filename);
            let dest_path = download_directory.join(split_file);
            let _protect_guard = ctx.protect_zone();
            let start_time = Instant::now();
            debug!(split_file=?split_file, dest_path=?dest_path, "download-file");
            self.storage.copy_to_file(split_file, &dest_path).await?;
            let elapsed = start_time.elapsed();
            debug!(split_file=?split_file, elapsed=?elapsed, "download-file-end");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_storage::RamStorageBuilder;

    use super::*;
    use crate::new_split_id;

    #[tokio::test]
    async fn test_merge_split_downloader() -> anyhow::Result<()> {
        let scratch_directory = ScratchDirectory::try_new_temp()?;
        let splits_to_merge: Vec<SplitMetadata> = iter::repeat_with(|| {
            let split_id = new_split_id();
            SplitMetadata {
                split_id,
                split_state: quickwit_metastore::SplitState::Published,
                ..Default::default()
            }
        })
        .take(10)
        .collect();

        let storage = Arc::new({
            let mut storage_builder = RamStorageBuilder::default();
            for split in &splits_to_merge {
                storage_builder =
                    storage_builder.put(&split_file(&split.split_id), &b"split_payload"[..]);
            }
            storage_builder.build()
        });

        let universe = Universe::new();
        let (merge_executor_mailbox, merge_executor_inbox) = create_test_mailbox();
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory,
            storage,
            merge_executor_mailbox,
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) =
            universe.spawn_actor(merge_split_downloader).spawn_async();
        let merge_operation = MergeOperation::new_merge_operation(splits_to_merge);
        universe
            .send_message(&merge_split_downloader_mailbox, merge_operation)
            .await?;
        merge_split_downloader_handler
            .process_pending_and_observe()
            .await;
        let merge_scratchs = merge_executor_inbox.drain_available_message_for_test();
        assert_eq!(merge_scratchs.len(), 1);
        let merge_scratch = merge_scratchs.into_iter().next().unwrap();
        assert!(matches!(
            merge_scratch.merge_operation,
            MergeOperation::Merge { .. }
        ));
        assert_eq!(merge_scratch.merge_operation.splits().len(), 10);
        for split in merge_scratch.merge_operation.splits() {
            let split_filename = split_file(&split.split_id);
            let split_filepath = merge_scratch
                .downloaded_splits_directory
                .path()
                .join(&split_filename);
            assert!(split_filepath.exists());
        }
        Ok(())
    }
}
