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

use std::path::Path;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::io::IoControls;
use quickwit_common::temp_dir::{self, TempDirectory};
use quickwit_metastore::SplitMetadata;
use tantivy::Directory;
use tracing::{debug, info, instrument};

use super::MergeExecutor;
use crate::merge_policy::MergeTask;
use crate::models::MergeScratch;
use crate::split_store::IndexingSplitStore;

#[derive(Clone)]
pub struct MergeSplitDownloader {
    pub scratch_directory: TempDirectory,
    pub split_store: IndexingSplitStore,
    pub executor_mailbox: Mailbox<MergeExecutor>,
    pub io_controls: IoControls,
}

impl Actor for MergeSplitDownloader {
    type ObservableState = ();
    fn observable_state(&self) -> Self::ObservableState {}

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }

    fn name(&self) -> String {
        "MergeSplitDownloader".to_string()
    }
}

#[async_trait]
impl Handler<MergeTask> for MergeSplitDownloader {
    type Reply = ();

    #[instrument(
        name = "merge_split_downloader",
        parent = merge_task.merge_parent_span.id(),
        skip_all,
    )]
    async fn handle(
        &mut self,
        merge_task: MergeTask,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        let merge_scratch_directory = temp_dir::Builder::default()
            .join("merge")
            .tempdir_in(self.scratch_directory.path())
            .map_err(|error| anyhow::anyhow!(error))?;
        info!(dir=%merge_scratch_directory.path().display(), "download-merge-splits");
        let downloaded_splits_directory = temp_dir::Builder::default()
            .join("downloaded-splits")
            .tempdir_in(merge_scratch_directory.path())
            .map_err(|error| anyhow::anyhow!(error))?;
        let tantivy_dirs = self
            .download_splits(
                merge_task.splits_as_slice(),
                downloaded_splits_directory.path(),
                ctx,
            )
            .await?;
        let msg = MergeScratch {
            merge_task,
            merge_scratch_directory,
            downloaded_splits_directory,
            tantivy_dirs,
        };
        ctx.send_message(&self.executor_mailbox, msg).await?;
        Ok(())
    }
}

impl MergeSplitDownloader {
    async fn download_splits(
        &self,
        splits: &[SplitMetadata],
        download_directory: &Path,
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<Box<dyn Directory>>, quickwit_actors::ActorExitStatus> {
        // we download all of the split files in the scratch directory.
        let mut tantivy_dirs = Vec::new();
        for split in splits {
            if ctx.kill_switch().is_dead() {
                debug!(
                    split_id = split.split_id(),
                    "Kill switch was activated. Cancelling download."
                );
                return Err(ActorExitStatus::Killed);
            }
            let io_controls = self
                .io_controls
                .clone()
                .set_progress(ctx.progress().clone())
                .set_kill_switch(ctx.kill_switch().clone());
            let _protect_guard = ctx.protect_zone();
            let tantivy_dir = self
                .split_store
                .fetch_and_open_split(split.split_id(), download_directory, &io_controls)
                .await
                .map_err(|error| {
                    let split_id = split.split_id();
                    anyhow::anyhow!(error).context(format!("failed to download split `{split_id}`"))
                })?;
            tantivy_dirs.push(tantivy_dir);
        }
        Ok(tantivy_dirs)
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::split_file;
    use quickwit_storage::{PutPayload, RamStorageBuilder, SplitPayloadBuilder};

    use super::*;
    use crate::merge_policy::MergeOperation;
    use crate::new_split_id;

    #[tokio::test]
    async fn test_merge_split_downloader() -> anyhow::Result<()> {
        let scratch_directory = TempDirectory::for_test();
        let splits_to_merge: Vec<SplitMetadata> = iter::repeat_with(|| {
            let split_id = new_split_id();
            SplitMetadata {
                split_id,
                ..Default::default()
            }
        })
        .take(10)
        .collect();

        let split_store = {
            let mut storage_builder = RamStorageBuilder::default();
            for split in &splits_to_merge {
                let buffer = SplitPayloadBuilder::get_split_payload(&[], &[], &[1, 2, 3])?
                    .read_all()
                    .await?;
                storage_builder = storage_builder.put(&split_file(split.split_id()), &buffer);
            }
            let ram_storage = storage_builder.build();
            IndexingSplitStore::create_without_local_store_for_test(Arc::new(ram_storage))
        };

        let universe = Universe::with_accelerated_time();
        let (merge_executor_mailbox, merge_executor_inbox) = universe.create_test_mailbox();
        let merge_split_downloader = MergeSplitDownloader {
            scratch_directory,
            split_store,
            executor_mailbox: merge_executor_mailbox,
            io_controls: IoControls::default(),
        };
        let (merge_split_downloader_mailbox, merge_split_downloader_handler) =
            universe.spawn_builder().spawn(merge_split_downloader);
        let merge_operation: MergeOperation = MergeOperation::new_merge_operation(splits_to_merge);
        let merge_task = MergeTask::from_merge_operation_for_test(merge_operation);
        merge_split_downloader_mailbox
            .send_message(merge_task)
            .await?;
        merge_split_downloader_handler
            .process_pending_and_observe()
            .await;
        let merge_scratches = merge_executor_inbox.drain_for_test();
        assert_eq!(merge_scratches.len(), 1);
        let merge_scratch = merge_scratches
            .into_iter()
            .next()
            .unwrap()
            .downcast::<MergeScratch>()
            .unwrap();
        assert_eq!(merge_scratch.merge_task.splits_as_slice().len(), 10);
        for split in merge_scratch.merge_task.splits_as_slice() {
            let split_filename = split_file(split.split_id());
            let split_filepath = merge_scratch
                .downloaded_splits_directory
                .path()
                .join(split_filename);
            assert!(split_filepath.try_exists().unwrap());
        }
        universe.assert_quit().await;
        Ok(())
    }
}
