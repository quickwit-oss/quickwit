// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::mem;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::models::PackagedSplit;
use crate::models::UploadedSplit;
use crate::semaphore::Semaphore;
use anyhow::bail;
use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_metastore::Metastore;
use quickwit_metastore::SplitMetadata;
use quickwit_metastore::SplitMetadataAndFooterOffsets;
use quickwit_metastore::SplitState;
use quickwit_storage::PutPayload;
use quickwit_storage::Storage;
use quickwit_storage::BUNDLE_FILENAME;
use tantivy::chrono::Utc;
use tokio::sync::oneshot::Receiver;
use tracing::info;
use tracing::warn;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 3;

pub struct Uploader {
    metastore: Arc<dyn Metastore>,
    index_storage: Arc<dyn Storage>,
    publisher_mailbox: Mailbox<Receiver<UploadedSplit>>,
    concurrent_upload_permits: Semaphore,
    counters: UploaderCounters,
}

impl Uploader {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        index_storage: Arc<dyn Storage>,
        publisher_mailbox: Mailbox<Receiver<UploadedSplit>>,
    ) -> Uploader {
        Uploader {
            metastore,
            index_storage,
            publisher_mailbox,
            concurrent_upload_permits: Semaphore::new(MAX_CONCURRENT_SPLIT_UPLOAD),
            counters: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

impl Actor for Uploader {
    type Message = PackagedSplit;

    type ObservableState = UploaderCounters;

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }
}

/// Upload all files within a single split to the storage
async fn put_split_file_to_storage(
    split: &PackagedSplit,
    storage: &dyn Storage,
) -> anyhow::Result<()> {
    let bundle_path = split.split_scratch_directory.path().join(BUNDLE_FILENAME);
    let key = PathBuf::from(format!("{}.split", &split.split_id));

    let start = Instant::now();

    info!(bundle_path=%bundle_path.display(), split_id=%split.split_id, "upload-split-bundle");
    let payload = PutPayload::from(bundle_path);
    storage.put(&key, payload).await.with_context(|| {
        format!(
            "Failed uploading key {} in bucket {}",
            key.display(),
            storage.uri()
        )
    })?;

    let elapsed_ms = start.elapsed().as_millis();
    let split_size_in_megabytes = split.footer_offsets.end / 1_000_000;
    let throughput_mb_s = split_size_in_megabytes as f32 / (elapsed_ms as f32 / 1_000.0f32);
    info!(
        split_size_in_megabytes = %split_size_in_megabytes,
        elapsed_ms = %elapsed_ms,
        throughput_mb_s = %throughput_mb_s,
        "Uploaded split to storage"
    );

    Ok(())
}

fn create_split_metadata(split: &PackagedSplit) -> SplitMetadataAndFooterOffsets {
    SplitMetadataAndFooterOffsets {
        split_metadata: SplitMetadata {
            split_id: split.split_id.clone(),
            num_records: split.num_docs as usize,
            time_range: split.time_range.clone(),
            size_in_bytes: split.size_in_bytes,
            generation: 0,
            split_state: SplitState::New,
            update_timestamp: Utc::now().timestamp(),
            tags: vec![], // TODO: handle tags collection and attaching to split
        },
        footer_offsets: split.footer_offsets.clone(),
    }
}

async fn stage_and_upload_split(
    split: PackagedSplit,
    index_storage: &dyn Storage,
    metastore: &dyn Metastore,
    counters: UploaderCounters,
) -> anyhow::Result<UploadedSplit> {
    let metadata = create_split_metadata(&split);
    metastore
        .stage_split(&split.index_id, metadata.clone())
        .await?;
    counters.num_staged_splits.fetch_add(1, Ordering::SeqCst);
    put_split_file_to_storage(&split, &*index_storage).await?;
    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
    Ok(UploadedSplit {
        index_id: split.index_id,
        metadata,
        checkpoint_delta: split.checkpoint_delta,
    })
}

#[async_trait]
impl AsyncActor for Uploader {
    async fn process_message(
        &mut self,
        split: PackagedSplit,
        ctx: &ActorContext<PackagedSplit>,
    ) -> Result<(), ActorExitStatus> {
        let (split_uploaded_tx, split_uploaded_rx) = tokio::sync::oneshot::channel();

        // We send the future to the publisher right away.
        // That way the publisher will process the uploaded split in order as opposed to
        // publishing in the order splits finish their uploading.
        ctx.send_message(&self.publisher_mailbox, split_uploaded_rx)
            .await?;

        // The permit will be added back manually to the semaphore the task after it is finished.
        let permit_guard = self.concurrent_upload_permits.acquire().await;

        let kill_switch = ctx.kill_switch().clone();
        let metastore = self.metastore.clone();
        let index_storage = self.index_storage.clone();

        let counters = self.counters.clone();

        tokio::spawn(async move {
            let stage_and_upload_res: anyhow::Result<()> =
                stage_and_upload_split(split, &*index_storage, &*metastore, counters)
                    .await
                    .and_then(|uploaded_split| {
                        if let Err(uploaded_split) = split_uploaded_tx.send(uploaded_split) {
                            bail!(
                                "Failed to send upload split `{}`. The publisher is probably dead.",
                                &uploaded_split.metadata.split_metadata.split_id
                            );
                        }
                        Ok(())
                    });

            if let Err(cause) = stage_and_upload_res {
                warn!(cause=%cause, "Failed to upload split. Killing!");
                kill_switch.kill();
            }

            // we explicitely drop it in order to force move the permit guard into the async task.
            mem::drop(permit_guard);
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::models::ScratchDirectory;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::ObservationType;
    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;
    use tantivy::SegmentId;

    use super::*;

    #[tokio::test]
    async fn test_uploader() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_stage_split()
            .withf(move |index_id, metadata| -> bool {
                (index_id == "test-index")
                    && &metadata.split_metadata.split_id == "test-split"
                    && metadata.split_metadata.time_range == Some(1628203589..=1628203640)
                    && metadata.split_metadata.split_state == SplitState::New
            })
            .times(1)
            .returning(|_, _| Ok(()));
        let ram_storage = RamStorage::default();
        let index_storage: Arc<dyn Storage> = Arc::new(ram_storage.clone());
        let uploader = Uploader::new(Arc::new(mock_metastore), index_storage.clone(), mailbox);
        let (uploader_mailbox, uploader_handle) = universe.spawn_async_actor(uploader);
        let split_scratch_directory = ScratchDirectory::try_new_temp()?;
        std::fs::write(
            split_scratch_directory.path().join(BUNDLE_FILENAME),
            &b"bubu"[..],
        )?;
        std::fs::write(
            split_scratch_directory.path().join("anyfile2"),
            &b"bubu2"[..],
        )?;
        let segment_ids = vec![SegmentId::from_uuid_string(
            "f45425f4-f67c-417e-9de7-8a8327115d47",
        )?];
        universe
            .send_message(
                &uploader_mailbox,
                PackagedSplit {
                    split_id: "test-split".to_string(),
                    index_id: "test-index".to_string(),
                    checkpoint_delta: CheckpointDelta::from(3..15),
                    time_range: Some(1_628_203_589i64..=1_628_203_640i64),
                    size_in_bytes: 1_000,
                    footer_offsets: 1000..2000,
                    segment_ids,
                    split_scratch_directory,
                    num_docs: 10,
                },
            )
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let publish_futures = inbox.drain_available_message_for_test();
        assert_eq!(publish_futures.len(), 1);
        let publish_future = publish_futures.into_iter().next().unwrap();
        let uploaded_split = publish_future.await?;
        assert_eq!(
            uploaded_split.metadata.split_metadata.split_id,
            "test-split".to_string()
        );
        assert_eq!(
            uploaded_split.checkpoint_delta,
            CheckpointDelta::from(3..15),
        );
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(&files, &[PathBuf::from("test-split.split")]);
        Ok(())
    }
}
