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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::models::Manifest;
use crate::models::PackagedSplit;
use crate::models::UploadedSplit;
use crate::semaphore::Semaphore;
use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::QueueCapacity;
use quickwit_metastore::BundleAndSplitMetadata;
use quickwit_metastore::Metastore;
use quickwit_metastore::SplitMetadata;
use quickwit_metastore::SplitState;
use quickwit_storage::PutPayload;
use quickwit_storage::Storage;
use quickwit_storage::BUNDLE_FILENAME;
use tantivy::chrono::Utc;
use tantivy::SegmentId;
use tokio::sync::oneshot::Receiver;
use tracing::info;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 3;

pub struct Uploader {
    metastore: Arc<dyn Metastore>,
    index_storage: Arc<dyn Storage>,
    publisher_mailbox: Mailbox<Receiver<UploadedSplit>>,
    concurrent_upload_permits: Semaphore,
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
        }
    }
}

impl Actor for Uploader {
    type Message = PackagedSplit;

    type ObservableState = ();

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }
}

fn create_manifest(segments: Vec<SegmentId>, split_metadata: SplitMetadata) -> Manifest {
    Manifest {
        split_metadata,
        segments,
    }
}

/// Upload all files within a single split to the storage
async fn put_split_files_to_storage(
    split: &PackagedSplit,
    split_metadata: SplitMetadata,
    storage: &dyn Storage,
) -> anyhow::Result<Manifest> {
    let bundle_path = split.split_scratch_directory.path().join(BUNDLE_FILENAME);
    info!("upload-split-bundle {:?} ", bundle_path);
    let start = Instant::now();

    let mut upload_res_futures = vec![];
    let key = PathBuf::from(BUNDLE_FILENAME);
    let payload = quickwit_storage::PutPayload::from(bundle_path);
    let upload_res_future = async move {
        storage.put(&key, payload).await.with_context(|| {
            format!(
                "Failed uploading key {} in bucket {}",
                key.display(),
                storage.uri()
            )
        })?;
        Result::<(), anyhow::Error>::Ok(())
    };
    upload_res_futures.push(upload_res_future);

    let manifest = create_manifest(split.segment_ids.clone(), split_metadata);
    futures::future::try_join_all(upload_res_futures).await?;

    let manifest_body = manifest.to_json()?.into_bytes();
    let manifest_path = PathBuf::from(".manifest");
    storage
        .put(&manifest_path, PutPayload::from(manifest_body))
        .await?;

    let elapsed_secs = start.elapsed().as_secs();
    let elapsed_ms = start.elapsed().as_millis();
    let file_statistics = split.file_statistics.clone();
    let split_size_in_megabytes = split.bundle_offsets.bundle_file_size / 1000000;
    let throughput_mb_s = split_size_in_megabytes as f32 / (elapsed_ms as f32 / 1000.0);
    info!(
        min_file_size_in_bytes = %file_statistics.min_file_size_in_bytes,
        max_file_size_in_bytes = %file_statistics.max_file_size_in_bytes,
        avg_file_size_in_bytes = %file_statistics.avg_file_size_in_bytes,
        split_size_in_megabytes = %split_size_in_megabytes,
        elapsed_secs = %elapsed_secs,
        throughput_mb_s = %throughput_mb_s,
        "Uploaded split to storage"
    );

    Ok(manifest)
}

fn create_split_metadata(split: &PackagedSplit) -> BundleAndSplitMetadata {
    BundleAndSplitMetadata {
        split_metadata: SplitMetadata {
            split_id: split.split_id.clone(),
            num_records: split.num_docs as usize,
            time_range: split.time_range.clone(),
            size_in_bytes: split.size_in_bytes,
            generation: 0,
            split_state: SplitState::New,
            update_timestamp: Utc::now().timestamp(),
            bundle_offsets: split.bundle_offsets.clone(),
        },
        bundle_offsets: split.bundle_offsets.clone(),
    }
}

async fn run_upload(
    split: PackagedSplit,
    split_storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<UploadedSplit> {
    let metadata = create_split_metadata(&split);
    metastore
        .stage_split(&split.index_id, metadata.clone())
        .await?;
    put_split_files_to_storage(&split, metadata.split_metadata.clone(), &*split_storage).await?;
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

        let split_storage = quickwit_storage::add_prefix_to_storage(
            self.index_storage.clone(),
            split.split_id.clone(),
        );
        let metastore = self.metastore.clone();
        let kill_switch = ctx.kill_switch().clone();
        tokio::task::spawn(async move {
            let run_upload_res = run_upload(split, split_storage, metastore).await;
            if run_upload_res.is_err() {
                kill_switch.kill();
            }
            let uploaded_split = run_upload_res?;
            if split_uploaded_tx.send(uploaded_split).is_err() {
                kill_switch.kill();
            }
            std::mem::drop(permit_guard); //< we explicitely drop the permit to allow for another upload to happen here.
            Result::<(), anyhow::Error>::Ok(())
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
    use quickwit_storage::BundleStorageOffsets;
    use quickwit_storage::RamStorage;

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
        let (uploader_mailbox, uploader_handle) = universe.spawn(uploader);
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
                    bundle_offsets: BundleStorageOffsets {
                        footer_offsets: 400..500,
                        hotcache_offset_start: 300,
                        bundle_file_size: 500,
                    },
                    file_statistics: Default::default(),
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
        assert_eq!(
            &files,
            &[
                PathBuf::from("test-split/.manifest"),
                PathBuf::from("test-split/bundle"),
            ]
        );
        Ok(())
    }
}
