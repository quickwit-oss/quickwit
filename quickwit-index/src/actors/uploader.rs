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
use crate::models::ManifestEntry;
use crate::models::PackagedSplit;
use crate::models::UploadedSplit;
use crate::semaphore::Semaphore;
use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::MessageProcessError;
use quickwit_actors::QueueCapacity;
use quickwit_metastore::Metastore;
use quickwit_metastore::SplitMetadata;
use quickwit_metastore::SplitState;
use quickwit_storage::PutPayload;
use quickwit_storage::Storage;
use tantivy::chrono::Utc;
use tantivy::SegmentId;
use tokio::sync::oneshot::Receiver;
use tracing::info;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 3;

pub struct Uploader {
    metastore: Arc<dyn Metastore>,
    index_storage: Arc<dyn Storage>,
    sink: Mailbox<Receiver<UploadedSplit>>,
    concurrent_upload_permits: Semaphore,
}

impl Uploader {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        index_storage: Arc<dyn Storage>,
        sink: Mailbox<Receiver<UploadedSplit>>,
    ) -> Uploader {
        Uploader {
            metastore,
            index_storage,
            sink,
            concurrent_upload_permits: Semaphore::new(MAX_CONCURRENT_SPLIT_UPLOAD),
        }
    }
}

impl Actor for Uploader {
    type Message = PackagedSplit;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }
}

fn create_manifest(
    files: Vec<ManifestEntry>,
    segments: Vec<SegmentId>,
    split_metadata: SplitMetadata,
) -> Manifest {
    let split_size_in_bytes = files.iter().map(|file| file.file_size_in_bytes).sum();
    Manifest {
        split_metadata,
        split_size_in_bytes,
        num_files: files.len() as u64,
        files,
        segments,
    }
}

/// Upload all files within a single split to the storage
async fn put_split_files_to_storage(
    storage: &dyn Storage,
    split: PackagedSplit,
) -> anyhow::Result<Manifest> {
    info!("upload-split");
    let start = Instant::now();

    let mut upload_res_futures = vec![];
    let mut manifest_entries = Vec::new();
    for (path, file_size_in_bytes) in &split.files_to_upload {
        let file_name = path
            .file_name()
            .and_then(|filename| filename.to_str())
            .map(|filename| filename.to_string())
            .with_context(|| format!("Failed to extract filename from path {}", path.display()))?;
        manifest_entries.push(ManifestEntry {
            file_name: file_name.to_string(),
            file_size_in_bytes: *file_size_in_bytes,
        });
        let key = PathBuf::from(file_name);
        let payload = quickwit_storage::PutPayload::from(path.clone());
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
    }

    let split_metadata = SplitMetadata {
        split_id: split.split_id.clone(),
        num_records: split.num_docs as usize,
        time_range: split.time_range.clone(),
        size_in_bytes: split.size_in_bytes,
        generation: 0,
        split_state: SplitState::New,
        update_timestamp: Utc::now().timestamp(),
    };
    let manifest = create_manifest(manifest_entries, split.segment_ids, split_metadata);
    futures::future::try_join_all(upload_res_futures).await?;

    let manifest_body = manifest.to_json()?.into_bytes();
    let manifest_path = PathBuf::from(".manifest");
    storage
        .put(&manifest_path, PutPayload::from(manifest_body))
        .await?;

    let elapsed_secs = start.elapsed().as_secs();
    let file_statistics = manifest.file_statistics();
    info!(
        min_file_size_in_bytes = %file_statistics.min_file_size_in_bytes,
        max_file_size_in_bytes = %file_statistics.max_file_size_in_bytes,
        avg_file_size_in_bytes = %file_statistics.avg_file_size_in_bytes,
        split_size_in_megabytes = %manifest.split_size_in_bytes / 1000,
        elapsed_secs = %elapsed_secs,
        throughput_mb_s = %manifest.split_size_in_bytes / 1000 / elapsed_secs.max(1),
        "Uploaded split to storage"
    );

    Ok(manifest)
}

fn create_split_metadata(split: &PackagedSplit) -> SplitMetadata {
    SplitMetadata {
        split_id: split.split_id.clone(),
        num_records: split.num_docs as usize,
        time_range: split.time_range.clone(),
        size_in_bytes: split.size_in_bytes,
        generation: 0,
        split_state: SplitState::New,
        update_timestamp: Utc::now().timestamp(),
    }
}

async fn stage_split(
    metastore: Arc<dyn Metastore>,
    split_storage: Arc<dyn Storage>,
    split: &PackagedSplit,
) -> anyhow::Result<SplitMetadata> {
    let split_metadata = create_split_metadata(split);
    metastore
        .stage_split(&split.index_id, split_metadata.clone())
        .await?;
    Ok(split_metadata)
}

async fn run_upload(
    split: PackagedSplit,
    split_storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<UploadedSplit> {
    Ok(UploadedSplit {
        index_id: split.index_id,
        split_id: split.split_id,
    })
}

#[async_trait]
impl AsyncActor for Uploader {
    async fn process_message(
        &mut self,
        split: PackagedSplit,
        context: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        let (split_uploaded_tx, split_uploaded_rx) = tokio::sync::oneshot::channel();

        // We send the future to the publisher right away.
        // That way the publisher will process the uploaded split in order as opposed to
        // publishing in the order splits finish their uploading.
        self.sink.send_async(split_uploaded_rx).await?;

        // The juggling here happens because the permit lifetime prevents it from being passed to a task.
        // Instead we acquire the resource, but forget the permit.
        //
        // The permit will be added back manually to the semaphore the task after it is finished.
        let permit = self.concurrent_upload_permits.acquire().await;

        let split_storage = quickwit_storage::add_prefix_to_storage(
            self.index_storage.clone(),
            split.split_id.clone(),
        );
        let metastore = self.metastore.clone();
        let kill_switch = context.kill_switch.clone();
        tokio::task::spawn(async move {
            let run_upload_res = run_upload(split, split_storage, metastore).await;
            if run_upload_res.is_err() {
                kill_switch.kill();
            }
            let uploaded_split = run_upload_res?;
            if split_uploaded_tx.send(uploaded_split).is_err() {
                kill_switch.kill();
            }
            std::mem::drop(permit); //< we explicitely drop the permit to allow for another upload to happen here.
            Result::<(), anyhow::Error>::Ok(())
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mockall::*;
    use quickwit_actors::create_mailbox;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::KillSwitch;
    use quickwit_metastore::MockMetastore;
    use quickwit_storage::RamStorage;

    use super::*;

    #[test]
    fn test_uploader() {
        let (mailbox, inbox) = create_test_mailbox();
        let mut mock_metastore = MockMetastore::default();
        let index_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
        let uploader = Uploader::new(Arc::new(mock_metastore), index_storage, mailbox);
        let uploader_handle = uploader.spawn(KillSwitch::default());
        // uploader_handle.mailbox().send_async(PackagedSplit {
        //     split_id: "test-split".to_string(),
        //     index_id: "test-index".to_string(),
        //     time_range: Some(1_628_203_589i64..=1_628_203_619i64),
        //     size_in_bytes: 10_000_000,
        //     segment_meta: Segm,
        //     split_scratch_dir: (),
        //     num_docs: (),
        // });
    }
}
