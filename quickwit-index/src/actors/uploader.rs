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

use std::io;
use std::os::unix::prelude::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::models::Manifest;
use crate::models::ManifestEntry;
use crate::models::PackagedSplit;
use crate::models::UploadedSplit;
use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use quickwit_actors::Mailbox;
use quickwit_actors::MessageProcessError;
use quickwit_directories::HOTCACHE_FILENAME;
use quickwit_metastore::Metastore;
use quickwit_metastore::SplitMetadata;
use quickwit_metastore::SplitState;
use quickwit_storage::PutPayload;
use quickwit_storage::Storage;
use tantivy::chrono::Utc;
use tantivy::SegmentId;
use tracing::info;

pub const MAX_CONCURRENT_SPLIT_UPLOAD: usize = 3;

pub struct Uploader {
    metastore: Arc<dyn Metastore>,
    index_storage: Arc<dyn Storage>,
    sink: Mailbox<UploadedSplit>,
    num_concurrent_upload: usize,
}

struct UploadTask;

impl Uploader {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        index_storage: Arc<dyn Storage>,
        sink: Mailbox<UploadedSplit>,
    ) -> Uploader {
        Uploader {
            metastore,
            index_storage,
            sink,
            num_concurrent_upload: 0,
        }
    }
}

impl Actor for Uploader {
    type Message = PackagedSplit;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

async fn list_files_to_upload(split: &PackagedSplit) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let mut files_to_upload = Vec::new();
    // list the segment files
    for relative_path in split.segment_meta.list_files() {
        let filepath = split.split_scratch_dir.path().join(&relative_path);
        match tokio::fs::metadata(&filepath).await {
            Ok(metadata) => {
                files_to_upload.push((filepath, metadata.size()));
            }
            Err(io_err) => {
                // If the file is missing, this is fine.
                // segment_meta.list_files() may actually returns files that are
                // do not exist.
                if io_err.kind() != io::ErrorKind::NotFound {
                    return Err(io_err).with_context(|| {
                        format!(
                            "Failed to read metadata of segment file `{}` for split {:?}",
                            relative_path.display(),
                            split
                        )
                    })?;
                }
            }
        }
    }
    // We also need the meta.json file and the hotcache. Contrary to segment files,
    // we return an error here if they are missing.
    for relative_path in [Path::new("meta.json"), Path::new(HOTCACHE_FILENAME)]
        .iter()
        .cloned()
    {
        let filepath = split.split_scratch_dir.path().join(&relative_path);
        let metadata = tokio::fs::metadata(&filepath).await.with_context(|| {
            format!(
                "Failed to read metadata of mandatory file `{}` for split {:?}",
                relative_path.display(),
                split
            )
        })?;
        files_to_upload.push((filepath, metadata.size()));
    }
    Ok(files_to_upload)
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
    split: &PackagedSplit,
) -> anyhow::Result<Manifest> {
    info!("upload-split");
    let start = Instant::now();

    let files_to_upload: Vec<(PathBuf, u64)> = list_files_to_upload(split).await?;

    let mut upload_res_futures = vec![];
    let mut manifest_entries = Vec::new();
    for (path, file_size_in_bytes) in files_to_upload {
        let file_name = path
            .file_name()
            .and_then(|filename| filename.to_str())
            .map(|filename| filename.to_string())
            .with_context(|| format!("Failed to extract filename from path {}", path.display()))?;
        manifest_entries.push(ManifestEntry {
            file_name: file_name.to_string(),
            file_size_in_bytes,
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
    let manifest = create_manifest(
        manifest_entries,
        vec![split.segment_meta.id()],
        split_metadata,
    );
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

#[async_trait]
impl AsyncActor for Uploader {
    async fn process_message(
        &mut self,
        split: PackagedSplit,
        _context: ActorContext<'_, Self::Message>,
    ) -> Result<(), MessageProcessError> {
        let split_storage =
            quickwit_storage::add_prefix_to_storage(self.index_storage.clone(), split.split_id);
        let metastore = self.metastore.clone();
        // upload_split(self.metastore);
        Ok(())
    }
}
