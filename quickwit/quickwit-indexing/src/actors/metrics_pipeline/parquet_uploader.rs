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

//! ParquetUploader actor for staging and uploading metrics splits.
//!
//! This actor mirrors the Uploader pattern for logs but handles ParquetSplit
//! produced by ParquetIndexer. It stages splits to the metastore and uploads
//! Parquet files to storage.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::spawn_named_task;
use quickwit_metastore::StageParquetSplitsRequestExt;
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient};
use quickwit_storage::Storage;
use tokio::sync::{Semaphore, SemaphorePermit, oneshot};
use tracing::{Instrument, Span, debug, info, instrument, warn};

use super::{ParquetSplitBatch, ParquetSplitsUpdate};
use crate::actors::sequencer::{Sequencer, SequencerCommand};
use crate::actors::{Publisher, UploaderCounters, UploaderType};
use crate::metrics::INDEXER_METRICS;

/// Concurrent upload permits for metrics ingest uploads.
static CONCURRENT_UPLOAD_PERMITS_METRICS_INDEX: OnceLock<Semaphore> = OnceLock::new();
/// Concurrent upload permits for metrics merge uploads.
static CONCURRENT_UPLOAD_PERMITS_METRICS_MERGE: OnceLock<Semaphore> = OnceLock::new();

/// Stage splits in the metastore, dispatching to the correct RPC based on split kind.
async fn stage_splits(
    metastore: MetastoreServiceClient,
    index_uid: quickwit_proto::types::IndexUid,
    splits: &[ParquetSplitMetadata],
) -> anyhow::Result<()> {
    if splits.is_empty() {
        return Ok(());
    }

    // All splits in a batch must be the same kind (metrics or sketches).
    // The pipeline guarantees this since each index uses a single SplitWriterKind.
    let kind = splits[0].kind;
    debug_assert!(
        splits.iter().all(|s| s.kind == kind),
        "mixed split types in a single batch"
    );

    match kind {
        ParquetSplitKind::Sketches => {
            let stage_request =
                quickwit_proto::metastore::StageSketchSplitsRequest::try_from_splits_metadata(
                    index_uid, splits,
                )?;
            metastore.stage_sketch_splits(stage_request).await?;
        }
        ParquetSplitKind::Metrics => {
            let stage_request =
                quickwit_proto::metastore::StageMetricsSplitsRequest::try_from_splits_metadata(
                    index_uid, splits,
                )?;
            metastore.stage_metrics_splits(stage_request).await?;
        }
    }

    Ok(())
}

/// ParquetUploader actor for staging and uploading metrics splits.
///
/// Receives ParquetSplitBatch from ParquetIndexer, stages splits to the metastore,
/// uploads Parquet files to storage, and sends ParquetSplitsUpdate downstream
/// via a Sequencer to preserve ordering.
#[derive(Clone)]
pub struct ParquetUploader {
    uploader_type: UploaderType,
    metastore: MetastoreServiceClient,
    split_store: Arc<dyn Storage>,
    sequencer_mailbox: Mailbox<Sequencer<Publisher>>,
    max_concurrent_uploads: usize,
    counters: UploaderCounters,
}

impl ParquetUploader {
    /// Create a new ParquetUploader.
    pub fn new(
        uploader_type: UploaderType,
        metastore: MetastoreServiceClient,
        split_store: Arc<dyn Storage>,
        sequencer_mailbox: Mailbox<Sequencer<Publisher>>,
        max_concurrent_uploads: usize,
    ) -> Self {
        Self {
            uploader_type,
            metastore,
            split_store,
            sequencer_mailbox,
            max_concurrent_uploads,
            counters: Default::default(),
        }
    }

    /// Acquire a semaphore permit for upload.
    async fn acquire_semaphore(
        &self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<SemaphorePermit<'static>> {
        let _guard = ctx.protect_zone();
        let (concurrent_upload_permits_once_cell, concurrent_upload_permits_gauge) =
            match self.uploader_type {
                UploaderType::IndexUploader => (
                    &CONCURRENT_UPLOAD_PERMITS_METRICS_INDEX,
                    INDEXER_METRICS
                        .available_concurrent_upload_permits
                        .with_label_values(["metrics_indexer"]),
                ),
                UploaderType::MergeUploader | UploaderType::DeleteUploader => (
                    &CONCURRENT_UPLOAD_PERMITS_METRICS_MERGE,
                    INDEXER_METRICS
                        .available_concurrent_upload_permits
                        .with_label_values(["metrics_merger"]),
                ),
            };
        let concurrent_upload_permits = concurrent_upload_permits_once_cell
            .get_or_init(|| Semaphore::const_new(self.max_concurrent_uploads));
        concurrent_upload_permits_gauge.set(concurrent_upload_permits.available_permits() as i64);
        concurrent_upload_permits
            .acquire()
            .await
            .context("the metrics uploader semaphore is closed (this should never happen)")
    }
}

#[async_trait]
impl Actor for ParquetUploader {
    type ObservableState = UploaderCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // Buffer several splits to decouple ParquetIndexer from S3 upload latency.
        QueueCapacity::Bounded(3)
    }

    fn name(&self) -> String {
        format!("ParquetUploader({:?})", self.uploader_type)
    }
}

#[async_trait]
impl Handler<ParquetSplitBatch> for ParquetUploader {
    type Reply = ();

    #[instrument(name = "parquet_uploader", skip_all, fields(index_uid, num_splits))]
    async fn handle(
        &mut self,
        batch: ParquetSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let index_uid = batch.index_uid.clone();
        let num_splits = batch.splits.len();

        tracing::Span::current().record("index_uid", index_uid.to_string());
        tracing::Span::current().record("num_splits", num_splits);

        if batch.splits.is_empty() {
            debug!("received empty ParquetSplitBatch, forwarding checkpoint only");
            // Even with no splits, the checkpoint delta may contain EOF positions
            // that must reach the publisher for graceful decommission.
            let (tx, rx) = oneshot::channel::<SequencerCommand<ParquetSplitsUpdate>>();
            if let Err(e) = ctx.send_message(&self.sequencer_mailbox, rx).await {
                warn!(error = %e, "failed to reserve sequencer position for empty batch");
                return Ok(());
            }

            let update = ParquetSplitsUpdate {
                index_uid: index_uid.clone(),
                new_splits: Vec::new(),
                replaced_split_ids: batch.replaced_split_ids,
                checkpoint_delta_opt: batch.checkpoint_delta_opt,
                publish_lock: batch.publish_lock,
                publish_token_opt: batch.publish_token_opt,
                parent_span: tracing::Span::current(),
                _merge_task_opt: batch._merge_task_opt,
            };
            if tx.send(SequencerCommand::Proceed(update)).is_err() {
                warn!("sequencer receiver dropped for empty batch");
            }
            return Ok(());
        }

        // Reserve position in sequencer BEFORE starting async work.
        // This ensures that even if uploads complete out of order, they will
        // be published in the order they were submitted.
        let (tx, rx) = oneshot::channel::<SequencerCommand<ParquetSplitsUpdate>>();
        if let Err(e) = ctx.send_message(&self.sequencer_mailbox, rx).await {
            warn!(error = %e, "failed to reserve sequencer position");
            return Ok(());
        }

        // Acquire upload permit
        let permit_guard = self.acquire_semaphore(ctx).await?;
        let kill_switch = ctx.kill_switch().clone();

        if kill_switch.is_dead() {
            warn!("kill switch was activated, cancelling metrics upload");
            let _ = tx.send(SequencerCommand::Discard);
            return Err(ActorExitStatus::Killed);
        }

        // Clone what we need for the async task
        let metastore = self.metastore.clone();
        let split_store = self.split_store.clone();
        let counters = self.counters.clone();

        let output_dir = batch.output_dir;
        let checkpoint_delta_opt = batch.checkpoint_delta_opt;
        let publish_lock = batch.publish_lock;
        let publish_token_opt = batch.publish_token_opt;
        let splits = batch.splits;
        let replaced_split_ids = batch.replaced_split_ids;
        let merge_task_opt = batch._merge_task_opt;
        // Hold the scratch directory alive until the upload task completes.
        // For the merge path, this prevents the TempDirectory from being
        // cleaned up before the upload task reads the merged files.
        let _scratch_directory_guard = batch._scratch_directory_opt;
        debug!(
            index_uid = %index_uid,
            num_splits = splits.len(),
            "starting metrics split staging and upload"
        );

        spawn_named_task(
            async move {
                // Check publish lock before doing any work. Between the time
                // the handler reserved the sequencer position and now, the lock
                // may have died (e.g. source reassignment).
                if publish_lock.is_dead() {
                    info!("splits' publish lock is dead");
                    let _ = tx.send(SequencerCommand::Discard);
                    return;
                }

                // Stage splits in metastore based on split type
                let stage_result =
                    stage_splits(metastore.clone(), index_uid.clone(), &splits).await;

                if let Err(e) = stage_result {
                    warn!(error = %e, "failed to stage splits");
                    // Discard sequencer position on error
                    let _ = tx.send(SequencerCommand::Discard);
                    kill_switch.kill();
                    return;
                }

                counters
                    .num_staged_splits
                    .fetch_add(splits.len() as u64, Ordering::SeqCst);
                info!(
                    index_uid = %index_uid,
                    num_splits = splits.len(),
                    "staged splits in metastore"
                );

                // Upload Parquet files to storage
                for split in &splits {
                    let parquet_file = split.parquet_filename();
                    // Read the local Parquet file from output_dir
                    let local_path = output_dir.join(&parquet_file);
                    let file_content = match tokio::fs::read(&local_path).await {
                        Ok(content) => content,
                        Err(e) => {
                            warn!(
                                error = %e,
                                local_path = %local_path.display(),
                                split_id = %split.split_id_str(),
                                parquet_file = %parquet_file,
                                "failed to read local parquet file"
                            );
                            // Discard sequencer position on error
                            let _ = tx.send(SequencerCommand::Discard);
                            kill_switch.kill();
                            return;
                        }
                    };

                    let file_size = file_content.len();
                    let payload: Box<dyn quickwit_storage::PutPayload> = Box::new(file_content);

                    // Upload to S3 using the filename directly (matches logs pipeline)
                    if let Err(e) = split_store.put(Path::new(&parquet_file), payload).await {
                        warn!(
                            error = %e,
                            split_id = %split.split_id_str(),
                            parquet_file = %parquet_file,
                            "failed to upload parquet file"
                        );
                        // Discard sequencer position on error
                        let _ = tx.send(SequencerCommand::Discard);
                        kill_switch.kill();
                        return;
                    }

                    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);

                    // Delete the local parquet file after successful upload.
                    if let Err(e) = tokio::fs::remove_file(&local_path).await {
                        warn!(
                            error = %e,
                            local_path = %local_path.display(),
                            "failed to delete local parquet file after upload"
                        );
                    }

                    debug!(
                        split_id = %split.split_id_str(),
                        parquet_file = %parquet_file,
                        file_size = file_size,
                        "uploaded parquet file to storage"
                    );
                }

                // Create ParquetSplitsUpdate and send downstream.
                // The merge task (if present) transfers to the update so the
                // planner guard and semaphore permit stay alive until publish.
                let update = ParquetSplitsUpdate {
                    index_uid,
                    new_splits: splits,
                    replaced_split_ids,
                    checkpoint_delta_opt,
                    publish_lock,
                    publish_token_opt,
                    parent_span: Span::current(),
                    _merge_task_opt: merge_task_opt,
                };

                if tx.send(SequencerCommand::Proceed(update)).is_err() {
                    warn!("sequencer receiver dropped");
                }

                // Drop permit to allow next upload
                drop(permit_guard);
                // Drop scratch directory guard after upload completes.
                drop(_scratch_directory_guard);
            }
            .instrument(Span::current()),
            "metrics_upload_task",
        );

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use quickwit_actors::{ObservationType, Universe};
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_parquet_engine::split::{ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_proto::types::IndexUid;
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::actors::{Publisher, Sequencer};
    use crate::models::PublishLock;

    fn create_test_metrics_split(index_uid: &str, split_id: &str) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .index_uid(index_uid)
            .split_id(quickwit_parquet_engine::split::ParquetSplitId::new(
                split_id,
            ))
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(1024)
            .build()
    }

    /// Create placeholder parquet files in the temp directory for testing.
    /// The uploader expects to read these files from output_dir.
    fn create_placeholder_parquet_files(
        temp_dir: &std::path::Path,
        splits: &[ParquetSplitMetadata],
    ) {
        for split in splits {
            let parquet_filename = split.parquet_filename();
            let file_path = temp_dir.join(&parquet_filename);
            // Write minimal valid content (actual parquet not needed for staging test)
            std::fs::write(&file_path, b"placeholder parquet content")
                .expect("Failed to create placeholder parquet file");
        }
    }

    #[tokio::test]
    async fn test_metrics_uploader_stages_and_uploads() {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox =
            super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .withf(|request| request.index_uid().index_id == "test-index")
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage.clone(),
            sequencer_mailbox,
            4,
        );

        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        // Create test batch with temp directory as output_dir
        let splits = vec![create_test_metrics_split("test-index", "test-split-1")];
        // Create placeholder parquet files that the uploader will read
        create_placeholder_parquet_files(temp_dir.path(), &splits);
        let checkpoint_delta = IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..10),
        };
        let batch = ParquetSplitBatch {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            splits,
            output_dir: temp_dir.path().to_path_buf(),
            checkpoint_delta_opt: Some(checkpoint_delta),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            replaced_split_ids: Vec::new(),
            _scratch_directory_opt: None,
            _merge_task_opt: None,
        };

        uploader_mailbox.send_message(batch).await.unwrap();

        let observation = uploader_handle.process_pending_and_observe().await;
        assert_eq!(observation.obs_type, ObservationType::Alive);

        // Verify counters
        assert!(observation.state.num_staged_splits.load(Ordering::Relaxed) >= 1);

        // Wait briefly for the async upload task to complete file deletion
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify local parquet files are deleted after successful upload
        let remaining_parquet_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            remaining_parquet_files.is_empty(),
            "expected parquet files to be deleted after upload, but found: {:?}",
            remaining_parquet_files
                .iter()
                .map(|e| e.path())
                .collect::<Vec<_>>()
        );

        // Verify the file was actually uploaded to storage
        assert!(
            ram_storage
                .exists(std::path::Path::new("test-split-1.parquet"))
                .await
                .unwrap()
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_uploader_deletes_local_files_after_upload() {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox =
            super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage.clone(),
            sequencer_mailbox,
            4,
        );

        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        // Create multiple splits with parquet files
        let splits = vec![
            create_test_metrics_split("test-index", "split-a"),
            create_test_metrics_split("test-index", "split-b"),
        ];
        create_placeholder_parquet_files(temp_dir.path(), &splits);

        // Verify files exist before upload
        assert!(temp_dir.path().join("split-a.parquet").exists());
        assert!(temp_dir.path().join("split-b.parquet").exists());

        let checkpoint_delta = IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..10),
        };
        let batch = ParquetSplitBatch {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            splits,
            output_dir: temp_dir.path().to_path_buf(),
            checkpoint_delta_opt: Some(checkpoint_delta),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            replaced_split_ids: Vec::new(),
            _scratch_directory_opt: None,
            _merge_task_opt: None,
        };

        uploader_mailbox.send_message(batch).await.unwrap();

        let observation = uploader_handle.process_pending_and_observe().await;
        assert_eq!(observation.obs_type, ObservationType::Alive);

        // Wait for async upload task to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Both local files should be deleted
        assert!(
            !temp_dir.path().join("split-a.parquet").exists(),
            "split-a.parquet should be deleted after upload"
        );
        assert!(
            !temp_dir.path().join("split-b.parquet").exists(),
            "split-b.parquet should be deleted after upload"
        );

        // Both files should exist in remote storage
        assert!(
            ram_storage
                .exists(std::path::Path::new("split-a.parquet"))
                .await
                .unwrap()
        );
        assert!(
            ram_storage
                .exists(std::path::Path::new("split-b.parquet"))
                .await
                .unwrap()
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_uploader_handles_empty_batch() {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox =
            super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);

        let mut mock_metastore = MockMetastoreService::new();
        // Should NOT call stage_metrics_splits for empty batch
        mock_metastore.expect_stage_metrics_splits().never();

        let ram_storage = Arc::new(RamStorage::default());
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage.clone(),
            sequencer_mailbox,
            4,
        );

        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        // Create empty batch with valid checkpoint delta (1 position)
        let checkpoint_delta = IndexCheckpointDelta {
            source_id: "test-source".to_string(),
            source_delta: SourceCheckpointDelta::from_range(0..1),
        };
        let batch = ParquetSplitBatch {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            splits: Vec::new(),
            output_dir: temp_dir.path().to_path_buf(),
            checkpoint_delta_opt: Some(checkpoint_delta),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            replaced_split_ids: Vec::new(),
            _scratch_directory_opt: None,
            _merge_task_opt: None,
        };

        uploader_mailbox.send_message(batch).await.unwrap();

        let observation = uploader_handle.process_pending_and_observe().await;
        assert_eq!(observation.obs_type, ObservationType::Alive);

        // Should not have staged any splits
        assert_eq!(
            observation.state.num_staged_splits.load(Ordering::Relaxed),
            0
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_uploader_with_sequencer_ordering() {
        // This test verifies that when using the Sequencer variant:
        // 1. Messages flow through the sequencer to the publisher
        // 2. The sequencer maintains FIFO ordering even if uploads complete out of order
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a simple receiver actor to collect ParquetSplitsUpdate messages
        // We use a test mailbox for Publisher to capture what would be sent
        let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();

        // Create sequencer that forwards to publisher
        let sequencer = Sequencer::new(publisher_mailbox);
        let (sequencer_mailbox, _sequencer_handle) = universe.spawn_builder().spawn(sequencer);

        let mut mock_metastore = MockMetastoreService::new();
        // Allow multiple stage calls for the batches
        mock_metastore
            .expect_stage_metrics_splits()
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage.clone(),
            sequencer_mailbox,
            4,
        );

        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        // Send batches with splits that can be identified by split_id
        for i in 1..=3 {
            let splits = vec![create_test_metrics_split(
                "test-index",
                &format!("split-{}", i),
            )];
            // Create placeholder parquet files that the uploader will read
            create_placeholder_parquet_files(temp_dir.path(), &splits);
            let checkpoint_delta = IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range((i * 10)..(i * 10 + 10)),
            };
            let batch = ParquetSplitBatch {
                index_uid: IndexUid::new_with_random_ulid("test-index"),
                splits,
                output_dir: temp_dir.path().to_path_buf(),
                checkpoint_delta_opt: Some(checkpoint_delta),
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                replaced_split_ids: Vec::new(),
                _scratch_directory_opt: None,
                _merge_task_opt: None,
            };
            uploader_mailbox.send_message(batch).await.unwrap();
        }

        // Wait for all messages to be processed
        // The uploader spawns background tasks, so we need to give them time
        uploader_handle.process_pending_and_observe().await;

        // Give background tasks time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Drain inbox and verify ordering
        let mut received_split_ids = Vec::new();
        let messages = publisher_inbox.drain_for_test();
        for msg in messages {
            // The inbox contains typed messages, we need to access the ParquetSplitsUpdate
            if let Some(update) = msg.downcast_ref::<ParquetSplitsUpdate>() {
                for split in &update.new_splits {
                    received_split_ids.push(split.split_id_str().to_string());
                }
            }
        }

        // Verify we received all 3 splits in order
        // The sequencer ensures FIFO delivery: split-1, split-2, split-3
        assert_eq!(
            received_split_ids,
            vec!["split-1", "split-2", "split-3"],
            "Sequencer should maintain FIFO ordering"
        );

        universe.assert_quit().await;
    }
}
