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

//! ParquetIndexer actor that accumulates RecordBatches for the metrics pipeline.
//!
//! This actor replaces the Tantivy-based Indexer for metrics workloads, accumulating
//! Arrow RecordBatches and forwarding concatenated batches to ParquetPackager for
//! Parquet encoding and file writing.

use std::path::PathBuf;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_parquet_engine::index::{ParquetBatchAccumulator, ParquetIndexingConfig};
use quickwit_parquet_engine::split::ParquetSplit;
use quickwit_proto::types::{IndexUid, PublishToken, SourceId};
use serde::Serialize;
use tokio::runtime::Handle;
use tracing::{debug, info, info_span, warn};
use ulid::Ulid;

use crate::actors::parquet_packager::{ParquetBatchForPackager, ParquetPackager};
use crate::models::{NewPublishLock, NewPublishToken, ProcessedParquetBatch, PublishLock};

/// Default commit timeout for ParquetIndexer (60 seconds).
// TODO: read from index config commit_timeout_secs.
const DEFAULT_COMMIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Message to trigger a commit after timeout.
#[derive(Debug)]
struct CommitTimeout {
    workbench_id: Ulid,
}

/// Counters for ParquetIndexer observability.
#[derive(Debug, Default, Serialize, Clone)]
pub struct ParquetIndexerCounters {
    /// Number of batches received.
    pub batches_received: u64,
    /// Number of rows indexed.
    pub rows_indexed: u64,
    /// Number of batches flushed to packager.
    pub batches_flushed: u64,
    /// Number of errors encountered.
    pub errors: u64,
}

impl ParquetIndexerCounters {
    /// Record a batch received.
    pub fn record_batch(&mut self, num_rows: usize) {
        self.batches_received += 1u64;
        self.rows_indexed += num_rows as u64;
    }

    /// Record a flush (concatenated batch sent to packager).
    pub fn record_flush(&mut self) {
        self.batches_flushed += 1;
    }

    /// Record an error.
    pub fn record_error(&mut self) {
        self.errors += 1;
    }
}

/// Message containing produced ParquetSplits for downstream processing.
///
/// This is sent when the accumulator produces splits (either via threshold or force commit).
#[derive(Debug)]
pub struct ParquetSplitBatch {
    /// Index unique identifier for the splits in this batch.
    pub index_uid: IndexUid,
    /// The splits produced.
    pub splits: Vec<ParquetSplit>,
    /// Directory containing the Parquet files referenced by splits.
    /// The uploader uses this to locate and upload the actual file content.
    pub output_dir: PathBuf,
    /// Checkpoint delta covering all data in these splits.
    pub checkpoint_delta: IndexCheckpointDelta,
    /// Publish lock for coordinating with sources.
    pub publish_lock: PublishLock,
    /// Optional publish token.
    pub publish_token_opt: Option<PublishToken>,
}

/// ParquetIndexer actor that accumulates RecordBatches and forwards them to ParquetPackager.
///
/// This actor:
/// - Receives ProcessedParquetBatch messages from ParquetDocProcessor
/// - Accumulates batches using ParquetBatchAccumulator (CPU-bound buffering + concat)
/// - Forwards concatenated RecordBatches to ParquetPackager when thresholds are exceeded or on
///   force commit
/// - Tracks checkpoint progress for reliable delivery
///
/// Unlike the Tantivy-based Indexer, this actor works entirely with Arrow data.
/// IO-bound work (Parquet encoding, compression, file writing) is delegated to
/// the ParquetPackager actor.
pub struct ParquetIndexer {
    /// Index unique identifier.
    index_uid: IndexUid,
    /// Source identifier.
    source_id: SourceId,
    /// Batch accumulator for buffering and concatenating RecordBatches.
    accumulator: ParquetBatchAccumulator,
    /// Accumulated checkpoint delta.
    checkpoint_delta: SourceCheckpointDelta,
    /// Publish lock for coordinating with sources.
    publish_lock: PublishLock,
    /// Optional publish token.
    publish_token_opt: Option<PublishToken>,
    /// Observability counters.
    counters: ParquetIndexerCounters,
    /// Current workbench ID for tracing.
    workbench_id: Ulid,
    /// Mailbox for sending concatenated batches to packager.
    packager_mailbox: Mailbox<ParquetPackager>,
    /// Commit timeout duration.
    commit_timeout: Duration,
    /// Whether a commit timeout has been scheduled for the current workbench.
    commit_timeout_scheduled: bool,
}

impl ParquetIndexer {
    /// Create a new ParquetIndexer.
    ///
    /// # Arguments
    /// * `index_uid` - The index unique identifier
    /// * `source_id` - The source identifier
    /// * `config` - Optional configuration for accumulation thresholds
    /// * `packager_mailbox` - Mailbox for sending concatenated batches to ParquetPackager
    /// * `commit_timeout` - Optional commit timeout (defaults to 60 seconds)
    pub fn new(
        index_uid: IndexUid,
        source_id: SourceId,
        config: Option<ParquetIndexingConfig>,
        packager_mailbox: Mailbox<ParquetPackager>,
        commit_timeout: Option<Duration>,
    ) -> Self {
        let config = config.unwrap_or_default();
        let accumulator = ParquetBatchAccumulator::new(config);
        let counters = ParquetIndexerCounters::default();
        let commit_timeout = commit_timeout.unwrap_or(DEFAULT_COMMIT_TIMEOUT);

        info!(
            index_uid = %index_uid,
            source_id = %source_id,
            commit_timeout_secs = commit_timeout.as_secs(),
            "ParquetIndexer created"
        );

        Self {
            index_uid,
            source_id,
            accumulator,
            checkpoint_delta: SourceCheckpointDelta::default(),
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            counters,
            workbench_id: Ulid::new(),
            packager_mailbox,
            commit_timeout,
            commit_timeout_scheduled: false,
        }
    }

    /// Process a batch and potentially produce a flushed RecordBatch.
    ///
    /// Returns at most one concatenated RecordBatch. A flush is triggered either by
    /// the accumulator exceeding its row/byte threshold, or by a force_commit request.
    /// This ensures exactly one checkpoint delta per flushed batch.
    fn process_batch(
        &mut self,
        batch: ProcessedParquetBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<Option<RecordBatch>, ActorExitStatus> {
        let _span = info_span!(
            target: "quickwit-indexing",
            "metrics-indexer",
            index_id = %self.index_uid,
            source_id = %self.source_id,
            workbench_id = %self.workbench_id,
        )
        .entered();

        // Record batch stats
        let num_rows = batch.num_rows();
        self.counters.record_batch(num_rows);

        debug!(
            num_rows = num_rows,
            force_commit = batch.force_commit,
            total_batches = self.counters.batches_received,
            total_rows = self.counters.rows_indexed,
            "received batch for accumulation"
        );

        // Extend checkpoint
        self.checkpoint_delta
            .extend(batch.checkpoint_delta)
            .map_err(|e| {
                warn!(error = %e, "Checkpoint delta conflict");
                ActorExitStatus::Failure(anyhow::anyhow!("{}", e).into())
            })?;

        let force_commit = batch.force_commit;

        // Add batch to accumulator (auto-flushes if threshold exceeded)
        let mut flushed = match self.accumulator.add_batch(batch.batch) {
            Ok(threshold_batch) => {
                if let Some(ref combined) = threshold_batch {
                    debug!(
                        num_rows = combined.num_rows(),
                        "accumulator flushed from threshold"
                    );
                }
                threshold_batch
            }
            Err(error) => {
                warn!(error = %error, "Failed to add batch to accumulator");
                self.counters.record_error();
                return Err(ActorExitStatus::Failure(
                    anyhow::anyhow!("{}", error).into(),
                ));
            }
        };

        // Reset actor state if the accumulator auto-flushed.
        if flushed.is_some() {
            self.workbench_id = Ulid::new();
            self.commit_timeout_scheduled = false;
        }

        ctx.record_progress();

        // Force flush if requested and threshold didn't already flush
        if force_commit && flushed.is_none() {
            debug!("force commit requested, flushing accumulator");
            flushed = self.flush_accumulator()?;
        }

        if let Some(ref combined) = flushed {
            self.counters.record_flush();
            info!(
                num_rows = combined.num_rows(),
                "flushed concatenated batch to packager"
            );
        }

        Ok(flushed)
    }

    /// Flush the accumulator.
    ///
    /// Returns the concatenated RecordBatch, or None if no pending data.
    fn flush_accumulator(&mut self) -> Result<Option<RecordBatch>, ActorExitStatus> {
        match self.accumulator.flush() {
            Ok(batch_opt) => {
                if batch_opt.is_some() {
                    // Reset workbench for next batch of work
                    self.workbench_id = Ulid::new();
                    self.commit_timeout_scheduled = false;
                }
                Ok(batch_opt)
            }
            Err(error) => {
                warn!(error = %error, "Failed to flush accumulator");
                self.counters.record_error();
                Err(ActorExitStatus::Failure(
                    anyhow::anyhow!("{}", error).into(),
                ))
            }
        }
    }

    /// Take the current checkpoint delta and reset it.
    fn take_checkpoint_delta(&mut self) -> SourceCheckpointDelta {
        std::mem::take(&mut self.checkpoint_delta)
    }

    /// Create an IndexCheckpointDelta from the current source delta.
    fn make_index_checkpoint_delta(&mut self) -> IndexCheckpointDelta {
        IndexCheckpointDelta {
            source_id: self.source_id.clone(),
            source_delta: self.take_checkpoint_delta(),
        }
    }
}

#[async_trait]
impl Actor for ParquetIndexer {
    type ObservableState = ParquetIndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn name(&self) -> String {
        "ParquetIndexer".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                // Flush remaining data on clean shutdown.
                let flushed_batch = match self.flush_accumulator() {
                    Ok(Some(combined)) => {
                        self.counters.record_flush();
                        info!(
                            num_rows = combined.num_rows(),
                            "flushed final batch on shutdown"
                        );
                        Some(combined)
                    }
                    Ok(None) => None,
                    Err(_) => None,
                };

                // Send even when batch is None — the checkpoint delta may contain
                // EOF positions that must flow through the pipeline for graceful
                // decommission to complete.
                let checkpoint_delta = self.make_index_checkpoint_delta();
                if flushed_batch.is_some() || !checkpoint_delta.source_delta.is_empty() {
                    let batch_for_packager = ParquetBatchForPackager {
                        batch: flushed_batch,
                        index_uid: self.index_uid.clone(),
                        checkpoint_delta,
                        publish_lock: self.publish_lock.clone(),
                        publish_token_opt: self.publish_token_opt.clone(),
                    };
                    let _ = ctx
                        .send_message(&self.packager_mailbox, batch_for_packager)
                        .await;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ProcessedParquetBatch> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        batch: ProcessedParquetBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.publish_lock.is_dead() {
            return Ok(());
        }

        // Schedule commit timeout on first batch for this workbench
        if !self.commit_timeout_scheduled {
            let commit_timeout_message = CommitTimeout {
                workbench_id: self.workbench_id,
            };
            ctx.schedule_self_msg(self.commit_timeout, commit_timeout_message);
            self.commit_timeout_scheduled = true;
            debug!(
                workbench_id = %self.workbench_id,
                commit_timeout_secs = self.commit_timeout.as_secs(),
                "scheduled commit timeout"
            );
        }

        let force_commit = batch.force_commit;
        let flushed_batch = self.process_batch(batch, ctx)?;

        // Send downstream if we have data, or if force_commit needs to push
        // the checkpoint through immediately (e.g. shard EOF).
        let should_send =
            flushed_batch.is_some() || (force_commit && !self.checkpoint_delta.is_empty());

        if should_send {
            let batch_for_packager = ParquetBatchForPackager {
                batch: flushed_batch,
                index_uid: self.index_uid.clone(),
                checkpoint_delta: self.make_index_checkpoint_delta(),
                publish_lock: self.publish_lock.clone(),
                publish_token_opt: self.publish_token_opt.clone(),
            };

            ctx.send_message(&self.packager_mailbox, batch_for_packager)
                .await
                .map_err(|e| {
                    ActorExitStatus::Failure(
                        anyhow::anyhow!("failed to send to packager: {}", e).into(),
                    )
                })?;

            // Reset so the next batch schedules a fresh timeout.
            self.workbench_id = Ulid::new();
            self.commit_timeout_scheduled = false;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishLock> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = message;

        // Discard pending data — it was accumulated under the old publish lock
        // and must not be published. This matches the standard Indexer behavior
        // which discards its workbench on publish lock change.
        self.accumulator.discard();

        // Reset state for new lock
        self.publish_lock = publish_lock;
        self.checkpoint_delta = SourceCheckpointDelta::default();
        self.workbench_id = Ulid::new();
        self.commit_timeout_scheduled = false;

        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishToken> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishToken,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishToken(publish_token) = message;
        self.publish_token_opt = Some(publish_token);
        Ok(())
    }
}

#[async_trait]
impl Handler<CommitTimeout> for ParquetIndexer {
    type Reply = ();

    async fn handle(
        &mut self,
        commit_timeout: CommitTimeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Ignore stale timeout messages from previous workbenches
        if self.workbench_id != commit_timeout.workbench_id {
            return Ok(());
        }

        debug!(
            workbench_id = %self.workbench_id,
            "commit timeout triggered, flushing accumulator"
        );

        // Flush the accumulator
        let flushed_batch = self.flush_accumulator()?;
        if let Some(ref combined) = flushed_batch {
            self.counters.record_flush();
            info!(
                num_rows = combined.num_rows(),
                "flushed batch on commit timeout"
            );
        }

        // Forward if we have data or a pending checkpoint delta.
        let should_send = flushed_batch.is_some() || !self.checkpoint_delta.is_empty();

        if should_send {
            let batch_for_packager = ParquetBatchForPackager {
                batch: flushed_batch,
                index_uid: self.index_uid.clone(),
                checkpoint_delta: self.make_index_checkpoint_delta(),
                publish_lock: self.publish_lock.clone(),
                publish_token_opt: self.publish_token_opt.clone(),
            };

            ctx.send_message(&self.packager_mailbox, batch_for_packager)
                .await
                .map_err(|e| {
                    ActorExitStatus::Failure(
                        anyhow::anyhow!(
                            "failed to send ParquetBatchForPackager to packager on commit \
                             timeout: {}",
                            e
                        )
                        .into(),
                    )
                })?;

            // Reset so the next batch schedules a fresh timeout.
            self.workbench_id = Ulid::new();
            self.commit_timeout_scheduled = false;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use quickwit_actors::{ActorHandle, Universe};
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
    use quickwit_parquet_engine::test_helpers::create_test_batch;
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::actors::{
        ParquetPackager, ParquetPublisher, ParquetUploader, SplitsUpdateMailbox, UploaderType,
    };

    /// Create a test ParquetUploader and return its mailbox.
    fn create_test_uploader(
        universe: &Universe,
    ) -> (Mailbox<ParquetUploader>, ActorHandle<ParquetUploader>) {
        let mock_metastore = MockMetastoreService::new();
        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) =
            universe.create_test_mailbox::<ParquetPublisher>();

        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            4,
        );
        universe.spawn_builder().spawn(uploader)
    }

    /// Create a test ParquetUploader with a mock that expects staging calls.
    fn create_test_uploader_with_staging_expectation(
        universe: &Universe,
        expected_index_id: &'static str,
    ) -> (Mailbox<ParquetUploader>, ActorHandle<ParquetUploader>) {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .withf(move |request| request.index_uid().index_id == expected_index_id)
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) =
            universe.create_test_mailbox::<ParquetPublisher>();

        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            4,
        );
        universe.spawn_builder().spawn(uploader)
    }

    /// Create a test ParquetPackager wired to an uploader.
    fn create_test_packager(
        universe: &Universe,
        temp_dir: &std::path::Path,
        uploader_mailbox: Mailbox<ParquetUploader>,
    ) -> (Mailbox<ParquetPackager>, ActorHandle<ParquetPackager>) {
        let writer_config = ParquetWriterConfig::default();
        let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
        let split_writer = ParquetSplitWriter::new(writer_config, temp_dir, &table_config);

        let packager = ParquetPackager::new(split_writer, uploader_mailbox);
        universe.spawn_builder().spawn(packager)
    }

    /// Wait for the uploader to have staged the expected number of splits.
    async fn wait_for_staged_splits(
        uploader_handle: &ActorHandle<ParquetUploader>,
        expected_splits: u64,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async {
                uploader_handle.process_pending_and_observe().await;
                let counters = uploader_handle.last_observation();
                counters.num_staged_splits.load(Ordering::Relaxed) >= expected_splits
            },
            Duration::from_secs(15),
            Duration::from_millis(50),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for {} staged splits", expected_splits))
    }

    #[tokio::test]
    async fn test_metrics_indexer_receives_batch() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, _packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send a batch
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.rows_indexed, 10);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_force_commit() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch with force_commit
        let batch = create_test_batch(5);
        let processed = ProcessedParquetBatch::new(
            batch,
            SourceCheckpointDelta::from_range(0..5),
            true, // force_commit
        );

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should have flushed a batch due to force_commit
        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);
        assert!(packager_counters.bytes_written.load(Ordering::Relaxed) > 0);

        // Verify uploader received the split
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_respects_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, _packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Set up and kill publish lock
        let publish_lock = PublishLock::default();
        indexer_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;
        publish_lock.kill().await;

        // Send batch after lock is dead
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should not process anything when publish lock is dead
        assert_eq!(counters.batches_received, 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_threshold_flush() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        // Configure low threshold for testing
        let config = ParquetIndexingConfig::default().with_max_rows(50);

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            Some(config),
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch that exceeds threshold
        let batch = create_test_batch(100);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..100), false);

        indexer_mailbox.send_message(processed).await.unwrap();

        let counters = indexer_handle.process_pending_and_observe().await.state;

        // Should have flushed due to exceeding threshold
        assert_eq!(counters.batches_received, 1);
        assert_eq!(counters.rows_indexed, 100);
        assert!(counters.batches_flushed >= 1);

        // Verify packager produced split(s)
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert!(packager_counters.splits_produced.load(Ordering::Relaxed) >= 1);

        // Verify uploader received the splits
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_flush_on_shutdown() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch without force_commit
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();
        indexer_handle.process_pending_and_observe().await;

        // Gracefully shut down - should flush remaining data
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, counters) = indexer_handle.join().await;

        assert!(exit_status.is_success());
        // Should have flushed on shutdown
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_commit_timeout() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) =
            create_test_uploader_with_staging_expectation(&universe, "test-index");
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        // Use a short commit timeout for testing
        let commit_timeout = Duration::from_secs(2);
        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            Some(commit_timeout),
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch without force_commit
        let batch = create_test_batch(10);
        let processed =
            ProcessedParquetBatch::new(batch, SourceCheckpointDelta::from_range(0..10), false);

        indexer_mailbox.send_message(processed).await.unwrap();
        indexer_handle.process_pending_and_observe().await;

        // No flush yet (waiting for threshold or timeout)
        let counters = indexer_handle.observe().await;
        assert_eq!(counters.batches_flushed, 0);

        // Wait for commit timeout to trigger
        universe
            .sleep(commit_timeout + Duration::from_millis(100))
            .await;
        indexer_handle.process_pending_and_observe().await;

        // Should have flushed due to commit timeout
        let counters = indexer_handle.observe().await;
        assert_eq!(counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        // Verify uploader received the split
        uploader_handle.process_pending_and_observe().await;

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_indexer_forwards_to_packager_and_uploader() {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create uploader with staging expectation
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) =
            universe.create_test_mailbox::<ParquetPublisher>();
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            SplitsUpdateMailbox::Publisher(publisher_mailbox),
            4,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);

        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );

        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        // Send batch with force_commit to trigger flush
        let batch = create_test_batch(20);
        let processed = ProcessedParquetBatch::new(
            batch,
            SourceCheckpointDelta::from_range(0..20),
            true, // force_commit
        );

        indexer_mailbox.send_message(processed).await.unwrap();

        // Process indexer messages
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;

        // Verify indexer flushed a batch
        assert_eq!(indexer_counters.batches_received, 1);
        assert_eq!(indexer_counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        // Wait for the uploader to stage the split
        wait_for_staged_splits(&uploader_handle, 1)
            .await
            .expect("Uploader should have staged 1 split");

        universe.assert_quit().await;
    }
}
