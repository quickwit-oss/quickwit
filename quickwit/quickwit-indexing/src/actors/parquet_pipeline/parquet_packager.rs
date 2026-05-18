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

//! ParquetPackager actor that writes sorted RecordBatches to Parquet split files.
//!
//! This actor sits between ParquetIndexer and ParquetUploader. It receives
//! concatenated RecordBatches from the indexer and performs the IO-bound work:
//! Parquet encoding, compression, and file writing. This decouples CPU-bound
//! accumulation (in the indexer) from IO-bound packaging (here), allowing
//! the indexer to continue buffering while the packager writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_parquet_engine::storage::ParquetSplitWriter;
use quickwit_proto::types::{IndexUid, PublishToken};
use serde::Serialize;
use tokio::runtime::Handle;
use tracing::{info, warn};

use super::ParquetUploader;
use super::parquet_indexer::ParquetSplitBatch;
use crate::models::PublishLock;

/// A flushed partition-local RecordBatch within a workbench commit.
#[derive(Debug)]
pub struct PartitionedRecordBatch {
    /// The concatenated RecordBatch for a single partition.
    pub batch: RecordBatch,
    /// Partition this batch belongs to.
    pub partition_id: u64,
}

/// A flushed workbench ready to be written to one or more Parquet files.
///
/// Sent from ParquetIndexer to ParquetPackager when the current workbench flushes
/// (either from threshold, timeout, or force commit).
#[derive(Debug)]
pub struct ParquetBatchForPackager {
    /// The concatenated partition-local RecordBatches in this workbench.
    ///
    /// Empty when the indexer is forwarding only a checkpoint delta.
    pub batches: Vec<PartitionedRecordBatch>,
    /// Index unique identifier for split metadata.
    pub index_uid: IndexUid,
    /// Checkpoint delta covering all data in this workbench.
    pub checkpoint_delta: IndexCheckpointDelta,
    /// Publish lock for coordination.
    pub publish_lock: PublishLock,
    /// Optional publish token.
    pub publish_token_opt: Option<PublishToken>,
}

/// Counters for ParquetPackager observability.
#[derive(Debug, Default, Serialize)]
pub struct ParquetPackagerCounters {
    /// Number of splits produced (Parquet files written).
    pub splits_produced: AtomicU64,
    /// Number of bytes written to Parquet files.
    pub bytes_written: AtomicU64,
    /// Number of errors encountered.
    pub errors: AtomicU64,
}

impl ParquetPackagerCounters {
    /// Record a split produced.
    pub fn record_split(&self, size_bytes: u64) {
        self.splits_produced.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(size_bytes, Ordering::Relaxed);
    }

    /// Record an error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// ParquetPackager actor that writes RecordBatches to Parquet split files.
///
/// This actor:
/// - Receives ParquetBatchForPackager messages from ParquetIndexer
/// - Writes the RecordBatch to a Parquet file via the configured split writer
/// - Extracts split metadata (time range, metric names)
/// - Forwards the completed ParquetSplitBatch to ParquetUploader
///
/// Runs on the blocking runtime since Parquet encoding and file IO are CPU/IO-bound.
pub struct ParquetPackager {
    /// Split writer for producing Parquet files.
    split_writer: ParquetSplitWriter,
    /// Mailbox for sending splits to uploader.
    uploader_mailbox: Mailbox<ParquetUploader>,
    /// Observability counters.
    counters: Arc<ParquetPackagerCounters>,
}

impl ParquetPackager {
    /// Create a new ParquetPackager.
    ///
    /// # Arguments
    /// * `split_writer` - Writer for producing Parquet files with metadata
    /// * `uploader_mailbox` - Mailbox for sending splits to ParquetUploader
    pub fn new(
        split_writer: ParquetSplitWriter,
        uploader_mailbox: Mailbox<ParquetUploader>,
    ) -> Self {
        let counters = Arc::new(ParquetPackagerCounters::default());

        info!(
            output_dir = %split_writer.base_path().display(),
            "ParquetPackager created"
        );

        Self {
            split_writer,
            uploader_mailbox,
            counters,
        }
    }

    /// Get a reference to the counters.
    pub fn counters(&self) -> &Arc<ParquetPackagerCounters> {
        &self.counters
    }
}

#[async_trait]
impl Actor for ParquetPackager {
    type ObservableState = Arc<ParquetPackagerCounters>;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // Bounded(1) provides backpressure on the indexer: if the packager is busy
        // writing a split, the indexer will block until the write completes.
        QueueCapacity::Bounded(1)
    }

    fn name(&self) -> String {
        "ParquetPackager".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
    }
}

#[async_trait]
impl Handler<ParquetBatchForPackager> for ParquetPackager {
    type Reply = ();

    async fn handle(
        &mut self,
        batch_for_packager: ParquetBatchForPackager,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let ParquetBatchForPackager {
            batches,
            index_uid,
            checkpoint_delta,
            publish_lock,
            publish_token_opt,
        } = batch_for_packager;

        let output_dir = self.split_writer.base_path().clone();
        let index_uid_str = index_uid.to_string();

        let mut splits = Vec::with_capacity(batches.len());
        for PartitionedRecordBatch {
            batch,
            partition_id,
        } in batches
        {
            let num_rows = batch.num_rows();

            match self.split_writer.write_split(&batch, &index_uid_str) {
                Ok(mut split_metadata) => {
                    split_metadata.partition_id = partition_id;
                    let size_bytes = split_metadata.size_bytes();
                    self.counters.record_split(size_bytes);

                    info!(
                        split_id = %split_metadata.split_id_str(),
                        partition_id,
                        num_rows,
                        size_bytes,
                        "ParquetPackager wrote split"
                    );
                    splits.push(split_metadata);
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        partition_id,
                        num_rows,
                        "ParquetPackager failed to write split"
                    );
                    self.counters.record_error();
                    return Err(ActorExitStatus::Failure(
                        anyhow::anyhow!("Failed to write Parquet split: {}", error).into(),
                    ));
                }
            }
        }

        ctx.record_progress();

        // Forward to uploader
        let split_batch = ParquetSplitBatch {
            index_uid,
            splits,
            output_dir,
            checkpoint_delta_opt: Some(checkpoint_delta),
            publish_lock,
            publish_token_opt,
            replaced_split_ids: Vec::new(),
            _scratch_directory_opt: None,
            _merge_task_opt: None,
        };

        ctx.send_message(&self.uploader_mailbox, split_batch)
            .await
            .context("failed to send ParquetSplitBatch to uploader")
            .map_err(|e| ActorExitStatus::Failure(e.into()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::time::Duration;

    use quickwit_actors::{ActorHandle, Universe};
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
    use quickwit_parquet_engine::storage::ParquetWriterConfig;
    use quickwit_parquet_engine::test_helpers::create_test_batch;
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::actors::{Publisher, UploaderType};

    fn create_test_uploader(
        universe: &Universe,
    ) -> (Mailbox<ParquetUploader>, ActorHandle<ParquetUploader>) {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_metrics_splits()
            .returning(|_| Ok(EmptyResponse {}));

        let ram_storage = Arc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox = super::super::spawn_sequencer_for_test(universe, publisher_mailbox);

        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            sequencer_mailbox,
            4,
        );
        universe.spawn_builder().spawn(uploader)
    }

    fn create_test_packager(
        universe: &Universe,
        temp_dir: &std::path::Path,
        uploader_mailbox: Mailbox<ParquetUploader>,
    ) -> (Mailbox<ParquetPackager>, ActorHandle<ParquetPackager>) {
        let writer_config = ParquetWriterConfig::default();
        let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
        let split_writer = ParquetSplitWriter::new(
            quickwit_parquet_engine::split::ParquetSplitKind::Metrics,
            writer_config,
            temp_dir,
            &table_config,
        )
        .unwrap();

        let packager = ParquetPackager::new(split_writer, uploader_mailbox);
        universe.spawn_builder().spawn(packager)
    }

    async fn wait_for_staged_splits(
        uploader_handle: &ActorHandle<ParquetUploader>,
        expected_splits: u64,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async {
                uploader_handle.process_pending_and_observe().await;
                let counters = uploader_handle.last_observation();
                counters.num_staged_splits.load(AtomicOrdering::Relaxed) >= expected_splits
            },
            Duration::from_secs(15),
            Duration::from_millis(50),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for {} staged splits", expected_splits))
    }

    #[tokio::test]
    async fn test_packager_writes_split() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        // Send a batch to the packager
        let batch = create_test_batch(10);
        let batch_for_packager = ParquetBatchForPackager {
            batches: vec![PartitionedRecordBatch {
                batch,
                partition_id: 0,
            }],
            index_uid: IndexUid::for_test("test-index", 0),
            checkpoint_delta: IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..10),
            },
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
        };

        packager_mailbox
            .send_message(batch_for_packager)
            .await
            .unwrap();

        let counters = packager_handle.process_pending_and_observe().await.state;

        assert_eq!(counters.splits_produced.load(AtomicOrdering::Relaxed), 1);
        assert!(counters.bytes_written.load(AtomicOrdering::Relaxed) > 0);

        // Verify uploader received the split
        wait_for_staged_splits(&uploader_handle, 1)
            .await
            .expect("Uploader should have staged 1 split");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_packager_forwards_empty_checkpoint() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, _uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        // Send a batch with no data (just checkpoint delta)
        let batch_for_packager = ParquetBatchForPackager {
            batches: Vec::new(),
            index_uid: IndexUid::for_test("test-index", 0),
            checkpoint_delta: IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..10),
            },
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
        };

        packager_mailbox
            .send_message(batch_for_packager)
            .await
            .unwrap();

        let counters = packager_handle.process_pending_and_observe().await.state;

        // No split should be produced
        assert_eq!(counters.splits_produced.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(counters.bytes_written.load(AtomicOrdering::Relaxed), 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_packager_writes_all_splits_in_workbench_batch() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_handle) = create_test_uploader(&universe);
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let batch_for_packager = ParquetBatchForPackager {
            batches: vec![
                PartitionedRecordBatch {
                    batch: create_test_batch(10),
                    partition_id: 1,
                },
                PartitionedRecordBatch {
                    batch: create_test_batch(20),
                    partition_id: 2,
                },
            ],
            index_uid: IndexUid::for_test("test-index", 0),
            checkpoint_delta: IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..30),
            },
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
        };

        packager_mailbox
            .send_message(batch_for_packager)
            .await
            .unwrap();

        let counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.splits_produced.load(AtomicOrdering::Relaxed), 2);
        assert!(counters.bytes_written.load(AtomicOrdering::Relaxed) > 0);

        wait_for_staged_splits(&uploader_handle, 2)
            .await
            .expect("Uploader should have staged 2 splits");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_packager_preserves_partition_ids_in_split_metadata() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        let (uploader_mailbox, uploader_inbox) = universe.create_test_mailbox::<ParquetUploader>();
        let (packager_mailbox, packager_handle) =
            create_test_packager(&universe, temp_dir.path(), uploader_mailbox);

        let batch_for_packager = ParquetBatchForPackager {
            batches: vec![
                PartitionedRecordBatch {
                    batch: create_test_batch(10),
                    partition_id: 1,
                },
                PartitionedRecordBatch {
                    batch: create_test_batch(20),
                    partition_id: 3,
                },
            ],
            index_uid: IndexUid::for_test("test-index", 0),
            checkpoint_delta: IndexCheckpointDelta {
                source_id: "test-source".to_string(),
                source_delta: SourceCheckpointDelta::from_range(0..30),
            },
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
        };

        packager_mailbox
            .send_message(batch_for_packager)
            .await
            .unwrap();

        let counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(counters.splits_produced.load(AtomicOrdering::Relaxed), 2);

        let split_batches: Vec<ParquetSplitBatch> = uploader_inbox.drain_for_test_typed();
        assert_eq!(split_batches.len(), 1);
        assert_eq!(split_batches[0].splits.len(), 2);
        assert_eq!(
            split_batches[0]
                .splits
                .iter()
                .map(|split| split.partition_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            split_batches[0]
                .checkpoint_delta_opt
                .as_ref()
                .unwrap()
                .source_delta,
            SourceCheckpointDelta::from_range(0..30)
        );

        universe.assert_quit().await;
    }
}
