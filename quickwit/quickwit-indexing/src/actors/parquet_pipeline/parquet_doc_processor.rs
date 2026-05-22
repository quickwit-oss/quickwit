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

//! ParquetDocProcessor actor for routing Arrow IPC batches to the metrics engine.
//!
//! This actor processes RawDocBatch messages containing Arrow IPC data and routes
//! them directly to the metrics engine, bypassing Tantivy document conversion.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::rate_limited_tracing::rate_limited_warn;
use quickwit_common::runtimes::RuntimeType;
use quickwit_parquet_engine::ingest::{
    IngestError, ParquetIngestProcessor, SketchParquetIngestProcessor,
};
use quickwit_proto::types::{IndexId, SourceId};
use serde::Serialize;
use tokio::runtime::Handle;
use tracing::{debug, info, instrument};

use super::{ParquetIndexer, ProcessedParquetBatch};
use crate::models::{NewPublishLock, NewPublishToken, PublishLock, RawDocBatch};

/// Arrow IPC stream continuation marker (4 bytes of 0xFF).
const ARROW_IPC_CONTINUATION_MARKER: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

/// Check if the bytes look like Arrow IPC stream format.
pub fn is_arrow_ipc(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0..4] == ARROW_IPC_CONTINUATION_MARKER
}

/// Counters for ParquetDocProcessor.
#[derive(Debug, Serialize, Clone)]
pub struct ParquetDocProcessorCounters {
    /// Index identifier.
    pub index_id: IndexId,
    /// Source identifier.
    pub source_id: SourceId,
    /// Number of valid batches processed.
    pub valid_batches: u64,
    /// Number of valid rows processed.
    pub valid_rows: u64,
    /// Number of batches that failed to parse.
    pub parse_errors: u64,
    /// Number of batches that were not Arrow IPC format.
    pub format_errors: u64,
    /// Total bytes processed.
    pub bytes_total: u64,
}

impl ParquetDocProcessorCounters {
    /// Create new counters for the given index and source.
    pub fn new(index_id: IndexId, source_id: SourceId) -> Self {
        Self {
            index_id,
            source_id,
            valid_batches: 0u64,
            valid_rows: 0u64,
            parse_errors: 0u64,
            format_errors: 0u64,
            bytes_total: 0u64,
        }
    }

    /// Record a successfully processed batch.
    pub fn record_valid(&mut self, num_rows: usize, num_bytes: usize) {
        self.valid_batches += 1;
        self.valid_rows += num_rows as u64;
        self.bytes_total += num_bytes as u64;
    }

    /// Record a parse error.
    pub fn record_parse_error(&mut self, num_bytes: usize) {
        self.parse_errors += 1;
        self.bytes_total += num_bytes as u64;
    }

    /// Record a format error (not Arrow IPC).
    pub fn record_format_error(&mut self, num_bytes: usize) {
        self.format_errors += 1;
        self.bytes_total += num_bytes as u64;
    }

    /// Get total number of batches processed (valid or not).
    pub fn num_processed_batches(&self) -> u64 {
        self.valid_batches + self.parse_errors + self.format_errors
    }

    /// Get total number of errors.
    pub fn num_errors(&self) -> u64 {
        self.parse_errors + self.format_errors
    }
}

/// Error type for ParquetDocProcessor.
#[derive(Debug, thiserror::Error)]
pub enum ParquetDocProcessorError {
    /// Input was not Arrow IPC format.
    #[error("Invalid format: expected Arrow IPC")]
    InvalidFormat,
    /// Arrow IPC parsing or schema validation failed.
    #[error("Ingest error: {0}")]
    Ingest(#[from] IngestError),
}

/// Enum wrapping the ingest processor variant for metrics vs sketches.
pub enum IngestProcessor {
    Metrics(ParquetIngestProcessor),
    Sketches(SketchParquetIngestProcessor),
}

impl IngestProcessor {
    fn process_ipc(&self, ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
        match self {
            Self::Metrics(p) => p.process_ipc(ipc_bytes),
            Self::Sketches(p) => p.process_ipc(ipc_bytes),
        }
    }
}

/// ParquetDocProcessor actor that routes Arrow IPC batches to the parquet engine.
///
/// This actor receives RawDocBatch messages containing Arrow IPC data and converts
/// them to RecordBatch using the configured ingest processor. The resulting batches
/// are forwarded to ParquetIndexer for accumulation and split production.
///
/// Unlike DocProcessor which converts to Tantivy documents, this actor works
/// exclusively with Arrow RecordBatch for high-throughput metrics/sketch ingestion.
pub struct ParquetDocProcessor {
    /// Processor for converting Arrow IPC to RecordBatch.
    processor: IngestProcessor,
    /// Processing counters.
    counters: ParquetDocProcessorCounters,
    /// Publish lock for coordinating with sources.
    publish_lock: PublishLock,
    /// Mailbox for forwarding batches to ParquetIndexer.
    indexer_mailbox: Mailbox<ParquetIndexer>,
}

impl ParquetDocProcessor {
    /// Creates a new ParquetDocProcessor.
    pub fn new(
        processor: IngestProcessor,
        index_id: IndexId,
        source_id: SourceId,
        indexer_mailbox: Mailbox<ParquetIndexer>,
    ) -> Self {
        let counters = ParquetDocProcessorCounters::new(index_id.clone(), source_id.clone());

        info!(
            index_id = %index_id,
            source_id = %source_id,
            "ParquetDocProcessor created"
        );

        Self {
            processor,
            counters,
            publish_lock: PublishLock::default(),
            indexer_mailbox,
        }
    }
}

#[async_trait]
impl Actor for ParquetDocProcessor {
    type ObservableState = ParquetDocProcessorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
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
                let _ = ctx.send_exit_with_success(&self.indexer_mailbox).await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RawDocBatch> for ParquetDocProcessor {
    type Reply = ();

    #[instrument(
        skip(self, raw_doc_batch, ctx),
        fields(
            index_id = %self.counters.index_id,
            source_id = %self.counters.source_id,
            batch_len = raw_doc_batch.docs.len(),
        )
    )]
    async fn handle(
        &mut self,
        raw_doc_batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.publish_lock.is_dead() {
            debug!("publish lock is dead, skipping batch");
            return Ok(());
        }

        let force_commit = raw_doc_batch.force_commit;
        let checkpoint_delta = raw_doc_batch.checkpoint_delta;
        let num_docs = raw_doc_batch.docs.len();

        debug!(
            num_docs = num_docs,
            force_commit = force_commit,
            "processing raw doc batch"
        );

        // Collect every processed Arrow batch from this source batch into a single
        // indexer message. This mirrors the logs pipeline: the checkpoint covers the
        // whole RawDocBatch, so the indexer must not be able to flush a prefix of it
        // before seeing the checkpoint.
        let mut processed_batches = Vec::new();

        // Process each raw document in the batch (expecting Arrow IPC format from OTLP gRPC)
        for raw_doc in &raw_doc_batch.docs {
            let _protected_zone_guard = ctx.protect_zone();
            let num_bytes = raw_doc.len();

            // Verify Arrow IPC format
            if !is_arrow_ipc(raw_doc) {
                rate_limited_warn!(
                    limit_per_min = 10,
                    index_id = %self.counters.index_id,
                    source_id = %self.counters.source_id,
                    "metrics pipeline only accepts Arrow IPC format (from OTLP gRPC)"
                );
                self.counters.record_format_error(num_bytes);
                continue;
            }

            // Convert Arrow IPC to RecordBatch
            match self.processor.process_ipc(raw_doc) {
                Ok(batch) => {
                    let num_rows = batch.num_rows();
                    self.counters.record_valid(num_rows, num_bytes);
                    processed_batches.push(batch);
                }
                Err(error) => {
                    rate_limited_warn!(
                        limit_per_min = 10,
                        index_id = %self.counters.index_id,
                        source_id = %self.counters.source_id,
                        "Arrow IPC processing failed: {error}"
                    );
                    self.counters.record_parse_error(num_bytes);
                }
            }

            ctx.record_progress();
        }

        // Even when all docs fail, forward the checkpoint so the pipeline can
        // advance past this batch. A force commit with no new rows must also reach
        // the indexer so buffered work is flushed.
        if !processed_batches.is_empty() || !checkpoint_delta.is_empty() || force_commit {
            let processed_batch = ProcessedParquetBatch::new_batches(
                processed_batches,
                checkpoint_delta,
                force_commit,
            );
            ctx.send_message(&self.indexer_mailbox, processed_batch)
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishLock> for ParquetDocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = &message;
        self.publish_lock = publish_lock.clone();

        ctx.send_message(&self.indexer_mailbox, message).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishToken> for ParquetDocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishToken,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.send_message(&self.indexer_mailbox, message).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_parquet_engine::ingest::record_batch_to_ipc;
    use quickwit_proto::types::IndexUid;

    use super::*;

    #[test]
    fn test_is_arrow_ipc() {
        // Valid Arrow IPC marker
        let valid = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];
        assert!(is_arrow_ipc(&valid));

        // Invalid - too short
        let short = [0xFF, 0xFF, 0xFF];
        assert!(!is_arrow_ipc(&short));

        // Invalid - wrong marker
        let wrong = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(!is_arrow_ipc(&wrong));

        // Invalid - JSON
        let json = b"{\"metric\":\"test\"}";
        assert!(!is_arrow_ipc(json));
    }

    #[test]
    fn test_counters() {
        let mut counters =
            ParquetDocProcessorCounters::new("test-index".to_string(), "test-source".to_string());

        counters.record_valid(100, 1024);
        counters.record_valid(50, 512);
        counters.record_parse_error(256);
        counters.record_format_error(128);

        assert_eq!(counters.valid_batches, 2);
        assert_eq!(counters.valid_rows, 150);
        assert_eq!(counters.parse_errors, 1);
        assert_eq!(counters.format_errors, 1);
        assert_eq!(counters.bytes_total, 1920);
        assert_eq!(counters.num_processed_batches(), 4);
        assert_eq!(counters.num_errors(), 2);
    }

    #[tokio::test]
    async fn test_metrics_doc_processor_valid_arrow_ipc() {
        use quickwit_parquet_engine::test_helpers::create_test_batch_with_tags;

        let universe = Universe::with_accelerated_time();

        let (indexer_mailbox, _indexer_inbox) = universe.create_test_mailbox::<ParquetIndexer>();
        let metrics_doc_processor = ParquetDocProcessor::new(
            IngestProcessor::Metrics(ParquetIngestProcessor),
            "test-metrics-index".to_string(),
            "test-source".to_string(),
            indexer_mailbox,
        );

        let (metrics_doc_processor_mailbox, metrics_doc_processor_handle) =
            universe.spawn_builder().spawn(metrics_doc_processor);

        let batch = create_test_batch_with_tags(3, &["service"]);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();

        // Create RawDocBatch with the IPC bytes
        let raw_doc_batch = RawDocBatch::for_test(&[&ipc_bytes], 0..1);

        // Send to processor
        metrics_doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        // Process and observe
        let counters = metrics_doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;

        // Verify counters
        assert_eq!(counters.valid_batches, 1);
        assert_eq!(counters.valid_rows, 3);
        assert_eq!(counters.parse_errors, 0);
        assert_eq!(counters.format_errors, 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_doc_processor_sends_one_message_per_raw_doc_batch() {
        use quickwit_parquet_engine::test_helpers::create_test_batch_with_tags;

        let universe = Universe::with_accelerated_time();

        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox::<ParquetIndexer>();
        let metrics_doc_processor = ParquetDocProcessor::new(
            IngestProcessor::Metrics(ParquetIngestProcessor),
            "test-metrics-index".to_string(),
            "test-source".to_string(),
            indexer_mailbox,
        );

        let (metrics_doc_processor_mailbox, metrics_doc_processor_handle) =
            universe.spawn_builder().spawn(metrics_doc_processor);

        let first_batch = create_test_batch_with_tags(3, &["service"]);
        let second_batch = create_test_batch_with_tags(5, &["service"]);
        let first_ipc_bytes = record_batch_to_ipc(&first_batch).unwrap();
        let second_ipc_bytes = record_batch_to_ipc(&second_batch).unwrap();

        let mut raw_doc_batch = RawDocBatch::for_test(&[&first_ipc_bytes, &second_ipc_bytes], 0..2);
        raw_doc_batch.force_commit = true;

        metrics_doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        let counters = metrics_doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;

        assert_eq!(counters.valid_batches, 2);
        assert_eq!(counters.valid_rows, 8);

        let processed_batches: Vec<ProcessedParquetBatch> = indexer_inbox.drain_for_test_typed();
        assert_eq!(processed_batches.len(), 1);

        let processed_batch = &processed_batches[0];
        assert_eq!(processed_batch.num_rows(), 8);
        assert_eq!(processed_batch.batches.len(), 2);
        assert_eq!(
            processed_batch.checkpoint_delta,
            SourceCheckpointDelta::from_range(0..2)
        );
        assert!(processed_batch.force_commit);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_doc_processor_invalid_format() {
        let universe = Universe::with_accelerated_time();

        let (indexer_mailbox, _indexer_inbox) = universe.create_test_mailbox::<ParquetIndexer>();
        let metrics_doc_processor = ParquetDocProcessor::new(
            IngestProcessor::Metrics(ParquetIngestProcessor),
            "test-metrics-index".to_string(),
            "test-source".to_string(),
            indexer_mailbox,
        );

        let (metrics_doc_processor_mailbox, metrics_doc_processor_handle) =
            universe.spawn_builder().spawn(metrics_doc_processor);

        // Send JSON data (not Arrow IPC) - should result in format error
        let json_data = br#"{"metric":"test","value":42}"#;
        let raw_doc_batch = RawDocBatch::for_test(&[json_data], 0..1);

        metrics_doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        let counters = metrics_doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;

        // JSON is rejected as format error (only Arrow IPC is accepted)
        assert_eq!(counters.valid_batches, 0);
        assert_eq!(counters.format_errors, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_doc_processor_respects_publish_lock() {
        let universe = Universe::with_accelerated_time();

        let (indexer_mailbox, _indexer_inbox) = universe.create_test_mailbox::<ParquetIndexer>();
        let metrics_doc_processor = ParquetDocProcessor::new(
            IngestProcessor::Metrics(ParquetIngestProcessor),
            "test-metrics-index".to_string(),
            "test-source".to_string(),
            indexer_mailbox,
        );

        let (metrics_doc_processor_mailbox, metrics_doc_processor_handle) =
            universe.spawn_builder().spawn(metrics_doc_processor);

        // Set up and kill publish lock
        let publish_lock = PublishLock::default();
        metrics_doc_processor_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        metrics_doc_processor_handle
            .process_pending_and_observe()
            .await;
        publish_lock.kill().await;

        // Send data after lock is dead
        let raw_doc_batch = RawDocBatch::for_test(&[b"some data"], 0..1);
        metrics_doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        let counters = metrics_doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;

        // Should not process anything when publish lock is dead
        assert_eq!(counters.num_processed_batches(), 0);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_metrics_doc_processor_with_indexer() {
        use std::sync::Arc as StdArc;

        use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
        use quickwit_parquet_engine::test_helpers::create_test_batch_with_tags;
        use quickwit_proto::metastore::MockMetastoreService;
        use quickwit_storage::RamStorage;

        use crate::actors::parquet_pipeline::{ParquetIndexer, ParquetPackager, ParquetUploader};
        use crate::actors::{Publisher, UploaderType};

        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create ParquetUploader
        let mock_metastore = MockMetastoreService::new();
        let ram_storage = StdArc::new(RamStorage::default());
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let sequencer_mailbox =
            super::super::spawn_sequencer_for_test(&universe, publisher_mailbox);
        let uploader = ParquetUploader::new(
            UploaderType::IndexUploader,
            quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore),
            ram_storage,
            sequencer_mailbox,
            4,
        );
        let (uploader_mailbox, _uploader_handle) = universe.spawn_builder().spawn(uploader);

        // Create ParquetPackager
        let writer_config = ParquetWriterConfig::default();
        let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
        let split_writer = ParquetSplitWriter::new(
            quickwit_parquet_engine::split::ParquetSplitKind::Metrics,
            writer_config,
            temp_dir.path(),
            &table_config,
        )
        .unwrap();
        let packager = ParquetPackager::new(split_writer, uploader_mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_builder().spawn(packager);

        // Create ParquetIndexer
        let indexer = ParquetIndexer::new(
            IndexUid::for_test("test-index", 0),
            "test-source".to_string(),
            None,
            packager_mailbox,
            None,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        let metrics_doc_processor = ParquetDocProcessor::new(
            IngestProcessor::Metrics(ParquetIngestProcessor),
            "test-index".to_string(),
            "test-source".to_string(),
            indexer_mailbox,
        );
        let (metrics_doc_processor_mailbox, metrics_doc_processor_handle) =
            universe.spawn_builder().spawn(metrics_doc_processor);

        let batch = create_test_batch_with_tags(5, &["service"]);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();

        // Create RawDocBatch with force_commit to trigger split production
        let mut raw_doc_batch = RawDocBatch::for_test(&[&ipc_bytes], 0..1);
        raw_doc_batch.force_commit = true;

        // Send to processor
        metrics_doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        // Process in doc processor
        let doc_counters = metrics_doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;

        // Verify doc processor counters
        assert_eq!(doc_counters.valid_batches, 1);
        assert_eq!(doc_counters.valid_rows, 5);

        // Process in indexer
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;

        // Verify indexer received and processed the batch
        assert_eq!(indexer_counters.batches_received, 1);
        assert_eq!(indexer_counters.rows_indexed, 5);
        // Should have flushed a batch due to force_commit
        assert_eq!(indexer_counters.batches_flushed, 1);

        // Verify packager produced a split
        let packager_counters = packager_handle.process_pending_and_observe().await.state;
        assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);

        universe.assert_quit().await;
    }
}
