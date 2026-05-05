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

//! ProcessedParquetBatch message type for the parquet indexing pipeline.
//!
//! This message carries Arrow RecordBatch data from ParquetDocProcessor to ParquetIndexer,
//! bypassing the Tantivy document path used by the standard indexing pipeline.

use std::fmt;

use arrow::record_batch::RecordBatch;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

/// Batch of parquet data as Arrow RecordBatch for the parquet indexing pipeline.
///
/// This message type is sent from ParquetDocProcessor to ParquetIndexer, carrying
/// pre-processed Arrow data that can be directly accumulated and written to Parquet.
pub struct ProcessedParquetBatch {
    /// The Arrow RecordBatches in this source batch.
    pub batches: Vec<RecordBatch>,
    /// Checkpoint delta for this batch.
    pub checkpoint_delta: SourceCheckpointDelta,
    /// Force commit flag - when true, accumulator should flush immediately.
    pub force_commit: bool,
    /// Memory tracking gauge guard.
    _gauge_guard: GaugeGuard<'static>,
}

impl ProcessedParquetBatch {
    /// Create a new ProcessedParquetBatch.
    ///
    /// # Arguments
    /// * `batch` - The Arrow RecordBatch containing parquet pipeline data
    /// * `checkpoint_delta` - Checkpoint progress for this batch
    /// * `force_commit` - Whether to force an immediate commit/flush
    pub fn new(
        batch: RecordBatch,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        Self::new_batches(vec![batch], checkpoint_delta, force_commit)
    }

    /// Create a new ProcessedParquetBatch from multiple processed Arrow batches.
    pub fn new_batches(
        batches: Vec<RecordBatch>,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        let memory_size: i64 = batches
            .iter()
            .flat_map(|batch| batch.columns())
            .map(|col| col.get_array_memory_size() as i64)
            .sum();

        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.indexer_mailbox);
        gauge_guard.add(memory_size);

        Self {
            batches,
            checkpoint_delta,
            force_commit,
            _gauge_guard: gauge_guard,
        }
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Returns the estimated memory size of the batch in bytes.
    pub fn memory_size(&self) -> usize {
        self.batches
            .iter()
            .flat_map(|batch| batch.columns())
            .map(|col| col.get_array_memory_size())
            .sum()
    }
}

impl fmt::Debug for ProcessedParquetBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessedParquetBatch")
            .field("num_rows", &self.num_rows())
            .field("num_batches", &self.batches.len())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_parquet_engine::test_helpers::create_test_batch;

    use super::*;

    #[test]
    fn test_processed_parquet_batch_new() {
        let batch = create_test_batch(10);
        let checkpoint_delta = SourceCheckpointDelta::from_range(0..10);

        let processed = ProcessedParquetBatch::new(batch, checkpoint_delta, false);

        assert_eq!(processed.num_rows(), 10);
        assert!(!processed.force_commit);
        assert!(processed.memory_size() > 0);
    }

    #[test]
    fn test_processed_parquet_batch_force_commit() {
        let batch = create_test_batch(5);
        let checkpoint_delta = SourceCheckpointDelta::from_range(0..5);

        let processed = ProcessedParquetBatch::new(batch, checkpoint_delta, true);

        assert!(processed.force_commit);
        assert_eq!(processed.num_rows(), 5);
    }

    #[test]
    fn test_processed_parquet_batch_debug() {
        let batch = create_test_batch(3);
        let checkpoint_delta = SourceCheckpointDelta::from_range(0..3);

        let processed = ProcessedParquetBatch::new(batch, checkpoint_delta, false);

        let debug_str = format!("{:?}", processed);
        assert!(debug_str.contains("ProcessedParquetBatch"));
        assert!(debug_str.contains("num_rows: 3"));
    }
}
