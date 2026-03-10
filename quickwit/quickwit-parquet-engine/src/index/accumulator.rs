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

//! Batch accumulator for producing splits from RecordBatches.

use std::time::Instant;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use tracing::{debug, info, warn};

use quickwit_proto::types::IndexUid;

use super::config::ParquetIndexingConfig;
use crate::metrics::PARQUET_ENGINE_METRICS;
use crate::schema::ParquetSchema;
use crate::split::ParquetSplit;
use crate::storage::ParquetSplitWriter;

/// Error type for index operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexingError {
    /// Arrow error during RecordBatch concatenation.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Storage error during split writing.
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::ParquetWriteError),
}


/// Accumulator that buffers RecordBatches and produces concatenated batches when thresholds
/// are exceeded.
///
/// Batches are accumulated until either `max_rows` or `max_bytes` threshold is reached,
/// at which point they are concatenated and returned for downstream processing (writing to
/// Parquet by the ParquetPackager actor).
pub struct ParquetBatchAccumulator {
    /// Index UID for split metadata.
    index_uid: IndexUid,
    /// Configuration for accumulation thresholds.
    config: ParquetIndexingConfig,
    /// Writer for producing splits.
    split_writer: ParquetSplitWriter,
    /// Metrics schema for concatenation.
    schema: ParquetSchema,
    /// Pending batches waiting to be flushed.
    pending_batches: Vec<RecordBatch>,
    /// Total rows in pending batches.
    pending_rows: usize,
    /// Estimated bytes in pending batches.
    pending_bytes: usize,
}

impl ParquetBatchAccumulator {
    /// Creates a new ParquetBatchAccumulator.
    ///
    /// # Arguments
    /// * `index_uid` - Index UID for split metadata
    /// * `config` - Configuration for accumulation thresholds
    /// * `base_path` - Directory where split files will be written
    pub fn new(
        index_uid: IndexUid,
        config: ParquetIndexingConfig,
        base_path: impl Into<PathBuf>,
    ) -> Self {
        let schema = ParquetSchema::new();

        Self {
            index_uid,
            config,
            schema,
            pending_batches: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
        }
    }

    /// Adds a RecordBatch to the accumulator.
    ///
    /// If thresholds are exceeded after adding the batch, flushes to create split(s).
    /// Returns list of splits produced (empty if none flushed).
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<Vec<ParquetSplit>, IndexingError> {
        let start = Instant::now();
        let batch_rows = batch.num_rows();
        let batch_bytes = estimate_batch_bytes(&batch);

        // Record index metrics
        PARQUET_ENGINE_METRICS.index_batches_total.inc();
        PARQUET_ENGINE_METRICS
            .index_rows_total
            .inc_by(batch_rows as u64);

        self.pending_batches.push(batch);
        self.pending_rows += batch_rows;
        self.pending_bytes += batch_bytes;

        debug!(
            batch_rows,
            batch_bytes,
            total_pending_rows = self.pending_rows,
            total_pending_bytes = self.pending_bytes,
            "Added batch to accumulator"
        );

        let flushed = if self.should_flush() {
            info!(
                pending_rows = self.pending_rows,
                pending_bytes = self.pending_bytes,
                max_rows = self.config.max_rows,
                max_bytes = self.config.max_bytes,
                "Threshold exceeded, triggering flush"
            );
            self.flush_internal()?
        } else {
            None
        };

        // Record batch processing duration
        PARQUET_ENGINE_METRICS
            .index_batch_duration_seconds
            .observe(start.elapsed().as_secs_f64());

        Ok(flushed)
    }

    /// Discard all pending data without producing output.
    pub fn discard(&mut self) {
        self.pending_batches.clear();
        self.pending_rows = 0;
        self.pending_bytes = 0;
    }

    /// Force flush all pending batches.
    ///
    /// Returns None if no pending data.
    pub fn flush(&mut self) -> Result<Option<ParquetSplit>, IndexingError> {
        self.flush_internal()
    }

    /// Internal flush implementation.
    fn flush_internal(&mut self) -> Result<Option<ParquetSplit>, IndexingError> {
        if self.pending_batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all pending batches into one
        let combined = concat_batches(self.schema.arrow_schema(), self.pending_batches.iter())?;

        let num_rows = combined.num_rows();
        info!(num_rows, "Flushed accumulated batches");

        // Reset state
        self.pending_batches.clear();
        self.pending_rows = 0;
        self.pending_bytes = 0;

        Ok(Some(combined))
    }

    /// Checks if pending data exceeds thresholds.
    fn should_flush(&self) -> bool {
        !self.pending_batches.is_empty()
            && (self.pending_rows >= self.config.max_rows
                || self.pending_bytes >= self.config.max_bytes)
    }

    /// Returns current pending row count.
    pub fn pending_rows(&self) -> usize {
        self.pending_rows
    }

    /// Returns current pending byte estimate.
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Returns the number of pending batches.
    pub fn pending_batch_count(&self) -> usize {
        self.pending_batches.len()
    }
}

/// Estimate the memory size of a RecordBatch.
fn estimate_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::Int32Type;
    use parquet::variant::{VariantArrayBuilder, VariantBuilderExt};

    use super::*;

    /// Create dictionary array for string fields with Int32 keys.
    fn create_dict_array(values: &[&str]) -> ArrayRef {
        let keys: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
        let string_array = StringArray::from(values.to_vec());
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
                .unwrap(),
        )
    }

    /// Create nullable dictionary array for optional string fields.
    fn create_nullable_dict_array(values: &[Option<&str>]) -> ArrayRef {
        let keys: Vec<Option<i32>> = values
            .iter()
            .enumerate()
            .map(|(i, v)| v.map(|_| i as i32))
            .collect();
        let string_values: Vec<&str> = values.iter().filter_map(|v| *v).collect();
        let string_array = StringArray::from(string_values);
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
                .unwrap(),
        )
    }

    /// Create a VARIANT array for testing with specified number of rows.
    fn create_variant_array(num_rows: usize, fields: Option<&[(&str, &str)]>) -> ArrayRef {
        let mut builder = VariantArrayBuilder::new(num_rows);
        for _ in 0..num_rows {
            match fields {
                Some(kv_pairs) => {
                    let mut obj = builder.new_object();
                    for (key, value) in kv_pairs {
                        obj = obj.with_field(key, *value);
                    }
                    obj.finish();
                }
                None => {
                    builder.append_null();
                }
            }
        }
        ArrayRef::from(builder.build())
    }

    /// Create a test batch matching the metrics schema.
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = ParquetSchema::new();

        let metric_names: Vec<&str> = vec!["cpu.usage"; num_rows];
        let metric_name: ArrayRef = create_dict_array(&metric_names);
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("bytes"); num_rows]));
        let timestamps: Vec<u64> = (0..num_rows).map(|i| 100 + i as u64).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let start_timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from(vec![None::<u64>; num_rows]));
        let values: Vec<f64> = (0..num_rows).map(|i| 42.0 + i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tag_service: ArrayRef = create_nullable_dict_array(&vec![Some("web"); num_rows]);
        let tag_env: ArrayRef = create_nullable_dict_array(&vec![Some("prod"); num_rows]);
        let tag_datacenter: ArrayRef =
            create_nullable_dict_array(&vec![Some("us-east-1"); num_rows]);
        let tag_region: ArrayRef = create_nullable_dict_array(&vec![None; num_rows]);
        let tag_host: ArrayRef = create_nullable_dict_array(&vec![Some("host-001"); num_rows]);
        let attributes: ArrayRef = create_variant_array(num_rows, None);
        let service_name: ArrayRef = create_dict_array(&vec!["my-service"; num_rows]);
        let resource_attributes: ArrayRef = create_variant_array(num_rows, None);

        RecordBatch::try_new(
            schema.arrow_schema().clone(),
            vec![
                metric_name,
                metric_type,
                metric_unit,
                timestamp_secs,
                start_timestamp_secs,
                value,
                tag_service,
                tag_env,
                tag_datacenter,
                tag_region,
                tag_host,
                attributes,
                service_name,
                resource_attributes,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_accumulator_below_threshold() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ParquetIndexingConfig::default().with_max_rows(1000);

        let mut accumulator = ParquetBatchAccumulator::new(IndexUid::for_test("test-index", 0), config, temp_dir.path());

        // Add batch below threshold
        let batch = create_test_batch(100);
        let flushed = accumulator.add_batch(batch).unwrap();

        // Should not flush
        assert!(flushed.is_none());
        assert_eq!(accumulator.pending_rows(), 100);
        assert_eq!(accumulator.pending_batch_count(), 1);
    }

    #[test]
    fn test_accumulator_row_threshold() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ParquetIndexingConfig::default().with_max_rows(150);

        let mut accumulator = ParquetBatchAccumulator::new(IndexUid::for_test("test-index", 0), config, temp_dir.path());

        // Add first batch (100 rows) - no flush
        let batch1 = create_test_batch(100);
        let flushed1 = accumulator.add_batch(batch1).unwrap();
        assert!(flushed1.is_none());
        assert_eq!(accumulator.pending_rows(), 100);

        // Add second batch (100 rows) - should flush (200 > 150)
        let batch2 = create_test_batch(100);
        let flushed2 = accumulator.add_batch(batch2).unwrap();
        let combined = flushed2.expect("should have flushed");
        assert_eq!(combined.num_rows(), 200);
        assert_eq!(accumulator.pending_rows(), 0);
    }

    #[test]
    fn test_accumulator_flush_produces_split() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ParquetIndexingConfig::default().with_max_rows(1000);

        let mut accumulator = ParquetBatchAccumulator::new(IndexUid::for_test("test-index", 0), config, temp_dir.path());

        // Add batch below threshold
        let batch = create_test_batch(50);
        let _ = accumulator.add_batch(batch).unwrap();
        assert_eq!(accumulator.pending_rows(), 50);

        // Force flush
        let flushed = accumulator.flush().unwrap();
        assert!(flushed.is_some());
        let combined = flushed.unwrap();
        assert_eq!(combined.num_rows(), 50);
        assert_eq!(accumulator.pending_rows(), 0);

        // Second flush should return None
        let flushed2 = accumulator.flush().unwrap();
        assert!(flushed2.is_none());
    }

    #[test]
    fn test_accumulator_multiple_batches_concatenated() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = ParquetIndexingConfig::default().with_max_rows(1000);

        let mut accumulator = ParquetBatchAccumulator::new(IndexUid::for_test("test-index", 0), config, temp_dir.path());

        // Add multiple batches
        for _ in 0..5 {
            let batch = create_test_batch(10);
            let _ = accumulator.add_batch(batch).unwrap();
        }

        assert_eq!(accumulator.pending_rows(), 50);
        assert_eq!(accumulator.pending_batch_count(), 5);

        // Flush and verify combined
        let combined = accumulator.flush().unwrap().unwrap();
        assert_eq!(combined.num_rows(), 50);
    }

    #[test]
    fn test_estimate_batch_bytes() {
        let batch = create_test_batch(100);
        let bytes = estimate_batch_bytes(&batch);

        // Should have some non-zero byte estimate
        assert!(bytes > 0);
    }
}
