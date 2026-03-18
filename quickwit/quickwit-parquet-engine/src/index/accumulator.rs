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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{new_null_array, ArrayRef};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::{debug, info};

use super::config::ParquetIndexingConfig;
use crate::metrics::PARQUET_ENGINE_METRICS;

/// Error type for index operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexingError {
    /// Arrow error during RecordBatch concatenation.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Accumulator that buffers RecordBatches and produces concatenated batches when thresholds
/// are exceeded.
///
/// Batches are accumulated until either `max_rows` or `max_bytes` threshold is reached,
/// at which point they are concatenated and returned for downstream processing (writing to
/// Parquet by the ParquetPackager actor).
///
/// Consecutive batches may have different column sets. The accumulator tracks the union
/// schema incrementally and aligns all batches to it on flush.
pub struct ParquetBatchAccumulator {
    /// Configuration for accumulation thresholds.
    config: ParquetIndexingConfig,
    /// Union of all fields seen across pending batches.
    union_fields: BTreeMap<String, (DataType, bool)>,
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
    /// * `config` - Configuration for accumulation thresholds
    pub fn new(config: ParquetIndexingConfig) -> Self {
        Self {
            config,
            union_fields: BTreeMap::new(),
            pending_batches: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
        }
    }

    /// Adds a RecordBatch to the accumulator.
    ///
    /// If thresholds are exceeded after adding the batch, concatenates all pending batches
    /// and returns the combined batch. Returns None if the threshold has not been reached.
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>, IndexingError> {
        let start = Instant::now();
        let batch_rows = batch.num_rows();
        let batch_bytes = estimate_batch_bytes(&batch);

        // Record index metrics
        PARQUET_ENGINE_METRICS.index_batches_total.inc();
        PARQUET_ENGINE_METRICS
            .index_rows_total
            .inc_by(batch_rows as u64);

        // Merge fields into union schema before pushing (we need the schema reference)
        for field in batch.schema().fields() {
            self.union_fields
                .entry(field.name().clone())
                .or_insert_with(|| (field.data_type().clone(), field.is_nullable()));
        }

        self.pending_batches.push(batch);
        self.pending_rows += batch_rows;
        self.pending_bytes += batch_bytes;

        debug!(
            batch_rows,
            batch_bytes,
            total_pending_rows = self.pending_rows,
            total_pending_bytes = self.pending_bytes,
            "added batch to accumulator"
        );

        let flushed = if self.should_flush() {
            info!(
                pending_rows = self.pending_rows,
                pending_bytes = self.pending_bytes,
                max_rows = self.config.max_rows,
                max_bytes = self.config.max_bytes,
                "threshold exceeded, triggering flush"
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
        self.union_fields.clear();
        self.pending_rows = 0;
        self.pending_bytes = 0;
    }

    /// Force flush all pending batches.
    ///
    /// Returns None if no pending data.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, IndexingError> {
        self.flush_internal()
    }

    /// Internal flush implementation.
    fn flush_internal(&mut self) -> Result<Option<RecordBatch>, IndexingError> {
        if self.pending_batches.is_empty() {
            return Ok(None);
        }

        // Build the union schema from accumulated fields.
        // All fields are marked nullable=true regardless of their source schema:
        // any field that appears in some batches but not others will be null-filled
        // for the missing batches, so non-nullable would cause Arrow to reject the concat.
        let fields: Vec<Field> = self
            .union_fields
            .iter()
            .map(|(name, (data_type, _nullable))| Field::new(name, data_type.clone(), true))
            .collect();
        let union_schema: SchemaRef = Arc::new(ArrowSchema::new(fields));

        // Align each pending batch to the union schema
        let aligned: Vec<RecordBatch> = self
            .pending_batches
            .iter()
            .map(|batch| align_batch_to_schema(batch, &union_schema))
            .collect::<Result<Vec<_>, _>>()?;

        // Concatenate all aligned batches
        let combined = concat_batches(&union_schema, aligned.iter())?;

        let num_rows = combined.num_rows();
        info!(num_rows, "flushed accumulated batches");

        // Reset state
        self.pending_batches.clear();
        self.union_fields.clear();
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

/// Align a RecordBatch to a target schema, inserting null columns where needed.
fn align_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let num_rows = batch.num_rows();
    let batch_schema = batch.schema();
    let columns: Vec<ArrayRef> = target_schema
        .fields()
        .iter()
        .map(|field| match batch_schema.index_of(field.name()) {
            Ok(idx) => Arc::clone(batch.column(idx)),
            Err(_) => new_null_array(field.data_type(), num_rows),
        })
        .collect();
    RecordBatch::try_new(target_schema.clone(), columns)
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
    use crate::test_helpers::{create_test_batch, create_test_batch_with_tags};

    use super::*;

    #[test]
    fn test_accumulator_below_threshold() {
        let config = ParquetIndexingConfig::default().with_max_rows(1000);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        let batch = create_test_batch(100);
        let flushed = accumulator.add_batch(batch).unwrap();

        assert!(flushed.is_none());
        assert_eq!(accumulator.pending_rows(), 100);
        assert_eq!(accumulator.pending_batch_count(), 1);
    }

    #[test]
    fn test_accumulator_row_threshold() {
        let config = ParquetIndexingConfig::default().with_max_rows(150);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        let batch1 = create_test_batch(100);
        let flushed1 = accumulator.add_batch(batch1).unwrap();
        assert!(flushed1.is_none());
        assert_eq!(accumulator.pending_rows(), 100);

        let batch2 = create_test_batch(100);
        let flushed2 = accumulator.add_batch(batch2).unwrap();
        let combined = flushed2.expect("should have flushed");
        assert_eq!(combined.num_rows(), 200);
        assert_eq!(accumulator.pending_rows(), 0);
    }

    #[test]
    fn test_accumulator_flush_produces_batch() {
        let config = ParquetIndexingConfig::default().with_max_rows(1000);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        let batch = create_test_batch(50);
        let _ = accumulator.add_batch(batch).unwrap();
        assert_eq!(accumulator.pending_rows(), 50);

        let flushed = accumulator.flush().unwrap();
        assert!(flushed.is_some());
        let combined = flushed.unwrap();
        assert_eq!(combined.num_rows(), 50);
        assert_eq!(accumulator.pending_rows(), 0);

        let flushed2 = accumulator.flush().unwrap();
        assert!(flushed2.is_none());
    }

    #[test]
    fn test_accumulator_multiple_batches_concatenated() {
        let config = ParquetIndexingConfig::default().with_max_rows(1000);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        for _ in 0..5 {
            let batch = create_test_batch(10);
            let _ = accumulator.add_batch(batch).unwrap();
        }

        assert_eq!(accumulator.pending_rows(), 50);
        assert_eq!(accumulator.pending_batch_count(), 5);

        let combined = accumulator.flush().unwrap().unwrap();
        assert_eq!(combined.num_rows(), 50);
    }

    #[test]
    fn test_accumulator_merges_different_tag_sets() {
        let config = ParquetIndexingConfig::default().with_max_rows(1000);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        // First batch has "service" tag
        let batch1 = create_test_batch_with_tags(3, &["service"]);
        let _ = accumulator.add_batch(batch1).unwrap();

        // Second batch has "host" tag
        let batch2 = create_test_batch_with_tags(2, &["host"]);
        let _ = accumulator.add_batch(batch2).unwrap();

        let combined = accumulator.flush().unwrap().unwrap();
        assert_eq!(combined.num_rows(), 5);

        // Union schema should have all 4 required fields + both tags
        let schema = combined.schema();
        assert!(schema.index_of("metric_name").is_ok());
        assert!(schema.index_of("metric_type").is_ok());
        assert!(schema.index_of("timestamp_secs").is_ok());
        assert!(schema.index_of("value").is_ok());
        assert!(schema.index_of("service").is_ok());
        assert!(schema.index_of("host").is_ok());
        assert_eq!(schema.fields().len(), 6);

        // First 3 rows should have null "host", last 2 rows should have null "service"
        let host_idx = schema.index_of("host").unwrap();
        let host_col = combined.column(host_idx);
        assert_eq!(host_col.null_count(), 3); // first batch had no host

        let service_idx = schema.index_of("service").unwrap();
        let service_col = combined.column(service_idx);
        assert_eq!(service_col.null_count(), 2); // second batch had no service

        // No duplicate column names — each name appears exactly once.
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let unique_count = field_names.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(
            unique_count,
            field_names.len(),
            "duplicate columns in union schema: {field_names:?}"
        );
    }

    #[test]
    fn test_accumulator_no_duplicates_with_overlapping_tags() {
        let config = ParquetIndexingConfig::default().with_max_rows(1000);
        let mut accumulator = ParquetBatchAccumulator::new(config);

        // Both batches share "service"; second also has "host".
        // "service" must appear exactly once in the flushed schema.
        let batch1 = create_test_batch_with_tags(3, &["service"]);
        let batch2 = create_test_batch_with_tags(2, &["service", "host"]);
        let _ = accumulator.add_batch(batch1).unwrap();
        let _ = accumulator.add_batch(batch2).unwrap();

        let combined = accumulator.flush().unwrap().unwrap();
        assert_eq!(combined.num_rows(), 5);

        let schema = combined.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let unique_count = field_names.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(
            unique_count,
            field_names.len(),
            "duplicate columns in union schema: {field_names:?}"
        );

        // 4 required + service + host = 6
        assert_eq!(schema.fields().len(), 6);

        // Rows from batch1 have no "host" → 3 nulls; batch2 has "host" for all 2 rows → 0 nulls.
        let host_idx = schema.index_of("host").unwrap();
        assert_eq!(combined.column(host_idx).null_count(), 3);

        // "service" present in both batches → 0 nulls total.
        let service_idx = schema.index_of("service").unwrap();
        assert_eq!(combined.column(service_idx).null_count(), 0);
    }

    #[test]
    fn test_estimate_batch_bytes() {
        let batch = create_test_batch(100);
        let bytes = estimate_batch_bytes(&batch);
        assert!(bytes > 0);
    }
}
