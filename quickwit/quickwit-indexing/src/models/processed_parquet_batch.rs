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
    /// The Arrow RecordBatch containing parquet pipeline data.
    pub batch: RecordBatch,
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
        // Estimate memory usage from the RecordBatch
        let memory_size: i64 = batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as i64)
            .sum();

        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.indexer_mailbox);
        gauge_guard.add(memory_size);

        Self {
            batch,
            checkpoint_delta,
            force_commit,
            _gauge_guard: gauge_guard,
        }
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the estimated memory size of the batch in bytes.
    pub fn memory_size(&self) -> usize {
        self.batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum()
    }
}

impl fmt::Debug for ProcessedParquetBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessedParquetBatch")
            .field("num_rows", &self.batch.num_rows())
            .field("num_columns", &self.batch.num_columns())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryViewArray, DictionaryArray, Float64Array, Int32Array, StringArray,
        StructArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};
    use quickwit_parquet_engine::schema::ParquetSchema;

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

        // Create empty Variant (Struct with metadata and value BinaryView fields)
        let metadata_array = Arc::new(BinaryViewArray::from(vec![b"" as &[u8]; num_rows]));
        let value_array = Arc::new(BinaryViewArray::from(vec![b"" as &[u8]; num_rows]));
        let attributes: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("metadata", DataType::BinaryView, false)),
                metadata_array.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::BinaryView, false)),
                value_array.clone() as ArrayRef,
            ),
        ]));

        let service_name: ArrayRef = create_dict_array(&vec!["my-service"; num_rows]);

        let resource_attributes: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("metadata", DataType::BinaryView, false)),
                metadata_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::BinaryView, false)),
                value_array as ArrayRef,
            ),
        ]));

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
