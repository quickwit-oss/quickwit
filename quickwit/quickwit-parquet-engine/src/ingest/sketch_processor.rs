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

//! DDSketch ingest processor for Arrow IPC to RecordBatch conversion.

use arrow::array::AsArray;
use arrow::record_batch::RecordBatch;
use tracing::{debug, instrument, warn};

use super::processor::IngestError;
use crate::metrics::PARQUET_ENGINE_METRICS;
use crate::schema::validate_required_sketch_fields;

/// Processor that converts Arrow IPC bytes to RecordBatch for DDSketch data.
///
/// Validates required sketch fields and DDSketch-specific invariants:
/// - keys and counts arrays must have the same length per row
#[derive(Default)]
pub struct SketchParquetIngestProcessor;

impl SketchParquetIngestProcessor {
    /// Create a new SketchParquetIngestProcessor.
    pub fn new() -> Self {
        Self
    }

    /// Convert Arrow IPC bytes to RecordBatch.
    ///
    /// Returns error if IPC is malformed, schema doesn't match, or
    /// sketch arrays are inconsistent.
    #[instrument(skip(self, ipc_bytes), fields(bytes_len = ipc_bytes.len()))]
    pub fn process_ipc(&self, ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
        PARQUET_ENGINE_METRICS
            .ingest_bytes_total
            .with_label_values(["sketches"])
            .inc_by(ipc_bytes.len() as u64);

        let batch = match super::processor::ipc_to_record_batch(ipc_bytes) {
            Ok(batch) => batch,
            Err(err) => {
                PARQUET_ENGINE_METRICS
                    .errors_total
                    .with_label_values(["ingest", "sketches"])
                    .inc();
                return Err(err);
            }
        };

        if let Err(err) = self.validate_schema(&batch) {
            PARQUET_ENGINE_METRICS
                .errors_total
                .with_label_values(["ingest", "sketches"])
                .inc();
            return Err(err);
        }

        if let Err(err) = self.validate_sketch_arrays(&batch) {
            PARQUET_ENGINE_METRICS
                .errors_total
                .with_label_values(["ingest", "sketches"])
                .inc();
            return Err(err);
        }

        debug!(
            num_rows = batch.num_rows(),
            "successfully decoded sketch IPC to RecordBatch"
        );
        Ok(batch)
    }

    /// Validate that the RecordBatch schema contains all required sketch fields.
    fn validate_schema(&self, batch: &RecordBatch) -> Result<(), IngestError> {
        validate_required_sketch_fields(batch.schema().as_ref())
            .map_err(|err| IngestError::SchemaValidation(err.to_string()))
    }

    /// Validate DDSketch-specific array invariants.
    fn validate_sketch_arrays(&self, batch: &RecordBatch) -> Result<(), IngestError> {
        let keys_idx = batch.schema().index_of("keys").map_err(|_| {
            IngestError::SchemaValidation("missing required sketch field 'keys'".to_string())
        })?;
        let counts_idx = batch.schema().index_of("counts").map_err(|_| {
            IngestError::SchemaValidation("missing required sketch field 'counts'".to_string())
        })?;

        let keys_col = batch.column(keys_idx);
        let counts_col = batch.column(counts_idx);

        let keys_list = keys_col.as_list::<i32>();
        let counts_list = counts_col.as_list::<i32>();

        for row in 0..batch.num_rows() {
            let keys_len = keys_list.value(row).len();
            let counts_len = counts_list.value(row).len();
            if keys_len != counts_len {
                warn!(
                    row,
                    keys_len,
                    counts_len,
                    "sketch array length mismatch: keys and counts must have same length"
                );
                return Err(IngestError::SchemaValidation(format!(
                    "sketch array length mismatch at row {}: keys has {} elements but counts has \
                     {}",
                    row, keys_len, counts_len,
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Float64Array, Int16Array, ListArray, UInt32Array, UInt64Array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::ingest::processor::record_batch_to_ipc;
    use crate::test_helpers::{create_dict_array, create_nullable_dict_array};

    /// Build a List<Int16> array from slices.
    fn create_int16_list_array(rows: &[&[i16]]) -> ArrayRef {
        let mut offsets = vec![0i32];
        let mut values = Vec::new();
        for row in rows {
            values.extend_from_slice(row);
            offsets.push(values.len() as i32);
        }
        let values_array = Int16Array::from(values);
        let offsets = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::Int16, false));
        Arc::new(ListArray::new(field, offsets, Arc::new(values_array), None))
    }

    /// Build a List<UInt64> array from slices.
    fn create_uint64_list_array(rows: &[&[u64]]) -> ArrayRef {
        let mut offsets = vec![0i32];
        let mut values = Vec::new();
        for row in rows {
            values.extend_from_slice(row);
            offsets.push(values.len() as i32);
        }
        let values_array = UInt64Array::from(values);
        let offsets = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::UInt64, false));
        Arc::new(ListArray::new(field, offsets, Arc::new(values_array), None))
    }

    /// Create a test sketch batch with the 9 required fields plus the specified
    /// nullable dictionary-encoded tag columns.
    pub(crate) fn create_test_sketch_batch_with_tags(
        num_rows: usize,
        tags: &[(&str, &str)],
    ) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        let mut fields = vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("sum", DataType::Float64, false),
            Field::new("min", DataType::Float64, false),
            Field::new("max", DataType::Float64, false),
            Field::new("flags", DataType::UInt32, false),
            Field::new(
                "keys",
                DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
                false,
            ),
            Field::new(
                "counts",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                false,
            ),
        ];
        for (tag_name, _) in tags {
            fields.push(Field::new(*tag_name, dict_type.clone(), true));
        }
        let schema = Arc::new(ArrowSchema::new(fields));

        let metric_name: ArrayRef = create_dict_array(&vec!["req.latency"; num_rows]);
        let timestamps: Vec<u64> = (0..num_rows).map(|i| 1000 + i as u64 * 10).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let count: ArrayRef = Arc::new(UInt64Array::from(vec![100u64; num_rows]));
        let sum: ArrayRef = Arc::new(Float64Array::from(vec![50.0; num_rows]));
        let min_arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0; num_rows]));
        let max_arr: ArrayRef = Arc::new(Float64Array::from(vec![100.0; num_rows]));
        let flags: ArrayRef = Arc::new(UInt32Array::from(vec![0u32; num_rows]));

        let keys_rows: Vec<&[i16]> = vec![&[100, 200, 300]; num_rows];
        let keys: ArrayRef = create_int16_list_array(&keys_rows);

        let counts_rows: Vec<&[u64]> = vec![&[50, 30, 20]; num_rows];
        let counts: ArrayRef = create_uint64_list_array(&counts_rows);

        let mut columns: Vec<ArrayRef> = vec![
            metric_name,
            timestamp_secs,
            count,
            sum,
            min_arr,
            max_arr,
            flags,
            keys,
            counts,
        ];

        for (_, tag_value) in tags {
            let tag_values: Vec<Option<&str>> = vec![Some(tag_value); num_rows];
            columns.push(create_nullable_dict_array(&tag_values));
        }

        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// Create a test sketch batch with default tags (service, host).
    pub(crate) fn create_test_sketch_batch(num_rows: usize) -> RecordBatch {
        create_test_sketch_batch_with_tags(num_rows, &[("service", "api"), ("host", "host-001")])
    }

    #[test]
    fn test_process_valid_sketch_ipc() {
        let processor = SketchParquetIngestProcessor::new();
        let batch = create_test_sketch_batch(10);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 10);
    }

    #[test]
    fn test_reject_wrong_schema() {
        let processor = SketchParquetIngestProcessor::new();

        // Create a batch missing required sketch fields (only has metric_name and timestamp)
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                create_dict_array(&["cpu.usage"]),
                Arc::new(UInt64Array::from(vec![100u64])) as ArrayRef,
            ],
        )
        .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_reject_mismatched_keys_counts_lengths() {
        let processor = SketchParquetIngestProcessor::new();

        // Build a batch with keys=[1,2] but counts=[10] (length mismatch)
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("sum", DataType::Float64, false),
            Field::new("min", DataType::Float64, false),
            Field::new("max", DataType::Float64, false),
            Field::new("flags", DataType::UInt32, false),
            Field::new(
                "keys",
                DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
                false,
            ),
            Field::new(
                "counts",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                false,
            ),
        ]));

        let metric_name: ArrayRef = create_dict_array(&["req.latency"]);
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![1000u64]));
        let count: ArrayRef = Arc::new(UInt64Array::from(vec![100u64]));
        let sum: ArrayRef = Arc::new(Float64Array::from(vec![50.0]));
        let min_arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let max_arr: ArrayRef = Arc::new(Float64Array::from(vec![100.0]));
        let flags: ArrayRef = Arc::new(UInt32Array::from(vec![0u32]));

        // keys has 2 elements, counts has 1 element (mismatch!)
        let keys: ArrayRef = create_int16_list_array(&[&[1i16, 2]]);
        let counts: ArrayRef = create_uint64_list_array(&[&[10u64]]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                timestamp_secs,
                count,
                sum,
                min_arr,
                max_arr,
                flags,
                keys,
                counts,
            ],
        )
        .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_accept_sketch_with_extra_tag_columns() {
        let processor = SketchParquetIngestProcessor::new();

        // Sketch batch with extra tag columns should be accepted
        let batch = create_test_sketch_batch_with_tags(
            3,
            &[
                ("service", "api"),
                ("host", "host-001"),
                ("region", "us-east-1"),
            ],
        );
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 3);
    }
}
