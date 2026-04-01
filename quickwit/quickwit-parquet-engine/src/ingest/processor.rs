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

//! Parquet ingest processor for Arrow IPC to RecordBatch conversion.

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tracing::{debug, instrument, warn};

use crate::metrics::PARQUET_ENGINE_METRICS;
use crate::schema::ParquetSchema;

/// Error type for ingest operations.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// DocBatchV2 is not in ArrowIpc format.
    #[error("Invalid format: expected ArrowIpc format")]
    InvalidFormat,

    /// Arrow IPC deserialization failed.
    #[error("IPC decode error: {0}")]
    IpcDecode(#[from] arrow::error::ArrowError),

    /// RecordBatch schema doesn't match expected metrics schema.
    #[error("Schema mismatch: expected {expected} fields, got {actual}")]
    SchemaMismatch { expected: usize, actual: usize },

    /// Expected exactly one RecordBatch in IPC stream.
    #[error("Expected 1 RecordBatch in IPC stream, got {0}")]
    UnexpectedBatchCount(usize),

    /// Storage error during split writing.
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::ParquetWriteError),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Processor that converts Arrow IPC bytes to RecordBatch.
///
/// Validates that the decoded batch matches the expected metrics schema.
pub struct ParquetIngestProcessor {
    schema: ParquetSchema,
}

impl ParquetIngestProcessor {
    /// Create a new ParquetIngestProcessor.
    pub fn new(schema: ParquetSchema) -> Self {
        Self { schema }
    }

    /// Convert Arrow IPC bytes to RecordBatch.
    ///
    /// Returns error if IPC is malformed or schema doesn't match.
    #[instrument(skip(self, ipc_bytes), fields(bytes_len = ipc_bytes.len()))]
    pub fn process_ipc(&self, ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
        // Record bytes ingested
        PARQUET_ENGINE_METRICS
            .ingest_bytes_total
            .inc_by(ipc_bytes.len() as u64);

        let batch = match ipc_to_record_batch(ipc_bytes) {
            Ok(batch) => batch,
            Err(e) => {
                PARQUET_ENGINE_METRICS
                    .errors_total
                    .with_label_values(["ingest"])
                    .inc();
                return Err(e);
            }
        };

        if let Err(e) = self.validate_schema(&batch) {
            PARQUET_ENGINE_METRICS
                .errors_total
                .with_label_values(["ingest"])
                .inc();
            return Err(e);
        }

        debug!(
            num_rows = batch.num_rows(),
            "Successfully decoded IPC to RecordBatch"
        );
        Ok(batch)
    }

    /// Validate that the RecordBatch schema matches expected metrics schema.
    fn validate_schema(&self, batch: &RecordBatch) -> Result<(), IngestError> {
        let expected_fields = self.schema.num_fields();
        let actual_fields = batch.schema().fields().len();

        if expected_fields != actual_fields {
            warn!(
                expected = expected_fields,
                actual = actual_fields,
                "Schema mismatch: field count differs"
            );
            return Err(IngestError::SchemaMismatch {
                expected: expected_fields,
                actual: actual_fields,
            });
        }

        // Verify field names match (in order)
        let schema_fields = self.schema.arrow_schema().fields();
        let batch_schema = batch.schema();
        let batch_fields = batch_schema.fields();

        for (expected, actual) in schema_fields.iter().zip(batch_fields.iter()) {
            if expected.name() != actual.name() {
                warn!(
                    expected_name = expected.name(),
                    actual_name = actual.name(),
                    "Schema mismatch: field name differs"
                );
                return Err(IngestError::SchemaMismatch {
                    expected: expected_fields,
                    actual: actual_fields,
                });
            }
            if expected.data_type() != actual.data_type() {
                warn!(
                    field = expected.name(),
                    expected_type = ?expected.data_type(),
                    actual_type = ?actual.data_type(),
                    "Schema mismatch: field type differs"
                );
                return Err(IngestError::SchemaMismatch {
                    expected: expected_fields,
                    actual: actual_fields,
                });
            }
        }

        Ok(())
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &ParquetSchema {
        &self.schema
    }
}

/// Deserialize Arrow IPC stream format to a RecordBatch.
fn ipc_to_record_batch(ipc_bytes: &[u8]) -> Result<RecordBatch, IngestError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    if batches.len() != 1 {
        return Err(IngestError::UnexpectedBatchCount(batches.len()));
    }

    // Safe: we verified exactly 1 batch above, but use ok_or for defensive programming
    batches
        .into_iter()
        .next()
        .ok_or(IngestError::UnexpectedBatchCount(0))
}

/// Serialize a RecordBatch to Arrow IPC stream format.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, IngestError> {
    use arrow::ipc::writer::StreamWriter;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::Int32Type;
    use parquet::variant::VariantArrayBuilder;

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

    /// Create a VARIANT array for testing with specified number of rows (all nulls).
    fn create_variant_array(num_rows: usize) -> ArrayRef {
        let mut builder = VariantArrayBuilder::new(num_rows);
        for _ in 0..num_rows {
            builder.append_null();
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
        let attributes: ArrayRef = create_variant_array(num_rows);
        let service_name: ArrayRef = create_dict_array(&vec!["my-service"; num_rows]);
        let resource_attributes: ArrayRef = create_variant_array(num_rows);

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
    fn test_process_ipc() {
        let schema = ParquetSchema::new();
        let processor = ParquetIngestProcessor::new(schema);

        // Create a valid batch
        let batch = create_test_batch(10);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();

        // Process the IPC bytes
        let result = processor.process_ipc(&ipc_bytes);
        assert!(result.is_ok());

        let recovered = result.unwrap();
        assert_eq!(recovered.num_rows(), 10);
        assert_eq!(recovered.num_columns(), 14);
    }

    #[test]
    fn test_process_ipc_invalid_bytes() {
        let schema = ParquetSchema::new();
        let processor = ParquetIngestProcessor::new(schema);

        let result = processor.process_ipc(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_ipc_round_trip() {
        let batch = create_test_batch(5);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let recovered = ipc_to_record_batch(&ipc_bytes).unwrap();

        assert_eq!(recovered.num_rows(), batch.num_rows());
        assert_eq!(recovered.num_columns(), batch.num_columns());
        assert_eq!(recovered.schema(), batch.schema());
    }
}
