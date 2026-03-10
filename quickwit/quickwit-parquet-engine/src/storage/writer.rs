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

//! Parquet writer for metrics RecordBatch data.

use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take_record_batch};
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use thiserror::Error;
use tracing::{debug, instrument};

use super::config::ParquetWriterConfig;
use crate::schema::{ParquetField, ParquetSchema};

/// Errors that can occur during parquet writing.
#[derive(Debug, Error)]
pub enum ParquetWriteError {
    /// Parquet write error.
    #[error("Parquet error: {0}")]
    ParquetError(#[from] ParquetError),

    /// IO error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Arrow compute error during sorting.
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Schema mismatch between RecordBatch and ParquetSchema.
    #[error("Schema mismatch: expected {expected} fields, got {got}")]
    SchemaMismatch { expected: usize, got: usize },
}

/// Writer for metrics data to Parquet format.
pub struct ParquetWriter {
    config: ParquetWriterConfig,
    schema: ParquetSchema,
}

impl ParquetWriter {
    /// Create a new ParquetWriter.
    pub fn new(schema: ParquetSchema, config: ParquetWriterConfig) -> Self {
        Self { config, schema }
    }

    /// Get the writer configuration.
    pub fn config(&self) -> &ParquetWriterConfig {
        &self.config
    }

    /// Get the metrics schema.
    pub fn schema(&self) -> &ParquetSchema {
        &self.schema
    }

    /// Validate that a RecordBatch matches the expected schema.
    fn validate_batch(&self, batch: &RecordBatch) -> Result<(), ParquetWriteError> {
        let expected = self.schema.num_fields();
        let got = batch.num_columns();
        if expected != got {
            return Err(ParquetWriteError::SchemaMismatch { expected, got });
        }
        Ok(())
    }

    /// Sort a RecordBatch by the metrics sort order.
    /// Order: metric_name, tag_service, tag_env, tag_datacenter, tag_region, tag_host,
    /// timestamp_secs. This sorting enables efficient pruning during query execution.
    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ParquetWriteError> {
        // Build sort columns from the defined sort order
        let sort_columns: Vec<SortColumn> = ParquetField::sort_order()
            .iter()
            .map(|field| {
                let col_idx = field.column_index();
                SortColumn {
                    values: Arc::clone(batch.column(col_idx)),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                }
            })
            .collect();

        // Compute sorted indices
        let indices = lexsort_to_indices(&sort_columns, None)?;

        // Reorder the batch using the sorted indices
        let sorted_batch = take_record_batch(batch, &indices)?;
        Ok(sorted_batch)
    }

    /// Write a RecordBatch to Parquet bytes in memory.
    /// The batch is sorted before writing by: metric_name, common tags, timestamp.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows()))]
    pub fn write_to_bytes(&self, batch: &RecordBatch) -> Result<Vec<u8>, ParquetWriteError> {
        self.validate_batch(batch)?;

        // Sort the batch before writing for efficient pruning
        let sorted_batch = self.sort_batch(batch)?;

        let props = self.config.to_writer_properties();
        let buffer = Cursor::new(Vec::new());

        let mut writer = ArrowWriter::try_new(buffer, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;
        let buffer = writer.into_inner()?;

        let bytes = buffer.into_inner();
        debug!(bytes_written = bytes.len(), "Completed write to bytes");
        Ok(bytes)
    }

    /// Write a RecordBatch to a Parquet file.
    /// The batch is sorted before writing by: metric_name, common tags, timestamp.
    ///
    /// Returns the number of bytes written.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows(), path = %path.display()))]
    pub fn write_to_file(
        &self,
        batch: &RecordBatch,
        path: &Path,
    ) -> Result<u64, ParquetWriteError> {
        self.validate_batch(batch)?;

        // Sort the batch before writing for efficient pruning
        let sorted_batch = self.sort_batch(batch)?;

        let props = self.config.to_writer_properties();
        let file = File::create(path)?;

        let mut writer = ArrowWriter::try_new(file, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;
        let file = writer.into_inner()?;

        let bytes_written = file.metadata()?.len();
        debug!(bytes_written, "Completed write to file");
        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use parquet::variant::{VariantArrayBuilder, VariantBuilderExt};

    use super::*;

    /// Create dictionary array for string fields with Int32 keys.
    fn create_dict_array(values: Vec<&str>) -> ArrayRef {
        let string_array = StringArray::from(values);
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(vec![0i32]),
                Arc::new(string_array),
            )
            .unwrap(),
        )
    }

    /// Create nullable dictionary array for optional string fields.
    fn create_nullable_dict_array(value: Option<&str>) -> ArrayRef {
        match value {
            Some(v) => {
                let string_array = StringArray::from(vec![v]);
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        arrow::array::Int32Array::from(vec![0i32]),
                        Arc::new(string_array),
                    )
                    .unwrap(),
                )
            }
            None => {
                let string_array = StringArray::from(vec![None::<&str>]);
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        arrow::array::Int32Array::from(vec![None::<i32>]),
                        Arc::new(string_array),
                    )
                    .unwrap(),
                )
            }
        }
    }

    /// Create a VARIANT array for testing.
    fn create_variant_array(fields: Option<&[(&str, &str)]>) -> ArrayRef {
        let mut builder = VariantArrayBuilder::new(1);
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
        ArrayRef::from(builder.build())
    }

    fn create_test_batch() -> RecordBatch {
        let schema = ParquetSchema::new();

        // Create arrays for all 14 fields in ParquetSchema matching fields.rs:
        // MetricName: Dictionary(Int32, Utf8)
        let metric_name: ArrayRef = create_dict_array(vec!["test.metric"]);

        // MetricType: UInt8
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8])); // gauge

        // MetricUnit: Utf8 (nullable)
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("bytes")]));

        // TimestampSecs: UInt64
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![1704067200u64]));

        // StartTimestampSecs: UInt64 (nullable)
        let start_timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![None::<u64>]));

        // Value: Float64
        let value: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));

        // TagService: Dictionary(Int32, Utf8) (nullable)
        let tag_service: ArrayRef = create_nullable_dict_array(Some("web"));

        // TagEnv: Dictionary(Int32, Utf8) (nullable)
        let tag_env: ArrayRef = create_nullable_dict_array(Some("prod"));

        // TagDatacenter: Dictionary(Int32, Utf8) (nullable)
        let tag_datacenter: ArrayRef = create_nullable_dict_array(Some("us-east-1"));

        // TagRegion: Dictionary(Int32, Utf8) (nullable)
        let tag_region: ArrayRef = create_nullable_dict_array(None);

        // TagHost: Dictionary(Int32, Utf8) (nullable)
        let tag_host: ArrayRef = create_nullable_dict_array(Some("host-001"));

        // Attributes: VARIANT (nullable)
        let attributes: ArrayRef = create_variant_array(Some(&[("key", "value")]));

        // ServiceName: Dictionary(Int32, Utf8)
        let service_name: ArrayRef = create_dict_array(vec!["my-service"]);

        // ResourceAttributes: VARIANT (nullable)
        let resource_attributes: ArrayRef = create_variant_array(None);

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
    fn test_writer_creation() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(schema, config);

        assert_eq!(writer.schema().num_fields(), 14);
    }

    #[test]
    fn test_write_to_bytes() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(schema, config);

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch).unwrap();

        // Parquet files start with PAR1 magic bytes
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_to_file() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(schema, config);

        let batch = create_test_batch();
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_metrics.parquet");

        let bytes_written = writer.write_to_file(&batch, &path).unwrap();
        assert!(bytes_written > 0);

        // Clean up
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_schema_mismatch() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(schema, config);

        // Create a batch with wrong number of columns
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "single_field",
            DataType::Utf8,
            false,
        )]));
        let wrong_batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["test"]))],
        )
        .unwrap();

        let result = writer.write_to_bytes(&wrong_batch);
        assert!(matches!(
            result,
            Err(ParquetWriteError::SchemaMismatch {
                expected: 14,
                got: 1
            })
        ));
    }

    #[test]
    fn test_write_with_snappy_compression() {
        use super::super::config::Compression;

        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let writer = ParquetWriter::new(schema, config);

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch).unwrap();

        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_sorts_data() {
        use std::fs::File;

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(schema.clone(), config);

        // Create unsorted batch with multiple rows:
        // Row 0: metric_b, service_a, timestamp=300
        // Row 1: metric_a, service_b, timestamp=100
        // Row 2: metric_a, service_a, timestamp=200
        // Expected sorted order: metric_a/service_a/200, metric_a/service_b/100,
        // metric_b/service_a/300

        // Build arrays for 3 rows (original unsorted order in comments above)
        let timestamps = [300u64, 100u64, 200u64];
        let values = [1.0, 2.0, 3.0];

        // metric_name: Dictionary(Int32, Utf8)
        let metric_name: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![0i32, 1, 1]);
            let values = StringArray::from(vec!["metric_b", "metric_a"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8, 0, 0]));
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![
            Some("bytes"),
            Some("bytes"),
            Some("bytes"),
        ]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps.to_vec()));
        let start_timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from(vec![None::<u64>, None, None]));
        let value: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));

        // tag_service: Dictionary(Int32, Utf8) (nullable)
        let tag_service: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![Some(0i32), Some(1), Some(0)]);
            let values = StringArray::from(vec!["service_a", "service_b"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let tag_env: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![Some(0i32), Some(0), Some(0)]);
            let values = StringArray::from(vec!["prod"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let tag_datacenter: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![None::<i32>, None, None]);
            let values = StringArray::from(vec![None::<&str>]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let tag_region: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![None::<i32>, None, None]);
            let values = StringArray::from(vec![None::<&str>]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let tag_host: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![None::<i32>, None, None]);
            let values = StringArray::from(vec![None::<&str>]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        // Build VARIANT arrays for 3 rows
        let attributes: ArrayRef = {
            let mut builder = VariantArrayBuilder::new(3);
            for _ in 0..3 {
                builder.append_null();
            }
            ArrayRef::from(builder.build())
        };

        let service_name: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![0i32, 1, 0]);
            let values = StringArray::from(vec!["service_a", "service_b"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let resource_attributes: ArrayRef = {
            let mut builder = VariantArrayBuilder::new(3);
            for _ in 0..3 {
                builder.append_null();
            }
            ArrayRef::from(builder.build())
        };

        let batch = RecordBatch::try_new(
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
        .unwrap();

        // Write to file (will be sorted)
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_sorting.parquet");
        writer.write_to_file(&batch, &path).unwrap();

        // Read back and verify sort order
        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);

        let result = &batches[0];
        assert_eq!(result.num_rows(), 3);

        // Extract metric names and timestamps to verify sort order
        let metric_col = result
            .column(ParquetField::MetricName.column_index())
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let ts_col = result
            .column(ParquetField::TimestampSecs.column_index())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let service_col = result
            .column(ParquetField::TagService.column_index())
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        // Get string values from dictionary
        let get_metric = |i: usize| -> &str {
            let key = metric_col.keys().value(i);
            metric_col
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(key as usize)
        };
        let get_service = |i: usize| -> &str {
            let key = service_col.keys().value(i);
            service_col
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(key as usize)
        };

        // Expected sort order: metric_name ASC, tag_service ASC, timestamp ASC
        // Row 0: metric_a, service_a, 200 (original row 2)
        // Row 1: metric_a, service_b, 100 (original row 1)
        // Row 2: metric_b, service_a, 300 (original row 0)

        assert_eq!(get_metric(0), "metric_a");
        assert_eq!(get_service(0), "service_a");
        assert_eq!(ts_col.value(0), 200);

        assert_eq!(get_metric(1), "metric_a");
        assert_eq!(get_service(1), "service_b");
        assert_eq!(ts_col.value(1), 100);

        assert_eq!(get_metric(2), "metric_b");
        assert_eq!(get_service(2), "service_a");
        assert_eq!(ts_col.value(2), 300);

        // Clean up
        std::fs::remove_file(&path).ok();
    }
}
