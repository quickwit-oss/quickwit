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
use crate::schema::{SORT_ORDER, validate_required_fields};

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

    /// Schema validation failed.
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),
}

/// Writer for metrics data to Parquet format.
pub struct ParquetWriter {
    config: ParquetWriterConfig,
}

impl ParquetWriter {
    /// Create a new ParquetWriter.
    ///
    /// The `schema` argument is accepted for backwards compatibility but ignored —
    /// the writer now validates and sorts dynamically from the batch at write time.
    pub fn new(_schema: crate::schema::ParquetSchema, config: ParquetWriterConfig) -> Self {
        Self { config }
    }

    /// Get the writer configuration.
    pub fn config(&self) -> &ParquetWriterConfig {
        &self.config
    }

    /// Sort a RecordBatch by the metrics sort order.
    /// Columns from SORT_ORDER that are present in the batch schema are used;
    /// missing columns are skipped. This sorting enables efficient pruning during
    /// query execution.
    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ParquetWriteError> {
        let schema = batch.schema();
        let mut sort_columns: Vec<SortColumn> = SORT_ORDER
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .map(|idx| SortColumn {
                values: Arc::clone(batch.column(idx)),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
            .collect();

        if sort_columns.is_empty() {
            return Ok(batch.clone());
        }

        // Append the original row index as a tiebreaker so that rows with
        // identical sort keys keep their arrival order (stable sort semantics).
        // lexsort_to_indices uses an unstable sort internally; the tiebreaker
        // makes it behave stably at negligible cost (one u32 comparison per
        // equal-key pair, 4 bytes × num_rows of extra allocation).
        let row_indices = Arc::new(arrow::array::UInt32Array::from_iter_values(
            0..batch.num_rows() as u32,
        ));
        sort_columns.push(SortColumn {
            values: row_indices,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });

        let indices = lexsort_to_indices(&sort_columns, None)?;
        Ok(take_record_batch(batch, &indices)?)
    }

    /// Write a RecordBatch to Parquet bytes in memory.
    /// The batch is sorted before writing by: metric_name, common tags, timestamp.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows()))]
    pub fn write_to_bytes(&self, batch: &RecordBatch) -> Result<Vec<u8>, ParquetWriteError> {
        validate_required_fields(&batch.schema())
            .map_err(ParquetWriteError::SchemaValidation)?;

        // Sort the batch before writing for efficient pruning
        let sorted_batch = self.sort_batch(batch)?;

        let props = self.config.to_writer_properties(&sorted_batch.schema());
        let buffer = Cursor::new(Vec::new());

        let mut writer = ArrowWriter::try_new(buffer, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;
        let buffer = writer.into_inner()?;

        let bytes = buffer.into_inner();
        debug!(bytes_written = bytes.len(), "completed write to bytes");
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
        validate_required_fields(&batch.schema())
            .map_err(ParquetWriteError::SchemaValidation)?;

        // Sort the batch before writing for efficient pruning
        let sorted_batch = self.sort_batch(batch)?;

        let props = self.config.to_writer_properties(&sorted_batch.schema());
        let file = File::create(path)?;

        let mut writer = ArrowWriter::try_new(file, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;
        let file = writer.into_inner()?;

        let bytes_written = file.metadata()?.len();
        debug!(bytes_written, "completed write to file");
        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, DictionaryArray, Float64Array, StringArray, UInt64Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use crate::test_helpers::create_test_batch_with_tags;

    use super::*;

    fn create_test_batch() -> RecordBatch {
        create_test_batch_with_tags(1, &["service", "env"])
    }

    #[test]
    fn test_writer_creation() {
        let config = ParquetWriterConfig::default();
        let _writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);
    }

    #[test]
    fn test_write_to_bytes() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch).unwrap();

        // Parquet files start with PAR1 magic bytes
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_to_file() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        let batch = create_test_batch();
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_metrics.parquet");

        let bytes_written = writer.write_to_file(&batch, &path).unwrap();
        assert!(bytes_written > 0);

        // Clean up
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_schema_validation_missing_field() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        // Create a batch missing required fields
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
        assert!(matches!(result, Err(ParquetWriteError::SchemaValidation(_))));
    }

    #[test]
    fn test_schema_validation_wrong_type() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        // Create a batch where metric_name has wrong type (Utf8 instead of Dictionary)
        let wrong_schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let wrong_batch = RecordBatch::try_new(
            wrong_schema,
            vec![
                Arc::new(StringArray::from(vec!["test"])) as ArrayRef,
                Arc::new(UInt8Array::from(vec![0u8])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![100u64])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = writer.write_to_bytes(&wrong_batch);
        assert!(matches!(result, Err(ParquetWriteError::SchemaValidation(_))));
    }

    #[test]
    fn test_write_with_snappy_compression() {
        use super::super::config::Compression;

        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch).unwrap();

        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_sorts_data() {
        use std::fs::File;

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(crate::schema::ParquetSchema::new(), config);

        // Create a schema with required fields + service tag for sort verification
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "metric_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new(
                "service",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));

        // Create unsorted batch with multiple rows:
        // Row 0: metric_b, service_a, timestamp=300
        // Row 1: metric_a, service_b, timestamp=100
        // Row 2: metric_a, service_a, timestamp=200
        // Expected sorted order: metric_a/service_a/200, metric_a/service_b/100,
        // metric_b/service_a/300

        let metric_name: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![0i32, 1, 1]);
            let values = StringArray::from(vec!["metric_b", "metric_a"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8, 0, 0]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![300u64, 100u64, 200u64]));
        let value: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));

        let service: ArrayRef = {
            let keys = arrow::array::Int32Array::from(vec![Some(0i32), Some(1), Some(0)]);
            let values = StringArray::from(vec!["service_a", "service_b"]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        };

        let batch = RecordBatch::try_new(
            schema,
            vec![metric_name, metric_type, timestamp_secs, value, service],
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
        let metric_idx = result.schema().index_of("metric_name").unwrap();
        let ts_idx = result.schema().index_of("timestamp_secs").unwrap();
        let service_idx = result.schema().index_of("service").unwrap();

        let metric_col = result
            .column(metric_idx)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let ts_col = result
            .column(ts_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let service_col = result
            .column(service_idx)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        // Get string values from dictionary
        let get_metric = |row: usize| -> &str {
            let key = metric_col.keys().value(row);
            metric_col
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(key as usize)
        };
        let get_service = |row: usize| -> &str {
            let key = service_col.keys().value(row);
            service_col
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(key as usize)
        };

        // Expected sort order: metric_name ASC, service ASC, timestamp_secs ASC
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
