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

//! Split writer that orchestrates Parquet file writing with metadata tracking.

use std::collections::HashSet;
use std::path::PathBuf;

use arrow::array::{Array, AsArray};
use arrow::compute::{max, min};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use tracing::{debug, info, instrument};

use super::config::ParquetWriterConfig;
use super::writer::{ParquetWriteError, ParquetWriter};
use crate::split::{
    ParquetSplit, ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TAG_SERVICE, TimeRange,
};

/// Writer that produces complete ParquetSplit with metadata from RecordBatch data.
pub struct ParquetSplitWriter {
    /// The underlying Parquet writer.
    writer: ParquetWriter,
    /// Base directory for split files.
    base_path: PathBuf,
}

impl ParquetSplitWriter {
    /// Create a new ParquetSplitWriter.
    ///
    /// # Arguments
    /// * `config` - Parquet writer configuration
    /// * `base_path` - Directory where split files will be written
    pub fn new(config: ParquetWriterConfig, base_path: impl Into<PathBuf>) -> Self {
        Self {
            writer: ParquetWriter::new(config),
            base_path: base_path.into(),
        }
    }

    /// Get the base path for split files.
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    /// Write a RecordBatch to a Parquet file and return a ParquetSplit with metadata.
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch to write
    /// * `index_uid` - The index unique identifier for the split metadata
    ///
    /// # Returns
    /// A ParquetSplit containing metadata extracted from the batch and the file path.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows()))]
    pub fn write_split(
        &self,
        batch: &RecordBatch,
        index_uid: &str,
    ) -> Result<ParquetSplit, ParquetWriteError> {
        // Generate unique split ID
        let split_id = ParquetSplitId::generate(ParquetSplitKind::Metrics);

        let file_path = self.base_path.join(format!("{}.parquet", split_id));

        // Ensure the base directory exists
        std::fs::create_dir_all(&self.base_path)?;

        // Write batch to file
        let size_bytes = self.writer.write_to_file(batch, &file_path)?;

        // Extract time range from batch
        let time_range = extract_time_range(batch)?;
        debug!(
            start_secs = time_range.start_secs,
            end_secs = time_range.end_secs,
            "extracted time range from batch"
        );

        // Extract distinct metric names from batch
        let metric_names = extract_metric_names(batch)?;

        // Extract distinct service names from batch
        let service_names = extract_service_names(batch)?;

        // Build metadata
        let metadata = ParquetSplitMetadata::builder()
            .kind(ParquetSplitKind::Metrics)
            .split_id(split_id.clone())
            .index_uid(index_uid)
            .time_range(time_range)
            .num_rows(batch.num_rows() as u64)
            .size_bytes(size_bytes);

        // Add metric names
        let metadata = metric_names
            .into_iter()
            .fold(metadata, |m, name| m.add_metric_name(name));

        // Add service names as low-cardinality tags
        let metadata = service_names.into_iter().fold(metadata, |m, name| {
            m.add_low_cardinality_tag(TAG_SERVICE, name)
        });

        let metadata = metadata.build();

        info!(
            split_id = %split_id,
            file_path = %file_path.display(),
            size_bytes,
            "split file written successfully"
        );

        Ok(ParquetSplit::new(metadata))
    }
}

/// Extracts the time range (min/max timestamp_secs) from a RecordBatch.
fn extract_time_range(batch: &RecordBatch) -> Result<TimeRange, ParquetWriteError> {
    let timestamp_idx = batch
        .schema()
        .index_of("timestamp_secs")
        .map_err(|_| ParquetWriteError::SchemaValidation("missing timestamp_secs column".into()))?;
    let timestamp_col = batch.column(timestamp_idx);
    let timestamp_array = timestamp_col.as_primitive::<UInt64Type>();

    let min_val = min(timestamp_array);
    let max_val = max(timestamp_array);

    match (min_val, max_val) {
        (Some(start), Some(end)) => {
            // End is exclusive, so add 1
            Ok(TimeRange::new(start, end + 1))
        }
        _ => {
            // Empty batch or all nulls - use sentinel values
            Ok(TimeRange::new(0, 0))
        }
    }
}

/// Extracts distinct metric names from a RecordBatch.
fn extract_metric_names(batch: &RecordBatch) -> Result<HashSet<String>, ParquetWriteError> {
    let metric_idx = batch
        .schema()
        .index_of("metric_name")
        .map_err(|_| ParquetWriteError::SchemaValidation("missing metric_name column".into()))?;
    let metric_col = batch.column(metric_idx);
    let mut names = HashSet::new();

    // The column is Dictionary(Int32, Utf8)
    if let Some(dict_array) = metric_col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
    {
        let values = dict_array.values();
        if let Some(string_values) = values.as_any().downcast_ref::<arrow::array::StringArray>() {
            // Get all dictionary values that are actually used
            for i in 0..dict_array.len() {
                if !dict_array.is_null(i)
                    && let Ok(key) = dict_array.keys().value(i).try_into()
                {
                    let key: usize = key;
                    if key < string_values.len() && !string_values.is_null(key) {
                        names.insert(string_values.value(key).to_string());
                    }
                }
            }
        }
    }

    Ok(names)
}

/// Extracts distinct service names from a RecordBatch.
fn extract_service_names(batch: &RecordBatch) -> Result<HashSet<String>, ParquetWriteError> {
    let service_idx = match batch.schema().index_of("service").ok() {
        Some(idx) => idx,
        None => return Ok(HashSet::new()),
    };
    let service_col = batch.column(service_idx);
    let mut names = HashSet::new();

    // The column is Dictionary(Int32, Utf8)
    if let Some(dict_array) = service_col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
    {
        let values = dict_array.values();
        if let Some(string_values) = values.as_any().downcast_ref::<arrow::array::StringArray>() {
            // Get all dictionary values that are actually used
            for i in 0..dict_array.len() {
                if !dict_array.is_null(i)
                    && let Ok(key) = dict_array.keys().value(i).try_into()
                {
                    let key: usize = key;
                    if key < string_values.len() && !string_values.is_null(key) {
                        names.insert(string_values.value(key).to_string());
                    }
                }
            }
        }
    }

    Ok(names)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, UInt8Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::test_helpers::{create_dict_array, create_nullable_dict_array};

    /// Create a test batch with required fields, optional service column, and specified tag
    /// columns.
    fn create_test_batch_with_options(
        num_rows: usize,
        metric_names: &[&str],
        timestamps: &[u64],
        service_names: Option<&[&str]>,
        tags: &[&str],
    ) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        let mut fields = vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ];
        if service_names.is_some() {
            fields.push(Field::new("service", dict_type.clone(), true));
        }
        for tag in tags {
            fields.push(Field::new(*tag, dict_type.clone(), true));
        }
        let schema = Arc::new(ArrowSchema::new(fields));

        let metric_name: ArrayRef = create_dict_array(metric_names);
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps.to_vec()));
        let values: Vec<f64> = (0..num_rows).map(|i| 42.0 + i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));

        let mut columns: Vec<ArrayRef> = vec![metric_name, metric_type, timestamp_secs, value];

        if let Some(svc_names) = service_names {
            columns.push(create_dict_array(svc_names));
        }

        for tag in tags {
            let tag_values: Vec<Option<&str>> = vec![Some(tag); num_rows];
            columns.push(create_nullable_dict_array(&tag_values));
        }

        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// Create a simple test batch with default values.
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let metric_names: Vec<&str> = (0..num_rows)
            .map(|i| {
                if i % 2 == 0 {
                    "cpu.usage"
                } else {
                    "memory.used"
                }
            })
            .collect();
        let timestamps: Vec<u64> = (0..num_rows).map(|i| 100 + i as u64 * 10).collect();
        let service_names: Vec<&str> = vec!["my-service"; num_rows];

        create_test_batch_with_options(
            num_rows,
            &metric_names,
            &timestamps,
            Some(&service_names),
            &["service", "host"],
        )
    }

    #[test]
    fn test_write_split_creates_file() {
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(config, temp_dir.path());

        let batch = create_test_batch(10);
        let split = writer.write_split(&batch, "test-index").unwrap();

        // Verify file exists
        let file_path = temp_dir.path().join(split.metadata.parquet_filename());
        assert!(
            std::fs::metadata(&file_path).is_ok(),
            "Parquet file should exist"
        );

        // Verify metadata
        assert_eq!(split.metadata.num_rows, 10);
        assert!(split.metadata.size_bytes > 0);
    }

    #[test]
    fn test_write_split_extracts_time_range() {
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(config, temp_dir.path());

        // Create batch with timestamps [100, 150, 200]
        let batch = create_test_batch_with_options(
            3,
            &["test.metric", "test.metric", "test.metric"],
            &[100, 150, 200],
            Some(&["my-service", "my-service", "my-service"]),
            &[],
        );
        let split = writer.write_split(&batch, "test-index").unwrap();

        // Verify time range
        assert_eq!(split.metadata.time_range.start_secs, 100);
        assert_eq!(split.metadata.time_range.end_secs, 201); // exclusive
    }

    #[test]
    fn test_write_split_extracts_metric_names() {
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(config, temp_dir.path());

        // Create batch with specific metric names
        let batch = create_test_batch_with_options(
            3,
            &["cpu.usage", "memory.used", "cpu.usage"],
            &[100, 100, 100],
            Some(&["my-service", "my-service", "my-service"]),
            &[],
        );
        let split = writer.write_split(&batch, "test-index").unwrap();

        // Verify metric names (distinct values)
        assert!(split.metadata.metric_names.contains("cpu.usage"));
        assert!(split.metadata.metric_names.contains("memory.used"));
        assert_eq!(split.metadata.metric_names.len(), 2);
    }
}
