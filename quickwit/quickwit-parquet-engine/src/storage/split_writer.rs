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
use crate::schema::{ParquetField, ParquetSchema};
use crate::split::{MetricsSplitMetadata, ParquetSplit, SplitId, TAG_SERVICE, TimeRange};

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
    /// * `schema` - The metrics schema for validation
    /// * `config` - Parquet writer configuration
    /// * `base_path` - Directory where split files will be written
    pub fn new(
        schema: ParquetSchema,
        config: ParquetWriterConfig,
        base_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            writer: ParquetWriter::new(schema, config),
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
    /// * `index_id` - The index identifier for the split metadata
    ///
    /// # Returns
    /// A ParquetSplit containing metadata extracted from the batch and the file path.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows()))]
    pub fn write_split(
        &self,
        batch: &RecordBatch,
        index_id: &str,
    ) -> Result<ParquetSplit, ParquetWriteError> {
        // Generate unique split ID
        let split_id = SplitId::generate();

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
            "Extracted time range from batch"
        );

        // Extract distinct metric names from batch
        let metric_names = extract_metric_names(batch)?;

        // Extract distinct service names from batch
        let service_names = extract_service_names(batch)?;

        // Build metadata
        let metadata = MetricsSplitMetadata::builder()
            .split_id(split_id.clone())
            .index_id(index_id)
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
            "Split file written successfully"
        );

        Ok(ParquetSplit::new(metadata))
    }
}

/// Extracts the time range (min/max timestamp_secs) from a RecordBatch.
fn extract_time_range(batch: &RecordBatch) -> Result<TimeRange, ParquetWriteError> {
    let timestamp_col = batch.column(ParquetField::TimestampSecs.column_index());
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
    let metric_col = batch.column(ParquetField::MetricName.column_index());
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
    let service_col = batch.column(ParquetField::ServiceName.column_index());
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

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::Int32Type;
    use parquet::variant::{VariantArrayBuilder, VariantBuilderExt};

    use super::*;

    #[test]
    fn test_column_indices_match_schema() {
        let schema = ParquetSchema::new();
        for field in ParquetField::all() {
            let expected: usize = schema.arrow_schema().index_of(field.name()).unwrap_or_else(|_| {
                panic!("field {:?} should exist in arrow schema", field.name())
            });
            assert_eq!(
                field.column_index(),
                expected,
                "column_index() for {:?} does not match arrow schema position",
                field.name()
            );
        }
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

    /// Create a test batch with specified number of rows and test data.
    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = ParquetSchema::new();

        // Generate test data
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

        // MetricName: Dictionary(Int32, Utf8)
        let metric_name: ArrayRef = create_dict_array(&metric_names);

        // MetricType: UInt8
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));

        // MetricUnit: Utf8 (nullable)
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("bytes"); num_rows]));

        // TimestampSecs: UInt64
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));

        // StartTimestampSecs: UInt64 (nullable)
        let start_timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from(vec![None::<u64>; num_rows]));

        // Value: Float64
        let values: Vec<f64> = (0..num_rows).map(|i| 42.0 + i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));

        // TagService: Dictionary(Int32, Utf8) (nullable)
        let tag_service: ArrayRef = create_nullable_dict_array(&vec![Some("web"); num_rows]);

        // TagEnv: Dictionary(Int32, Utf8) (nullable)
        let tag_env: ArrayRef = create_nullable_dict_array(&vec![Some("prod"); num_rows]);

        // TagDatacenter: Dictionary(Int32, Utf8) (nullable)
        let tag_datacenter: ArrayRef =
            create_nullable_dict_array(&vec![Some("us-east-1"); num_rows]);

        // TagRegion: Dictionary(Int32, Utf8) (nullable)
        let tag_region: ArrayRef = create_nullable_dict_array(&vec![None; num_rows]);

        // TagHost: Dictionary(Int32, Utf8) (nullable)
        let tag_host: ArrayRef = create_nullable_dict_array(&vec![Some("host-001"); num_rows]);

        // Attributes: VARIANT (nullable)
        let attributes: ArrayRef = create_variant_array(num_rows, Some(&[("key", "value")]));

        // ServiceName: Dictionary(Int32, Utf8)
        let service_names: Vec<&str> = (0..num_rows).map(|_| "my-service").collect();
        let service_name: ArrayRef = create_dict_array(&service_names);

        // ResourceAttributes: VARIANT (nullable)
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
    fn test_write_split_creates_file() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(schema, config, temp_dir.path());

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
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(schema, config, temp_dir.path());

        // Create batch with timestamps [100, 150, 200]
        let batch = create_test_batch_with_timestamps(&[100, 150, 200]);
        let split = writer.write_split(&batch, "test-index").unwrap();

        // Verify time range
        assert_eq!(split.metadata.time_range.start_secs, 100);
        assert_eq!(split.metadata.time_range.end_secs, 201); // exclusive
    }

    #[test]
    fn test_write_split_extracts_metric_names() {
        let schema = ParquetSchema::new();
        let config = ParquetWriterConfig::default();
        let temp_dir = tempfile::tempdir().unwrap();

        let writer = ParquetSplitWriter::new(schema, config, temp_dir.path());

        // Create batch with specific metric names
        let batch = create_test_batch_with_metric_names(&["cpu.usage", "memory.used", "cpu.usage"]);
        let split = writer.write_split(&batch, "test-index").unwrap();

        // Verify metric names (distinct values)
        assert!(split.metadata.metric_names.contains("cpu.usage"));
        assert!(split.metadata.metric_names.contains("memory.used"));
        assert_eq!(split.metadata.metric_names.len(), 2);
    }

    /// Create a test batch with specific timestamps.
    fn create_test_batch_with_timestamps(timestamps: &[u64]) -> RecordBatch {
        let schema = ParquetSchema::new();
        let num_rows = timestamps.len();

        let metric_names: Vec<&str> = vec!["test.metric"; num_rows];
        let metric_name: ArrayRef = create_dict_array(&metric_names);
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("bytes"); num_rows]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps.to_vec()));
        let start_timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from(vec![None::<u64>; num_rows]));
        let value: ArrayRef = Arc::new(Float64Array::from(vec![42.0; num_rows]));
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

    /// Create a test batch with specific metric names.
    fn create_test_batch_with_metric_names(metric_names: &[&str]) -> RecordBatch {
        let schema = ParquetSchema::new();
        let num_rows = metric_names.len();

        let metric_name: ArrayRef = create_dict_array(metric_names);
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("bytes"); num_rows]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![100u64; num_rows]));
        let start_timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from(vec![None::<u64>; num_rows]));
        let value: ArrayRef = Arc::new(Float64Array::from(vec![42.0; num_rows]));
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
}
