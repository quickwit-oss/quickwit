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
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tracing::{debug, instrument};

use super::config::ParquetWriterConfig;
use crate::schema::validate_required_fields;
use crate::sort_fields::parse_sort_fields;
use crate::split::MetricsSplitMetadata;
use crate::table_config::TableConfig;

/// Parquet key_value_metadata keys for compaction metadata.
/// Prefixed with "qh." to avoid collision with standard Parquet/Arrow keys.
pub(crate) const PARQUET_META_SORT_FIELDS: &str = "qh.sort_fields";
pub(crate) const PARQUET_META_WINDOW_START: &str = "qh.window_start";
pub(crate) const PARQUET_META_WINDOW_DURATION: &str = "qh.window_duration_secs";
pub(crate) const PARQUET_META_NUM_MERGE_OPS: &str = "qh.num_merge_ops";
pub(crate) const PARQUET_META_ROW_KEYS: &str = "qh.row_keys";
pub(crate) const PARQUET_META_ROW_KEYS_JSON: &str = "qh.row_keys_json";

/// Build Parquet key_value_metadata entries for compaction metadata.
/// Returns Vec<KeyValue> that can be added to WriterProperties.
///
/// Only populated fields are included -- pre-Phase-31 splits produce an empty vec.
pub(crate) fn build_compaction_key_value_metadata(
    metadata: &MetricsSplitMetadata,
) -> Vec<KeyValue> {
    // TW-2: window_duration must divide 3600 (also checked at build time,
    // but belt-and-suspenders at the serialization boundary).
    quickwit_dst::check_invariant!(
        quickwit_dst::invariants::InvariantId::TW2,
        metadata.window_duration_secs() == 0 || 3600 % metadata.window_duration_secs() == 0,
        " at Parquet write: window_duration_secs={} does not divide 3600",
        metadata.window_duration_secs()
    );

    let mut kvs = Vec::new();

    if !metadata.sort_fields.is_empty() {
        kvs.push(KeyValue::new(
            PARQUET_META_SORT_FIELDS.to_string(),
            metadata.sort_fields.clone(),
        ));
    }

    if let Some(ws) = metadata.window_start() {
        kvs.push(KeyValue::new(
            PARQUET_META_WINDOW_START.to_string(),
            ws.to_string(),
        ));
    }

    if metadata.window_duration_secs() > 0 {
        kvs.push(KeyValue::new(
            PARQUET_META_WINDOW_DURATION.to_string(),
            metadata.window_duration_secs().to_string(),
        ));
    }

    if metadata.num_merge_ops > 0 {
        kvs.push(KeyValue::new(
            PARQUET_META_NUM_MERGE_OPS.to_string(),
            metadata.num_merge_ops.to_string(),
        ));
    }

    if let Some(ref row_keys_bytes) = metadata.row_keys_proto {
        kvs.push(KeyValue::new(
            PARQUET_META_ROW_KEYS.to_string(),
            BASE64.encode(row_keys_bytes),
        ));

        // Debug: human-readable JSON (best-effort).
        if let Ok(row_keys) = <quickwit_proto::sortschema::RowKeys as prost::Message>::decode(
            row_keys_bytes.as_slice(),
        ) && let Ok(json) = serde_json::to_string(&row_keys)
        {
            kvs.push(KeyValue::new(PARQUET_META_ROW_KEYS_JSON.to_string(), json));
        }
    }

    kvs
}

/// SS-5: Verify that the kv_metadata entries match the source MetricsSplitMetadata.
fn verify_ss5_kv_consistency(metadata: &MetricsSplitMetadata, kvs: &[KeyValue]) {
    let find_kv = |key: &str| -> Option<&str> {
        kvs.iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_deref())
    };

    if !metadata.sort_fields.is_empty() {
        quickwit_dst::check_invariant!(
            quickwit_dst::invariants::InvariantId::SS5,
            find_kv(PARQUET_META_SORT_FIELDS) == Some(metadata.sort_fields.as_str()),
            ": sort_fields in kv_metadata does not match MetricsSplitMetadata"
        );
    }

    if let Some(ws) = metadata.window_start() {
        quickwit_dst::check_invariant!(
            quickwit_dst::invariants::InvariantId::SS5,
            find_kv(PARQUET_META_WINDOW_START) == Some(ws.to_string()).as_deref(),
            ": window_start in kv_metadata does not match MetricsSplitMetadata"
        );
    }
}

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

/// A resolved sort field: a column name with its sort direction.
struct ResolvedSortField {
    name: String,
    descending: bool,
}

/// Writer for metrics data to Parquet format.
pub struct ParquetWriter {
    config: ParquetWriterConfig,
    /// Physical sort columns resolved from TableConfig, in sort priority order.
    resolved_sort_fields: Vec<ResolvedSortField>,
    /// The original sort fields string from TableConfig, stored verbatim in metadata.
    sort_fields_string: String,
}

impl ParquetWriter {
    /// Create a new ParquetWriter with sort order driven by `table_config`.
    ///
    /// Parses `table_config.effective_sort_fields()`, resolves each column name
    /// to a `ParquetField`. Columns not in the physical schema (e.g., `timeseries_id`)
    /// are skipped for sorting but recorded in the metadata string.
    ///
    /// The writer validates and sorts dynamically from the batch at write time.
    pub fn new(config: ParquetWriterConfig, table_config: &TableConfig) -> Self {
        let sort_fields_string = table_config.effective_sort_fields().to_string();
        let resolved_sort_fields = resolve_sort_fields(&sort_fields_string);
        Self {
            config,
            resolved_sort_fields,
            sort_fields_string,
        }
    }

    /// Get the writer configuration.
    pub fn config(&self) -> &ParquetWriterConfig {
        &self.config
    }

    /// Get the sort fields string (for metadata).
    pub fn sort_fields_string(&self) -> &str {
        &self.sort_fields_string
    }

    /// Build `SortingColumn` entries for Parquet file metadata.
    /// Columns from resolved sort fields that are present in the batch schema are included.
    fn sorting_columns(&self, batch: &RecordBatch) -> Vec<SortingColumn> {
        let schema = batch.schema();
        self.resolved_sort_fields
            .iter()
            .filter_map(|sf| {
                schema
                    .index_of(sf.name.as_str())
                    .ok()
                    .map(|idx| SortingColumn {
                        column_idx: idx as i32,
                        descending: sf.descending,
                        nulls_first: true,
                    })
            })
            .collect()
    }

    /// Sort a RecordBatch according to the table_config sort fields.
    /// Columns from the resolved sort fields that are present in the batch schema
    /// are used; missing columns are skipped. This sorting enables efficient pruning
    /// during query execution.
    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ParquetWriteError> {
        let schema = batch.schema();
        let mut sort_columns: Vec<SortColumn> = self
            .resolved_sort_fields
            .iter()
            .filter_map(|sf| {
                schema
                    .index_of(sf.name.as_str())
                    .ok()
                    .map(|idx| SortColumn {
                        values: Arc::clone(batch.column(idx)),
                        options: Some(SortOptions {
                            descending: sf.descending,
                            nulls_first: true,
                        }),
                    })
            })
            .collect();

        if sort_columns.is_empty() {
            return Ok(batch.clone());
        }

        // Append the original row index as a tiebreaker so that rows with
        // identical sort keys keep their arrival order (stable sort semantics).
        // lexsort_to_indices uses an unstable sort internally; the tiebreaker
        // makes it behave stably at a small cost (one u32 comparison per
        // equal-key pair, 4 bytes × num_rows of extra allocation).
        let row_indices = Arc::new(arrow::array::UInt32Array::from_iter_values(
            0..batch.num_rows() as u32,
        ));
        sort_columns.push(SortColumn {
            values: row_indices,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });

        let indices = lexsort_to_indices(&sort_columns, None)?;
        let sorted_batch = take_record_batch(batch, &indices)?;

        // SS-1: verify the output is actually sorted.
        #[cfg(debug_assertions)]
        {
            if sorted_batch.num_rows() > 1 {
                let verify_columns: Vec<SortColumn> = self
                    .resolved_sort_fields
                    .iter()
                    .filter_map(|sf| {
                        schema
                            .index_of(sf.name.as_str())
                            .ok()
                            .map(|idx| SortColumn {
                                values: Arc::clone(sorted_batch.column(idx)),
                                options: Some(SortOptions {
                                    descending: sf.descending,
                                    nulls_first: true,
                                }),
                            })
                    })
                    .collect();
                let verify_indices = lexsort_to_indices(&verify_columns, None)
                    .expect("SS-1 verification sort failed");
                for i in 0..verify_indices.len() {
                    quickwit_dst::check_invariant!(
                        quickwit_dst::invariants::InvariantId::SS1,
                        verify_indices.value(i) as usize == i,
                        ": row {} is out of sort order after sort_batch()",
                        i
                    );
                }
            }
        }

        Ok(sorted_batch)
    }

    /// Reorder columns for optimal physical layout in the Parquet file.
    ///
    /// Sort schema columns are placed first (in their configured sort order),
    /// followed by all remaining data columns in alphabetical order. This
    /// layout enables a two-GET streaming merge during compaction: the first
    /// GET reads the footer, the second streams from the start of the row
    /// group — sort columns arrive first, allowing the compactor to compute
    /// the global merge order before data columns arrive.
    fn reorder_columns(&self, batch: &RecordBatch) -> RecordBatch {
        let schema = batch.schema();
        let mut ordered_indices: Vec<usize> = Vec::with_capacity(schema.fields().len());
        let mut used = vec![false; schema.fields().len()];

        // Phase 1: sort schema columns in their configured order.
        for sf in &self.resolved_sort_fields {
            if let Ok(idx) = schema.index_of(sf.name.as_str()) {
                if !used[idx] {
                    ordered_indices.push(idx);
                    used[idx] = true;
                }
            }
        }

        // Phase 2: remaining columns, alphabetically by name.
        let mut remaining: Vec<(usize, &str)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| !used[*i])
            .map(|(i, f)| (i, f.name().as_str()))
            .collect();
        remaining.sort_by_key(|(_, name)| *name);
        for (idx, _) in remaining {
            ordered_indices.push(idx);
        }

        // Build reordered schema and columns.
        let new_fields: Vec<Arc<arrow::datatypes::Field>> = ordered_indices
            .iter()
            .map(|&i| Arc::new(schema.field(i).clone()))
            .collect();
        let new_columns: Vec<Arc<dyn arrow::array::Array>> = ordered_indices
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));

        RecordBatch::try_new(new_schema, new_columns)
            .expect("reorder_columns: schema and columns must be consistent")
    }

    /// Validate, sort, reorder columns, and build WriterProperties for a batch.
    fn prepare_write(
        &self,
        batch: &RecordBatch,
        split_metadata: Option<&MetricsSplitMetadata>,
    ) -> Result<(RecordBatch, WriterProperties), ParquetWriteError> {
        validate_required_fields(&batch.schema())
            .map_err(|e| ParquetWriteError::SchemaValidation(e.to_string()))?;
        let sorted_batch = self.reorder_columns(&self.sort_batch(batch)?);

        let kv_metadata = split_metadata.map(build_compaction_key_value_metadata);

        // SS-5: verify kv_metadata sort_fields matches source.
        if let (Some(meta), Some(kvs)) = (split_metadata, &kv_metadata) {
            verify_ss5_kv_consistency(meta, kvs);
        }

        let props = self.config.to_writer_properties_with_metadata(
            &sorted_batch.schema(),
            self.sorting_columns(&sorted_batch),
            kv_metadata,
        );
        Ok((sorted_batch, props))
    }

    /// Write a RecordBatch to Parquet bytes in memory.
    #[instrument(skip(self, batch), fields(batch_rows = batch.num_rows()))]
    pub fn write_to_bytes(
        &self,
        batch: &RecordBatch,
        split_metadata: Option<&MetricsSplitMetadata>,
    ) -> Result<Vec<u8>, ParquetWriteError> {
        let (sorted_batch, props) = self.prepare_write(batch, split_metadata)?;

        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(buffer, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;
        let bytes = writer.into_inner()?.into_inner();

        debug!(bytes_written = bytes.len(), "completed write to bytes");
        Ok(bytes)
    }

    /// Write a RecordBatch to a Parquet file with optional compaction metadata.
    ///
    /// Returns the number of bytes written.
    #[instrument(skip(self, batch, split_metadata), fields(batch_rows = batch.num_rows(), path = %path.display()))]
    pub fn write_to_file_with_metadata(
        &self,
        batch: &RecordBatch,
        path: &Path,
        split_metadata: Option<&MetricsSplitMetadata>,
    ) -> Result<u64, ParquetWriteError> {
        let (sorted_batch, props) = self.prepare_write(batch, split_metadata)?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, sorted_batch.schema(), Some(props))?;
        writer.write(&sorted_batch)?;

        let bytes_written = writer.into_inner()?.metadata()?.len();
        debug!(bytes_written, "completed write to file");
        Ok(bytes_written)
    }
}

/// Parse a sort fields string and resolve column names to physical `ParquetField`s.
///
/// Columns not present in the current schema (e.g., `timeseries_id`) are silently
/// skipped — they are recorded in the metadata string but do not affect physical sort.
fn resolve_sort_fields(sort_fields_str: &str) -> Vec<ResolvedSortField> {
    let schema = match parse_sort_fields(sort_fields_str) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(sort_fields = sort_fields_str, error = %e, "failed to parse sort fields, using empty sort order");
            return Vec::new();
        }
    };

    schema
        .column
        .iter()
        .map(|col| {
            let descending = col.sort_direction
                == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32;
            ResolvedSortField {
                name: col.name.clone(),
                descending,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};

    use super::*;
    use crate::test_helpers::create_test_batch_with_tags;

    fn create_test_batch() -> RecordBatch {
        create_test_batch_with_tags(1, &["service", "env"])
    }

    #[test]
    fn test_writer_creation() {
        let config = ParquetWriterConfig::default();
        let _writer = ParquetWriter::new(config, &TableConfig::default());
    }

    #[test]
    fn test_write_to_bytes() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch, None).unwrap();

        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_to_file() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch();
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_metrics.parquet");

        let bytes_written = writer
            .write_to_file_with_metadata(&batch, &path, None)
            .unwrap();
        assert!(bytes_written > 0);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_schema_validation_missing_field() {
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

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

        let result = writer.write_to_bytes(&wrong_batch, None);
        assert!(matches!(
            result,
            Err(ParquetWriteError::SchemaValidation(_))
        ));
    }

    #[test]
    fn test_write_with_snappy_compression() {
        use super::super::config::Compression;

        let config = ParquetWriterConfig::new().with_compression(Compression::Snappy);
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch();
        let bytes = writer.write_to_bytes(&batch, None).unwrap();

        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_sorts_data() {
        use std::fs::File;

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

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

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_sorting.parquet");
        writer
            .write_to_file_with_metadata(&batch, &path, None)
            .unwrap();

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

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_to_file_with_compaction_metadata() {
        use std::fs::File;

        use parquet::file::reader::{FileReader, SerializedFileReader};

        use crate::split::{SplitId, TimeRange};

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch();

        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("e2e-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .window_start_secs(1700000000)
            .window_duration_secs(900)
            .sort_fields("metric_name|host|timestamp/V2")
            .num_merge_ops(3)
            .build();

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_compaction_metadata.parquet");

        writer
            .write_to_file_with_metadata(&batch, &path, Some(&metadata))
            .unwrap();

        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let file_metadata = reader.metadata().file_metadata();
        let kv_metadata = file_metadata
            .key_value_metadata()
            .expect("should have kv metadata");

        let find_kv = |key: &str| -> Option<String> {
            kv_metadata
                .iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.clone())
        };

        assert_eq!(
            find_kv(PARQUET_META_SORT_FIELDS).unwrap(),
            "metric_name|host|timestamp/V2"
        );
        assert_eq!(find_kv(PARQUET_META_WINDOW_START).unwrap(), "1700000000");
        assert_eq!(find_kv(PARQUET_META_WINDOW_DURATION).unwrap(), "900");
        assert_eq!(find_kv(PARQUET_META_NUM_MERGE_OPS).unwrap(), "3");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_to_file_without_metadata_has_no_qh_keys() {
        use std::fs::File;

        use parquet::file::reader::{FileReader, SerializedFileReader};

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch();
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_no_compaction_metadata.parquet");

        writer
            .write_to_file_with_metadata(&batch, &path, None)
            .unwrap();

        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let file_metadata = reader.metadata().file_metadata();

        if let Some(kv_metadata) = file_metadata.key_value_metadata() {
            let qh_keys: Vec<_> = kv_metadata
                .iter()
                .filter(|kv| kv.key.starts_with("qh."))
                .collect();
            assert!(
                qh_keys.is_empty(),
                "should have no qh.* keys without metadata, got: {:?}",
                qh_keys
            );
        }

        std::fs::remove_file(&path).ok();
    }

    /// META-07 compliance: Prove the Parquet file is truly self-describing by
    /// writing compaction metadata, reading it back from a cold file (no in-memory
    /// state), and reconstructing the MetricsSplitMetadata compaction fields from
    /// ONLY the Parquet key_value_metadata.
    #[test]
    fn test_meta07_self_describing_parquet_roundtrip() {
        use std::fs::File;

        use parquet::file::reader::{FileReader, SerializedFileReader};

        use crate::split::{SplitId, TimeRange};

        let sort_schema_str = "metric_name|host|env|timestamp/V2";
        let window_start_secs: i64 = 1700006400;
        let window_duration: u32 = 900;
        let merge_ops: u32 = 7;
        let row_keys_bytes: Vec<u8> = vec![0x0A, 0x03, 0x63, 0x70, 0x75];

        let original = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("self-describing-test"))
            .index_uid("metrics-prod:00000000000000000000000000")
            .time_range(TimeRange::new(1700006400, 1700007300))
            .window_start_secs(window_start_secs)
            .window_duration_secs(window_duration)
            .sort_fields(sort_schema_str)
            .num_merge_ops(merge_ops)
            .row_keys_proto(row_keys_bytes.clone())
            .build();

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());
        let batch = create_test_batch();

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_self_describing_roundtrip.parquet");
        writer
            .write_to_file_with_metadata(&batch, &path, Some(&original))
            .unwrap();

        // Read phase: open a cold file and reconstruct fields from kv_metadata.
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let file_metadata = reader.metadata().file_metadata();
        let kv_metadata = file_metadata
            .key_value_metadata()
            .expect("self-describing file must have kv_metadata");

        let find_kv = |key: &str| -> Option<String> {
            kv_metadata
                .iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.clone())
        };

        let recovered_sort_schema = find_kv(PARQUET_META_SORT_FIELDS)
            .expect("self-describing file must contain qh.sort_fields");
        let recovered_window_start: i64 = find_kv(PARQUET_META_WINDOW_START)
            .expect("self-describing file must contain qh.window_start")
            .parse()
            .expect("window_start must be parseable as i64");
        let recovered_window_duration: u32 = find_kv(PARQUET_META_WINDOW_DURATION)
            .expect("self-describing file must contain qh.window_duration_secs")
            .parse()
            .expect("window_duration must be parseable as u32");
        let recovered_merge_ops: u32 = find_kv(PARQUET_META_NUM_MERGE_OPS)
            .expect("self-describing file must contain qh.num_merge_ops")
            .parse()
            .expect("num_merge_ops must be parseable as u32");
        let recovered_row_keys_b64 =
            find_kv(PARQUET_META_ROW_KEYS).expect("self-describing file must contain qh.row_keys");
        let recovered_row_keys = BASE64
            .decode(&recovered_row_keys_b64)
            .expect("row_keys must be valid base64");

        assert_eq!(recovered_sort_schema, sort_schema_str);
        assert_eq!(recovered_window_start, window_start_secs);
        assert_eq!(recovered_window_duration, window_duration);
        assert_eq!(recovered_merge_ops, merge_ops);
        assert_eq!(recovered_row_keys, row_keys_bytes);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_build_compaction_kv_metadata_fully_populated() {
        use crate::split::{SplitId, TimeRange};

        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("kv-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .window_start_secs(1700000000)
            .window_duration_secs(3600)
            .sort_fields("metric_name|host|timestamp/V2")
            .num_merge_ops(5)
            .row_keys_proto(vec![0x08, 0x01, 0x10, 0x02])
            .build();

        let kvs = build_compaction_key_value_metadata(&metadata);

        assert!(
            kvs.len() >= 5,
            "expected at least 5 kv entries, got {}",
            kvs.len()
        );

        let find_kv = |key: &str| -> Option<String> {
            kvs.iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.clone())
        };

        assert_eq!(
            find_kv(PARQUET_META_SORT_FIELDS).unwrap(),
            "metric_name|host|timestamp/V2"
        );
        assert_eq!(find_kv(PARQUET_META_WINDOW_START).unwrap(), "1700000000");
        assert_eq!(find_kv(PARQUET_META_WINDOW_DURATION).unwrap(), "3600");
        assert_eq!(find_kv(PARQUET_META_NUM_MERGE_OPS).unwrap(), "5");

        let row_keys_b64 = find_kv(PARQUET_META_ROW_KEYS).unwrap();
        let decoded = BASE64.decode(&row_keys_b64).unwrap();
        assert_eq!(decoded, vec![0x08, 0x01, 0x10, 0x02]);
    }

    #[test]
    fn test_build_compaction_kv_metadata_default_pre_phase31() {
        use crate::split::{SplitId, TimeRange};

        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("old-split"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();

        let kvs = build_compaction_key_value_metadata(&metadata);

        assert!(
            kvs.is_empty(),
            "pre-Phase-31 metadata should produce empty kv vec, got {} entries",
            kvs.len()
        );
    }

    #[test]
    fn test_row_keys_base64_roundtrip() {
        use crate::split::{SplitId, TimeRange};

        let row_keys = quickwit_proto::sortschema::RowKeys {
            min_row_values: Some(quickwit_proto::sortschema::ColumnValues {
                column: vec![quickwit_proto::sortschema::ColumnValue {
                    value: Some(quickwit_proto::sortschema::column_value::Value::TypeString(
                        b"cpu.usage".to_vec(),
                    )),
                }],
            }),
            max_row_values: Some(quickwit_proto::sortschema::ColumnValues {
                column: vec![quickwit_proto::sortschema::ColumnValue {
                    value: Some(quickwit_proto::sortschema::column_value::Value::TypeString(
                        b"memory.used".to_vec(),
                    )),
                }],
            }),
            all_inclusive_max_row_values: None,
            expired: false,
        };

        let proto_bytes = prost::Message::encode_to_vec(&row_keys);

        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("roundtrip-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .sort_fields("metric_name|timestamp/V2")
            .row_keys_proto(proto_bytes.clone())
            .build();

        let kvs = build_compaction_key_value_metadata(&metadata);

        let b64_entry = kvs
            .iter()
            .find(|kv| kv.key == PARQUET_META_ROW_KEYS)
            .expect("should have row_keys entry");
        let decoded = BASE64
            .decode(b64_entry.value.as_ref().unwrap())
            .expect("should decode base64");
        assert_eq!(decoded, proto_bytes);

        let recovered: quickwit_proto::sortschema::RowKeys =
            prost::Message::decode(decoded.as_slice()).expect("should decode proto");
        assert_eq!(recovered, row_keys);

        let json_entry = kvs
            .iter()
            .find(|kv| kv.key == PARQUET_META_ROW_KEYS_JSON)
            .expect("should have row_keys_json entry");
        let json_str = json_entry.value.as_ref().unwrap();
        assert!(
            json_str.contains("min_row_values") && json_str.contains("TypeString"),
            "JSON should contain RowKeys structure, got: {}",
            json_str
        );
    }

    #[test]
    fn test_column_ordering_sort_columns_first_then_alphabetical() {
        // Default metrics sort fields: metric_name|service|env|datacenter|region|host|
        //                               timeseries_id|timestamp_secs
        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        // Create a batch with columns in a deliberately scrambled order.
        // The tag columns (service, env, region, host) plus two extra data
        // columns (zzz_extra, aaa_extra) that are NOT in the sort schema.
        let batch =
            create_test_batch_with_tags(3, &["host", "zzz_extra", "env", "region", "service", "aaa_extra"]);
        let input_schema = batch.schema();
        let input_names: Vec<&str> = input_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        // Sanity: the input batch has the columns in the order we gave them.
        assert!(input_names.contains(&"zzz_extra"));
        assert!(input_names.contains(&"aaa_extra"));

        let reordered = writer.reorder_columns(&batch);
        let schema = reordered.schema();
        let names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Sort schema columns that are present should come first, in sort order.
        // From the default: metric_name, service, env, region, host, timestamp_secs
        // (datacenter and timeseries_id are not in the batch).
        // metric_type and value are required fields but NOT sort columns.
        let expected_prefix = [
            "metric_name",
            "service",
            "env",
            "region",
            "host",
            "timestamp_secs",
        ];
        let sort_prefix: Vec<&str> = names
            .iter()
            .map(|s| s.as_str())
            .take_while(|n| expected_prefix.contains(n))
            .collect();
        assert_eq!(
            sort_prefix, expected_prefix,
            "sort schema columns should appear first in configured order, got: {:?}",
            names
        );

        // Remaining columns should be alphabetical.
        let remaining: Vec<&str> = names.iter().skip(sort_prefix.len()).map(|s| s.as_str()).collect();
        let mut sorted_remaining = remaining.clone();
        sorted_remaining.sort();
        assert_eq!(
            remaining, sorted_remaining,
            "non-sort columns should be in alphabetical order, got: {:?}",
            remaining
        );

        // All original columns must be present (no data loss).
        assert_eq!(reordered.num_columns(), batch.num_columns());
        assert_eq!(reordered.num_rows(), batch.num_rows());
    }

    #[test]
    fn test_column_ordering_preserved_in_parquet_file() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config, &TableConfig::default());

        let batch = create_test_batch_with_tags(3, &["host", "zzz_extra", "env", "service"]);

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_column_ordering.parquet");
        writer
            .write_to_file_with_metadata(&batch, &path, None)
            .unwrap();

        // Read back and verify physical column order from the Parquet schema.
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_schema = reader.metadata().file_metadata().schema_descr();
        let col_names: Vec<String> = (0..parquet_schema.num_columns())
            .map(|i| parquet_schema.column(i).name().to_string())
            .collect();

        // Sort columns first: metric_name, service, env, host, timestamp_secs
        // Then remaining alphabetically: metric_type, value, zzz_extra
        assert_eq!(col_names[0], "metric_name");
        assert_eq!(col_names[1], "service");
        assert_eq!(col_names[2], "env");
        assert_eq!(col_names[3], "host");
        assert_eq!(col_names[4], "timestamp_secs");

        let remaining = &col_names[5..];
        let mut sorted = remaining.to_vec();
        sorted.sort();
        assert_eq!(remaining, &sorted, "data columns should be alphabetical");

        std::fs::remove_file(&path).ok();
    }
}
