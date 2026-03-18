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

//! Arrow-based batch building for metrics with dynamic schema discovery.
//!
//! This module provides Arrow RecordBatch construction with dictionary-encoded
//! string columns for efficient storage of metrics with dynamic tag keys.
//! The schema is discovered at `finish()` time by scanning all accumulated
//! data points for the union of tag keys.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, RecordBatch, StringDictionaryBuilder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use quickwit_proto::bytes::Bytes;
use quickwit_proto::ingest::{DocBatchV2, DocFormat};
use quickwit_proto::types::DocUid;

use super::otel_metrics::{MetricDataPoint, MetricType};

/// Builder for creating Arrow RecordBatch from MetricDataPoints.
///
/// Accumulates data points and discovers the schema dynamically at `finish()`
/// time. Uses dictionary encoding for string columns (metric_name, all tags).
pub struct ArrowMetricsBatchBuilder {
    data_points: Vec<MetricDataPoint>,
}

impl ArrowMetricsBatchBuilder {
    /// Creates a new builder with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data_points: Vec::with_capacity(capacity),
        }
    }

    /// Appends a MetricDataPoint to the batch.
    pub fn append(&mut self, data_point: MetricDataPoint) {
        self.data_points.push(data_point);
    }

    /// Finalizes and returns the RecordBatch.
    ///
    /// Performs two passes:
    /// 1. Schema discovery: scans all data points to collect the union of tag keys.
    /// 2. Array building: creates per-column builders and populates them.
    pub fn finish(self) -> RecordBatch {
        let num_rows = self.data_points.len();

        // Pass 1: discover all tag keys across all data points.
        let mut tag_keys: BTreeSet<&str> = BTreeSet::new();
        for dp in &self.data_points {
            for key in dp.tags.keys() {
                tag_keys.insert(key.as_str());
            }
        }
        let sorted_tag_keys: Vec<&str> = tag_keys.into_iter().collect();

        // Build the Arrow schema dynamically
        let mut fields = Vec::with_capacity(4 + sorted_tag_keys.len());
        fields.push(Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ));
        fields.push(Field::new("metric_type", DataType::UInt8, false));
        fields.push(Field::new("timestamp_secs", DataType::UInt64, false));
        fields.push(Field::new("value", DataType::Float64, false));

        for &tag_key in &sorted_tag_keys {
            fields.push(Field::new(
                tag_key,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ));
        }

        let schema = Arc::new(ArrowSchema::new(fields));

        // Pass 2: build arrays
        let mut metric_name_builder: StringDictionaryBuilder<Int32Type> =
            StringDictionaryBuilder::new();
        let mut metric_type_builder = UInt8Builder::with_capacity(num_rows);
        let mut timestamp_secs_builder = UInt64Builder::with_capacity(num_rows);
        let mut value_builder = Float64Builder::with_capacity(num_rows);

        let mut tag_builders: Vec<StringDictionaryBuilder<Int32Type>> = sorted_tag_keys
            .iter()
            .map(|_| StringDictionaryBuilder::new())
            .collect();

        for dp in &self.data_points {
            metric_name_builder.append_value(&dp.metric_name);
            metric_type_builder.append_value(dp.metric_type as u8);
            timestamp_secs_builder.append_value(dp.timestamp_secs);
            value_builder.append_value(dp.value);

            for (tag_idx, tag_key) in sorted_tag_keys.iter().enumerate() {
                match dp.tags.get(*tag_key) {
                    Some(tag_val) => tag_builders[tag_idx].append_value(tag_val),
                    None => tag_builders[tag_idx].append_null(),
                }
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(4 + sorted_tag_keys.len());
        arrays.push(Arc::new(metric_name_builder.finish()));
        arrays.push(Arc::new(metric_type_builder.finish()));
        arrays.push(Arc::new(timestamp_secs_builder.finish()));
        arrays.push(Arc::new(value_builder.finish()));

        for tag_builder in &mut tag_builders {
            arrays.push(Arc::new(tag_builder.finish()));
        }

        RecordBatch::try_new(schema, arrays)
            .expect("record batch should match Arrow schema")
    }

    /// Returns the number of rows appended so far.
    pub fn len(&self) -> usize {
        self.data_points.len()
    }

    /// Returns true if no rows have been appended.
    pub fn is_empty(&self) -> bool {
        self.data_points.is_empty()
    }
}

/// Error type for Arrow IPC operations.
#[derive(Debug, thiserror::Error)]
pub enum ArrowIpcError {
    #[error("Arrow IPC error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Expected exactly 1 RecordBatch in IPC stream, got {0}")]
    UnexpectedBatchCount(usize),
}

/// Serializes a RecordBatch to Arrow IPC stream format.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ArrowIpcError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Deserializes Arrow IPC stream format to a RecordBatch.
pub fn ipc_to_record_batch(ipc_bytes: &[u8]) -> Result<RecordBatch, ArrowIpcError> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    if batches.len() != 1 {
        return Err(ArrowIpcError::UnexpectedBatchCount(batches.len()));
    }

    let batch = batches
        .into_iter()
        .next()
        .expect("batches should contain exactly one element");
    Ok(batch)
}

/// Converts Arrow IPC bytes to JSON objects (one per row).
///
/// Returns a Vec of (json_value, estimated_bytes) tuples.
pub fn ipc_to_json_values(
    ipc_bytes: &[u8],
    total_bytes: usize,
) -> Result<Vec<(serde_json::Value, usize)>, ArrowIpcError> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::DataType;

    let record_batch = ipc_to_record_batch(ipc_bytes)?;
    let num_rows = record_batch.num_rows();
    let bytes_per_row = total_bytes / num_rows.max(1);
    let schema = record_batch.schema();

    let mut results = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut json_obj = serde_json::Map::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = record_batch.column(col_idx);
            let field_name = field.name().clone();

            if column.is_null(row_idx) {
                continue;
            }

            let value = match field.data_type() {
                DataType::Utf8 => {
                    let arr = column.as_string::<i32>();
                    serde_json::Value::String(arr.value(row_idx).to_string())
                }
                DataType::Dictionary(_, value_type) if **value_type == DataType::Utf8 => {
                    // Dictionary-encoded string
                    let dict_arr = column
                        .as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(
                        )
                        .unwrap();
                    let values = dict_arr.values().as_string::<i32>();
                    let key = dict_arr.keys().value(row_idx) as usize;
                    serde_json::Value::String(values.value(key).to_string())
                }
                DataType::UInt8 => {
                    // Special handling for metric_type enum
                    let arr = column.as_primitive::<arrow::datatypes::UInt8Type>();
                    let val = arr.value(row_idx);
                    if field_name == "metric_type" {
                        // Convert metric_type u8 to string
                        let type_str = MetricType::from_u8(val)
                            .map(|mt| mt.as_str())
                            .unwrap_or("unknown");
                        serde_json::Value::String(type_str.to_string())
                    } else {
                        serde_json::Value::Number(val.into())
                    }
                }
                DataType::UInt64 => {
                    let arr = column.as_primitive::<arrow::datatypes::UInt64Type>();
                    serde_json::Value::Number(arr.value(row_idx).into())
                }
                DataType::Float64 => {
                    let arr = column.as_primitive::<arrow::datatypes::Float64Type>();
                    let val = arr.value(row_idx);
                    serde_json::Number::from_f64(val)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                }
                _ => {
                    // For other types, skip
                    continue;
                }
            };

            json_obj.insert(field_name, value);
        }

        results.push((serde_json::Value::Object(json_obj), bytes_per_row));
    }

    Ok(results)
}

/// Builder for creating DocBatchV2 with Arrow IPC format.
///
/// Unlike JsonDocBatchV2Builder which serializes each document separately,
/// this builder takes a complete RecordBatch and serializes it as a single
/// Arrow IPC stream. The doc_uids track individual rows within the batch.
pub struct ArrowDocBatchV2Builder {
    doc_uids: Vec<DocUid>,
    ipc_buffer: Vec<u8>,
}

impl ArrowDocBatchV2Builder {
    /// Creates a new builder from a RecordBatch and corresponding doc UIDs.
    ///
    /// The number of doc_uids must equal the number of rows in the RecordBatch.
    pub fn from_record_batch(
        batch: &RecordBatch,
        doc_uids: Vec<DocUid>,
    ) -> Result<Self, ArrowIpcError> {
        assert_eq!(
            batch.num_rows(),
            doc_uids.len(),
            "doc_uids length must match RecordBatch row count"
        );

        let ipc_buffer = record_batch_to_ipc(batch)?;

        Ok(Self {
            doc_uids,
            ipc_buffer,
        })
    }

    /// Builds the DocBatchV2 with Arrow IPC format.
    ///
    /// The doc_buffer contains the entire Arrow IPC stream.
    /// doc_lengths contains a single entry with the total IPC buffer size.
    /// doc_format is set to ArrowIpc.
    pub fn build(self) -> DocBatchV2 {
        let buffer_len = self.ipc_buffer.len() as u32;
        DocBatchV2 {
            doc_uids: self.doc_uids,
            doc_buffer: Bytes::from(self.ipc_buffer),
            doc_lengths: vec![buffer_len],
            doc_format: DocFormat::ArrowIpc as i32,
        }
    }

    /// Returns the number of documents (rows) in this batch.
    pub fn num_docs(&self) -> usize {
        self.doc_uids.len()
    }

    /// Returns the size of the IPC buffer in bytes.
    pub fn ipc_size(&self) -> usize {
        self.ipc_buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn make_test_data_point() -> MetricDataPoint {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "api".to_string());
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("datacenter".to_string(), "us-east-1a".to_string());
        tags.insert("region".to_string(), "us-east-1".to_string());
        tags.insert("host".to_string(), "server-001".to_string());
        tags.insert("endpoint".to_string(), "/health".to_string());
        tags.insert("metric_unit".to_string(), "%".to_string());
        tags.insert("start_timestamp_secs".to_string(), "1704067190".to_string());
        tags.insert("service_name".to_string(), "api-service".to_string());

        MetricDataPoint {
            metric_name: "cpu.usage".to_string(),
            metric_type: MetricType::Gauge,
            timestamp_secs: 1704067200,
            value: 85.5,
            tags,
        }
    }

    #[test]
    fn test_arrow_batch_builder_single_row() {
        let dp = make_test_data_point();
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1);
        builder.append(dp);

        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1);
        // 4 fixed columns + 9 tag columns
        assert_eq!(batch.num_columns(), 13);
    }

    #[test]
    fn test_arrow_batch_builder_multiple_rows() {
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(100);

        for idx in 0..100 {
            let mut tags = HashMap::new();
            tags.insert("service".to_string(), format!("service-{}", idx % 10));
            tags.insert("env".to_string(), "prod".to_string());
            tags.insert("host".to_string(), format!("host-{}", idx % 5));
            tags.insert("service_name".to_string(), "test-service".to_string());

            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                timestamp_secs: 1704067200 + idx as u64,
                value: idx as f64 * 0.1,
                tags,
            };
            builder.append(dp);
        }

        assert_eq!(builder.len(), 100);
        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_dictionary_encoding_deduplication() {
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1000);

        // Create 1000 data points with only 10 unique service values
        for idx in 0..1000 {
            let mut tags = HashMap::new();
            tags.insert("service".to_string(), format!("service-{}", idx % 10));
            tags.insert("env".to_string(), "prod".to_string());
            tags.insert("datacenter".to_string(), format!("dc-{}", idx % 4));
            tags.insert("service_name".to_string(), format!("svc-{}", idx % 5));

            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                timestamp_secs: 1704067200 + idx as u64,
                value: idx as f64,
                tags,
            };
            builder.append(dp);
        }

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1000);

        // Verify the batch was created successfully with dictionary encoding
        let schema = batch.schema();

        // Check that the service tag uses dictionary encoding
        let service_field = schema.field_with_name("service").unwrap();
        assert!(matches!(
            service_field.data_type(),
            DataType::Dictionary(_, _)
        ));
    }

    #[test]
    fn test_null_handling() {
        let mut tags = HashMap::new();
        tags.insert("service_name".to_string(), "unknown".to_string());

        let dp = MetricDataPoint {
            metric_name: "minimal.metric".to_string(),
            metric_type: MetricType::Gauge,
            timestamp_secs: 1704067200,
            value: 0.0,
            tags,
        };

        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1);
        builder.append(dp);
        let batch = builder.finish();

        assert_eq!(batch.num_rows(), 1);
        // 4 fixed columns + 1 tag column (service_name)
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_dynamic_schema_discovery() {
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(2);

        // First data point has tags: env, host
        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        tags1.insert("host".to_string(), "server-1".to_string());

        builder.append(MetricDataPoint {
            metric_name: "metric.a".to_string(),
            metric_type: MetricType::Gauge,
            timestamp_secs: 1704067200,
            value: 1.0,
            tags: tags1,
        });

        // Second data point has tags: env, region (different set)
        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "staging".to_string());
        tags2.insert("region".to_string(), "us-west".to_string());

        builder.append(MetricDataPoint {
            metric_name: "metric.b".to_string(),
            metric_type: MetricType::Sum,
            timestamp_secs: 1704067201,
            value: 2.0,
            tags: tags2,
        });

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 2);
        // 4 fixed + 3 tag columns (env, host, region) - sorted alphabetically
        assert_eq!(batch.num_columns(), 7);

        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "metric_name",
                "metric_type",
                "timestamp_secs",
                "value",
                "env",
                "host",
                "region",
            ]
        );
    }

    #[test]
    fn test_empty_builder() {
        let builder = ArrowMetricsBatchBuilder::with_capacity(0);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_ipc_round_trip() {
        // Build a RecordBatch
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(10);
        for idx in 0..10 {
            let mut tags = HashMap::new();
            tags.insert("service".to_string(), format!("service-{}", idx % 3));
            tags.insert("env".to_string(), "prod".to_string());
            tags.insert("service_name".to_string(), "test-service".to_string());

            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                timestamp_secs: 1704067200 + idx as u64,
                value: idx as f64 * 0.1,
                tags,
            };
            builder.append(dp);
        }
        let original_batch = builder.finish();

        // Serialize to IPC
        let ipc_bytes = record_batch_to_ipc(&original_batch).unwrap();
        assert!(!ipc_bytes.is_empty());

        // Deserialize back
        let recovered_batch = ipc_to_record_batch(&ipc_bytes).unwrap();

        // Verify
        assert_eq!(recovered_batch.num_rows(), original_batch.num_rows());
        assert_eq!(recovered_batch.num_columns(), original_batch.num_columns());
        assert_eq!(recovered_batch.schema(), original_batch.schema());
    }

    #[test]
    fn test_arrow_doc_batch_v2_builder() {
        use quickwit_proto::ingest::DocFormat;
        use quickwit_proto::types::DocUidGenerator;

        // Build a RecordBatch
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(5);
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut doc_uids = Vec::new();

        for idx in 0..5 {
            let mut tags = HashMap::new();
            tags.insert("service_name".to_string(), "test".to_string());

            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                timestamp_secs: 1704067200 + idx as u64,
                value: idx as f64,
                tags,
            };
            builder.append(dp);
            doc_uids.push(doc_uid_generator.next_doc_uid());
        }
        let batch = builder.finish();

        // Build DocBatchV2 with Arrow IPC
        let arrow_builder =
            ArrowDocBatchV2Builder::from_record_batch(&batch, doc_uids.clone()).unwrap();
        assert_eq!(arrow_builder.num_docs(), 5);

        let doc_batch = arrow_builder.build();

        // Verify DocBatchV2 properties
        assert_eq!(doc_batch.doc_uids.len(), 5);
        assert_eq!(doc_batch.doc_format, DocFormat::ArrowIpc as i32);
        assert_eq!(doc_batch.doc_lengths.len(), 1); // Single IPC buffer
        assert!(!doc_batch.doc_buffer.is_empty());

        // Verify we can recover the RecordBatch
        let recovered = ipc_to_record_batch(&doc_batch.doc_buffer).unwrap();
        assert_eq!(recovered.num_rows(), 5);
    }
}
