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

//! Arrow-based batch building for metrics with dictionary encoding.
//!
//! This module provides Arrow RecordBatch construction with dictionary-encoded
//! string columns for efficient storage of metrics with low cardinality tags.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, Float64Builder, RecordBatch, StringBuilder, StringDictionaryBuilder,
    UInt8Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema as ArrowSchema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use parquet::variant::{VariantArrayBuilder, VariantBuilderExt, VariantType};
use quickwit_proto::bytes::Bytes;
use quickwit_proto::ingest::{DocBatchV2, DocFormat};
use quickwit_proto::types::DocUid;

use super::otel_metrics::{MetricDataPoint, MetricType};

/// Creates the Arrow schema for metrics with dictionary-encoded string columns.
///
/// Dictionary encoding stores unique string values once and references them by
/// integer index, providing significant compression for low cardinality tag values.
pub fn metrics_arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        // Dictionary-encoded string columns for low cardinality fields
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        // MetricType enum stored as UInt8 (only ~5 possible values)
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("metric_unit", DataType::Utf8, true),
        // Measurement timestamp in seconds since Unix epoch.
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("start_timestamp_secs", DataType::UInt64, true),
        Field::new("value", DataType::Float64, false),
        // Dictionary-encoded tag columns (low cardinality expected)
        Field::new(
            "tag_service",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_env",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_datacenter",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_region",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_host",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        // VARIANT fields for semi-structured attributes
        // VariantArrayBuilder produces BinaryView fields, not Binary
        Field::new(
            "attributes",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, false),
            ])),
            true,
        )
        .with_extension_type(VariantType),
        // Service name (low cardinality)
        Field::new(
            "service_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "resource_attributes",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, false),
            ])),
            true,
        )
        .with_extension_type(VariantType),
    ])
}

/// Builder for creating Arrow RecordBatch from MetricDataPoints.
///
/// Uses dictionary encoding for low cardinality string columns
/// (tags, service names, metric names) to achieve significant compression.
/// Uses VARIANT encoding for semi-structured attributes.
pub struct ArrowMetricsBatchBuilder {
    metric_name: StringDictionaryBuilder<Int32Type>,
    metric_type: UInt8Builder,
    metric_unit: StringBuilder,
    timestamp_secs: UInt64Builder,
    start_timestamp_secs: UInt64Builder,
    value: Float64Builder,
    tag_service: StringDictionaryBuilder<Int32Type>,
    tag_env: StringDictionaryBuilder<Int32Type>,
    tag_datacenter: StringDictionaryBuilder<Int32Type>,
    tag_region: StringDictionaryBuilder<Int32Type>,
    tag_host: StringDictionaryBuilder<Int32Type>,
    attributes: VariantArrayBuilder,
    service_name: StringDictionaryBuilder<Int32Type>,
    resource_attributes: VariantArrayBuilder,
}

impl ArrowMetricsBatchBuilder {
    /// Creates a new builder with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            metric_name: StringDictionaryBuilder::new(),
            metric_type: UInt8Builder::with_capacity(capacity),
            metric_unit: StringBuilder::with_capacity(capacity, capacity * 8),
            timestamp_secs: UInt64Builder::with_capacity(capacity),
            start_timestamp_secs: UInt64Builder::with_capacity(capacity),
            value: Float64Builder::with_capacity(capacity),
            tag_service: StringDictionaryBuilder::new(),
            tag_env: StringDictionaryBuilder::new(),
            tag_datacenter: StringDictionaryBuilder::new(),
            tag_region: StringDictionaryBuilder::new(),
            tag_host: StringDictionaryBuilder::new(),
            attributes: VariantArrayBuilder::new(capacity),
            service_name: StringDictionaryBuilder::new(),
            resource_attributes: VariantArrayBuilder::new(capacity),
        }
    }

    /// Appends a MetricDataPoint to the batch.
    pub fn append(&mut self, data_point: &MetricDataPoint) {
        self.metric_name.append_value(&data_point.metric_name);
        self.metric_type.append_value(data_point.metric_type as u8);

        match &data_point.metric_unit {
            Some(unit) => self.metric_unit.append_value(unit),
            None => self.metric_unit.append_null(),
        }

        self.timestamp_secs.append_value(data_point.timestamp_secs);
        match data_point.start_timestamp_secs {
            Some(ts) => self.start_timestamp_secs.append_value(ts),
            None => self.start_timestamp_secs.append_null(),
        }
        self.value.append_value(data_point.value);

        append_optional_dict(&mut self.tag_service, &data_point.tag_service);
        append_optional_dict(&mut self.tag_env, &data_point.tag_env);
        append_optional_dict(&mut self.tag_datacenter, &data_point.tag_datacenter);
        append_optional_dict(&mut self.tag_region, &data_point.tag_region);
        append_optional_dict(&mut self.tag_host, &data_point.tag_host);

        if data_point.attributes.is_empty() {
            self.attributes.append_null();
        } else {
            append_variant_object(&mut self.attributes, &data_point.attributes);
        }

        self.service_name.append_value(&data_point.service_name);

        if data_point.resource_attributes.is_empty() {
            self.resource_attributes.append_null();
        } else {
            append_variant_object(
                &mut self.resource_attributes,
                &data_point.resource_attributes,
            );
        }
    }

    /// Finalizes and returns the RecordBatch.
    pub fn finish(mut self) -> RecordBatch {
        // Build variant arrays and convert to ArrayRef
        let attributes_array = self.attributes.build();
        let resource_attributes_array = self.resource_attributes.build();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.metric_type.finish()),
            Arc::new(self.metric_unit.finish()),
            Arc::new(self.timestamp_secs.finish()),
            Arc::new(self.start_timestamp_secs.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.tag_service.finish()),
            Arc::new(self.tag_env.finish()),
            Arc::new(self.tag_datacenter.finish()),
            Arc::new(self.tag_region.finish()),
            Arc::new(self.tag_host.finish()),
            ArrayRef::from(attributes_array),
            Arc::new(self.service_name.finish()),
            ArrayRef::from(resource_attributes_array),
        ];

        RecordBatch::try_new(Arc::new(metrics_arrow_schema()), arrays)
            .expect("record batch should match Arrow schema")
    }

    /// Returns the number of rows appended so far.
    pub fn len(&self) -> usize {
        self.timestamp_secs.len()
    }

    /// Returns true if no rows have been appended.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Helper to append optional string values to dictionary builder.
fn append_optional_dict(builder: &mut StringDictionaryBuilder<Int32Type>, value: &Option<String>) {
    match value {
        Some(s) => builder.append_value(s),
        None => builder.append_null(),
    }
}

/// Helper to append a HashMap as a VARIANT object to the builder.
fn append_variant_object(
    builder: &mut VariantArrayBuilder,
    map: &std::collections::HashMap<String, serde_json::Value>,
) {
    // Use a macro-like approach with fold to build the object
    // We need to chain with_field calls which consume and return the builder
    let obj_builder = builder.new_object();

    // Build object by folding over the map entries
    let final_builder = map.iter().fold(obj_builder, |b, (key, value)| {
        match value {
            serde_json::Value::Null => b.with_field(key.as_str(), ()),
            serde_json::Value::Bool(v) => b.with_field(key.as_str(), *v),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    b.with_field(key.as_str(), i)
                } else if let Some(f) = n.as_f64() {
                    b.with_field(key.as_str(), f)
                } else {
                    b.with_field(key.as_str(), ())
                }
            }
            serde_json::Value::String(s) => b.with_field(key.as_str(), s.as_str()),
            serde_json::Value::Array(arr) => {
                // For arrays, serialize to JSON string as fallback
                let json_str = serde_json::to_string(arr).unwrap_or_default();
                b.with_field(key.as_str(), json_str.as_str())
            }
            serde_json::Value::Object(obj) => {
                // For nested objects, serialize to JSON string as fallback
                let json_str = serde_json::to_string(obj).unwrap_or_default();
                b.with_field(key.as_str(), json_str.as_str())
            }
        }
    });

    final_builder.finish();
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

    use serde_json::Value as JsonValue;

    use super::*;

    fn make_test_data_point() -> MetricDataPoint {
        MetricDataPoint {
            metric_name: "cpu.usage".to_string(),
            metric_type: MetricType::Gauge,
            metric_unit: Some("%".to_string()),
            timestamp_secs: 1704067200,
            start_timestamp_secs: Some(1704067190),
            value: 85.5,
            tag_service: Some("api".to_string()),
            tag_env: Some("prod".to_string()),
            tag_datacenter: Some("us-east-1a".to_string()),
            tag_region: Some("us-east-1".to_string()),
            tag_host: Some("server-001".to_string()),
            attributes: HashMap::from([(
                "endpoint".to_string(),
                JsonValue::String("/health".to_string()),
            )]),
            service_name: "api-service".to_string(),
            resource_attributes: HashMap::from([(
                "k8s.pod".to_string(),
                JsonValue::String("pod-123".to_string()),
            )]),
        }
    }

    #[test]
    fn test_arrow_batch_builder_single_row() {
        let dp = make_test_data_point();
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1);
        builder.append(&dp);

        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);
    }

    #[test]
    fn test_arrow_batch_builder_multiple_rows() {
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(100);

        for i in 0..100 {
            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                metric_unit: None,
                timestamp_secs: 1704067200 + i as u64,
                start_timestamp_secs: None,
                value: i as f64 * 0.1,
                tag_service: Some(format!("service-{}", i % 10)), // 10 unique values
                tag_env: Some("prod".to_string()),                // 1 unique value
                tag_datacenter: None,
                tag_region: None,
                tag_host: Some(format!("host-{}", i % 5)), // 5 unique values
                attributes: HashMap::new(),
                service_name: "test-service".to_string(),
                resource_attributes: HashMap::new(),
            };
            builder.append(&dp);
        }

        assert_eq!(builder.len(), 100);
        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_dictionary_encoding_deduplication() {
        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1000);

        // Create 1000 data points with only 10 unique service values
        for i in 0..1000 {
            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                metric_unit: None,
                timestamp_secs: 1704067200 + i as u64,
                start_timestamp_secs: None,
                value: i as f64,
                tag_service: Some(format!("service-{}", i % 10)),
                tag_env: Some("prod".to_string()),
                tag_datacenter: Some(format!("dc-{}", i % 4)),
                tag_region: None,
                tag_host: None,
                attributes: HashMap::new(),
                service_name: format!("svc-{}", i % 5),
                resource_attributes: HashMap::new(),
            };
            builder.append(&dp);
        }

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1000);

        // Verify the batch was created successfully with dictionary encoding
        // The dictionary arrays should have far fewer unique values than rows
        let schema = batch.schema();

        // Check that tag_service uses dictionary encoding
        let tag_service_field = schema.field_with_name("tag_service").unwrap();
        assert!(matches!(
            tag_service_field.data_type(),
            DataType::Dictionary(_, _)
        ));
    }

    #[test]
    fn test_null_handling() {
        let dp = MetricDataPoint {
            metric_name: "minimal.metric".to_string(),
            metric_type: MetricType::Gauge,
            metric_unit: None, // null
            timestamp_secs: 1704067200,
            start_timestamp_secs: None, // null
            value: 0.0,
            tag_service: None, // null
            tag_env: None,     // null
            tag_datacenter: None,
            tag_region: None,
            tag_host: None,
            attributes: HashMap::new(), // empty -> null
            service_name: "unknown".to_string(),
            resource_attributes: HashMap::new(), // empty -> null
        };

        let mut builder = ArrowMetricsBatchBuilder::with_capacity(1);
        builder.append(&dp);
        let batch = builder.finish();

        assert_eq!(batch.num_rows(), 1);
        // The batch should handle nulls correctly
    }

    #[test]
    fn test_schema_field_count() {
        let schema = metrics_arrow_schema();
        assert_eq!(schema.fields().len(), 14);

        // Verify field names
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"metric_name"));
        assert!(field_names.contains(&"metric_type"));
        assert!(field_names.contains(&"metric_unit"));
        assert!(field_names.contains(&"timestamp_secs"));
        assert!(field_names.contains(&"start_timestamp_secs"));
        assert!(field_names.contains(&"value"));
        assert!(field_names.contains(&"tag_service"));
        assert!(field_names.contains(&"tag_env"));
        assert!(field_names.contains(&"tag_datacenter"));
        assert!(field_names.contains(&"tag_region"));
        assert!(field_names.contains(&"tag_host"));
        assert!(field_names.contains(&"attributes"));
        assert!(field_names.contains(&"service_name"));
        assert!(field_names.contains(&"resource_attributes"));
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
        for i in 0..10 {
            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                metric_unit: None,
                timestamp_secs: 1704067200 + i as u64,
                start_timestamp_secs: None,
                value: i as f64 * 0.1,
                tag_service: Some(format!("service-{}", i % 3)),
                tag_env: Some("prod".to_string()),
                tag_datacenter: None,
                tag_region: None,
                tag_host: None,
                attributes: HashMap::new(),
                service_name: "test-service".to_string(),
                resource_attributes: HashMap::new(),
            };
            builder.append(&dp);
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

        for i in 0..5 {
            let dp = MetricDataPoint {
                metric_name: "test.metric".to_string(),
                metric_type: MetricType::Gauge,
                metric_unit: None,
                timestamp_secs: 1704067200 + i as u64,
                start_timestamp_secs: None,
                value: i as f64,
                tag_service: None,
                tag_env: None,
                tag_datacenter: None,
                tag_region: None,
                tag_host: None,
                attributes: HashMap::new(),
                service_name: "test".to_string(),
                resource_attributes: HashMap::new(),
            };
            builder.append(&dp);
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
