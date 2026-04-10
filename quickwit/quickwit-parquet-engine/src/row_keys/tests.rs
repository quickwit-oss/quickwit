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

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt8Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use quickwit_proto::sortschema::column_value;

use super::*;
use crate::test_helpers::{create_dict_array, create_nullable_dict_array};

/// Default metrics sort fields string.
const METRICS_SORT_FIELDS: &str =
    "metric_name|service|env|datacenter|region|host|timeseries_id|timestamp_secs/V2";

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

/// Build a sorted test batch. Rows must be provided in sort order.
fn build_sorted_batch(rows: &[SortedRow]) -> RecordBatch {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("service", dict_type.clone(), true),
        Field::new("host", dict_type.clone(), true),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    let metric_names: Vec<&str> = rows.iter().map(|r| r.metric_name).collect();
    let metric_name_col: ArrayRef = create_dict_array(&metric_names);
    let metric_type_col: ArrayRef = Arc::new(UInt8Array::from(
        rows.iter().map(|r| r.metric_type).collect::<Vec<_>>(),
    ));
    let ts_col: ArrayRef = Arc::new(UInt64Array::from(
        rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
    ));
    let val_col: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.value).collect::<Vec<_>>(),
    ));
    let service_col: ArrayRef =
        create_nullable_dict_array(&rows.iter().map(|r| r.service).collect::<Vec<_>>());
    let host_col: ArrayRef =
        create_nullable_dict_array(&rows.iter().map(|r| r.host).collect::<Vec<_>>());
    let ts_id_col: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.timeseries_id).collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(
        schema,
        vec![
            metric_name_col,
            metric_type_col,
            ts_col,
            val_col,
            service_col,
            host_col,
            ts_id_col,
        ],
    )
    .unwrap()
}

struct SortedRow {
    metric_name: &'static str,
    metric_type: u8,
    timestamp: u64,
    value: f64,
    service: Option<&'static str>,
    host: Option<&'static str>,
    timeseries_id: i64,
}

impl SortedRow {
    fn new(
        metric_name: &'static str,
        service: Option<&'static str>,
        host: Option<&'static str>,
        timeseries_id: i64,
        timestamp: u64,
    ) -> Self {
        Self {
            metric_name,
            metric_type: 0,
            timestamp,
            value: 42.0,
            service,
            host,
            timeseries_id,
        }
    }
}

/// Helper to extract the string from a TypeString ColumnValue.
fn col_string(cv: &ColumnValue) -> &str {
    match cv.value.as_ref().expect("expected non-null ColumnValue") {
        column_value::Value::TypeString(bytes) => {
            std::str::from_utf8(bytes).expect("TypeString must be valid UTF-8")
        }
        other => panic!("expected TypeString, got {:?}", other),
    }
}

/// Helper to extract the i64 from a TypeInt ColumnValue.
fn col_int(cv: &ColumnValue) -> i64 {
    match cv.value.as_ref().expect("expected non-null ColumnValue") {
        column_value::Value::TypeInt(v) => *v,
        other => panic!("expected TypeInt, got {:?}", other),
    }
}

// -----------------------------------------------------------------------
// Core: extract_row_keys
// -----------------------------------------------------------------------

#[test]
fn test_empty_batch_returns_none() {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::new_empty(schema);

    let result = extract_row_keys(METRICS_SORT_FIELDS, &batch).unwrap();
    assert!(result.is_none(), "empty batch must return None");
}

#[test]
fn test_single_row_min_equals_max() {
    let batch = build_sorted_batch(&[SortedRow::new(
        "cpu.usage",
        Some("api"),
        Some("node-1"),
        42,
        1000,
    )]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .expect("single-row batch should produce RowKeys");

    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;

    // For a single row, min and max must be identical.
    assert_eq!(min_cols.len(), max_cols.len());
    for (i, (min_cv, max_cv)) in min_cols.iter().zip(max_cols.iter()).enumerate() {
        assert_eq!(
            min_cv, max_cv,
            "column {} min and max must be equal for single-row batch",
            i
        );
    }

    // Verify specific values.
    // Sort schema: metric_name(0), service(1), env(2), datacenter(3),
    //              region(4), host(5), timeseries_id(6), timestamp_secs(7)
    assert_eq!(col_string(&min_cols[0]), "cpu.usage");
    assert_eq!(col_string(&min_cols[1]), "api");
    assert!(min_cols[2].value.is_none(), "env should be null");
    assert!(min_cols[3].value.is_none(), "datacenter should be null");
    assert!(min_cols[4].value.is_none(), "region should be null");
    assert_eq!(col_string(&min_cols[5]), "node-1");
    assert_eq!(col_int(&min_cols[6]), 42);
    assert_eq!(col_int(&min_cols[7]), 1000);
}

#[test]
fn test_multi_row_extracts_first_and_last() {
    // Rows are pre-sorted by metric_name, then service, then timestamp.
    let batch = build_sorted_batch(&[
        SortedRow::new("cpu.usage", Some("api"), Some("node-1"), 10, 100),
        SortedRow::new("cpu.usage", Some("api"), Some("node-1"), 10, 200),
        SortedRow::new("cpu.usage", Some("web"), Some("node-2"), 20, 300),
        SortedRow::new("mem.usage", Some("api"), Some("node-3"), 30, 400),
    ]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;

    // Min = first row: cpu.usage / api / node-1 / ts_id=10 / ts=100
    assert_eq!(col_string(&min_cols[0]), "cpu.usage");
    assert_eq!(col_string(&min_cols[1]), "api");
    assert_eq!(col_string(&min_cols[5]), "node-1");
    assert_eq!(col_int(&min_cols[6]), 10);
    assert_eq!(col_int(&min_cols[7]), 100);

    // Max = last row: mem.usage / api / node-3 / ts_id=30 / ts=400
    assert_eq!(col_string(&max_cols[0]), "mem.usage");
    assert_eq!(col_string(&max_cols[1]), "api");
    assert_eq!(col_string(&max_cols[5]), "node-3");
    assert_eq!(col_int(&max_cols[6]), 30);
    assert_eq!(col_int(&max_cols[7]), 400);
}

#[test]
fn test_null_columns_encoded_as_none() {
    // First row has service, last row has null service.
    let batch = build_sorted_batch(&[
        SortedRow::new("cpu.usage", Some("api"), None, 10, 100),
        SortedRow::new("cpu.usage", None, None, 20, 200),
    ]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;

    // Min row: service = "api"
    assert_eq!(col_string(&min_cols[1]), "api");

    // Max row: service = null
    assert!(
        max_cols[1].value.is_none(),
        "null service must be encoded as None"
    );

    // Both rows: host = null
    assert!(min_cols[5].value.is_none(), "min host should be null");
    assert!(max_cols[5].value.is_none(), "max host should be null");
}

#[test]
fn test_all_inclusive_max_equals_max() {
    let batch = build_sorted_batch(&[
        SortedRow::new("cpu.usage", Some("api"), None, 10, 100),
        SortedRow::new("mem.usage", Some("web"), None, 20, 200),
    ]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    // No multi-valued columns, so all_inclusive_max == max.
    assert_eq!(
        row_keys.all_inclusive_max_row_values, row_keys.max_row_values,
        "all_inclusive_max must equal max when no multi-values exist"
    );
}

#[test]
fn test_column_count_matches_sort_schema() {
    let batch = build_sorted_batch(&[SortedRow::new("cpu.usage", Some("api"), None, 10, 100)]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;

    // Sort schema has 8 columns.
    assert_eq!(
        min_cols.len(),
        8,
        "must have one ColumnValue per sort column"
    );
    assert_eq!(max_cols.len(), 8);
}

#[test]
fn test_missing_batch_columns_encoded_as_none() {
    // Batch only has metric_name and timestamp_secs (no tags at all).
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            create_dict_array(&["cpu.usage"]),
            Arc::new(UInt8Array::from(vec![0u8])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100u64])) as ArrayRef,
            Arc::new(Float64Array::from(vec![42.0])) as ArrayRef,
        ],
    )
    .unwrap();

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;

    // metric_name present, service/env/datacenter/region/host/timeseries_id missing.
    assert_eq!(col_string(&min_cols[0]), "cpu.usage");
    assert!(min_cols[1].value.is_none(), "service not in batch → None");
    assert!(min_cols[2].value.is_none(), "env not in batch → None");
    assert!(
        min_cols[6].value.is_none(),
        "timeseries_id not in batch → None"
    );
    // timestamp_secs is present.
    assert_eq!(col_int(&min_cols[7]), 100);
}

// -----------------------------------------------------------------------
// Proto serialization round-trip
// -----------------------------------------------------------------------

#[test]
fn test_proto_round_trip() {
    let batch = build_sorted_batch(&[
        SortedRow::new("cpu.usage", Some("api"), Some("node-1"), 42, 100),
        SortedRow::new("mem.usage", Some("web"), None, 99, 500),
    ]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    // Serialize to proto bytes.
    let proto_bytes = encode_row_keys_proto(&row_keys);
    assert!(!proto_bytes.is_empty());

    // Deserialize back.
    let recovered: RowKeys =
        prost::Message::decode(proto_bytes.as_slice()).expect("proto decode must succeed");
    assert_eq!(recovered, row_keys, "proto round-trip must be lossless");
}

#[test]
fn test_proto_round_trip_with_nulls() {
    let batch = build_sorted_batch(&[
        SortedRow::new("cpu.usage", None, None, 10, 100),
        SortedRow::new("cpu.usage", None, None, 10, 200),
    ]);

    let row_keys = extract_row_keys(METRICS_SORT_FIELDS, &batch)
        .unwrap()
        .unwrap();

    let proto_bytes = encode_row_keys_proto(&row_keys);
    let recovered: RowKeys = prost::Message::decode(proto_bytes.as_slice()).unwrap();

    // Null ColumnValues survive the round-trip.
    let min_cols = &recovered.min_row_values.as_ref().unwrap().column;
    assert!(
        min_cols[1].value.is_none(),
        "null service survives round-trip"
    );
    assert!(min_cols[5].value.is_none(), "null host survives round-trip");
}

// -----------------------------------------------------------------------
// Full pipeline: write through ParquetWriter, read back, verify
// -----------------------------------------------------------------------

#[test]
fn test_row_keys_in_parquet_kv_metadata() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::storage::{ParquetWriter, ParquetWriterConfig};
    use crate::table_config::TableConfig;

    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config, &TableConfig::default());

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("service", dict_type.clone(), true),
        Field::new("host", dict_type.clone(), true),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    // Unsorted input — the writer will sort it.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            create_dict_array(&["mem.usage", "cpu.usage", "cpu.usage"]),
            Arc::new(UInt8Array::from(vec![0u8; 3])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![300u64, 100, 200])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            create_nullable_dict_array(&[Some("web"), Some("api"), Some("api")]),
            create_nullable_dict_array(&[None, Some("node-1"), None]),
            Arc::new(Int64Array::from(vec![30i64, 10, 20])) as ArrayRef,
        ],
    )
    .unwrap();

    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join("test_row_keys_kv.parquet");
    let (_, row_keys_proto) = writer
        .write_to_file_with_metadata(&batch, &path, None)
        .unwrap();

    // Verify row_keys were returned.
    let proto_bytes = row_keys_proto.expect("row_keys must be returned from write");
    let row_keys: RowKeys = prost::Message::decode(proto_bytes.as_slice()).unwrap();

    // After sorting by metric_name|service|...|timestamp_secs:
    // First row = cpu.usage/api (smallest metric + service)
    // Last row  = mem.usage/web
    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;
    assert_eq!(col_string(&min_cols[0]), "cpu.usage");
    assert_eq!(col_string(&max_cols[0]), "mem.usage");

    // Verify row_keys are in the Parquet file's KV metadata.
    let file = std::fs::File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_meta = reader.metadata().file_metadata();
    let kv_meta = file_meta
        .key_value_metadata()
        .expect("must have KV metadata");

    let row_keys_entry = kv_meta
        .iter()
        .find(|kv| kv.key == "qh.row_keys")
        .expect("qh.row_keys must be in Parquet KV metadata");
    assert!(
        row_keys_entry.value.is_some(),
        "qh.row_keys value must not be empty"
    );

    // Decode the base64 and verify it matches what the write returned.
    let b64_value = row_keys_entry.value.as_ref().unwrap();
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64_value)
        .expect("qh.row_keys must be valid base64");
    let parquet_row_keys: RowKeys = prost::Message::decode(decoded.as_slice()).unwrap();
    assert_eq!(parquet_row_keys, row_keys);

    // Verify human-readable JSON is also present.
    let json_entry = kv_meta
        .iter()
        .find(|kv| kv.key == "qh.row_keys_json")
        .expect("qh.row_keys_json must be in Parquet KV metadata");
    assert!(json_entry.value.is_some());

    std::fs::remove_file(&path).ok();
}

// -----------------------------------------------------------------------
// Split writer integration
// -----------------------------------------------------------------------

#[test]
fn test_split_writer_populates_row_keys_proto() {
    use crate::storage::ParquetWriterConfig;
    use crate::table_config::TableConfig;

    let config = ParquetWriterConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let split_writer =
        crate::storage::ParquetSplitWriter::new(config, temp_dir.path(), &TableConfig::default());

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("service", dict_type.clone(), true),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            create_dict_array(&["cpu.usage", "mem.usage"]),
            Arc::new(UInt8Array::from(vec![0u8; 2])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100u64, 200])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
            create_nullable_dict_array(&[Some("api"), Some("web")]),
            Arc::new(Int64Array::from(vec![10i64, 20])) as ArrayRef,
        ],
    )
    .unwrap();

    let split = split_writer.write_split(&batch, "test-index").unwrap();

    // The metadata must have row_keys_proto populated.
    let proto_bytes = split
        .metadata
        .row_keys_proto
        .as_ref()
        .expect("row_keys_proto must be set by split writer");

    let row_keys: RowKeys = prost::Message::decode(proto_bytes.as_slice()).unwrap();
    let min_cols = &row_keys.min_row_values.as_ref().unwrap().column;
    let max_cols = &row_keys.max_row_values.as_ref().unwrap().column;

    // After sorting: first = cpu.usage, last = mem.usage
    assert_eq!(col_string(&min_cols[0]), "cpu.usage");
    assert_eq!(col_string(&max_cols[0]), "mem.usage");
}
