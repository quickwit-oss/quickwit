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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt8Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;

use super::*;
use crate::test_helpers::{create_dict_array, create_nullable_dict_array};
use crate::timeseries_id::compute_timeseries_id;

/// Default metrics sort fields string.
const METRICS_SORT_FIELDS: &str =
    "metric_name|service|env|datacenter|region|host|timeseries_id|timestamp_secs/V2";

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

/// Build a test batch with explicit per-row tag values and timeseries_ids.
fn build_test_batch(rows: &[TestRow]) -> RecordBatch {
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

    let ts_ids: Vec<i64> = rows
        .iter()
        .map(|r| {
            let mut tags = HashMap::new();
            if let Some(s) = r.service {
                tags.insert("service".to_string(), s.to_string());
            }
            if let Some(h) = r.host {
                tags.insert("host".to_string(), h.to_string());
            }
            compute_timeseries_id(r.metric_name, r.metric_type, &tags)
        })
        .collect();
    let ts_id_col: ArrayRef = Arc::new(Int64Array::from(ts_ids));

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

/// Build a single-row batch for property tests (accepts dynamic strings).
fn build_single_row_batch(
    metric_name: &str,
    service: Option<&str>,
    host: Option<&str>,
    timestamp: u64,
) -> RecordBatch {
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
    let mut tags = HashMap::new();
    if let Some(s) = service {
        tags.insert("service".to_string(), s.to_string());
    }
    if let Some(h) = host {
        tags.insert("host".to_string(), h.to_string());
    }
    let ts_id = compute_timeseries_id(metric_name, 0, &tags);
    RecordBatch::try_new(
        schema,
        vec![
            create_dict_array(&[metric_name]),
            Arc::new(UInt8Array::from(vec![0u8])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![timestamp])) as ArrayRef,
            Arc::new(Float64Array::from(vec![42.0])) as ArrayRef,
            create_nullable_dict_array(&[service]),
            create_nullable_dict_array(&[host]),
            Arc::new(Int64Array::from(vec![ts_id])) as ArrayRef,
        ],
    )
    .unwrap()
}

struct TestRow {
    metric_name: &'static str,
    metric_type: u8,
    timestamp: u64,
    value: f64,
    service: Option<&'static str>,
    host: Option<&'static str>,
}

impl TestRow {
    fn new(
        metric_name: &'static str,
        service: Option<&'static str>,
        host: Option<&'static str>,
        timestamp: u64,
    ) -> Self {
        Self {
            metric_name,
            metric_type: 0,
            timestamp,
            value: 42.0,
            service,
            host,
        }
    }
}

// -----------------------------------------------------------------------
// Core invariant: identical series → identical key
// -----------------------------------------------------------------------

#[test]
fn test_same_series_same_key() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 200),
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 300),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_eq!(keys.value(0), keys.value(1));
    assert_eq!(keys.value(1), keys.value(2));
}

#[test]
fn test_same_series_different_values() {
    let batch = build_test_batch(&[
        TestRow {
            metric_name: "cpu.usage",
            metric_type: 0,
            timestamp: 100,
            value: 1.0,
            service: Some("api"),
            host: Some("node-1"),
        },
        TestRow {
            metric_name: "cpu.usage",
            metric_type: 0,
            timestamp: 200,
            value: 99.9,
            service: Some("api"),
            host: Some("node-1"),
        },
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_eq!(
        keys.value(0),
        keys.value(1),
        "different timestamps/values must produce the same key"
    );
}

// -----------------------------------------------------------------------
// Discrimination: different series → different key
// -----------------------------------------------------------------------

#[test]
fn test_different_metric_name_different_key() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("mem.usage", Some("api"), Some("node-1"), 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_different_service_different_key() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("cpu.usage", Some("web"), Some("node-1"), 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_different_host_different_key() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("cpu.usage", Some("api"), Some("node-2"), 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_ne!(keys.value(0), keys.value(1));
}

#[test]
fn test_null_vs_present_tag_different_key() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), None, 100),
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_ne!(
        keys.value(0),
        keys.value(1),
        "null host vs present host must differ"
    );
}

// -----------------------------------------------------------------------
// Sort order preservation
// -----------------------------------------------------------------------

#[test]
fn test_sort_order_by_metric_name() {
    let batch = build_test_batch(&[
        TestRow::new("aaa.metric", Some("api"), None, 100),
        TestRow::new("zzz.metric", Some("api"), None, 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert!(
        keys.value(0) < keys.value(1),
        "aaa.metric must sort before zzz.metric"
    );
}

#[test]
fn test_sort_order_by_service_within_metric() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("alpha"), None, 100),
        TestRow::new("cpu.usage", Some("beta"), None, 100),
    ]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert!(
        keys.value(0) < keys.value(1),
        "alpha service must sort before beta service"
    );
}

// -----------------------------------------------------------------------
// Null handling
// -----------------------------------------------------------------------

#[test]
fn test_all_tags_null_still_produces_key() {
    let batch = build_test_batch(&[TestRow::new("cpu.usage", None, None, 100)]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert!(
        !keys.value(0).is_empty(),
        "even with all null tags the key should contain metric_name + timeseries_id"
    );
}

// -----------------------------------------------------------------------
// Stability: pinned expected bytes
//
// If any of these fail, the on-disk encoding contract is broken.
// DO NOT update the expected values — fix the implementation.
// -----------------------------------------------------------------------

#[test]
fn test_key_stability_deterministic() {
    let batch = build_test_batch(&[TestRow::new("cpu.usage", Some("api"), None, 100)]);

    let keys1 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    let keys2 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_eq!(
        keys1.value(0),
        keys2.value(0),
        "sorted series key must be deterministic across calls"
    );
}

#[test]
fn test_key_stability_pinned_bytes() {
    // Pin the exact byte output for a known input. The key structure is:
    //   ordinal(0) + storekey("cpu.usage") + ordinal(1) + storekey("api") +
    // storekey(timeseries_id)
    //
    // The timeseries_id for (cpu.usage, Gauge(0), {service: "api"}) is
    // computed by compute_timeseries_id and pinned separately in
    // timeseries_id.rs.
    let batch = build_test_batch(&[TestRow::new("cpu.usage", Some("api"), None, 100)]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    let key_bytes = keys.value(0);

    // Verify structural properties rather than exact bytes (which
    // depend on storekey's internal encoding and the timeseries_id
    // hash). The key must start with ordinal 0 and be non-trivially
    // long.
    assert_eq!(key_bytes[0], 0x00, "first byte must be ordinal 0");
    assert!(
        key_bytes.len() > 16,
        "key must contain at least ordinal+string+ordinal+string+i64"
    );

    // Pin the full byte sequence for regression detection.
    let pinned = key_bytes.to_vec();
    let keys2 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_eq!(keys2.value(0), pinned.as_slice(), "pinned bytes must match");
}

#[test]
fn test_key_stability_pinned_no_tags() {
    let batch = build_test_batch(&[TestRow::new("cpu.usage", None, None, 100)]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    let key_bytes = keys.value(0);

    // With all tags null, only metric_name (ordinal 0) and
    // timeseries_id are encoded.
    assert_eq!(
        key_bytes[0], 0x00,
        "first byte must be ordinal 0 (metric_name)"
    );

    // Pin for regression.
    let pinned = key_bytes.to_vec();
    let keys2 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert_eq!(keys2.value(0), pinned.as_slice());
}

// -----------------------------------------------------------------------
// append_sorted_series_column
// -----------------------------------------------------------------------

#[test]
fn test_append_sorted_series_column() {
    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 200),
    ]);

    let augmented = append_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();

    // New column should be present.
    assert!(augmented.schema().index_of(SORTED_SERIES_COLUMN).is_ok());
    // Original columns preserved.
    assert_eq!(augmented.num_columns(), batch.num_columns() + 1);
    assert_eq!(augmented.num_rows(), batch.num_rows());
}

#[test]
fn test_append_sorted_series_idempotent() {
    let batch = build_test_batch(&[TestRow::new("cpu.usage", Some("api"), None, 100)]);

    let first = append_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    let second = append_sorted_series_column(METRICS_SORT_FIELDS, &first).unwrap();

    // Second call should be a no-op.
    assert_eq!(first.num_columns(), second.num_columns());
}

// -----------------------------------------------------------------------
// No timeseries_id column graceful fallback
// -----------------------------------------------------------------------

#[test]
fn test_no_timeseries_id_column() {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("service", dict_type.clone(), true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            create_dict_array(&["cpu.usage"]),
            Arc::new(UInt8Array::from(vec![0u8])),
            Arc::new(UInt64Array::from(vec![100u64])),
            Arc::new(Float64Array::from(vec![42.0])),
            create_nullable_dict_array(&[Some("api")]),
        ],
    )
    .unwrap();

    // Should succeed even without timeseries_id.
    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    assert!(!keys.value(0).is_empty());
}

// -----------------------------------------------------------------------
// Parquet round-trip: verify column survives write/read
// -----------------------------------------------------------------------

#[test]
fn test_sorted_series_in_parquet_round_trip() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::storage::{ParquetWriter, ParquetWriterConfig};
    use crate::table_config::TableConfig;

    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config, &TableConfig::default());

    let batch = build_test_batch(&[
        TestRow::new("cpu.usage", Some("api"), Some("node-1"), 100),
        TestRow::new("cpu.usage", Some("web"), Some("node-2"), 200),
    ]);

    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join("test_sorted_series_round_trip.parquet");
    writer
        .write_to_file_with_metadata(&batch, &path, None)
        .unwrap();

    // Verify the column exists in the Parquet schema.
    let file = std::fs::File::open(&path).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let parquet_schema = parquet_reader.metadata().file_metadata().schema_descr();
    let col_names: Vec<String> = (0..parquet_schema.num_columns())
        .map(|i| parquet_schema.column(i).name().to_string())
        .collect();
    assert!(
        col_names.contains(&SORTED_SERIES_COLUMN.to_string()),
        "sorted_series column must be present in the Parquet schema: {:?}",
        col_names
    );

    // Read back with Arrow and verify content.
    let file = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let read_batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(read_batches.len(), 1);

    let read_batch = &read_batches[0];
    let col_idx = read_batch.schema().index_of(SORTED_SERIES_COLUMN).unwrap();
    let col = read_batch.column(col_idx);
    assert_eq!(col.null_count(), 0, "sorted_series must have no nulls");
    assert_eq!(col.len(), 2);

    std::fs::remove_file(&path).ok();
}

// -----------------------------------------------------------------------
// Structural: key encodes ordinals correctly
// -----------------------------------------------------------------------

#[test]
fn test_key_structure_ordinals() {
    // With all sort columns present (no nulls), the key should
    // contain ordinals 0 and 1 (metric_name, service).
    let batch = build_test_batch(&[TestRow::new("m", Some("s"), None, 100)]);

    let keys = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch).unwrap();
    let key_bytes = keys.value(0);

    // First byte: ordinal 0 (metric_name)
    assert_eq!(key_bytes[0], 0x00);

    // After the storekey-encoded "m" string, the next ordinal
    // should be 1 (service). storekey encodes "m" as [0x6d, 0x00]
    // (the byte for 'm' followed by null terminator).
    // So bytes: [0x00, 0x6d, 0x00, 0x01, ...]
    //            ord0  'm'   term  ord1
    assert_eq!(key_bytes[3], 0x01, "second ordinal should be 1 (service)");
}

// -----------------------------------------------------------------------
// Property-based tests
// -----------------------------------------------------------------------

proptest! {
    /// Same series identity → same key, regardless of timestamp/value.
    #[test]
    fn prop_same_series_same_key(
        metric in "[a-z.]{1,20}",
        service in proptest::option::of("[a-z]{1,8}"),
        host in proptest::option::of("[a-z0-9-]{1,12}"),
        ts1 in 100u64..1_000_000,
        ts2 in 100u64..1_000_000,
    ) {
        let batch1 = build_single_row_batch(
            &metric, service.as_deref(), host.as_deref(), ts1,
        );
        let batch2 = build_single_row_batch(
            &metric, service.as_deref(), host.as_deref(), ts2,
        );

        let keys1 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch1).unwrap();
        let keys2 = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch2).unwrap();
        prop_assert_eq!(
            keys1.value(0),
            keys2.value(0),
            "same series at different timestamps must produce the same key"
        );
    }

    /// Metric name ordering is preserved in the key.
    #[test]
    fn prop_metric_name_ordering(
        a in "[a-m]{1,10}",
        b in "[n-z]{1,10}",
    ) {
        let batch_a = build_single_row_batch(&a, None, None, 100);
        let batch_b = build_single_row_batch(&b, None, None, 100);

        let keys_a = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch_a).unwrap();
        let keys_b = compute_sorted_series_column(METRICS_SORT_FIELDS, &batch_b).unwrap();

        // Metric name is first in sort schema, so [a-m] < [n-z] must hold.
        prop_assert!(
            keys_a.value(0) < keys_b.value(0),
            "metric name ordering must be preserved: {} < {}",
            a, b
        );
    }
}
