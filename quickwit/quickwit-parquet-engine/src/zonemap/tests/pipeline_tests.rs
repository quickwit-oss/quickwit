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

//! Pipeline integration tests: ParquetWriter → KV metadata → SplitWriter.
//!
//! Verifies that zonemap regexes are correctly extracted, stored in Parquet
//! KV metadata, and propagated to ParquetSplitMetadata.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt8Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::split::ParquetSplitKind;
use crate::storage::{
    PARQUET_META_ZONEMAP_REGEXES, ParquetSplitWriter, ParquetWriter, ParquetWriterConfig,
};
use crate::table_config::TableConfig;
use crate::test_helpers::{create_dict_array, create_nullable_dict_array};

/// Create a batch suitable for writing through the full pipeline.
fn create_pipeline_test_batch() -> RecordBatch {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("service", dict_type.clone(), true),
        Field::new("env", dict_type.clone(), true),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_names: ArrayRef = create_dict_array(&["cpu.usage", "memory.used", "cpu.usage"]);
    let services: ArrayRef = create_nullable_dict_array(&[Some("web"), Some("api"), Some("web")]);
    let envs: ArrayRef = create_nullable_dict_array(&[Some("prod"), Some("staging"), Some("prod")]);
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; 3]));
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![100u64, 200, 300]));
    let value: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    let timeseries_id: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));

    RecordBatch::try_new(
        schema,
        vec![
            metric_names,
            services,
            envs,
            metric_type,
            timestamp_secs,
            value,
            timeseries_id,
        ],
    )
    .unwrap()
}

#[test]
fn test_zonemap_in_parquet_kv_metadata() {
    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config, &TableConfig::default()).unwrap();

    let batch = create_pipeline_test_batch();
    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join("test_zonemap_kv.parquet");

    let (_size, write_meta) = writer
        .write_to_file_with_metadata(&batch, &path, None)
        .unwrap();
    let (_row_keys, zonemap_regexes): (Option<Vec<u8>>, HashMap<String, String>) = write_meta;

    // Verify regexes were returned from the write.
    assert!(
        !zonemap_regexes.is_empty(),
        "zonemap_regexes should be populated"
    );
    assert!(zonemap_regexes.contains_key("metric_name"));
    assert!(zonemap_regexes.contains_key("service"));
    assert!(zonemap_regexes.contains_key("env"));

    // Verify the regexes are valid patterns.
    for (col_name, regex_str) in &zonemap_regexes {
        assert!(
            regex_str.starts_with('^'),
            "regex for {} should start with ^: {}",
            col_name,
            regex_str
        );
        assert!(
            regex_str.ends_with('$'),
            "regex for {} should end with $: {}",
            col_name,
            regex_str
        );
    }

    // Verify the regexes appear in the Parquet file's KV metadata.
    let file = std::fs::File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();
    let kv_metadata = file_metadata
        .key_value_metadata()
        .expect("file should have kv_metadata");

    let zonemap_kv = kv_metadata
        .iter()
        .find(|kv| kv.key == PARQUET_META_ZONEMAP_REGEXES)
        .expect("qh.zonemap_regexes should be present in KV metadata");

    let kv_value = zonemap_kv.value.as_ref().expect("value should be set");
    let deserialized: HashMap<String, String> =
        serde_json::from_str(kv_value).expect("should be valid JSON");

    assert_eq!(deserialized.len(), zonemap_regexes.len());
    for (col, regex_str) in &zonemap_regexes {
        assert_eq!(
            deserialized.get(col).unwrap(),
            regex_str,
            "KV regex for {} should match",
            col
        );
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_zonemap_in_write_to_bytes() {
    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config, &TableConfig::default()).unwrap();

    let batch = create_pipeline_test_batch();
    let (bytes, write_meta) = writer.write_to_bytes(&batch, None).unwrap();
    let (_row_keys, zonemap_regexes): (Option<Vec<u8>>, HashMap<String, String>) = write_meta;

    assert!(bytes.len() > 4);
    assert_eq!(&bytes[0..4], b"PAR1");
    assert!(
        !zonemap_regexes.is_empty(),
        "zonemap_regexes should be populated from write_to_bytes"
    );
    assert!(zonemap_regexes.contains_key("metric_name"));
}

#[test]
fn test_zonemap_in_split_writer() {
    let config = ParquetWriterConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();

    let split_writer = ParquetSplitWriter::new(
        ParquetSplitKind::Metrics,
        config,
        temp_dir.path(),
        &TableConfig::default(),
    )
    .unwrap();

    let batch = create_pipeline_test_batch();
    let split = split_writer.write_split(&batch, "test-index").unwrap();

    // Verify zonemap regexes are stored in ParquetSplitMetadata.
    assert!(!split.zonemap_regexes.is_empty());
    assert!(split.zonemap_regexes.contains_key("metric_name"));
    assert!(split.zonemap_regexes.contains_key("service"));
    assert!(split.zonemap_regexes.contains_key("env"));

    let metric_regex = &split.zonemap_regexes["metric_name"];
    assert!(
        metric_regex.contains("cpu") || metric_regex.contains(".+"),
        "metric_name regex should reference cpu values: {}",
        metric_regex
    );

    // Verify the Parquet file itself also has the KV metadata.
    let parquet_path = temp_dir.path().join(split.parquet_filename());
    let file = std::fs::File::open(&parquet_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();
    let kv_metadata = file_metadata
        .key_value_metadata()
        .expect("parquet file should have kv_metadata");

    let has_zonemap = kv_metadata
        .iter()
        .any(|kv| kv.key == PARQUET_META_ZONEMAP_REGEXES);
    assert!(
        has_zonemap,
        "Parquet KV metadata should contain qh.zonemap_regexes"
    );
}

#[test]
fn test_zonemap_metadata_json_roundtrip() {
    let config = ParquetWriterConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();

    let split_writer = ParquetSplitWriter::new(
        ParquetSplitKind::Metrics,
        config,
        temp_dir.path(),
        &TableConfig::default(),
    )
    .unwrap();

    let batch = create_pipeline_test_batch();
    let split = split_writer.write_split(&batch, "test-index").unwrap();

    // Serialize metadata to JSON and back.
    let json = serde_json::to_string(&split).unwrap();
    let recovered: crate::split::ParquetSplitMetadata = serde_json::from_str(&json).unwrap();

    assert_eq!(
        recovered.zonemap_regexes, split.zonemap_regexes,
        "zonemap_regexes should survive JSON round-trip"
    );
}

#[test]
fn test_zonemap_consistency_across_write_paths() {
    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config.clone(), &TableConfig::default()).unwrap();

    let batch = create_pipeline_test_batch();

    // Write to bytes.
    let (_bytes, meta1) = writer.write_to_bytes(&batch, None).unwrap();
    let (_rk1, zm1): (Option<Vec<u8>>, HashMap<String, String>) = meta1;

    // Write to file.
    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join("test_zonemap_consistency.parquet");
    let (_size, meta2) = writer
        .write_to_file_with_metadata(&batch, &path, None)
        .unwrap();
    let (_rk2, zm2): (Option<Vec<u8>>, HashMap<String, String>) = meta2;

    assert_eq!(
        zm1, zm2,
        "write_to_bytes and write_to_file should produce identical zonemaps"
    );

    std::fs::remove_file(&path).ok();
}
