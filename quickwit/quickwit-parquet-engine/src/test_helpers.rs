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

//! Shared test helpers for building Arrow RecordBatches in unit tests.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

/// Creates a dictionary-encoded string array with compact 0-based keys.
pub fn create_dict_array(values: &[&str]) -> ArrayRef {
    let keys: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
    let string_array = StringArray::from(values.to_vec());
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
            .unwrap(),
    )
}

/// Creates a nullable dictionary-encoded string array.
///
/// Each `Some(value)` gets a key into the dictionary; `None` values produce
/// a null key.
pub fn create_nullable_dict_array(values: &[Option<&str>]) -> ArrayRef {
    let mut unique_values: Vec<&str> = Vec::new();
    let keys: Vec<Option<i32>> = values
        .iter()
        .map(|v| {
            v.map(|s| {
                if let Some(pos) = unique_values.iter().position(|&u| u == s) {
                    pos as i32
                } else {
                    let pos = unique_values.len();
                    unique_values.push(s);
                    pos as i32
                }
            })
        })
        .collect();
    let string_array = StringArray::from(unique_values);
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
            .unwrap(),
    )
}

/// Creates a RecordBatch with the 4 required fields plus the specified
/// nullable dictionary-encoded tag columns.
///
/// - `metric_name`: all rows set to `"cpu.usage"`
/// - `metric_type`: all rows `0` (Gauge)
/// - `timestamp_secs`: sequential, starting at `100`
/// - `value`: sequential `f64` starting at `42.0`
/// - each tag column: all rows set to the column name as the value
pub fn create_test_batch_with_tags(num_rows: usize, tags: &[&str]) -> RecordBatch {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let mut fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ];
    for tag in tags {
        fields.push(Field::new(*tag, dict_type.clone(), true));
    }
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_names: Vec<&str> = vec!["cpu.usage"; num_rows];
    let metric_name: ArrayRef = create_dict_array(&metric_names);
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
    let timestamps: Vec<u64> = (0..num_rows).map(|i| 100 + i as u64).collect();
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
    let values: Vec<f64> = (0..num_rows).map(|i| 42.0 + i as f64).collect();
    let value: ArrayRef = Arc::new(Float64Array::from(values));

    let mut columns: Vec<ArrayRef> = vec![metric_name, metric_type, timestamp_secs, value];
    for tag in tags {
        let tag_values: Vec<Option<&str>> = vec![Some(tag); num_rows];
        columns.push(create_nullable_dict_array(&tag_values));
    }

    RecordBatch::try_new(schema, columns).unwrap()
}

/// Creates a RecordBatch with the 4 required fields and default tags
/// (`service`, `host`).
pub fn create_test_batch(num_rows: usize) -> RecordBatch {
    create_test_batch_with_tags(num_rows, &["service", "host"])
}
