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

//! RowKeys extraction from sorted RecordBatches.
//!
//! RowKeys define the sort-key boundaries of a split file: the values of each
//! sort column at the first and last rows. The compactor uses these to
//! determine key-range overlap between splits and to select merge boundaries.
//!
//! The proto format (`sortschema::RowKeys`) is defined in
//! `event_store_sortschema.proto` and stored as base64-encoded bytes in
//! Parquet key_value_metadata and in the PostgreSQL metastore.

#[cfg(test)]
mod tests;

use anyhow::Result;
use arrow::array::{Array, DictionaryArray, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use quickwit_proto::sortschema::{ColumnValue, ColumnValues, RowKeys, column_value};

use crate::sort_fields::parse_sort_fields;

/// Extract [`RowKeys`] from a **sorted** [`RecordBatch`].
///
/// Reads the sort schema columns at row 0 (min) and the last row (max),
/// building a `RowKeys` proto with `min_row_values`, `max_row_values`,
/// and `all_inclusive_max_row_values`.
///
/// Returns `None` if the batch is empty.
///
/// # Errors
///
/// Returns an error if the sort fields string cannot be parsed.
pub fn extract_row_keys(
    sort_fields_str: &str,
    sorted_batch: &RecordBatch,
) -> Result<Option<RowKeys>> {
    if sorted_batch.num_rows() == 0 {
        return Ok(None);
    }

    let sort_schema = parse_sort_fields(sort_fields_str)?;
    let batch_schema = sorted_batch.schema();

    let first_row = 0;
    let last_row = sorted_batch.num_rows() - 1;

    let mut min_values = Vec::with_capacity(sort_schema.column.len());
    let mut max_values = Vec::with_capacity(sort_schema.column.len());

    for col_def in &sort_schema.column {
        let batch_idx = match batch_schema.index_of(&col_def.name) {
            Ok(idx) => idx,
            Err(_) => {
                // Column not in batch — encode as empty ColumnValue (None value).
                min_values.push(ColumnValue { value: None });
                max_values.push(ColumnValue { value: None });
                continue;
            }
        };

        let col = sorted_batch.column(batch_idx);
        min_values.push(extract_column_value(col.as_ref(), first_row));
        max_values.push(extract_column_value(col.as_ref(), last_row));
    }

    let min_row = ColumnValues { column: min_values };
    let max_row = ColumnValues {
        column: max_values.clone(),
    };

    Ok(Some(RowKeys {
        min_row_values: Some(min_row),
        max_row_values: Some(max_row.clone()),
        // No multi-valued columns in our schema, so all-inclusive max
        // equals max.
        all_inclusive_max_row_values: Some(max_row),
        expired: false,
    }))
}

/// Serialize a [`RowKeys`] proto to bytes.
pub fn encode_row_keys_proto(row_keys: &RowKeys) -> Vec<u8> {
    prost::Message::encode_to_vec(row_keys)
}

/// Extract a single [`ColumnValue`] from an Arrow column at the given row.
///
/// Maps Arrow types to the proto's oneof:
/// - `Dictionary(Int32, Utf8)` / `Utf8` → `TypeString` (bytes)
/// - `Int64` → `TypeInt`
/// - `UInt64` → `TypeInt` (cast to i64)
/// - `Float64` → `TypeFloat`
///
/// Null values produce a `ColumnValue` with `value: None`.
fn extract_column_value(array: &dyn Array, row: usize) -> ColumnValue {
    if array.is_null(row) {
        return ColumnValue { value: None };
    }

    let value = match array.data_type() {
        DataType::Dictionary(_, _) => extract_dict_string(array, row)
            .map(|s| column_value::Value::TypeString(s.as_bytes().to_vec())),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| column_value::Value::TypeString(a.value(row).as_bytes().to_vec())),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| column_value::Value::TypeInt(a.value(row))),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| column_value::Value::TypeInt(a.value(row) as i64)),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .map(|a| column_value::Value::TypeFloat(a.value(row))),
        _ => None,
    };

    ColumnValue { value }
}

/// Extract a string from a Dictionary(Int32, Utf8) column.
fn extract_dict_string(array: &dyn Array, row: usize) -> Option<&str> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()?;
    let key_idx = dict.keys().value(row) as usize;
    let values = dict.values();
    let str_values = values.as_any().downcast_ref::<StringArray>()?;
    Some(str_values.value(key_idx))
}
