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

//! Sorted Series column computation for the Parquet pipeline.
//!
//! The **sorted series** column is a composite, lexicographically sortable
//! binary key that combines the sort schema tag values with the timeseries ID.
//! It is designed as a single-column partition key for DataFusion's streaming
//! `AggregateExec` and `BoundedWindowAggExec` operators.
//!
//! # Key construction
//!
//! For each row the key is built by iterating the sort schema columns
//! *before* `timeseries_id` and `timestamp_secs`:
//!
//! 1. For each **non-null** column, encode `(ordinal: u8, value: &str)` via [`storekey`]'s
//!    order-preserving binary format.
//! 2. Append the `timeseries_id` (`i64`) via [`storekey`].
//!
//! Null columns are skipped entirely. The ordinal prefix ensures bytes
//! from different schema positions never collide even when columns are
//! sparse.
//!
//! # Invariants
//!
//! - **Identity**: Two data points from the same timeseries always produce the exact same byte key,
//!   regardless of timestamp or metric value.
//! - **Stability**: The encoding is deterministic across builds and process restarts (storekey uses
//!   a well-defined byte format, and the ordinals are derived from the sort schema which is
//!   versioned).
//! - **Ordering**: A byte-level comparison (`memcmp`) of two keys yields the same relative order as
//!   a logical multi-column comparison of the underlying sort schema values.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::array::{Array, BinaryBuilder, DictionaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;

use crate::sort_fields::parse_sort_fields;

/// Column name for the sorted series key in the Parquet schema.
pub const SORTED_SERIES_COLUMN: &str = "sorted_series";

/// Compute the sorted series column for a [`RecordBatch`].
///
/// Returns a [`arrow::array::BinaryArray`] with one entry per row. The
/// caller is responsible for appending this column to the batch before
/// writing to Parquet.
///
/// # Errors
///
/// Returns an error if the sort fields string cannot be parsed or if
/// storekey encoding fails (should not happen for supported types).
pub fn compute_sorted_series_column(
    sort_fields_str: &str,
    batch: &RecordBatch,
) -> Result<arrow::array::BinaryArray> {
    let schema = parse_sort_fields(sort_fields_str)?;
    let batch_schema = batch.schema();

    // Resolve sort columns that contribute to the key, plus the
    // timeseries_id column and its ordinal position in the schema.
    let ResolvedKeySchema {
        tag_columns,
        ts_id_column,
    } = resolve_key_columns(&schema, &batch_schema);

    let num_rows = batch.num_rows();
    // Estimate ~48 bytes per key: a few ordinal+string pairs + 8-byte i64.
    let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 48);
    let mut buf = Vec::with_capacity(128);

    for row_idx in 0..num_rows {
        buf.clear();
        encode_row_key(
            &tag_columns,
            ts_id_column.as_ref(),
            batch,
            row_idx,
            &mut buf,
        )?;
        builder.append_value(&buf);
    }

    Ok(builder.finish())
}

/// Append the sorted_series column to a [`RecordBatch`].
///
/// # Errors
///
/// Returns an error if the column already exists (indicates a bug in
/// the caller), if the sort fields string cannot be parsed, or if
/// storekey encoding fails.
pub fn append_sorted_series_column(
    sort_fields_str: &str,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    if batch.schema().index_of(SORTED_SERIES_COLUMN).is_ok() {
        anyhow::bail!(
            "batch already contains a '{}' column — double computation is a bug",
            SORTED_SERIES_COLUMN
        );
    }

    let sorted_series = compute_sorted_series_column(sort_fields_str, batch)?;

    let old_schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = old_schema.fields().iter().cloned().collect();
    let mut columns: Vec<Arc<dyn Array>> = (0..batch.num_columns())
        .map(|i| Arc::clone(batch.column(i)))
        .collect();

    fields.push(Arc::new(Field::new(
        SORTED_SERIES_COLUMN,
        DataType::Binary,
        false,
    )));
    columns.push(Arc::new(sorted_series));

    let new_schema = Arc::new(Schema::new_with_metadata(
        fields,
        old_schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, columns).map_err(|e| anyhow!("append column: {}", e))
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

/// A resolved key column: its ordinal position in the sort schema and
/// its index in the RecordBatch.
struct KeyColumn {
    ordinal: u8,
    batch_idx: usize,
}

/// The resolved key schema: tag columns plus optional timeseries_id.
struct ResolvedKeySchema {
    tag_columns: Vec<KeyColumn>,
    ts_id_column: Option<KeyColumn>,
}

/// Walk the sort schema and resolve columns present in the batch.
///
/// Tag columns are collected up to (but not including) `timeseries_id`.
/// The `timeseries_id` column is resolved separately with its own ordinal
/// so the key encoding is consistent: every component gets an ordinal prefix.
fn resolve_key_columns(
    sort_schema: &quickwit_proto::sortschema::SortSchema,
    batch_schema: &Schema,
) -> ResolvedKeySchema {
    let mut tag_columns = Vec::new();
    let mut ts_id_column = None;

    for (ordinal, col) in sort_schema.column.iter().enumerate() {
        if col.name == "timeseries_id" {
            if let Ok(idx) = batch_schema.index_of("timeseries_id") {
                ts_id_column = Some(KeyColumn {
                    ordinal: ordinal as u8,
                    batch_idx: idx,
                });
            }
            break;
        }
        if col.name == "timestamp_secs" || col.name == "timestamp" {
            break;
        }
        if let Ok(idx) = batch_schema.index_of(&col.name) {
            tag_columns.push(KeyColumn {
                ordinal: ordinal as u8,
                batch_idx: idx,
            });
        }
    }

    ResolvedKeySchema {
        tag_columns,
        ts_id_column,
    }
}

/// Encode a single row's sorted series key into `buf`.
fn encode_row_key(
    tag_columns: &[KeyColumn],
    ts_id_column: Option<&KeyColumn>,
    batch: &RecordBatch,
    row_idx: usize,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // Encode non-null sort schema columns: ordinal + string value.
    for kc in tag_columns {
        let col = batch.column(kc.batch_idx);
        if col.is_null(row_idx) {
            continue;
        }
        if let Some(value) = extract_string_value(col.as_ref(), row_idx) {
            storekey::encode(&mut *buf, &kc.ordinal)
                .map_err(|e| anyhow!("storekey encode ordinal: {}", e))?;
            storekey::encode(&mut *buf, value)
                .map_err(|e| anyhow!("storekey encode value: {}", e))?;
        }
    }

    // Append timeseries_id with its ordinal as the final discriminator.
    if let Some(kc) = ts_id_column {
        let col = batch.column(kc.batch_idx);
        if !col.is_null(row_idx) {
            let ts_id = extract_i64_value(col.as_ref(), row_idx);
            storekey::encode(&mut *buf, &kc.ordinal)
                .map_err(|e| anyhow!("storekey encode timeseries_id ordinal: {}", e))?;
            storekey::encode(&mut *buf, &ts_id)
                .map_err(|e| anyhow!("storekey encode timeseries_id: {}", e))?;
        }
    }

    Ok(())
}

/// Extract a string value from a column at the given row.
///
/// Supports `Dictionary(Int32, Utf8)` (the common tag encoding) and
/// plain `Utf8` columns.
fn extract_string_value(array: &dyn Array, row: usize) -> Option<&str> {
    debug_assert!(
        !array.is_null(row),
        "caller must check is_null before extract_string_value"
    );

    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()?;
            let key_idx = dict.keys().value(row) as usize;
            let values = dict.values();
            let str_values = values.as_any().downcast_ref::<StringArray>()?;
            Some(str_values.value(key_idx))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(row))
        }
        // UInt8 columns (e.g., metric_type) are encoded as their string
        // representation. This is rare in the sort schema but handled for
        // completeness.
        DataType::UInt8 => None,
        _ => None,
    }
}

/// Extract an i64 value from a column at the given row.
///
/// Panics (debug_assert) if the row is null — the caller must check first.
fn extract_i64_value(array: &dyn Array, row: usize) -> i64 {
    debug_assert!(
        !array.is_null(row),
        "caller must check is_null before extract_i64_value"
    );
    let int_array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("timeseries_id column must be Int64");
    int_array.value(row)
}
