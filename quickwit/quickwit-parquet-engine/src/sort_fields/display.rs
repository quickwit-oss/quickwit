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

//! Sort schema to string serialization -- direct port of Go `SchemaToString` and
//! `SchemaToStringShort`.
//!
//! The proto `SortColumn.name` stores the bare Parquet column name (no type suffix).
//! These functions reconstruct the Husky-format suffixed name for serialization
//! using `SortColumn.column_type` to determine the suffix.

use quickwit_proto::sortschema::{SortColumn, SortColumnDirection, SortSchema};

use super::column_type::{ColumnTypeId, default_is_descending, default_type_for_name};

fn direction_str(sort_direction: i32) -> &'static str {
    match SortColumnDirection::try_from(sort_direction) {
        Ok(SortColumnDirection::SortDirectionAscending) => ":+",
        Ok(SortColumnDirection::SortDirectionDescending) => ":-",
        _ => ":???",
    }
}

fn type_str(column_type: u64) -> &'static str {
    match ColumnTypeId::try_from(column_type) {
        Ok(ct) => ct.as_str(),
        Err(_) => "unknown",
    }
}

/// Reconstruct the column name for the Husky string format.
///
/// Only appends the type suffix when the column's type differs from the
/// default for its bare name. This keeps the string short and readable:
///   `metric_name` (default String) → no suffix needed
///   `timestamp` (default Int64) → no suffix needed
///   `my_counter__i` → suffix needed (Int64 differs from default String)
fn display_name(col: &SortColumn) -> String {
    let col_type = match ColumnTypeId::try_from(col.column_type) {
        Ok(ct) => ct,
        Err(_) => return col.name.clone(),
    };
    let default_type = default_type_for_name(&col.name);
    if col_type == default_type {
        col.name.clone()
    } else {
        format!("{}{}", col.name, col_type.suffix())
    }
}

/// Convert a `SortSchema` to its full string representation.
///
/// Format: `[name=]column__suffix:type:+/-[|...][/V#]`
///
/// Direct port of Go `SchemaToString`.
pub fn schema_to_string(schema: &SortSchema) -> String {
    schema_to_string_inner(schema, true)
}

/// Convert a `SortSchema` to its short string representation.
///
/// Format: `[name=]column__suffix[|...][/V#]`
///
/// Omits the explicit type and skips the sort direction when it matches the
/// default (ascending for non-timestamp, descending for timestamp).
///
/// Direct port of Go `SchemaToStringShort`.
pub fn schema_to_string_short(schema: &SortSchema) -> String {
    schema_to_string_inner(schema, false)
}

/// Shared implementation for both full and short schema string formats.
///
/// When `verbose` is true, includes the explicit type and always emits direction.
/// When `verbose` is false, omits type and skips direction when it matches the default.
fn schema_to_string_inner(schema: &SortSchema, verbose: bool) -> String {
    let mut rv = String::new();

    if !schema.name.is_empty() {
        rv.push_str(&schema.name);
        rv.push('=');
    }

    for (i, col) in schema.column.iter().enumerate() {
        if i > 0 {
            rv.push('|');
        }
        if schema.lsm_comparison_cutoff > 0 && i == schema.lsm_comparison_cutoff as usize {
            rv.push('&');
        }
        rv.push_str(&display_name(col));

        if verbose {
            rv.push(':');
            rv.push_str(type_str(col.column_type));
            rv.push_str(direction_str(col.sort_direction));
        } else {
            let is_default_direction = if default_is_descending(&col.name) {
                col.sort_direction == SortColumnDirection::SortDirectionDescending as i32
            } else {
                col.sort_direction == SortColumnDirection::SortDirectionAscending as i32
            };
            if !is_default_direction {
                rv.push_str(direction_str(col.sort_direction));
            }
        }
    }

    if schema.sort_version > 0 {
        rv.push_str(&format!("/V{}", schema.sort_version));
    }

    rv
}
