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

//! Zonemap regex generation for Parquet split metadata.
//!
//! A zonemap stores a compact superset regex per sort-schema column. At query
//! time the regex can prune entire splits whose column values cannot match the
//! predicate — analogous to Parquet min/max statistics but for string columns
//! where ranges are less useful.
//!
//! Ported from Go: `logs-event-store/zonemap/`.

pub(crate) mod automaton;
pub mod minmax;
pub(crate) mod regex_builder;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use anyhow::Result;
use arrow::array::{Array, DictionaryArray, LargeStringArray, StringArray};
use arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::record_batch::RecordBatch;
pub use minmax::{MinMax, MinMaxBuilder};
pub use regex_builder::SupersetRegex;

use crate::sort_fields::parse_sort_fields;

/// Default maximum number of transitions in the regex automaton.
/// Bounds the regex size: regex length ≤ 6 * max_transitions + 4.
pub const DEFAULT_SUPERSET_REGEX_MAX_SIZE: i32 = 64;

/// Default number of registrations between periodic pruning passes.
pub const DEFAULT_PRUNE_EVERY: usize = 1000;

/// Default multiplier for periodic pruning (keeps more transitions during
/// building to avoid local minima, then prunes harder at build time).
pub const DEFAULT_MULTIPLIER: f32 = 2.0;

/// Options controlling zonemap regex generation.
#[derive(Debug, Clone)]
pub struct ZonemapOptions {
    /// Max transitions in the final regex automaton.
    pub superset_regex_max_size: i32,
    /// Values to register before periodic pruning (0 = no periodic pruning).
    pub prune_every: usize,
    /// Multiplier for periodic pruning transitions.
    pub multiplier: f32,
}

impl Default for ZonemapOptions {
    fn default() -> Self {
        ZonemapOptions {
            superset_regex_max_size: DEFAULT_SUPERSET_REGEX_MAX_SIZE,
            prune_every: DEFAULT_PRUNE_EVERY,
            multiplier: DEFAULT_MULTIPLIER,
        }
    }
}

/// Extract zonemap regexes for string-valued sort schema columns.
///
/// For each string column in the sort schema that is present in the batch,
/// builds a prefix-preserving superset regex from the column's distinct values.
///
/// Returns a map of `column_name → regex_string`. Empty map if no string
/// columns or batch is empty.
///
/// # Errors
///
/// Returns an error if the sort fields string cannot be parsed.
pub fn extract_zonemap_regexes(
    sort_fields_str: &str,
    batch: &RecordBatch,
    opts: &ZonemapOptions,
) -> Result<HashMap<String, String>> {
    if batch.num_rows() == 0 || opts.superset_regex_max_size <= 0 || sort_fields_str.is_empty() {
        return Ok(HashMap::new());
    }

    let sort_schema = parse_sort_fields(sort_fields_str)?;
    let batch_schema = batch.schema();
    let mut regexes = HashMap::new();
    let mut builder = regex_builder::PrefixPreservingRegexBuilder::new();

    // Respect LSM comparison cutoff: only columns before the cutoff get
    // zonemaps, matching the Go FragmentZoneMapBuilder.Reset() behavior.
    let cutoff = sort_schema.lsm_comparison_cutoff as usize;
    let columns = if cutoff > 0 && cutoff < sort_schema.column.len() {
        &sort_schema.column[..cutoff]
    } else {
        &sort_schema.column
    };

    for col_def in columns {
        let batch_idx = match batch_schema.index_of(&col_def.name) {
            Ok(idx) => idx,
            Err(_) => continue,
        };

        let col = batch.column(batch_idx);
        if !is_string_column(col.data_type()) {
            continue;
        }

        builder.reset(
            opts.superset_regex_max_size,
            opts.prune_every,
            opts.multiplier,
        );

        register_string_values(col.as_ref(), &mut builder);
        let superset = builder.build();

        if !superset.regex.is_empty() {
            regexes.insert(col_def.name.clone(), superset.regex);
        }
    }

    Ok(regexes)
}

/// Generate a superset regex from a list of strings.
///
/// Convenience function for testing and CLI tools.
pub fn generate_regex_from_strings(
    values: &[&str],
    max_transitions: i32,
    prune_every: usize,
    multiplier: f32,
) -> String {
    let mut builder = regex_builder::PrefixPreservingRegexBuilder::new();
    let effective_max = if max_transitions > 0 {
        max_transitions
    } else {
        DEFAULT_SUPERSET_REGEX_MAX_SIZE
    };
    builder.reset(effective_max, prune_every, multiplier);
    for value in values {
        builder.register(value);
    }
    builder.build().regex
}

/// Check if an Arrow data type represents a string column.
fn is_string_column(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Dictionary(_, _)
                if is_string_dict(dt)
    )
}

/// Check if a Dictionary data type has a string value type.
fn is_string_dict(dt: &DataType) -> bool {
    match dt {
        DataType::Dictionary(_, value_type) => {
            matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
        }
        DataType::Utf8 | DataType::LargeUtf8 => true,
        _ => false,
    }
}

/// Register all non-null string values from an Arrow column into the builder.
///
/// Handles `Utf8`, `LargeUtf8`, and `Dictionary(K, Utf8|LargeUtf8)` for all
/// Arrow dictionary key types (Int8 through UInt64).
fn register_string_values(
    array: &dyn Array,
    builder: &mut regex_builder::PrefixPreservingRegexBuilder,
) {
    /// Register string values from a dictionary array with a specific key type.
    macro_rules! register_dict {
        ($array:expr, $builder:expr, $key_type:ty) => {
            if let Some(dict) = $array.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                let values = dict.values();
                if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                    for i in 0..str_values.len() {
                        if !str_values.is_null(i) {
                            $builder.register(str_values.value(i));
                        }
                    }
                } else if let Some(str_values) = values.as_any().downcast_ref::<LargeStringArray>()
                {
                    for i in 0..str_values.len() {
                        if !str_values.is_null(i) {
                            $builder.register(str_values.value(i));
                        }
                    }
                }
            }
        };
    }

    match array.data_type() {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => register_dict!(array, builder, Int8Type),
            DataType::Int16 => register_dict!(array, builder, Int16Type),
            DataType::Int32 => register_dict!(array, builder, Int32Type),
            DataType::Int64 => register_dict!(array, builder, Int64Type),
            DataType::UInt8 => register_dict!(array, builder, UInt8Type),
            DataType::UInt16 => register_dict!(array, builder, UInt16Type),
            DataType::UInt32 => register_dict!(array, builder, UInt32Type),
            DataType::UInt64 => register_dict!(array, builder, UInt64Type),
            _ => {}
        },
        DataType::Utf8 => {
            if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..str_array.len() {
                    if !str_array.is_null(i) {
                        builder.register(str_array.value(i));
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            if let Some(str_array) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..str_array.len() {
                    if !str_array.is_null(i) {
                        builder.register(str_array.value(i));
                    }
                }
            }
        }
        _ => {}
    }
}
