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

//! Union schema resolution, column alignment, and per-output schema
//! optimization for merge inputs and outputs.
//!
//! The internal "union schema" uses plain types (Utf8 for all string-like
//! columns) so that `take` works uniformly across concatenated inputs.
//! After permutation, each output file's schema is optimized based on the
//! actual data: all-null columns are stripped, and string columns are
//! dictionary-encoded when cardinality is low relative to row count.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::sort_fields::parse_sort_fields;
use crate::sorted_series::SORTED_SERIES_COLUMN;

/// Maximum ratio of distinct values to total rows for dictionary encoding.
/// If distinct/total <= this threshold, use Dictionary(Int32, Utf8).
const DICTIONARY_ENCODING_THRESHOLD: f64 = 0.5;

/// Align all input batches to a common union schema.
///
/// Returns the union schema and a vector of batches where every batch has
/// exactly the same schema. Missing columns are filled with null arrays.
///
/// The union schema uses plain types for internal alignment: all string-like
/// types (Utf8, LargeUtf8, Dictionary) are normalized to Utf8 so that
/// `take` works uniformly across concatenated inputs. The actual output
/// file types are determined later by [`optimize_output_batch`] based on
/// each output file's data characteristics.
///
/// Columns are ordered in "Husky order":
/// 1. Sort schema columns (in configured order)
/// 2. `sorted_series` column
/// 3. Remaining columns in alphabetical order
pub fn align_inputs_to_union_schema(
    inputs: &[RecordBatch],
    sort_fields_str: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    if inputs.is_empty() {
        bail!("no inputs to align");
    }

    // Collect all fields across all inputs, checking for type conflicts.
    // String-like types are normalized to Utf8 for internal alignment.
    let mut field_map: BTreeMap<String, Arc<Field>> = BTreeMap::new();

    for (input_idx, batch) in inputs.iter().enumerate() {
        for field in batch.schema().fields() {
            let normalized_type = normalize_type(field.data_type());

            match field_map.get(field.name().as_str()) {
                Some(existing) => {
                    if *existing.data_type() != normalized_type {
                        bail!(
                            "type conflict for column '{}': input 0 has {:?}, input {} has {:?} \
                             (normalized: {:?} vs {:?})",
                            field.name(),
                            existing.data_type(),
                            input_idx,
                            field.data_type(),
                            existing.data_type(),
                            normalized_type,
                        );
                    }
                    // If either side is nullable, the union must be too.
                    if field.is_nullable() && !existing.is_nullable() {
                        let nullable_field =
                            Arc::new(Field::new(field.name(), normalized_type, true));
                        field_map.insert(field.name().clone(), nullable_field);
                    }
                }
                None => {
                    // Columns that don't appear in every input must be nullable.
                    let nullable_field = Arc::new(Field::new(field.name(), normalized_type, true));
                    field_map.insert(field.name().clone(), nullable_field);
                }
            }
        }
    }

    // Build the union schema in Husky column order.
    let union_schema = build_husky_ordered_schema(&field_map, sort_fields_str)?;
    let union_schema_ref = Arc::new(union_schema);

    // Align each input to the union schema, casting columns as needed.
    let mut aligned = Vec::with_capacity(inputs.len());
    for batch in inputs {
        let aligned_batch = align_batch_to_schema(batch, &union_schema_ref)?;
        aligned.push(aligned_batch);
    }

    Ok((union_schema_ref, aligned))
}

/// Optimize an output batch for writing: strip all-null columns and choose
/// the best encoding for each column based on the actual data.
///
/// - All-null columns are removed.
/// - String columns (Utf8) are dictionary-encoded if their distinct value count is low relative to
///   the row count.
/// - Binary columns (like sorted_series) are left as-is.
/// - Non-string columns are left as-is.
pub fn optimize_output_batch(batch: &RecordBatch) -> RecordBatch {
    if batch.num_rows() == 0 {
        return batch.clone();
    }

    let schema = batch.schema();
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (i, col) in batch.columns().iter().enumerate() {
        let field = schema.field(i);

        // Strip all-null columns.
        if col.null_count() == col.len() {
            continue;
        }

        // For Utf8 columns, decide whether to dictionary-encode based on
        // the actual cardinality of the data in this output file.
        if *field.data_type() == DataType::Utf8 {
            let str_array = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column must downcast to StringArray");

            if should_dictionary_encode(str_array) {
                let dict_type =
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
                let dict_col = cast(col.as_ref(), &dict_type)
                    .expect("cast Utf8 -> Dictionary(Int32, Utf8) must succeed");
                new_fields.push(Arc::new(Field::new(
                    field.name(),
                    dict_type,
                    field.is_nullable(),
                )));
                new_columns.push(dict_col);
                continue;
            }
        }

        new_fields.push(Arc::new(field.clone()));
        new_columns.push(Arc::clone(col));
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .expect("optimize_output_batch: schema and columns must be consistent")
}

/// Decide whether a string column should be dictionary-encoded.
///
/// Uses the ratio of distinct non-null values to total non-null values.
/// If the ratio is at or below the threshold, dictionary encoding is
/// worthwhile (the dictionary is small relative to the data).
fn should_dictionary_encode(col: &StringArray) -> bool {
    let non_null_count = col.len() - col.null_count();
    if non_null_count == 0 {
        return false;
    }

    // Count distinct values.
    let mut distinct: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(non_null_count.min(1024));
    for i in 0..col.len() {
        if !col.is_null(i) {
            distinct.insert(col.value(i));
        }
    }

    let ratio = distinct.len() as f64 / non_null_count as f64;
    ratio <= DICTIONARY_ENCODING_THRESHOLD
}

/// Build a Schema with Husky column ordering:
/// 1. Sort schema columns in their configured order
/// 2. `sorted_series` immediately after
/// 3. Remaining columns alphabetically
fn build_husky_ordered_schema(
    field_map: &BTreeMap<String, Arc<Field>>,
    sort_fields_str: &str,
) -> Result<Schema> {
    let sort_schema = parse_sort_fields(sort_fields_str)?;

    let mut ordered_fields: Vec<Arc<Field>> = Vec::with_capacity(field_map.len());
    let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

    // Phase 1: sort schema columns in configured order.
    for col in &sort_schema.column {
        if let Some(field) = field_map.get(col.name.as_str()) {
            ordered_fields.push(Arc::clone(field));
            used.insert(col.name.as_str());
        }
    }

    // Phase 1b: sorted_series immediately after sort columns.
    // Allow nested if: let-chain form triggers a rustfmt parse error.
    #[allow(clippy::collapsible_if)]
    if let Some(field) = field_map.get(SORTED_SERIES_COLUMN) {
        if !used.contains(SORTED_SERIES_COLUMN) {
            ordered_fields.push(Arc::clone(field));
            used.insert(SORTED_SERIES_COLUMN);
        }
    }

    // Phase 2: remaining columns alphabetically (BTreeMap is already sorted).
    for (name, field) in field_map {
        if !used.contains(name.as_str()) {
            ordered_fields.push(Arc::clone(field));
        }
    }

    Ok(Schema::new(ordered_fields))
}

/// Align a single RecordBatch to a target schema by reordering existing
/// columns, inserting null arrays for missing columns, and casting
/// compatible types to the target type.
fn align_batch_to_schema(batch: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let num_rows = batch.num_rows();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for target_field in target_schema.fields() {
        match source_schema.index_of(target_field.name()) {
            Ok(idx) => {
                let col = batch.column(idx);
                let source_type = source_schema.field(idx).data_type();
                let target_type = target_field.data_type();

                if source_type == target_type {
                    columns.push(Arc::clone(col));
                } else {
                    // Cast to the target type (e.g., Dictionary -> Utf8).
                    let casted = cast(col.as_ref(), target_type)?;
                    columns.push(casted);
                }
            }
            Err(_) => {
                // Column missing from this input — fill with nulls.
                columns.push(new_null_array(target_field.data_type(), num_rows));
            }
        }
    }

    let aligned = RecordBatch::try_new(Arc::clone(target_schema), columns)?;
    Ok(aligned)
}

/// Normalize an Arrow data type for the internal union schema.
///
/// All string-like types (Utf8, LargeUtf8, Dictionary(*, Utf8/LargeUtf8))
/// are normalized to Utf8. This ensures `take` works uniformly across
/// concatenated inputs regardless of their original encoding.
///
/// Non-string types are returned as-is.
fn normalize_type(dt: &DataType) -> DataType {
    if is_string_type(dt) {
        return DataType::Utf8;
    }
    dt.clone()
}

/// Returns true if the data type represents strings.
fn is_string_type(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => true,
        DataType::Dictionary(_, value_type) => is_string_type(value_type),
        _ => false,
    }
}
