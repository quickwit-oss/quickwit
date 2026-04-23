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

//! Union schema resolution and column alignment for merge inputs.
//!
//! Computes the union of all column sets across inputs, detects type
//! conflicts, and pads missing columns with nulls so every input has
//! an identical schema.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{ArrayRef, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::sort_fields::parse_sort_fields;
use crate::sorted_series::SORTED_SERIES_COLUMN;

/// Align all input batches to a common union schema.
///
/// Returns the union schema and a vector of batches where every batch has
/// exactly the same schema. Missing columns are filled with null arrays.
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
    let mut field_map: BTreeMap<String, Arc<Field>> = BTreeMap::new();

    for (input_idx, batch) in inputs.iter().enumerate() {
        for field in batch.schema().fields() {
            match field_map.get(field.name().as_str()) {
                Some(existing) => {
                    if !types_compatible(existing.data_type(), field.data_type()) {
                        bail!(
                            "type conflict for column '{}': input 0 has {:?}, \
                             input {} has {:?}",
                            field.name(),
                            existing.data_type(),
                            input_idx,
                            field.data_type()
                        );
                    }
                    // If one input has nullable and another doesn't, the union
                    // must be nullable (missing columns are null-padded).
                    if field.is_nullable() && !existing.is_nullable() {
                        let nullable_field =
                            Arc::new(Field::new(field.name(), existing.data_type().clone(), true));
                        field_map.insert(field.name().clone(), nullable_field);
                    }
                }
                None => {
                    // Columns that don't appear in every input must be nullable.
                    let nullable_field =
                        Arc::new(Field::new(field.name(), field.data_type().clone(), true));
                    field_map.insert(field.name().clone(), nullable_field);
                }
            }
        }
    }

    // Build the union schema in Husky column order.
    let union_schema = build_husky_ordered_schema(&field_map, sort_fields_str)?;
    let union_schema_ref = Arc::new(union_schema);

    // Align each input to the union schema.
    let mut aligned = Vec::with_capacity(inputs.len());
    for batch in inputs {
        let aligned_batch = align_batch_to_schema(batch, &union_schema_ref)?;
        aligned.push(aligned_batch);
    }

    Ok((union_schema_ref, aligned))
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
        // Missing sort columns are fine — they'll be null-padded. But we only
        // include them in the schema if at least one input has the column.
    }

    // Phase 1b: sorted_series immediately after sort columns.
    if let Some(field) = field_map.get(SORTED_SERIES_COLUMN)
        && !used.contains(SORTED_SERIES_COLUMN)
    {
        ordered_fields.push(Arc::clone(field));
        used.insert(SORTED_SERIES_COLUMN);
    }

    // Phase 2: remaining columns alphabetically (BTreeMap is already sorted).
    for (name, field) in field_map {
        if !used.contains(name.as_str()) {
            ordered_fields.push(Arc::clone(field));
        }
    }

    Ok(Schema::new(ordered_fields))
}

/// Align a single RecordBatch to a target schema by reordering existing columns
/// and inserting null arrays for missing columns.
fn align_batch_to_schema(batch: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let num_rows = batch.num_rows();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for target_field in target_schema.fields() {
        match source_schema.index_of(target_field.name()) {
            Ok(idx) => {
                columns.push(Arc::clone(batch.column(idx)));
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

/// Check if two Arrow data types are compatible for merging.
///
/// Exact match is required, with the exception of dictionary types where
/// different key types are allowed (the values must match).
fn types_compatible(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::Dictionary(_, va), DataType::Dictionary(_, vb)) => va == vb,
        _ => a == b,
    }
}
