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

//! K-way merge order computation for sorted Parquet files.
//!
//! Uses `(sorted_series ASC, timestamp_secs <direction>)` as the merge key,
//! where the timestamp direction comes from the sort schema.
//! Produces a run-length encoded merge order that naturally captures
//! contiguous runs from the same input.
//!
//! The merge operates at the individual row level — any row from any input
//! can end up at any position in any output. Input row group boundaries are
//! irrelevant; they are erased when each file is read into a flat RecordBatch.

use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{BinaryArray, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};

use crate::sort_fields::{TIMESTAMP_SECS, is_timestamp_column_name, parse_sort_fields};
use crate::sorted_series::SORTED_SERIES_COLUMN;

/// A contiguous run of rows from a single input in the merged output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRun {
    /// Index of the input file (0-based).
    pub input_index: usize,
    /// Starting row within that input file.
    pub start_row: usize,
    /// Number of consecutive rows from this input.
    pub row_count: usize,
}

/// Shared state for the k-way merge heap.
///
/// Holds the converted row arrays so that heap entries can reference rows
/// by (input_index, row_pos) without copying key bytes.
struct MergeHeapState {
    row_arrays: Vec<Option<Rows>>,
    input_lengths: Vec<usize>,
}

/// An entry in the merge heap. Stores only indices — row bytes are looked
/// up from the shared [`MergeHeapState`] during comparison via `Arc`.
struct HeapEntry {
    /// Which input this row comes from.
    input_index: usize,
    /// Current row position within that input.
    row_pos: usize,
    /// Shared state for row lookup during comparison.
    state: Arc<MergeHeapState>,
}

impl HeapEntry {
    /// Compare this entry's row with another's by looking up from shared state.
    fn cmp_rows(&self, other: &Self) -> std::cmp::Ordering {
        let self_rows = self.state.row_arrays[self.input_index]
            .as_ref()
            .expect("heap entry references a non-empty input");
        let other_rows = other.state.row_arrays[other.input_index]
            .as_ref()
            .expect("heap entry references a non-empty input");

        let self_row = self_rows.row(self.row_pos);
        let other_row = other_rows.row(other.row_pos);

        self_row.cmp(&other_row)
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_rows(other) == std::cmp::Ordering::Equal && self.input_index == other.input_index
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse so BinaryHeap (max-heap) pops the smallest row first.
        // Break ties by input_index for determinism.
        match other.cmp_rows(self) {
            std::cmp::Ordering::Equal => other.input_index.cmp(&self.input_index),
            ord => ord,
        }
    }
}

/// Compute the merge order across all inputs.
///
/// Each input must be a RecordBatch with `sorted_series` (Binary, ascending)
/// and `timestamp_secs` (descending) columns. Inputs must already be sorted
/// by these columns.
///
/// The merge operates at individual row granularity — input row group
/// boundaries are irrelevant. Any row from any input can end up at any
/// position in the output, depending on sort order.
///
/// Returns an RLE-encoded merge order: contiguous runs from the same input
/// are collapsed into a single `MergeRun`.
pub fn compute_merge_order(inputs: &[RecordBatch], sort_fields_str: &str) -> Result<Vec<MergeRun>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    // Parse the sort schema to determine timestamp sort direction.
    // Legacy schemas may use "timestamp" instead of "timestamp_secs".
    let sort_schema = parse_sort_fields(sort_fields_str)?;

    let ts_column = sort_schema
        .column
        .iter()
        .find(|c| is_timestamp_column_name(&c.name))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "sort schema '{}' does not contain a timestamp column",
                sort_fields_str,
            )
        })?;

    let ts_descending = ts_column.sort_direction
        == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32;

    // Determine the timestamp column data type from the first non-empty input.
    let ts_data_type = inputs
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            let schema = b.schema();
            let (_, field) = schema
                .column_with_name(TIMESTAMP_SECS)
                .expect("timestamp_secs column must exist");
            field.data_type().clone()
        })
        .unwrap_or(DataType::UInt64);

    // Build a RowConverter for (sorted_series ASC, timestamp_secs <direction>).
    // The timestamp direction comes from the sort schema, not hardcoded.
    let sort_fields = vec![
        SortField::new(DataType::Binary), // sorted_series ASC
        SortField::new_with_options(
            ts_data_type,
            SortOptions {
                descending: ts_descending,
                nulls_first: false,
            },
        ),
    ];
    let converter = RowConverter::new(sort_fields)?;

    // Convert sort columns from each input into row format.
    let mut row_arrays = Vec::with_capacity(inputs.len());
    let mut input_lengths = Vec::with_capacity(inputs.len());

    for (idx, batch) in inputs.iter().enumerate() {
        if batch.num_rows() == 0 {
            row_arrays.push(None);
            input_lengths.push(0);
            continue;
        }

        let ss_col = get_column(batch, SORTED_SERIES_COLUMN, idx)?;
        let ts_col = get_column(batch, TIMESTAMP_SECS, idx)?;

        let rows = converter.convert_columns(&[ss_col, ts_col])?;
        input_lengths.push(batch.num_rows());
        row_arrays.push(Some(rows));
    }

    // Extract sorted_series arrays for checking series boundaries during
    // run extension. A run must not span multiple series — otherwise
    // output boundary computation cannot find transitions within runs.
    let ss_arrays: Vec<Option<&BinaryArray>> = inputs
        .iter()
        .map(|batch| {
            if batch.num_rows() == 0 {
                return None;
            }
            let idx = batch
                .schema()
                .index_of(SORTED_SERIES_COLUMN)
                .expect("sorted_series column must exist");
            Some(
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("sorted_series must be Binary"),
            )
        })
        .collect();

    // Build shared state for heap entries.
    let state = Arc::new(MergeHeapState {
        row_arrays,
        input_lengths: input_lengths.clone(),
    });

    // Estimate total rows for capacity hint.
    let total_rows: usize = input_lengths.iter().sum();
    let mut merge_order: Vec<MergeRun> = Vec::with_capacity(total_rows.min(1024));

    // Initialize the min-heap with the first row from each non-empty input.
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(inputs.len());
    for (input_index, rows_opt) in state.row_arrays.iter().enumerate() {
        if rows_opt.is_some() {
            heap.push(HeapEntry {
                input_index,
                row_pos: 0,
                state: Arc::clone(&state),
            });
        }
    }

    // K-way merge: pop smallest, extend or start a run, push next row.
    //
    // A run is only extended when the new row is consecutive from the same
    // input AND has the same sorted_series value as the run's start row.
    // This guarantees every MergeRun contains exactly one series, so
    // compute_output_boundaries can find all series transitions by
    // comparing adjacent runs.
    while let Some(entry) = heap.pop() {
        // Extend the current run or start a new one.
        let extends_current = match merge_order.last() {
            Some(run) => {
                run.input_index == entry.input_index
                    && run.start_row + run.row_count == entry.row_pos
                    && ss_arrays[entry.input_index].unwrap().value(run.start_row)
                        == ss_arrays[entry.input_index].unwrap().value(entry.row_pos)
            }
            None => false,
        };

        if extends_current {
            merge_order.last_mut().unwrap().row_count += 1;
        } else {
            merge_order.push(MergeRun {
                input_index: entry.input_index,
                start_row: entry.row_pos,
                row_count: 1,
            });
        }

        // Push the next row from this input, if any.
        let next_pos = entry.row_pos + 1;
        if next_pos < state.input_lengths[entry.input_index] {
            heap.push(HeapEntry {
                input_index: entry.input_index,
                row_pos: next_pos,
                state: Arc::clone(&state),
            });
        }
    }

    Ok(merge_order)
}

/// Compute boundaries for splitting the merge order into M output files.
///
/// Each boundary is a range of indices into `merge_order`. Splits happen at
/// `sorted_series` transitions — the sorted_series value in the last row of
/// one output differs from the first row of the next output.
///
/// If there are fewer distinct sorted_series values than `num_outputs`,
/// fewer outputs are produced.
///
/// Returns: a vector of `(start_run_idx, end_run_idx)` pairs, where each
/// pair is a half-open range into `merge_order`.
pub fn compute_output_boundaries(
    merge_order: &[MergeRun],
    inputs: &[RecordBatch],
    num_outputs: usize,
) -> Result<Vec<std::ops::Range<usize>>> {
    if merge_order.is_empty() {
        return Ok(Vec::new());
    }

    let total_rows: usize = merge_order.iter().map(|r| r.row_count).sum();

    if num_outputs == 1 {
        let all = 0..merge_order.len();
        return Ok(vec![all]);
    }

    // Find the sorted_series transitions in the merge order. A transition
    // is a run index where the sorted_series value changes compared to the
    // previous row.
    let ss_arrays: Vec<&BinaryArray> = inputs
        .iter()
        .map(|batch| {
            let idx = batch
                .schema()
                .index_of(SORTED_SERIES_COLUMN)
                .expect("sorted_series column must exist");
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("sorted_series must be Binary")
        })
        .collect();

    // Walk the merge order and record run indices where sorted_series changes.
    let mut transition_indices: Vec<usize> = Vec::new();
    let mut prev_ss: Option<&[u8]> = None;

    for (run_idx, run) in merge_order.iter().enumerate() {
        let current_ss = ss_arrays[run.input_index].value(run.start_row);

        let is_transition = match prev_ss {
            Some(prev) => current_ss != prev,
            None => false, // First run is not a transition.
        };

        if is_transition {
            transition_indices.push(run_idx);
        }

        // Track the sorted_series of the last row in this run.
        let last_row = run.start_row + run.row_count - 1;
        prev_ss = Some(ss_arrays[run.input_index].value(last_row));
    }

    // If fewer transitions than num_outputs - 1, produce fewer outputs.
    let actual_outputs = (transition_indices.len() + 1).min(num_outputs);

    if actual_outputs <= 1 {
        let all = 0..merge_order.len();
        return Ok(vec![all]);
    }

    let transition_set: std::collections::HashSet<usize> =
        transition_indices.iter().cloned().collect();

    let target_rows_per_output = total_rows / actual_outputs;

    // Walk the merge order, accumulating row counts. At each transition
    // point, decide whether to place a boundary.
    //
    // Strategy: split at a transition when we've accumulated at least
    // target_rows_per_output. But also track remaining transitions and
    // outputs — if we're running low on transitions relative to the
    // outputs we still need, split eagerly to avoid producing too few files.
    let mut boundaries: Vec<std::ops::Range<usize>> = Vec::with_capacity(actual_outputs);
    let mut current_start: usize = 0;
    let mut rows_in_current: usize = 0;
    let mut outputs_remaining = actual_outputs;
    let mut transitions_remaining = transition_indices.len();

    for (run_idx, run) in merge_order.iter().enumerate() {
        rows_in_current += run.row_count;

        let at_transition = transition_set.contains(&(run_idx + 1));
        let is_last_run = run_idx + 1 == merge_order.len();

        if is_last_run {
            // Always emit the final boundary.
            boundaries.push(current_start..(run_idx + 1));
            break;
        }

        if at_transition && outputs_remaining > 1 {
            let past_target = rows_in_current >= target_rows_per_output;

            // We must split here if the remaining transitions (including
            // this one) equal the remaining splits needed. The last
            // boundary is emitted by is_last_run without needing a
            // transition, so we need (outputs_remaining - 1) transition-
            // splits total.
            let must_split = transitions_remaining < outputs_remaining;

            if past_target || must_split {
                boundaries.push(current_start..(run_idx + 1));
                current_start = run_idx + 1;
                rows_in_current = 0;
                outputs_remaining -= 1;
            }

            transitions_remaining -= 1;
        }
    }

    Ok(boundaries)
}

/// Get a column by name from a RecordBatch, with a clear error message.
fn get_column(
    batch: &RecordBatch,
    name: &str,
    input_index: usize,
) -> Result<arrow::array::ArrayRef> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| anyhow::anyhow!("input {} is missing column '{}'", input_index, name))?;
    Ok(Arc::clone(batch.column(idx)))
}
