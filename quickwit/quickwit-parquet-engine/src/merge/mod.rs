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

//! Sorted k-way merge for Parquet files.
//!
//! Takes N sorted Parquet files sharing the same sort schema and produces M
//! sorted output files. The merge preserves sort order using a k-way merge on
//! `(sorted_series, timestamp_secs)` and splits outputs at series boundaries
//! so each output file has non-overlapping key ranges.

mod merge_order;
mod schema;
mod writer;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::storage::ParquetWriterConfig;

pub use self::merge_order::MergeRun;

/// Configuration for a merge operation.
pub struct MergeConfig {
    /// Sort fields string (e.g.,
    /// "metric_name|host|env|timeseries_id|timestamp_secs/V2").
    /// Must be identical across all input files.
    pub sort_fields: String,

    /// Number of output files to produce. The merger splits at
    /// `sorted_series` boundaries so output files have non-overlapping
    /// key ranges. If there are fewer distinct series than `num_outputs`,
    /// fewer files are produced.
    pub num_outputs: usize,

    /// Parquet writer configuration (compression, page size, etc.).
    pub writer_config: ParquetWriterConfig,

    /// Window start timestamp (epoch seconds) for output metadata.
    pub window_start_secs: Option<i64>,

    /// Window duration in seconds for output metadata.
    pub window_duration_secs: u32,

    /// Number of merge operations already applied to the most-merged input.
    /// The output will be stamped with `input_num_merge_ops + 1`.
    pub input_num_merge_ops: u32,
}

/// Result of a single output file from the merge.
pub struct MergeOutputFile {
    /// Path to the output Parquet file.
    pub path: PathBuf,

    /// Number of rows in this output file.
    pub num_rows: usize,

    /// File size in bytes.
    pub size_bytes: u64,

    /// Row keys proto bytes (first/last row boundaries).
    pub row_keys_proto: Option<Vec<u8>>,

    /// Per-column zonemap regex strings.
    pub zonemap_regexes: std::collections::HashMap<String, String>,
}

/// Merge N sorted Parquet files into M sorted output files.
///
/// All inputs must share the same sort schema and contain a `sorted_series`
/// column. The merge key is `(sorted_series ASC, timestamp_secs DESC)`.
/// Outputs are split at `sorted_series` boundaries to ensure non-overlapping
/// key ranges.
///
/// Each output file is written with complete metadata: row keys, zonemap
/// regexes, Parquet KV metadata (`qh.*` keys), native `sorting_columns`,
/// and page-level column index statistics.
pub fn merge_sorted_parquet_files(
    input_paths: &[PathBuf],
    output_dir: &Path,
    config: &MergeConfig,
) -> Result<Vec<MergeOutputFile>> {
    if input_paths.is_empty() {
        bail!("merge requires at least one input file");
    }
    if config.num_outputs == 0 {
        bail!("num_outputs must be at least 1");
    }

    info!(
        num_inputs = input_paths.len(),
        num_outputs = config.num_outputs,
        sort_fields = %config.sort_fields,
        "starting sorted parquet merge"
    );

    // Step 1: Read all input files into RecordBatches.
    let inputs = read_inputs(input_paths)?;
    let total_rows: usize = inputs.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        info!("all inputs empty, producing no output");
        return Ok(Vec::new());
    }

    // Step 2: Resolve union schema and align all inputs.
    let (union_schema, aligned_inputs) =
        schema::align_inputs_to_union_schema(&inputs, &config.sort_fields)?;

    // Step 3: Compute merge order using (sorted_series, timestamp_secs).
    let merge_order = merge_order::compute_merge_order(&aligned_inputs)?;

    // Step 4: Compute output boundaries at sorted_series transitions.
    let boundaries =
        merge_order::compute_output_boundaries(&merge_order, &aligned_inputs, config.num_outputs)?;

    info!(
        total_rows,
        num_outputs = boundaries.len(),
        "merge order computed"
    );

    // MC-4: verify union schema contains all columns from all inputs.
    {
        let union_field_names: std::collections::HashSet<&str> = union_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        for (i, input) in inputs.iter().enumerate() {
            for field in input.schema().fields() {
                quickwit_dst::check_invariant!(
                    quickwit_dst::invariants::InvariantId::MC4,
                    union_field_names.contains(field.name().as_str()),
                    ": input {} column '{}' missing from union schema",
                    i,
                    field.name()
                );
            }
        }
    }

    // Step 5: Write output files.
    let outputs = writer::write_merge_outputs(
        &aligned_inputs,
        &union_schema,
        &merge_order,
        &boundaries,
        output_dir,
        config,
    )?;

    // MC-1: verify total row count is preserved through merge.
    let output_total_rows: usize = outputs.iter().map(|o| o.num_rows).sum();
    quickwit_dst::check_invariant!(
        quickwit_dst::invariants::InvariantId::MC1,
        output_total_rows == total_rows,
        ": input rows={}, output rows={}",
        total_rows,
        output_total_rows
    );

    Ok(outputs)
}

/// Read each input Parquet file into a single RecordBatch.
fn read_inputs(paths: &[PathBuf]) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(paths.len());

    for path in paths {
        let file = std::fs::File::open(path)
            .with_context(|| format!("opening input file: {}", path.display()))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading parquet footer: {}", path.display()))?;

        let reader = builder
            .build()
            .with_context(|| format!("building reader: {}", path.display()))?;

        let file_batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("reading batches: {}", path.display()))?;

        if file_batches.is_empty() {
            // Empty file — produce a zero-row batch with the file's schema.
            let schema = {
                let f = std::fs::File::open(path)?;
                let b = ParquetRecordBatchReaderBuilder::try_new(f)?;
                b.schema().clone()
            };
            batches.push(RecordBatch::new_empty(schema));
            continue;
        }

        let schema = file_batches[0].schema();
        let concatenated = arrow::compute::concat_batches(&schema, &file_batches)
            .with_context(|| format!("concatenating batches: {}", path.display()))?;

        // Verify sorted_series column exists.
        if concatenated.schema().index_of(SORTED_SERIES_COLUMN).is_err() {
            bail!(
                "input file {} is missing the '{}' column",
                path.display(),
                SORTED_SERIES_COLUMN
            );
        }

        batches.push(concatenated);
    }

    Ok(batches)
}
