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
//! `(sorted_series, timestamp_secs)` where the timestamp sort direction comes
//! from the sort schema. Outputs are split at series boundaries so each output
//! file has non-overlapping key ranges.

mod merge_order;
pub mod metadata_aggregation;
pub mod policy;
mod schema;
mod writer;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

pub use self::merge_order::MergeRun;
use crate::sort_fields::{equivalent_schemas_for_compaction, parse_sort_fields};
use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::storage::{
    PARQUET_META_NUM_MERGE_OPS, PARQUET_META_RG_PARTITION_PREFIX_LEN, PARQUET_META_SORT_FIELDS,
    PARQUET_META_WINDOW_DURATION, PARQUET_META_WINDOW_START, ParquetWriterConfig,
};

/// Configuration for a merge operation.
///
/// The sort schema, window metadata, and merge ops count are read from the
/// input files' Parquet KV metadata — they are not provided by the caller.
/// The compaction planner ensures all inputs share the same sort schema and
/// window; the merge engine validates this.
pub struct MergeConfig {
    /// Number of output files to produce. The merger splits at
    /// `sorted_series` boundaries so output files have non-overlapping
    /// key ranges. If there are fewer distinct series than `num_outputs`,
    /// fewer files are produced.
    pub num_outputs: usize,

    /// Parquet writer configuration (compression, page size, etc.).
    pub writer_config: ParquetWriterConfig,
}

/// Metadata extracted from input files' Parquet KV metadata.
/// All inputs must agree on sort_fields, window_start, window_duration,
/// and rg_partition_prefix_len.
struct InputMetadata {
    sort_fields: String,
    window_start_secs: Option<i64>,
    window_duration_secs: u32,
    num_merge_ops: u32,
    /// Number of leading sort columns whose transitions align with row
    /// group boundaries. All input files must agree on this value (it's
    /// part of the compaction scope key). Splitting row groups at the
    /// claimed prefix boundary is not implemented by the current merge
    /// writer — it lands in PR-6 (streaming column-major merge engine).
    /// Until then, the *output* file is written with prefix 0 regardless
    /// of this value.
    #[allow(dead_code)] // wired for PR-6 streaming engine; PR-1 only validates.
    rg_partition_prefix_len: u32,
}

/// Result of a single output file from the merge.
///
/// Contains both physical metadata (file size, row count) and per-output
/// logical metadata (metric names, tags, time range) extracted from the
/// actual rows in this output file. When the merge produces multiple
/// outputs, each has metadata reflecting only its own rows.
#[derive(Debug)]
pub struct MergeOutputFile {
    /// Path to the output Parquet file.
    pub path: PathBuf,

    /// Number of rows in this output file.
    pub num_rows: usize,

    /// Number of row groups the writer produced for this file. Used by
    /// `merge_parquet_split_metadata` to decide whether the input prefix
    /// alignment claim (`rg_partition_prefix_len`) can be propagated to
    /// the output: a single-RG file vacuously satisfies any claim, so
    /// we keep the inputs' prefix; a multi-RG file with arbitrary
    /// boundaries (the only kind the current writer can produce) must
    /// reset the claim to 0.
    pub num_row_groups: usize,

    /// File size in bytes.
    pub size_bytes: u64,

    /// Row keys proto bytes (first/last row boundaries).
    pub row_keys_proto: Option<Vec<u8>>,

    /// Per-column zonemap regex strings.
    pub zonemap_regexes: std::collections::HashMap<String, String>,

    /// Distinct metric names in this output file.
    pub metric_names: std::collections::HashSet<String>,

    /// Time range covered by rows in this output file.
    pub time_range: crate::split::TimeRange,

    /// Low-cardinality tag values extracted from this output file's rows.
    /// Currently tracks "service" to match the ingest path.
    pub low_cardinality_tags: std::collections::HashMap<String, std::collections::HashSet<String>>,
}

/// Merge N sorted Parquet files into M sorted output files.
///
/// All inputs must share the same sort schema and contain a `sorted_series`
/// column. The merge key is `(sorted_series ASC, timestamp_secs <direction>)`,
/// where the timestamp direction comes from the sort schema.
/// Outputs are split at `sorted_series` boundaries to ensure non-overlapping
/// key ranges.
///
/// The sort schema, window metadata, and merge operation count are read from
/// the input files' Parquet KV metadata — the caller only provides the desired
/// output count and writer configuration.
///
/// Each output file is written with complete metadata: row keys, zonemap
/// regexes, Parquet KV metadata (`qh.*` keys), native `sorting_columns`,
/// and page-level column index statistics.
pub fn merge_sorted_parquet_files(
    input_paths: &[PathBuf],
    output_dir: &Path,
    config: &MergeConfig,
) -> Result<Vec<MergeOutputFile>> {
    merge_sorted_parquet_files_impl(input_paths, output_dir, config, None)
}

/// Test-only variant that forces a small Parquet read batch size to exercise
/// the multi-RecordBatch concatenation path.
#[cfg(test)]
pub(crate) fn merge_sorted_parquet_files_with_read_batch_size(
    input_paths: &[PathBuf],
    output_dir: &Path,
    config: &MergeConfig,
    read_batch_size: usize,
) -> Result<Vec<MergeOutputFile>> {
    merge_sorted_parquet_files_impl(input_paths, output_dir, config, Some(read_batch_size))
}

fn merge_sorted_parquet_files_impl(
    input_paths: &[PathBuf],
    output_dir: &Path,
    config: &MergeConfig,
    read_batch_size: Option<usize>,
) -> Result<Vec<MergeOutputFile>> {
    if input_paths.is_empty() {
        bail!("merge requires at least one input file");
    }
    if config.num_outputs == 0 {
        bail!("num_outputs must be at least 1");
    }

    // Step 0: Read and validate metadata from all input files.
    // Sort schema, window, and merge ops are derived from the files themselves.
    let input_meta = extract_and_validate_input_metadata(input_paths)?;

    info!(
        num_inputs = input_paths.len(),
        num_outputs = config.num_outputs,
        sort_fields = %input_meta.sort_fields,
        "starting sorted parquet merge"
    );

    // Step 1: Read all input files into RecordBatches.
    let inputs = read_inputs(input_paths, read_batch_size)?;
    let total_rows: usize = inputs.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        info!("all inputs empty, producing no output");
        return Ok(Vec::new());
    }

    // Step 2: Resolve union schema and align all inputs.
    let (union_schema, aligned_inputs) =
        schema::align_inputs_to_union_schema(&inputs, &input_meta.sort_fields)?;

    // Step 3: Compute merge order using (sorted_series, timestamp_secs).
    // The timestamp sort direction comes from the sort schema.
    let merge_order = merge_order::compute_merge_order(&aligned_inputs, &input_meta.sort_fields)?;

    // Step 4: Compute output boundaries at sorted_series transitions.
    let boundaries =
        merge_order::compute_output_boundaries(&merge_order, &aligned_inputs, config.num_outputs)?;

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

    info!(
        total_rows,
        num_outputs = boundaries.len(),
        "merge order computed"
    );

    // Step 5: Write output files.
    let outputs = writer::write_merge_outputs(
        &aligned_inputs,
        &union_schema,
        &merge_order,
        &boundaries,
        output_dir,
        config,
        &input_meta,
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
///
/// When `read_batch_size` is `Some(n)`, the Parquet reader yields batches
/// of at most `n` rows, which are then concatenated. This exercises the
/// multi-batch concatenation path in tests. In production, `None` uses
/// the reader's default batch size.
fn read_inputs(paths: &[PathBuf], read_batch_size: Option<usize>) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(paths.len());

    for path in paths {
        let file = std::fs::File::open(path)
            .with_context(|| format!("opening input file: {}", path.display()))?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading parquet footer: {}", path.display()))?;

        if let Some(batch_size) = read_batch_size {
            builder = builder.with_batch_size(batch_size);
        }

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
        if concatenated
            .schema()
            .index_of(SORTED_SERIES_COLUMN)
            .is_err()
        {
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

/// Extract and validate metadata from all input files.
///
/// Reads `qh.*` keys from each file's Parquet KV metadata. Validates that
/// all inputs share the same sort schema (via `equivalent_schemas_for_compaction`),
/// window_start, and window_duration. Returns the consensus metadata plus
/// `max(num_merge_ops) + 1` for the output.
fn extract_and_validate_input_metadata(paths: &[PathBuf]) -> Result<InputMetadata> {
    let mut consensus_sort_fields: Option<String> = None;
    let mut consensus_window_start: Option<Option<i64>> = None;
    let mut consensus_window_duration: Option<u32> = None;
    let mut consensus_prefix_len: Option<u32> = None;
    let mut max_merge_ops: u32 = 0;

    for path in paths {
        let file = std::fs::File::open(path)
            .with_context(|| format!("opening file for metadata: {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading footer for metadata: {}", path.display()))?;

        let kv_metadata = builder.metadata().file_metadata().key_value_metadata();

        let find_kv = |key: &str| -> Option<String> {
            kv_metadata.and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.clone())
            })
        };

        // Sort fields: required, must be consistent across all inputs.
        let file_sort_fields = find_kv(PARQUET_META_SORT_FIELDS).ok_or_else(|| {
            anyhow::anyhow!(
                "input file {} is missing {} metadata",
                path.display(),
                PARQUET_META_SORT_FIELDS
            )
        })?;

        match &consensus_sort_fields {
            Some(expected) => {
                let expected_schema = parse_sort_fields(expected)?;
                let file_schema = parse_sort_fields(&file_sort_fields).with_context(|| {
                    format!(
                        "parsing sort schema from {}: '{}'",
                        path.display(),
                        file_sort_fields
                    )
                })?;
                if !equivalent_schemas_for_compaction(&expected_schema, &file_schema) {
                    bail!(
                        "sort schema mismatch in {}: expected '{}', found '{}'",
                        path.display(),
                        expected,
                        file_sort_fields
                    );
                }
            }
            None => {
                // Validate the schema is parseable.
                parse_sort_fields(&file_sort_fields).with_context(|| {
                    format!(
                        "parsing sort schema from {}: '{}'",
                        path.display(),
                        file_sort_fields
                    )
                })?;
                consensus_sort_fields = Some(file_sort_fields.clone());
            }
        }

        // Window start: must be consistent.
        let file_window_start = find_kv(PARQUET_META_WINDOW_START)
            .map(|s| s.parse::<i64>())
            .transpose()
            .with_context(|| format!("parsing window_start from {}", path.display()))?;

        match &consensus_window_start {
            Some(expected) => {
                if file_window_start != *expected {
                    bail!(
                        "window_start mismatch in {}: expected {:?}, found {:?}",
                        path.display(),
                        expected,
                        file_window_start
                    );
                }
            }
            None => {
                consensus_window_start = Some(file_window_start);
            }
        }

        // Window duration: must be consistent.
        let file_window_duration = find_kv(PARQUET_META_WINDOW_DURATION)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing window_duration from {}", path.display()))?
            .unwrap_or(0);

        match &consensus_window_duration {
            Some(expected) => {
                if file_window_duration != *expected {
                    bail!(
                        "window_duration_secs mismatch in {}: expected {}, found {}",
                        path.display(),
                        expected,
                        file_window_duration
                    );
                }
            }
            None => {
                consensus_window_duration = Some(file_window_duration);
            }
        }

        // Merge ops: take the max across all inputs.
        let file_merge_ops = find_kv(PARQUET_META_NUM_MERGE_OPS)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing num_merge_ops from {}", path.display()))?
            .unwrap_or(0);

        if file_merge_ops > max_merge_ops {
            max_merge_ops = file_merge_ops;
        }

        // Row group partition prefix length: must be consistent across all
        // inputs. Absent KV → 0 (legacy default; no alignment claim).
        let file_prefix_len = find_kv(PARQUET_META_RG_PARTITION_PREFIX_LEN)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing rg_partition_prefix_len from {}", path.display()))?
            .unwrap_or(0);

        match consensus_prefix_len {
            Some(expected) => {
                if file_prefix_len != expected {
                    bail!(
                        "rg_partition_prefix_len mismatch in {}: expected {}, found {} — splits \
                         with different prefix lengths must not appear in the same merge",
                        path.display(),
                        expected,
                        file_prefix_len
                    );
                }
            }
            None => {
                consensus_prefix_len = Some(file_prefix_len);
            }
        }
    }

    Ok(InputMetadata {
        sort_fields: consensus_sort_fields.expect("at least one input required"),
        window_start_secs: consensus_window_start.expect("at least one input required"),
        window_duration_secs: consensus_window_duration.unwrap_or(0),
        num_merge_ops: max_merge_ops + 1,
        rg_partition_prefix_len: consensus_prefix_len.unwrap_or(0),
    })
}
