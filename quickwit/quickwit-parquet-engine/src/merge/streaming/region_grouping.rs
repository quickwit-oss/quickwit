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

//! Region grouping for multi-RG streaming merge.
//!
//! A "region" is one merge unit: a contiguous slice of the merged
//! output where all contributing inputs share the same sort-prefix
//! value. For multi-RG metric-aligned inputs each region pairs with
//! at most one row group per input, which is the property that makes
//! per-region streaming work without column-chunk-bounded buffering.
//!
//! This module owns the composite prefix-key encoding (sort-order
//! preserving across multiple columns / per-type encoding rules) and
//! the BTreeMap-based region grouping. It also enforces MS-2: the
//! BTreeMap iteration order must agree with each input's physical
//! row-group order, otherwise the streaming engine would crash
//! mid-merge.
//!
//! The encoding rules per parquet physical type:
//! - `ByteArray` / `FixedLenByteArray`: 4-byte BE length prefix + bytes.
//! - `Int32` / `Int64`: sign-flipped big-endian so byte order matches numeric order across the full
//!   signed range.
//! - `Boolean`: single 0/1 byte.
//! - DESC columns: per-byte complement of the encoding above so smaller values' bytes sort
//!   *larger*.
//! - `Float` / `Double` / `Int96`: rejected with a clear error.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use parquet::file::metadata::ParquetMetaData;

use super::super::InputMetadata;
use super::InputDecoderState;
use crate::sort_fields::{is_timestamp_column_name, parse_sort_fields};

/// One merge region: a contiguous slice of the merged output, where all
/// contributing inputs share the same sort-prefix value (e.g., one
/// `metric_name` when `rg_partition_prefix_len == 1`).
///
/// For multi-RG metric-aligned inputs, each region corresponds to **at
/// most one row group per input** — the property that makes per-region
/// streaming work without column-chunk-bounded buffering.
#[derive(Debug, Clone)]
pub(crate) struct Region {
    /// Sort-prefix value identifying this region (e.g., `metric_name`
    /// bytes for `prefix_len == 1`). Used only for ordering and
    /// diagnostics; the merge engine doesn't decode this value.
    pub(crate) prefix_key: Vec<u8>,
    /// `(input_idx, rg_idx, num_rows)` for each input that contributes
    /// to this region. Ordered by `input_idx`.
    pub(crate) contributing: Vec<(usize, usize, usize)>,
}

impl Region {
    pub(crate) fn total_rows(&self) -> usize {
        self.contributing.iter().map(|(_, _, n)| *n).sum()
    }
}

/// A prefix column's location in the parquet schema, plus the sort
/// direction declared for it. `name` is the sort-schema name (used in
/// error messages); `parquet_col_idx` is the resolved index in the
/// parquet schema's flat column list (after applying the
/// `timestamp` / `timestamp_secs` alias).
#[derive(Debug, Clone)]
pub(crate) struct PrefixColumn {
    pub(crate) name: String,
    pub(crate) parquet_col_idx: usize,
    pub(crate) descending: bool,
}

/// Resolve the first `prefix_len` sort columns to parquet leaf
/// indices. Honours the legacy `timestamp` → `timestamp_secs` alias.
/// Errors if the sort schema has fewer columns than `prefix_len` or
/// if any column is missing from the parquet schema.
pub(crate) fn find_prefix_parquet_col_indices(
    metadata: &ParquetMetaData,
    sort_fields_str: &str,
    prefix_len: usize,
    input_idx: usize,
) -> Result<Vec<PrefixColumn>> {
    let sort_field_schema = parse_sort_fields(sort_fields_str)?;
    if sort_field_schema.column.len() < prefix_len {
        bail!(
            "sort schema has {} columns but rg_partition_prefix_len = {prefix_len}",
            sort_field_schema.column.len(),
        );
    }
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut prefix_cols = Vec::with_capacity(prefix_len);
    for (pos, sort_col) in sort_field_schema.column.iter().take(prefix_len).enumerate() {
        // Apply the same `timestamp` / `timestamp_secs` alias the rest
        // of the engine uses.
        let resolved = if is_timestamp_column_name(&sort_col.name)
            && parquet_has_column(parquet_schema, "timestamp_secs")
        {
            "timestamp_secs"
        } else {
            sort_col.name.as_str()
        };
        let mut found = None;
        for (col_idx, col) in parquet_schema.columns().iter().enumerate() {
            if col.path().parts()[0] == resolved {
                found = Some(col_idx);
                break;
            }
        }
        let parquet_col_idx = found.ok_or_else(|| {
            anyhow!(
                "input {input_idx} parquet schema is missing prefix sort column '{}' (position \
                 {pos})",
                sort_col.name,
            )
        })?;
        let descending = sort_col.sort_direction
            == quickwit_proto::sortschema::SortColumnDirection::SortDirectionDescending as i32;
        prefix_cols.push(PrefixColumn {
            name: sort_col.name.clone(),
            parquet_col_idx,
            descending,
        });
    }
    Ok(prefix_cols)
}

fn parquet_has_column(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    name: &str,
) -> bool {
    parquet_schema
        .columns()
        .iter()
        .any(|c| c.path().parts()[0] == name)
}

/// Build the composite byte key identifying a row group's prefix
/// values for grouping into a region. The key concatenates each
/// prefix column's value bytes in declared order, with each column's
/// encoding chosen so that lexicographic order on the composite
/// matches the sort schema's order across the prefix columns. Each
/// column is required to have `min == max` statistics on this RG (the
/// "metric-aligned" invariant declared by `rg_partition_prefix_len`).
pub(crate) fn extract_rg_composite_prefix_key(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    prefix_cols: &[PrefixColumn],
    input_idx: usize,
) -> Result<Vec<u8>> {
    let rg_meta = metadata.row_group(rg_idx);
    let mut key = Vec::new();
    for col in prefix_cols {
        let chunk = rg_meta.column(col.parquet_col_idx);
        let stats = chunk.statistics().ok_or_else(|| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{}' has no statistics — cannot determine \
                 prefix alignment without min/max",
                col.name,
            )
        })?;
        let value_bytes = extract_aligned_prefix_value(stats, &col.name, rg_idx, input_idx)?;
        let encoded = if col.descending {
            invert_for_descending(&value_bytes)
        } else {
            value_bytes
        };
        key.extend_from_slice(&encoded);
    }
    Ok(key)
}

/// Verify min == max for the column chunk and return the value
/// encoded as order-preserving big-endian bytes (without applying any
/// direction inversion — that's the caller's job).
fn extract_aligned_prefix_value(
    stats: &parquet::file::statistics::Statistics,
    sort_col_name: &str,
    rg_idx: usize,
    input_idx: usize,
) -> Result<Vec<u8>> {
    use parquet::file::statistics::Statistics;

    fn require_eq<T: PartialEq + std::fmt::Debug>(
        min: Option<T>,
        max: Option<T>,
        col: &str,
        rg_idx: usize,
        input_idx: usize,
    ) -> Result<T> {
        let min = min.ok_or_else(|| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{col}' has no min in stats — cannot determine \
                 prefix alignment"
            )
        })?;
        let max = max.ok_or_else(|| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{col}' has no max in stats — cannot determine \
                 prefix alignment"
            )
        })?;
        if min != max {
            bail!(
                "input {input_idx} rg {rg_idx} is NOT prefix-aligned on col '{col}': min ({:?}) \
                 != max ({:?}). Multi-RG inputs declaring `rg_partition_prefix_len >= 1` must \
                 carry one prefix-value per RG.",
                min,
                max,
            );
        }
        Ok(min)
    }

    match stats {
        Statistics::ByteArray(v) => {
            let value = require_eq(
                v.min_bytes_opt().map(|b| b.to_vec()),
                v.max_bytes_opt().map(|b| b.to_vec()),
                sort_col_name,
                rg_idx,
                input_idx,
            )?;
            Ok(encode_byte_array_prefix(&value))
        }
        Statistics::FixedLenByteArray(v) => {
            let value = require_eq(
                v.min_bytes_opt().map(|b| b.to_vec()),
                v.max_bytes_opt().map(|b| b.to_vec()),
                sort_col_name,
                rg_idx,
                input_idx,
            )?;
            Ok(encode_byte_array_prefix(&value))
        }
        Statistics::Int32(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                sort_col_name,
                rg_idx,
                input_idx,
            )?;
            // Sign-flip so the byte order of the BE encoding matches
            // numeric order across the full i32 range.
            Ok(((value as u32) ^ 0x8000_0000u32).to_be_bytes().to_vec())
        }
        Statistics::Int64(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                sort_col_name,
                rg_idx,
                input_idx,
            )?;
            Ok(((value as u64) ^ 0x8000_0000_0000_0000u64)
                .to_be_bytes()
                .to_vec())
        }
        Statistics::Boolean(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                sort_col_name,
                rg_idx,
                input_idx,
            )?;
            Ok(vec![value as u8])
        }
        Statistics::Float(_) | Statistics::Double(_) => bail!(
            "prefix col '{sort_col_name}' is floating-point; composite-key extraction does not \
             yet support IEEE-754 ordering. Open an issue if you hit this — the encoding needs a \
             sign-aware bit flip on negative values."
        ),
        Statistics::Int96(_) => bail!(
            "prefix col '{sort_col_name}' is Int96 (deprecated timestamp type); use Int64-encoded \
             `timestamp_secs` instead."
        ),
    }
}

/// Length-prefix a variable-width value so the resulting bytes
/// concatenate unambiguously and lex-order matches column order even
/// when adjacent columns have different lengths.
pub(crate) fn encode_byte_array_prefix(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Bytewise complement: turns an ASC-ordered byte string into one
/// that sorts in reverse. Applied to each prefix column whose sort
/// direction is DESC so the BTreeMap iteration order matches the
/// declared sort order for that column.
pub(crate) fn invert_for_descending(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|b| !b).collect()
}

/// MS-2: verify that, for each input, the regions list visits its
/// row groups in physical (on-disk) order. The streaming engine
/// drains each input sequentially — once we've moved past RG K's
/// pages we cannot go back. The composite-key BTreeMap iteration
/// must agree with that physical ordering for every input.
///
/// Disagreement usually means the input file's sort direction was
/// declared one way but the data was written the other — e.g., the
/// sort schema says `metric_name ASC` but the file has RG 0 with
/// metric `z` and RG 1 with metric `a`. Reject upfront with a clear
/// message rather than letting `process_region` crash mid-merge
/// with "page from rg X while draining rg Y".
pub(crate) fn validate_region_order_matches_physical_rg_order(
    regions: &[Region],
    num_inputs: usize,
) -> Result<()> {
    let mut last_rg_per_input: Vec<Option<usize>> = vec![None; num_inputs];
    for (region_idx, region) in regions.iter().enumerate() {
        for &(input_idx, rg_idx, _) in &region.contributing {
            if let Some(prev_rg) = last_rg_per_input[input_idx]
                && rg_idx <= prev_rg
            {
                bail!(
                    "region iteration disagrees with input {input_idx}'s physical RG order: \
                     region {region_idx} wants rg {rg_idx} but a previous region already used rg \
                     {prev_rg}. The composite prefix key encoding does not match the input's \
                     physical layout — check that the sort schema's direction matches how the \
                     file is actually sorted on disk."
                );
            }
            last_rg_per_input[input_idx] = Some(rg_idx);
        }
    }
    Ok(())
}

/// Build the region list across all inputs.
///
/// - If `rg_partition_prefix_len == 0`: all inputs must be single-RG (caller's job to validate);
///   produces ONE region with each input's only RG. The region's `prefix_key` is empty (no
///   alignment claim).
/// - If `rg_partition_prefix_len >= 1`: reads each input's per-RG prefix col stats (must have `min
///   == max`), groups RGs across inputs by composite prefix key, sorts regions by that key.
///
/// Returns regions in sort order (sort prefix ASC).
pub(crate) fn extract_regions_from_metadata(
    decoders_state: &[InputDecoderState],
    input_meta: &InputMetadata,
) -> Result<Vec<Region>> {
    if input_meta.rg_partition_prefix_len == 0 {
        // No alignment claim: single region covering each input's only RG.
        // Multi-RG inputs with prefix_len == 0 are rejected earlier; here
        // each input is single-RG (or zero-RG).
        let mut contributing = Vec::new();
        for (idx, state) in decoders_state.iter().enumerate() {
            if state.metadata.num_row_groups() == 0 {
                continue;
            }
            let rg_meta = state.metadata.row_group(0);
            contributing.push((idx, 0, rg_meta.num_rows() as usize));
        }
        if contributing.is_empty() {
            return Ok(Vec::new());
        }
        return Ok(vec![Region {
            prefix_key: Vec::new(),
            contributing,
        }]);
    }

    // Prefix_len >= 1: build regions by composite prefix key from
    // per-RG stats. See `extract_rg_composite_prefix_key` for the
    // per-type encoding.
    let mut by_prefix: BTreeMap<Vec<u8>, Vec<(usize, usize, usize)>> = BTreeMap::new();
    let prefix_len = input_meta.rg_partition_prefix_len as usize;

    for (input_idx, state) in decoders_state.iter().enumerate() {
        if state.metadata.num_row_groups() == 0 {
            continue;
        }
        let prefix_cols = find_prefix_parquet_col_indices(
            &state.metadata,
            &input_meta.sort_fields,
            prefix_len,
            input_idx,
        )
        .with_context(|| format!("resolving prefix cols for input {input_idx}"))?;
        for rg_idx in 0..state.metadata.num_row_groups() {
            let prefix_key =
                extract_rg_composite_prefix_key(&state.metadata, rg_idx, &prefix_cols, input_idx)?;
            let num_rows = state.metadata.row_group(rg_idx).num_rows() as usize;
            by_prefix
                .entry(prefix_key)
                .or_default()
                .push((input_idx, rg_idx, num_rows));
        }
    }

    Ok(by_prefix
        .into_iter()
        .map(|(prefix_key, contributing)| Region {
            prefix_key,
            contributing,
        })
        .collect())
}

/// Assign each region to an output file index.
///
/// Splits the region list across `num_outputs` files, balancing
/// cumulative row count. Each output file gets a contiguous slice of
/// the region list (preserving sort-prefix order so output files have
/// non-overlapping key ranges). Returns a `Vec<usize>` indexed by
/// `region_idx` with the target output file index.
///
/// If `regions.len() < num_outputs`, fewer output files are produced
/// (matches the non-streaming engine's behaviour when there aren't
/// enough split points).
pub(crate) fn assign_regions_to_output_files(regions: &[Region], num_outputs: usize) -> Vec<usize> {
    let total_rows: usize = regions.iter().map(|r| r.total_rows()).sum();
    let effective_num_outputs = num_outputs.min(regions.len()).max(1);
    let target_rows_per_output = total_rows.div_ceil(effective_num_outputs).max(1);

    let mut assignments = Vec::with_capacity(regions.len());
    let mut current_output = 0;
    let mut accumulated = 0;
    for region in regions {
        // If this region would push us past the target AND we have
        // budget to start a new output AND the current output already
        // has rows, advance to next output BEFORE assigning.
        if accumulated > 0
            && accumulated + region.total_rows() > target_rows_per_output
            && current_output + 1 < effective_num_outputs
        {
            current_output += 1;
            accumulated = 0;
        }
        assignments.push(current_output);
        accumulated += region.total_rows();
    }
    assignments
}
