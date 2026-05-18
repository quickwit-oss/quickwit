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
//! row-group order, otherwise the streaming engine would `bail!`
//! mid-merge when a page arrives from an unexpected row group.
//!
//! The encoding rules per parquet physical type:
//! - `ByteArray` / `FixedLenByteArray`: byte-stuffed escape encoding — each `0x00` byte in the
//!   value becomes `0x00 0x01`, and a `0x00 0x00` terminator is appended. This preserves
//!   lexicographic order both for single columns (`"aa"` < `"b"`) and across concatenated composite
//!   keys (the `0x00 0x00` terminator is the smallest possible 2-byte sequence under escaping, so
//!   shorter values sort before longer values when prefixes match).
//! - `Int32` / `Int64`: sign-flipped big-endian so byte order matches numeric order across the full
//!   signed range.
//! - `Boolean`: single 0/1 byte.
//! - DESC columns: per-byte complement of the encoding above so smaller values' bytes sort
//!   *larger*.
//! - `Float` / `Double` / `Int96`: rejected with a clear error.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::RecordBatch;
use parquet::file::metadata::ParquetMetaData;

use super::super::InputMetadata;
use super::super::merge_order::MergeRun;
use super::InputDecoderState;
use crate::sort_fields::{is_timestamp_column_name, parse_sort_fields};
use crate::sorted_series::append_prefix_col_to_key;

/// One merge region: a contiguous slice of the merged output, where all
/// contributing inputs share the same sort-prefix value (e.g., one
/// `metric_name` when `rg_partition_prefix_len == 1`).
///
/// A region pairs with **at most one row group per input** — the
/// property that makes per-region streaming work without
/// column-chunk-bounded buffering. The `start_row` field on each
/// contribution lets a single row group be sliced across multiple
/// adjacent regions, which is how the engine subdivides a region at
/// `sorted_series` transitions to honor `num_outputs` when one
/// region (e.g. a giant single metric with `prefix_len=0`) would
/// otherwise occupy a single output file.
#[derive(Debug, Clone)]
pub(crate) struct Region {
    /// Sort-prefix value identifying this region (e.g., `metric_name`
    /// bytes for `prefix_len == 1`). Used only for ordering and
    /// diagnostics; the merge engine doesn't decode this value.
    pub(crate) prefix_key: Vec<u8>,
    /// Per contributing input: which slice of which row group belongs
    /// to this region. Ordered by `input_idx`.
    pub(crate) contributing: Vec<RegionContribution>,
}

/// One input's contribution to a region: the input index, the row
/// group within that input, and the row range within that row group
/// that belongs to the region.
///
/// For top-level regions from `extract_regions_from_metadata` each
/// contribution covers a whole RG: `start_row == 0` and
/// `num_rows == rg.num_rows()`. Sub-regions produced by
/// `split_region_at_sorted_series` reference the same
/// `(input_idx, rg_idx)` as their parent with disjoint contiguous row
/// ranges.
#[derive(Debug, Clone)]
pub(crate) struct RegionContribution {
    pub(crate) input_idx: usize,
    pub(crate) rg_idx: usize,
    pub(crate) start_row: usize,
    pub(crate) num_rows: usize,
}

impl Region {
    pub(crate) fn total_rows(&self) -> usize {
        self.contributing.iter().map(|c| c.num_rows).sum()
    }
}

/// A prefix column's location in the parquet schema, plus the sort
/// direction and ordinal position declared for it. `name` is the
/// sort-schema name (used in error messages); `parquet_col_idx` is
/// the resolved index in the parquet schema's flat column list
/// (after applying the `timestamp` / `timestamp_secs` alias);
/// `ordinal` is the column's 0-based position in `qh.sort_fields`,
/// matching `sorted_series`'s ordinal assignment so the per-RG
/// prefix key composes as a literal prefix of every row's
/// `sorted_series` key.
#[derive(Debug, Clone)]
pub(crate) struct PrefixColumn {
    pub(crate) name: String,
    pub(crate) parquet_col_idx: usize,
    pub(crate) descending: bool,
    pub(crate) ordinal: u8,
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
        // Ordinal matches the column's position in `qh.sort_fields`.
        // For prefix cols (always the first `prefix_len` entries of
        // the sort schema) the ordinal equals the iteration index
        // `pos`, which is also the ordinal `sorted_series` would
        // assign — so the per-RG prefix key composes as a literal
        // byte prefix of every sorted_series key.
        prefix_cols.push(PrefixColumn {
            name: sort_col.name.clone(),
            parquet_col_idx,
            descending,
            ordinal: pos as u8,
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
/// values for grouping into a region. Per prefix column, contributes
/// `storekey(ordinal) || storekey(value)` (with value bytes inverted
/// for DESC columns) using
/// [`crate::sorted_series::append_prefix_col_to_key`], the same
/// encoding `sorted_series` produces — so this per-RG key is a
/// literal byte prefix of every `sorted_series` key emitted by rows
/// in this RG.
///
/// Null handling:
/// - **All-null RG on a prefix column**: the column is skipped entirely (the next column's higher
///   ordinal byte appears in its place), so the RG sorts after any RG carrying a non-null value
///   for this column. This mirrors the row-level convention in `sorted_series` and gives
///   nulls-last ordering for free.
/// - **Mixed null + non-null in one RG**: rows in the RG would encode to two distinct prefix keys
///   (the non-null value's key and the column-skipped key), breaking the
///   at-most-one-prefix-value-per-RG invariant (PA-1). Reject.
/// - **No nulls**: standard `min == max` check on stats, then encode that single value.
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

        // Parquet's `num_values` is total cell count including nulls.
        // `null_count_opt()` returns the explicitly-recorded null
        // count (defaulting to 0 when absent, per parquet-rs guidance).
        let num_values = chunk.num_values().max(0) as u64;
        let null_count = stats.null_count_opt().unwrap_or(0);

        if num_values > 0 && null_count == num_values {
            // All-null RG: skip the column entirely (don't write its
            // ordinal byte or value). The trailing prefix-length
            // sentinel below ensures the resulting all-null key
            // still sorts after any non-null key. See the sentinel
            // comment for the full argument.
            continue;
        }
        if null_count > 0 {
            // PA-1 violation: see function doc. Parquet's min/max
            // hide nulls, so an RG with N nulls + 1 non-null cell
            // reports `min == max == non_null_value` even though
            // rows in that RG encode to two distinct prefix keys.
            bail!(
                "input {input_idx} rg {rg_idx} col '{}' is NOT prefix-aligned: contains \
                 {null_count} nulls plus {} non-null values. PA-1 requires each row group to \
                 carry a single prefix value; rows with null on this column encode to a \
                 different prefix key (with the column skipped) than rows with the non-null \
                 value.",
                col.name,
                num_values - null_count,
            );
        }

        encode_prefix_col_value(stats, col, rg_idx, input_idx, &mut key)?;
    }

    // Trailing prefix-length sentinel: an additional `u8(prefix_len)`
    // ordinal byte that does two things at once:
    //
    // 1. **Forces nulls-last ordering across RGs.** For prefix_len=1
    //    an all-null RG produces an empty per-column body and would
    //    otherwise lex-sort *before* any non-null RG. With the
    //    sentinel, the all-null key becomes `[prefix_len]` and the
    //    non-null key becomes `[ord(0), storekey(value), ..., prefix_len]`.
    //    The non-null key starts with `ord(0) = 0x00`, smaller than
    //    `prefix_len >= 1`, so non-null RGs sort first — matching
    //    `sorted_series`'s row-level nulls-last convention via the
    //    same "the next ordinal byte appears in the skipped slot"
    //    mechanism.
    // 2. **Preserves the "literal prefix of sorted_series" property.**
    //    The byte we append is exactly what `sorted_series` writes
    //    right after the prefix columns: the ordinal of the next
    //    sort-schema column (`u8(prefix_len)`). So the per-RG key
    //    remains a byte-for-byte prefix of every row's
    //    `sorted_series` value in that RG.
    storekey::encode(&mut key, &(prefix_cols.len() as u8))
        .map_err(|e| anyhow!("storekey encode prefix-length sentinel: {}", e))?;

    Ok(key)
}

/// Verify `min == max` on the column chunk's non-null stats and
/// append the single value to `key` via
/// [`crate::sorted_series::append_prefix_col_to_key`] (which handles
/// the ordinal prefix + descending-direction byte inversion). Caller
/// has already filtered out all-null and mixed-null cases.
///
/// `Statistics::ByteArray` values are routed through the
/// `Encode for str` impl after a UTF-8 check — every realistic sort
/// prefix column (`metric_name`, `service`, tag names) is UTF-8
/// text, and `sorted_series` itself only encodes strings, so the
/// "byte prefix of sorted_series" property only holds for UTF-8
/// values. Non-UTF-8 byte-array prefix cols would never match a
/// `sorted_series` key in practice (sorted_series would not encode
/// them either) and so are rejected up front.
fn encode_prefix_col_value(
    stats: &parquet::file::statistics::Statistics,
    col: &PrefixColumn,
    rg_idx: usize,
    input_idx: usize,
    key: &mut Vec<u8>,
) -> Result<()> {
    use parquet::file::statistics::Statistics;

    fn require_eq<T: PartialEq + std::fmt::Debug>(
        min: Option<T>,
        max: Option<T>,
        col_name: &str,
        rg_idx: usize,
        input_idx: usize,
    ) -> Result<T> {
        let min = min.ok_or_else(|| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{col_name}' has no min in stats — cannot \
                 determine prefix alignment"
            )
        })?;
        let max = max.ok_or_else(|| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{col_name}' has no max in stats — cannot \
                 determine prefix alignment"
            )
        })?;
        if min != max {
            bail!(
                "input {input_idx} rg {rg_idx} is NOT prefix-aligned on col '{col_name}': min \
                 ({:?}) != max ({:?}). Multi-RG inputs declaring `rg_partition_prefix_len >= 1` \
                 must carry one prefix-value per RG.",
                min,
                max,
            );
        }
        Ok(min)
    }

    fn encode_byte_array_value(
        min_bytes: Option<&[u8]>,
        max_bytes: Option<&[u8]>,
        col: &PrefixColumn,
        rg_idx: usize,
        input_idx: usize,
        key: &mut Vec<u8>,
    ) -> Result<()> {
        let value = require_eq(
            min_bytes.map(|b| b.to_vec()),
            max_bytes.map(|b| b.to_vec()),
            &col.name,
            rg_idx,
            input_idx,
        )?;
        let s = std::str::from_utf8(&value).map_err(|_| {
            anyhow!(
                "input {input_idx} rg {rg_idx} col '{}' has non-UTF-8 byte-array prefix value; \
                 only UTF-8 string prefix columns are supported (matching sorted_series's `&str` \
                 encoding)",
                col.name,
            )
        })?;
        append_prefix_col_to_key(key, col.ordinal, s, col.descending)
    }

    match stats {
        Statistics::ByteArray(v) => {
            encode_byte_array_value(
                v.min_bytes_opt(),
                v.max_bytes_opt(),
                col,
                rg_idx,
                input_idx,
                key,
            )?;
        }
        Statistics::FixedLenByteArray(v) => {
            encode_byte_array_value(
                v.min_bytes_opt(),
                v.max_bytes_opt(),
                col,
                rg_idx,
                input_idx,
                key,
            )?;
        }
        Statistics::Int32(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                &col.name,
                rg_idx,
                input_idx,
            )?;
            append_prefix_col_to_key(key, col.ordinal, &value, col.descending)?;
        }
        Statistics::Int64(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                &col.name,
                rg_idx,
                input_idx,
            )?;
            append_prefix_col_to_key(key, col.ordinal, &value, col.descending)?;
        }
        Statistics::Boolean(v) => {
            let value = require_eq(
                v.min_opt().copied(),
                v.max_opt().copied(),
                &col.name,
                rg_idx,
                input_idx,
            )?;
            append_prefix_col_to_key(key, col.ordinal, &value, col.descending)?;
        }
        Statistics::Float(_) | Statistics::Double(_) => bail!(
            "prefix col '{}' is floating-point; composite-key extraction does not yet support \
             IEEE-754 ordering. Open an issue if you hit this — the encoding needs a sign-aware \
             bit flip on negative values.",
            col.name,
        ),
        Statistics::Int96(_) => bail!(
            "prefix col '{}' is Int96 (deprecated timestamp type); use Int64-encoded \
             `timestamp_secs` instead.",
            col.name,
        ),
    }
    Ok(())
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
/// message rather than letting `process_region` bail mid-merge with
/// "page from rg X while draining rg Y" — same error class, but
/// caught at a point where we can report the offending input and
/// region instead of the lower-level page mismatch.
pub(crate) fn validate_region_order_matches_physical_rg_order(
    regions: &[Region],
    num_inputs: usize,
) -> Result<()> {
    let mut last_position_per_input: Vec<Option<(usize, usize)>> = vec![None; num_inputs];
    for (region_idx, region) in regions.iter().enumerate() {
        for c in &region.contributing {
            let position = (c.rg_idx, c.start_row);
            if let Some(prev) = last_position_per_input[c.input_idx]
                && position < prev
            {
                bail!(
                    "region iteration disagrees with input {}'s physical row order: region \
                     {region_idx} wants rg {} row {} but a previous region already passed \
                     position rg {} row {}. The composite prefix key encoding does not match the \
                     input's physical layout — check that the sort schema's direction matches how \
                     the file is actually sorted on disk.",
                    c.input_idx,
                    c.rg_idx,
                    c.start_row,
                    prev.0,
                    prev.1,
                );
            }
            last_position_per_input[c.input_idx] = Some((c.rg_idx, c.start_row + c.num_rows));
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
        //
        // The caller (`run_synthesized_prefix_path`) will split this
        // region further once it has read the merge order and can see
        // where the first sort col's value transitions.
        let mut contributing = Vec::new();
        for (idx, state) in decoders_state.iter().enumerate() {
            if state.metadata.num_row_groups() == 0 {
                continue;
            }
            let rg_meta = state.metadata.row_group(0);
            contributing.push(RegionContribution {
                input_idx: idx,
                rg_idx: 0,
                start_row: 0,
                num_rows: rg_meta.num_rows() as usize,
            });
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
    //
    // **Strong invariant** (enforced here on the merge read path, and
    // mirrored on both write paths — see `assert_unique_rg_prefix_keys`):
    // no single input may have two row groups sharing the same composite
    // prefix key. The streaming engine pairs at most one RG per input
    // per region (`process_region` keys `sort_col_batches` by
    // `input_idx`), so a duplicate prefix would silently overwrite the
    // first RG's sort batch while `Region::total_rows` still counts both
    // — dropping rows and corrupting body-col / sort-col alignment.
    //
    // Cross-input duplicates are fine (and expected — that's the whole
    // point of region merging). The constraint is **same input, same
    // prefix key, multiple RGs**: producers must ensure prefix
    // transitions align with RG boundaries.
    let mut by_prefix: BTreeMap<Vec<u8>, Vec<RegionContribution>> = BTreeMap::new();
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
        let mut seen_for_input: HashSet<Vec<u8>> = HashSet::new();
        for rg_idx in 0..state.metadata.num_row_groups() {
            let prefix_key =
                extract_rg_composite_prefix_key(&state.metadata, rg_idx, &prefix_cols, input_idx)?;
            if !seen_for_input.insert(prefix_key.clone()) {
                bail!(
                    "input {input_idx} has rg {rg_idx} sharing a prefix key with an earlier RG in \
                     the same file. The streaming merge engine requires at-most-one-RG-per-input \
                     per prefix value (rg_partition_prefix_len = {prefix_len}); the producer must \
                     ensure prefix transitions align with RG boundaries. Either lower \
                     rg_partition_prefix_len to include fewer columns, or rewrite the producer to \
                     start a new RG at every prefix-value change."
                );
            }
            let num_rows = state.metadata.row_group(rg_idx).num_rows() as usize;
            by_prefix
                .entry(prefix_key)
                .or_default()
                .push(RegionContribution {
                    input_idx,
                    rg_idx,
                    start_row: 0,
                    num_rows,
                });
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

/// Post-write check: verify the parquet file at `metadata` has no two
/// row groups sharing the same composite prefix key, for the first
/// `prefix_len` sort columns. Returns `Ok(())` immediately if
/// `prefix_len == 0` (no alignment claim).
///
/// This is the writer-side mirror of the read-side check in
/// `extract_regions_from_metadata` — both indexing and the compaction
/// merge output writer call this after sealing a parquet file so a
/// producer bug never lets a duplicate-prefix file land on disk. See
/// the doc-comment on `extract_regions_from_metadata` for why
/// at-most-one-RG-per-prefix is load-bearing for the streaming
/// engine.
///
/// `context` is included in the error message — e.g.,
/// `"indexing write at <path>"` or `"merge output <split_id>"`.
pub(crate) fn assert_unique_rg_prefix_keys(
    metadata: &ParquetMetaData,
    sort_fields_str: &str,
    prefix_len: u32,
    context: &str,
) -> Result<()> {
    if prefix_len == 0 {
        return Ok(());
    }
    let num_rgs = metadata.num_row_groups();
    if num_rgs <= 1 {
        // Single-RG (or zero-RG) files vacuously satisfy the invariant.
        return Ok(());
    }
    let prefix_cols =
        find_prefix_parquet_col_indices(metadata, sort_fields_str, prefix_len as usize, 0)
            .with_context(|| format!("resolving prefix cols for {context}"))?;
    let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(num_rgs);
    for rg_idx in 0..num_rgs {
        let key = extract_rg_composite_prefix_key(metadata, rg_idx, &prefix_cols, 0)
            .with_context(|| format!("extracting prefix key at {context} rg {rg_idx}"))?;
        if !seen.insert(key) {
            bail!(
                "{context}: rg {rg_idx} shares a prefix key with an earlier row group. \
                 rg_partition_prefix_len = {prefix_len} requires prefix transitions to align with \
                 row group boundaries. Either lower the prefix length to include fewer columns, \
                 or change the writer so each RG carries a unique value of the first {prefix_len} \
                 sort columns."
            );
        }
    }
    Ok(())
}

/// Subdivide a region into a sequence of sub-regions whose cumulative
/// row counts approach `target_per_output`, splitting only at
/// `sorted_series` transitions within the region's merge order. A
/// single `sorted_series` run is never broken — if one run exceeds
/// the remaining budget, the whole run goes to one output anyway.
///
/// `first_target` is the budget for the FIRST sub-region (typically
/// the remaining capacity of the current output being filled by the
/// caller). Subsequent sub-regions target `target_per_output`.
/// `outputs_remaining` is the number of output files still available;
/// when it hits 1 we stop splitting and emit the rest as one sub-
/// region.
///
/// The returned sub-regions:
/// - Cover the full input region in sort order.
/// - Each carries per-input row ranges (`start_row`/`num_rows`) inside the same `(input_idx,
///   rg_idx)` as the parent — sub-regions of one region all share their parent's RGs.
/// - Inherit the parent's `prefix_key`; the prefix value is constant across the parent and
///   therefore across every sub-region.
pub(crate) fn split_region_at_sorted_series(
    region: &Region,
    merge_order: &[MergeRun],
    aligned_sort_batches: &[RecordBatch],
    first_target: usize,
    target_per_output: usize,
    outputs_remaining: usize,
) -> Result<Vec<Region>> {
    use arrow::array::BinaryArray;

    use crate::sorted_series::SORTED_SERIES_COLUMN;

    if merge_order.is_empty() {
        return Ok(Vec::new());
    }
    if outputs_remaining <= 1 {
        return Ok(vec![region.clone()]);
    }

    // Per-input sorted_series array. compute_merge_order already
    // requires this column on every input, so a missing-column case
    // here is a bug rather than a configuration error.
    let mut ss_arrays: Vec<Option<&BinaryArray>> = Vec::with_capacity(aligned_sort_batches.len());
    for batch in aligned_sort_batches {
        match batch.schema().index_of(SORTED_SERIES_COLUMN) {
            Ok(idx) => {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("`{SORTED_SERIES_COLUMN}` must be Binary-typed"))?;
                ss_arrays.push(Some(arr));
            }
            Err(_) => ss_arrays.push(None),
        }
    }

    let ss_at = |run_idx: usize| -> Option<&[u8]> {
        let run = &merge_order[run_idx];
        ss_arrays[run.input_index].map(|a| a.value(run.start_row))
    };

    // Walk runs, splitting before a run whose preceding sorted_series
    // transition crosses the current target. We can only split at run
    // boundaries (a run has constant sorted_series internally), so
    // breaking inside a run is impossible — a giant single-series run
    // simply lands in one output regardless of size.
    let mut splits: Vec<std::ops::Range<usize>> = Vec::new();
    let mut current_start: usize = 0;
    let mut accumulated: usize = 0;
    let mut current_target = first_target;
    let mut outputs_left = outputs_remaining;

    for (run_idx, run) in merge_order.iter().enumerate() {
        if run_idx > 0 && outputs_left > 1 && accumulated >= current_target {
            let prev_ss = ss_at(run_idx - 1);
            let curr_ss = ss_at(run_idx);
            let at_transition = match (prev_ss, curr_ss) {
                (Some(a), Some(b)) => a != b,
                _ => true,
            };
            if at_transition {
                splits.push(current_start..run_idx);
                current_start = run_idx;
                accumulated = 0;
                outputs_left -= 1;
                current_target = target_per_output;
            }
        }
        accumulated += run.row_count;
    }
    splits.push(current_start..merge_order.len());

    // Build each sub-region's contributing list from the runs in its
    // range. Within a sub-region, each input's rows are contiguous
    // (the merge engine consumes rows in increasing input-row order
    // and the parent region's contributions are themselves
    // contiguous), so a `(min_run.start_row, sum_row_count)` range
    // captures the full slice.
    let rg_for_input: std::collections::HashMap<usize, usize> = region
        .contributing
        .iter()
        .map(|c| (c.input_idx, c.rg_idx))
        .collect();
    let parent_start_row: std::collections::HashMap<usize, usize> = region
        .contributing
        .iter()
        .map(|c| (c.input_idx, c.start_row))
        .collect();

    let mut sub_regions: Vec<Region> = Vec::with_capacity(splits.len());
    for range in splits {
        let mut ranges: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
        for run in &merge_order[range.clone()] {
            let entry = ranges
                .entry(run.input_index)
                .or_insert((run.start_row, run.start_row));
            entry.0 = entry.0.min(run.start_row);
            entry.1 = entry.1.max(run.start_row + run.row_count);
        }
        let contributing: Vec<RegionContribution> = ranges
            .into_iter()
            .map(|(input_idx, (start, end))| RegionContribution {
                input_idx,
                rg_idx: *rg_for_input.get(&input_idx).expect("rg_idx from parent"),
                // The merge order's run.start_row is local to the
                // aligned sort batch (which itself is the drained
                // contribution); add the parent's start_row to get
                // the absolute row inside the RG.
                start_row: parent_start_row.get(&input_idx).copied().unwrap_or(0) + start,
                num_rows: end - start,
            })
            .collect();
        sub_regions.push(Region {
            prefix_key: region.prefix_key.clone(),
            contributing,
        });
    }

    Ok(sub_regions)
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
