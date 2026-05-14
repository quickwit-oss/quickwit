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

//! Streaming column-major merge engine with page-bounded body cols.
//!
//! Architecture (multi-input → multi-output sorted merge with sort-cols-first
//! column ordering):
//!
//! 1. **Phase 0 (async): drain sort cols** from each input. With the storage convention that sort
//!    cols + `sorted_series` precede all body cols within each row group, we can stop the decoder
//!    after those are fully decoded. The remaining body col pages stay un-read in the input stream,
//!    ready for phase 3.
//! 2. **Phase 1: compute merge order** via the existing k-way merge on `(sorted_series,
//!    timestamp_secs)` from the per-input sort col [`RecordBatch`]es. Produces a run-length-encoded
//!    merge plan over input row positions.
//! 3. **Phase 2: compute output boundaries** with the caller's `num_outputs`, splitting at
//!    `sorted_series` transitions so each output file's key range is non-overlapping with adjacent
//!    files.
//! 4. **Phase 3 (blocking + block_on bridges): streaming write**. All output writers are alive for
//!    the duration. For each column in storage order, every output's col K is written in turn:
//!    - Sort col / `sorted_series`: applied via `take` from the already-buffered phase 0 data.
//!    - Body col: each output page is assembled via [`arrow::compute::interleave`] from input page
//!      slices, with decoders advanced page-by-page via `Handle::block_on` from inside a sync
//!      iterator. Pages flush to the writer's sink as [`SerializedColumnWriter`]'s page-size
//!      threshold trips — memory stays bounded by the in-flight output page plus a small number of
//!      in-flight input pages.
//!
//! After all M outputs' col K is done, every input decoder is at the
//! start of col K+1 in its single row group. Move to col K+1.
//!
//! ## Single-RG inputs assumption
//!
//! PR-6b.2 only handles **single-row-group inputs**. With multi-RG
//! inputs the body bytes interleave with successive RGs' sort cols
//! (`sort_cols_RG0`, `body_cols_RG0`, `sort_cols_RG1`, ...), so
//! draining sort cols from RG1 onwards requires either consuming +
//! discarding body cols of RG0 from the stream or buffering them.
//! Neither fits the page-bounded contract; multi-RG-input streaming
//! lands in a follow-up. Today's real inputs are: (a) post-PR-3
//! single-RG ingest splits, or (b) PR-5's legacy adapter that
//! presents arbitrary multi-RG splits as one synthetic RG. Both
//! satisfy the assumption.
//!
//! [`SerializedColumnWriter`]: parquet::file::writer::SerializedColumnWriter

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow::compute::interleave;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use parquet::file::metadata::ParquetMetaData;
use tokio::runtime::Handle;
use tracing::info;
use ulid::Ulid;

use super::merge_order::{MergeRun, compute_merge_order, compute_output_boundaries};
use super::schema::{align_inputs_to_union_schema, optimize_output_batch};
use super::writer::{
    apply_merge_permutation, build_merge_kv_metadata, build_sorting_columns,
    resolve_sort_field_names, verify_sort_order,
};
use super::{InputMetadata, MergeConfig, MergeOutputFile};
use crate::row_keys;
use crate::sort_fields::{
    equivalent_schemas_for_compaction, is_timestamp_column_name, parse_sort_fields,
};
use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::split::TAG_SERVICE;
use crate::storage::page_decoder::{DecodedPage, StreamDecoder};
use crate::storage::split_writer::{extract_metric_names, extract_time_range};
use crate::storage::streaming_writer::StreamingParquetWriter;
use crate::storage::{
    ColumnPageStream, PARQUET_META_NUM_MERGE_OPS, PARQUET_META_RG_PARTITION_PREFIX_LEN,
    PARQUET_META_SORT_FIELDS, PARQUET_META_WINDOW_DURATION, PARQUET_META_WINDOW_START,
};
use crate::zonemap::{self, ZonemapOptions};

/// Output page size in rows for body-col assembly. Each call to the
/// sync iterator passed to [`write_next_column_arrays`] yields one
/// `ArrayRef` of up to this many rows; the parquet writer flushes
/// physical pages independently as encoded bytes cross
/// `data_page_size_limit`. 1024 keeps assembled arrays small enough
/// to bound per-output memory but large enough to amortise per-page
/// fixed costs.
///
/// [`write_next_column_arrays`]: crate::storage::streaming_writer::RowGroupBuilder::write_next_column_arrays
const OUTPUT_PAGE_ROWS: usize = 1024;

/// Test-only peak observed length of any input's `body_col_page_cache`
/// since the last reset. Used by the MS-7 page-bounded-memory test to
/// assert that the cache stays bounded by a small constant regardless
/// of input column size. Set unconditionally inside the merge so the
/// invariant is observable in any test build; reset on each merge entry.
#[cfg(test)]
pub(crate) static PEAK_BODY_COL_PAGE_CACHE_LEN: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn record_body_col_page_cache_len(len: usize) {
    use std::sync::atomic::Ordering;
    let mut prev = PEAK_BODY_COL_PAGE_CACHE_LEN.load(Ordering::Relaxed);
    while len > prev {
        match PEAK_BODY_COL_PAGE_CACHE_LEN.compare_exchange_weak(
            prev,
            len,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => prev = observed,
        }
    }
}

#[cfg(not(test))]
fn record_body_col_page_cache_len(_len: usize) {}

/// Streaming N-input → M-output column-major merge.
///
/// See module docs for the four phases. Returns one
/// [`MergeOutputFile`] per output file produced (zero-row outputs are
/// dropped). Caller's `config.num_outputs` is the upper bound on the
/// number of files; fewer are returned when there are not enough
/// `sorted_series` transitions to split at.
pub async fn streaming_merge_sorted_parquet_files(
    inputs: Vec<Box<dyn ColumnPageStream>>,
    output_dir: &Path,
    config: &MergeConfig,
) -> Result<Vec<MergeOutputFile>> {
    if inputs.is_empty() {
        bail!("merge requires at least one input");
    }
    if config.num_outputs == 0 {
        bail!("num_outputs must be at least 1");
    }

    // Validate that all inputs are single-RG (or zero-RG, which means
    // the file has no data). PR-6b.2 simplification — see module docs.
    for (idx, stream) in inputs.iter().enumerate() {
        let num_rgs = stream.metadata().num_row_groups();
        if num_rgs > 1 {
            bail!(
                "streaming merge requires single-row-group inputs in PR-6b.2 (input {idx} has \
                 {num_rgs} row groups); multi-RG metric-aligned inputs land in a follow-up. \
                 Legacy multi-RG (rg_partition_prefix_len=0) inputs must go through the PR-5 \
                 adapter, which presents them as a single synthetic row group."
            );
        }
    }

    let input_meta = extract_and_validate_input_metadata(&inputs)?;

    info!(
        num_inputs = inputs.len(),
        num_outputs = config.num_outputs,
        sort_fields = %input_meta.sort_fields,
        "starting streaming sorted parquet merge"
    );

    let output_dir = output_dir.to_path_buf();
    let writer_config = config.writer_config.clone();
    let num_outputs = config.num_outputs;

    // Move everything onto a blocking task. Inside, the decoders need to
    // make async I/O calls (page fetches over the network); we drive
    // those via `handle.block_on(...)` from inside sync iterators that
    // feed the parquet writer's column-write methods. The writer is
    // sync; this single-task pattern avoids the lifetime complexity of
    // moving borrowed `RowGroupBuilder`s across tokio tasks.
    let result = tokio::task::spawn_blocking(move || -> Result<Vec<MergeOutputFile>> {
        let handle = Handle::current();

        let mut inputs = inputs;
        let mut decoders_state = build_input_decoders_state(&mut inputs)?;

        // Phase 0
        let sort_col_batches =
            drain_sort_cols_all_inputs(&handle, &mut decoders_state, &input_meta.sort_fields)?;

        if sort_col_batches.iter().all(|b| b.num_rows() == 0) {
            info!("all inputs empty, producing no output");
            return Ok(Vec::new());
        }

        // Phase 1: align inputs to a union sort-col schema so the merge-order
        // comparator sees uniformly-typed `sorted_series` + `timestamp_secs`.
        let (sort_union_schema, aligned_sort_batches) =
            align_inputs_to_union_schema(&sort_col_batches, &input_meta.sort_fields)?;
        let merge_order = compute_merge_order(&aligned_sort_batches, &input_meta.sort_fields)?;

        // Phase 2: split merge order into M outputs at sorted_series boundaries.
        let boundaries =
            compute_output_boundaries(&merge_order, &aligned_sort_batches, num_outputs)?;

        let total_rows: usize = aligned_sort_batches.iter().map(|b| b.num_rows()).sum();
        info!(
            total_rows,
            num_outputs = boundaries.len(),
            "streaming merge order computed"
        );

        // Pre-compute per-input row → (output_idx, output_position) destination map.
        // Used by every column write to slice take/interleave indices per page.
        let destinations =
            build_input_row_destinations(&aligned_sort_batches, &merge_order, &boundaries);

        // Phase 3
        let outputs = write_streaming_outputs(
            &handle,
            &mut decoders_state,
            &aligned_sort_batches,
            &sort_union_schema,
            &merge_order,
            &boundaries,
            &destinations,
            &input_meta,
            &writer_config,
            &output_dir,
        )?;

        // MC-1: total row count preserved.
        let output_total: usize = outputs.iter().map(|o| o.num_rows).sum();
        quickwit_dst::check_invariant!(
            quickwit_dst::invariants::InvariantId::MC1,
            output_total == total_rows,
            ": streaming merge input rows={}, output rows={}",
            total_rows,
            output_total,
        );

        Ok(outputs)
    })
    .await
    .context("streaming merge blocking task panicked")??;

    Ok(result)
}

/// Per-input state held across phase 0 → phase 3 inside the blocking
/// task. The decoder owns its stream so it persists across phases and
/// across all output writes for a given input — critical for body
/// columns whose pages may need to be visited multiple times (one page
/// can supply rows for more than one output, or one output may need
/// rows from more than one page). Reconstructing the decoder mid-pass
/// would reset the per-column `rows_decoded` counter (so `row_start`
/// becomes wrong) and discard cached dictionary / queued pages.
struct InputDecoderState {
    decoder: StreamDecoder<'static>,
    metadata: Arc<ParquetMetaData>,
    /// Arrow schema of this input (from parquet → arrow conversion).
    arrow_schema: SchemaRef,
    /// Per-input page cache for the *currently active* body column.
    /// Pages are appended as the decoder produces them and evicted from
    /// the front once their last row is below `body_col_cursor`. The
    /// cache must persist across all outputs that consume rows from
    /// this column so that a page whose range straddles two outputs is
    /// not re-decoded (the stream has already advanced past it). At
    /// the start of each body column [`reset_body_col_state`] clears
    /// this cache and zeroes the cursor.
    ///
    /// **Memory bound (horizontal, not vertical).** This cache is
    /// per-input: total memory across inputs is
    /// `N_inputs × per_input_peak`. The per-input peak is bounded by
    /// `ceil(OUTPUT_PAGE_ROWS / input_page_rows) + small_slack` —
    /// driven by [`fill_page_cache_to_row`], which fetches only enough
    /// pages to cover the rows from *this* input that contribute to
    /// the *current output page* (a 1024-row slice), and by the
    /// eviction loop in [`assemble_one_output_page`], which drops any
    /// page whose last row falls below the cursor after each output
    /// page emits. The cache never holds an input column-chunk's worth
    /// of pages — a regression that ever did would break the MS-7
    /// invariant asserted by
    /// `test_ms7_body_col_page_cache_bounded_regardless_of_input_size`.
    body_col_page_cache: Vec<DecodedPage>,
    /// Next unconsumed input row for the active body column. Advances
    /// monotonically across outputs because the merge plan assigns each
    /// input's rows to outputs in input-row order.
    body_col_cursor: usize,
}

impl InputDecoderState {
    /// Clear the per-input body-column cache. Called at the start of
    /// each new body column so leftover pages from the previous column
    /// (which have a different `col_idx`) don't poison the new column's
    /// row-start arithmetic. The decoder itself is *not* reset — its
    /// per-(rg, col) `rows_decoded` counters and queued pages must
    /// survive so subsequent decode calls return correct row offsets.
    fn reset_body_col_state(&mut self) {
        self.body_col_page_cache.clear();
        self.body_col_cursor = 0;
    }
}

/// Build per-input state. The streams are moved in from the caller and
/// installed in long-lived [`StreamDecoder`]s so per-column state
/// survives every phase of the merge.
fn build_input_decoders_state(
    inputs: &mut Vec<Box<dyn ColumnPageStream>>,
) -> Result<Vec<InputDecoderState>> {
    let mut states = Vec::with_capacity(inputs.len());
    for stream in inputs.drain(..) {
        let metadata = Arc::clone(stream.metadata());
        let parquet_schema = metadata.file_metadata().schema_descr();
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(parquet_schema, None)
            .context("converting parquet schema → arrow")?;
        let decoder = StreamDecoder::from_owned(stream);
        states.push(InputDecoderState {
            decoder,
            metadata,
            arrow_schema: Arc::new(arrow_schema),
            body_col_page_cache: Vec::new(),
            body_col_cursor: 0,
        });
    }
    Ok(states)
}

/// Extract sort schema, window, and merge-ops metadata from each
/// input stream and validate consistency across inputs. Reads
/// `qh.*` KVs from [`ColumnPageStream::metadata`].
fn extract_and_validate_input_metadata(
    inputs: &[Box<dyn ColumnPageStream>],
) -> Result<InputMetadata> {
    let mut consensus_sort_fields: Option<String> = None;
    let mut consensus_window_start: Option<Option<i64>> = None;
    let mut consensus_window_duration: Option<u32> = None;
    let mut consensus_prefix_len: Option<u32> = None;
    let mut max_merge_ops: u32 = 0;

    for (idx, stream) in inputs.iter().enumerate() {
        let metadata = stream.metadata();
        let kv_metadata = metadata.file_metadata().key_value_metadata();

        let find_kv = |key: &str| -> Option<String> {
            kv_metadata.and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.clone())
            })
        };

        let file_sort_fields = match find_kv(PARQUET_META_SORT_FIELDS) {
            Some(s) => s,
            None => bail!(
                "input {idx} is missing {} metadata",
                PARQUET_META_SORT_FIELDS,
            ),
        };

        match &consensus_sort_fields {
            Some(expected) => {
                let expected_schema = parse_sort_fields(expected)?;
                let file_schema = parse_sort_fields(&file_sort_fields).with_context(|| {
                    format!("parsing sort schema from input {idx}: '{file_sort_fields}'")
                })?;
                if !equivalent_schemas_for_compaction(&expected_schema, &file_schema) {
                    bail!(
                        "sort schema mismatch in input {idx}: expected '{expected}', found \
                         '{file_sort_fields}'",
                    );
                }
            }
            None => {
                parse_sort_fields(&file_sort_fields).with_context(|| {
                    format!("parsing sort schema from input {idx}: '{file_sort_fields}'")
                })?;
                consensus_sort_fields = Some(file_sort_fields.clone());
            }
        }

        let file_window_start = find_kv(PARQUET_META_WINDOW_START)
            .map(|s| s.parse::<i64>())
            .transpose()
            .with_context(|| format!("parsing window_start from input {idx}"))?;
        match &consensus_window_start {
            Some(expected) if file_window_start != *expected => {
                bail!(
                    "window_start mismatch in input {idx}: expected {:?}, found {:?}",
                    expected,
                    file_window_start,
                );
            }
            Some(_) => {}
            None => consensus_window_start = Some(file_window_start),
        }

        let file_window_duration = find_kv(PARQUET_META_WINDOW_DURATION)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing window_duration from input {idx}"))?
            .unwrap_or(0);
        match &consensus_window_duration {
            Some(expected) if file_window_duration != *expected => {
                bail!(
                    "window_duration_secs mismatch in input {idx}: expected {expected}, found \
                     {file_window_duration}",
                );
            }
            Some(_) => {}
            None => consensus_window_duration = Some(file_window_duration),
        }

        let file_merge_ops = find_kv(PARQUET_META_NUM_MERGE_OPS)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing num_merge_ops from input {idx}"))?
            .unwrap_or(0);
        if file_merge_ops > max_merge_ops {
            max_merge_ops = file_merge_ops;
        }

        let file_prefix_len = find_kv(PARQUET_META_RG_PARTITION_PREFIX_LEN)
            .map(|s| s.parse::<u32>())
            .transpose()
            .with_context(|| format!("parsing rg_partition_prefix_len from input {idx}"))?
            .unwrap_or(0);
        match &consensus_prefix_len {
            Some(expected) if file_prefix_len != *expected => {
                bail!(
                    "rg_partition_prefix_len mismatch in input {idx}: expected {expected}, found \
                     {file_prefix_len}",
                );
            }
            Some(_) => {}
            None => consensus_prefix_len = Some(file_prefix_len),
        }
    }

    let sort_fields = match consensus_sort_fields {
        Some(s) => s,
        None => bail!("at least one input is required"),
    };

    // `rg_partition_prefix_len` is intentionally optional in the data
    // model. Splits written before the prefix-aligned-RG era (and any
    // split not written by the streaming engine) simply lack the KV.
    // Inputs that *do* declare a value all had to agree above (else we
    // already bailed), so falling through to 0 here means "none of the
    // inputs claimed a prefix" rather than "we lost the value." The
    // legacy-promotion path in PR-6423 handles mixing 0 with non-zero
    // prefixes via the `mixed_prefix_ok` escape hatch.
    Ok(InputMetadata {
        sort_fields,
        window_start_secs: consensus_window_start.unwrap_or(None),
        window_duration_secs: consensus_window_duration.unwrap_or(0),
        num_merge_ops: max_merge_ops + 1,
        rg_partition_prefix_len: consensus_prefix_len.unwrap_or(0),
    })
}

// ============================================================================
// Phase 0: drain sort cols from each input
// ============================================================================

/// Drive each input's decoder via `block_on` until its sort cols +
/// `sorted_series` are fully decoded for the (single) row group.
/// Returns one [`RecordBatch`] per input with just those columns; the
/// rest of each input's body bytes stay un-read in the stream, ready
/// for phase 3 to consume page-by-page.
fn drain_sort_cols_all_inputs(
    handle: &Handle,
    decoders_state: &mut [InputDecoderState],
    sort_fields_str: &str,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(decoders_state.len());
    for (idx, state) in decoders_state.iter_mut().enumerate() {
        let batch = drain_sort_cols_one_input(handle, state, sort_fields_str, idx)?;
        if batch.num_columns() > 0 && batch.schema().index_of(SORTED_SERIES_COLUMN).is_err() {
            bail!(
                "input {idx} is missing the '{}' column required for merge",
                SORTED_SERIES_COLUMN,
            );
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn drain_sort_cols_one_input(
    handle: &Handle,
    state: &mut InputDecoderState,
    sort_fields_str: &str,
    input_idx: usize,
) -> Result<RecordBatch> {
    if state.metadata.num_row_groups() == 0 {
        // Empty input — no rows to drain. Return a zero-row batch with the
        // sort cols' fields preserved so downstream merge order code sees a
        // uniform schema across inputs.
        return empty_sort_col_record_batch(state, sort_fields_str);
    }
    let sort_field_schema = parse_sort_fields(sort_fields_str)?;

    // The set of column names we treat as "sort columns" for drain
    // purposes: every sort-schema column name that is present in this
    // input's arrow schema, plus `sorted_series` (always required).
    let sort_col_names: HashSet<String> =
        sort_col_names_for_input(&sort_field_schema, state.arrow_schema.as_ref());

    // Map each sort col name → its parquet leaf column index. The
    // page decoder reports pages by parquet column index (matches arrow
    // top-level field index when there are no nested types).
    let parquet_schema = state.metadata.file_metadata().schema_descr();
    let mut sort_col_parquet_indices: HashMap<usize, String> = HashMap::new();
    for (col_idx, col) in parquet_schema.columns().iter().enumerate() {
        // For flat schemas (one leaf per top-level field), the parquet
        // column index equals the arrow top-level field index. We
        // match by name: parquet `column_path` root → arrow field name.
        let name = col.path().parts()[0].to_string();
        if sort_col_names.contains(&name) {
            sort_col_parquet_indices.insert(col_idx, name);
        }
    }

    if sort_col_parquet_indices.is_empty() {
        // No sort cols present in this input — return an empty batch
        // with the input's arrow schema. Downstream merge order check
        // will catch the missing `sorted_series`.
        return Ok(RecordBatch::new_empty(Arc::clone(&state.arrow_schema)));
    }

    // Target row count per sort col (from row group's column chunk metadata).
    let rg_meta = state.metadata.row_group(0);
    let mut target_rows_per_col: HashMap<usize, usize> = HashMap::new();
    for &col_idx in sort_col_parquet_indices.keys() {
        target_rows_per_col.insert(col_idx, rg_meta.column(col_idx).num_values() as usize);
    }

    // Drain pages into per-col buffers until all sort cols are fully
    // decoded. The streaming engine relies on a hard storage-ordering
    // contract: within a row group, parquet emits column chunks in
    // schema order (sort cols are declared first in our schema, body
    // cols after), so all sort col pages appear before any body col
    // page. Cross-file we don't require identical body-col ordering —
    // the body-col loop drives from the union schema and looks each
    // column up by name. The contract we DO require cross-file is
    // "sort cols come first." A page from a body col arriving here
    // means a producer violated that contract; bail rather than guess.
    let mut per_col_pages: HashMap<usize, Vec<ArrayRef>> = HashMap::new();
    let mut rows_done_per_col: HashMap<usize, usize> =
        sort_col_parquet_indices.keys().map(|&i| (i, 0)).collect();
    let mut sort_cols_finished = 0usize;
    let sort_col_target = sort_col_parquet_indices.len();

    while sort_cols_finished < sort_col_target {
        let decoded = handle
            .block_on(state.decoder.decode_next_page())
            .with_context(|| format!("decoding sort col page (input {input_idx})"))?;
        let page = match decoded {
            Some(p) => p,
            None => bail!(
                "stream ended before sort cols fully drained for input {input_idx}: \
                 {sort_cols_finished}/{sort_col_target} cols complete",
            ),
        };

        if !sort_col_parquet_indices.contains_key(&page.col_idx) {
            bail!(
                "input {input_idx} returned a non-sort page (col {}) before all sort cols were \
                 drained — sort-cols-first storage ordering violated",
                page.col_idx,
            );
        }
        if page.rg_idx != 0 {
            // PR-6b.2 (this PR) only supports single-RG inputs. The
            // multi-RG path with prefix-aligned row groups is added
            // in PR-6c.2 (#6424) along with `process_region` /
            // composite-key encoding.
            bail!(
                "input {input_idx} returned a page from rg {} during sort col drain — only \
                 single-RG inputs are supported in PR-6b.2",
                page.rg_idx,
            );
        }

        let array_len = page.array.len();
        let rows_done = rows_done_per_col
            .get_mut(&page.col_idx)
            .expect("sort_col_parquet_indices.contains_key check above guarantees presence");
        *rows_done += array_len;
        per_col_pages
            .entry(page.col_idx)
            .or_default()
            .push(page.array);

        if *rows_done == target_rows_per_col[&page.col_idx] {
            sort_cols_finished += 1;
        } else if *rows_done > target_rows_per_col[&page.col_idx] {
            bail!(
                "input {input_idx} col {} decoded more rows ({}) than expected ({})",
                page.col_idx,
                rows_done,
                target_rows_per_col[&page.col_idx],
            );
        }
    }

    // Build a RecordBatch holding just the sort cols. Field order
    // matches the arrow schema's order (so downstream consumers see
    // the same field order whether or not body cols are present).
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (field_idx, field) in state.arrow_schema.fields().iter().enumerate() {
        let Some(_name) = sort_col_parquet_indices.get(&field_idx) else {
            continue;
        };
        let pages = per_col_pages.remove(&field_idx).expect("col drained");
        let concatenated = concat_arrays(&pages).with_context(|| {
            format!(
                "concatenating sort col '{}' pages for input {input_idx}",
                field.name(),
            )
        })?;
        fields.push(Arc::clone(field));
        columns.push(concatenated);
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, columns)
        .with_context(|| format!("building sort col record batch for input {input_idx}"))
}

/// Set of column names treated as "sort cols" for phase 0 drain.
fn sort_col_names_for_input(
    sort_field_schema: &quickwit_proto::sortschema::SortSchema,
    arrow_schema: &ArrowSchema,
) -> HashSet<String> {
    let mut names: HashSet<String> = HashSet::new();
    for sf in &sort_field_schema.column {
        if arrow_schema.field_with_name(&sf.name).is_ok() {
            names.insert(sf.name.clone());
        }
        // Legacy schemas may declare `timestamp` but the column is named
        // `timestamp_secs`. The merge order code already handles this
        // alias; we want both candidates drained whichever matches.
        if is_timestamp_column_name(&sf.name)
            && arrow_schema.field_with_name("timestamp_secs").is_ok()
        {
            names.insert("timestamp_secs".to_string());
        }
    }
    if arrow_schema.field_with_name(SORTED_SERIES_COLUMN).is_ok() {
        names.insert(SORTED_SERIES_COLUMN.to_string());
    }
    names
}

/// Build a zero-row RecordBatch with the input's sort cols + `sorted_series`.
/// Used when an input file has zero rows (no row groups) so that downstream
/// k-way merge sees a consistent schema shape across inputs.
fn empty_sort_col_record_batch(
    state: &InputDecoderState,
    sort_fields_str: &str,
) -> Result<RecordBatch> {
    let sort_field_schema = parse_sort_fields(sort_fields_str)?;
    let sort_col_names = sort_col_names_for_input(&sort_field_schema, state.arrow_schema.as_ref());
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in state.arrow_schema.fields() {
        if !sort_col_names.contains(field.name()) {
            continue;
        }
        fields.push(Arc::clone(field));
        columns.push(new_null_array(field.data_type(), 0));
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, columns).context("building empty sort col record batch")
}

fn concat_arrays(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    if arrays.len() == 1 {
        return Ok(Arc::clone(&arrays[0]));
    }
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    Ok(arrow::compute::concat(&refs)?)
}

// ============================================================================
// Pre-compute input row → output destination map
// ============================================================================

/// `destinations[input_idx][input_row] = Some((output_idx, output_pos))`
/// if that input row contributes to output `output_idx` at position
/// `output_pos` within that output's row range. `None` means the row
/// is not in any output (only possible for rows beyond the merge
/// plan's coverage; shouldn't happen with our merge order).
#[derive(Debug)]
struct InputRowDestinations {
    /// One Vec per input. Length = input's sort-col row count.
    per_input: Vec<Vec<Option<(usize, usize)>>>,
    /// Total rows per output index (cumulative writer "expected" rows).
    rows_per_output: Vec<usize>,
}

fn build_input_row_destinations(
    aligned_sort_batches: &[RecordBatch],
    merge_order: &[MergeRun],
    boundaries: &[Range<usize>],
) -> InputRowDestinations {
    let mut per_input: Vec<Vec<Option<(usize, usize)>>> = aligned_sort_batches
        .iter()
        .map(|b| vec![None; b.num_rows()])
        .collect();
    let mut rows_per_output: Vec<usize> = vec![0; boundaries.len()];

    for (out_idx, boundary) in boundaries.iter().enumerate() {
        let runs = &merge_order[boundary.clone()];
        let mut rows_for_current_output = 0usize;
        for run in runs {
            for r in 0..run.row_count {
                let input_row = run.start_row + r;
                per_input[run.input_index][input_row] = Some((out_idx, rows_for_current_output));
                rows_for_current_output += 1;
            }
        }
        rows_per_output[out_idx] = rows_for_current_output;
    }

    InputRowDestinations {
        per_input,
        rows_per_output,
    }
}

// ============================================================================
// Phase 3: streaming write with one writer per output
// ============================================================================

/// Per-output state owned across phase 3 (writer + bookkeeping).
/// The row group lives in a parallel Vec so its borrow into `writer`
/// is tracked by the compiler instead of through a `'static`
/// transmute.
struct OutputWriterStorage {
    output_idx: usize,
    output_path: PathBuf,
    writer: StreamingParquetWriter<std::fs::File>,
    /// Service-name set built during the body col write of "service"
    /// (or empty if no service col).
    service_names: HashSet<String>,
    /// Per-output total row count = sum of merge runs in this output's boundary.
    num_rows: usize,
}

#[allow(clippy::too_many_arguments)]
fn write_streaming_outputs(
    handle: &Handle,
    decoders_state: &mut [InputDecoderState],
    aligned_sort_batches: &[RecordBatch],
    sort_union_schema: &SchemaRef,
    merge_order: &[MergeRun],
    boundaries: &[Range<usize>],
    destinations: &InputRowDestinations,
    input_meta: &InputMetadata,
    writer_config: &crate::storage::ParquetWriterConfig,
    output_dir: &Path,
) -> Result<Vec<MergeOutputFile>> {
    // 1. Build the union schema across full input arrow schemas (so the output covers every column
    //    that appears in any input). The sort union schema covers only sort cols.
    let input_arrow_schemas: Vec<SchemaRef> = decoders_state
        .iter()
        .map(|s| Arc::clone(&s.arrow_schema))
        .collect();
    let (full_union_schema, _aligned_full_placeholder) =
        build_full_union_schema_from_arrow_schemas(&input_arrow_schemas, &input_meta.sort_fields)?;

    // 2. Build per-output metadata (KV entries, row keys, zonemaps) up front from sort col data —
    //    these are what the schema + writer props depend on.
    let per_output_static = boundaries
        .iter()
        .enumerate()
        .map(|(out_idx, boundary)| {
            build_per_output_static(
                out_idx,
                boundary,
                aligned_sort_batches,
                sort_union_schema,
                merge_order,
                input_meta,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // 3. Decide per-output schema: optimise based on each output's sort col data (which determines
    //    metric_name cardinality, etc.). Body cols stay as declared by the union schema; we don't
    //    probe their cardinality here since we haven't read them yet. This is a slight regression
    //    vs. the non-streaming engine — it would dict-encode low-cardinality string body cols too.
    //    PR-6c.2 or later can revisit by gathering body-col cardinality during the streaming pass.
    let per_output_schemas: Vec<SchemaRef> = per_output_static
        .iter()
        .map(|s| derive_output_schema(&full_union_schema, sort_union_schema, &s.sort_optimised))
        .collect::<Result<Vec<_>>>()?;

    // 4. Open M writers, one per output. Writers + bookkeeping live in `writer_states`; the row
    //    group borrows mutably from each writer and is held in a parallel `row_groups` Vec for the
    //    col loop.
    let mut writer_states: Vec<OutputWriterStorage> = Vec::with_capacity(boundaries.len());
    for (out_idx, (schema, static_meta)) in per_output_schemas
        .iter()
        .zip(per_output_static.iter())
        .enumerate()
    {
        if destinations.rows_per_output[out_idx] == 0 {
            continue;
        }
        writer_states.push(open_output_writer(
            out_idx,
            output_dir,
            Arc::clone(schema),
            static_meta,
            input_meta,
            writer_config,
        )?);
    }

    // Snapshot the (output_idx, num_rows) for each storage entry BEFORE
    // calling `start_row_group`, which borrows `writer_states` mutably
    // for the rest of phase 3's col loop.
    let writer_index_view = writer_states_index_view(&writer_states);
    let num_storages = writer_states.len();

    let mut row_groups: Vec<crate::storage::streaming_writer::RowGroupBuilder<'_, std::fs::File>> =
        writer_states
            .iter_mut()
            .map(|s| {
                s.writer
                    .start_row_group()
                    .with_context(|| format!("opening row group for output {}", s.output_idx))
            })
            .collect::<Result<Vec<_>>>()?;

    // Service names are collected into a separate Vec<HashSet<String>>
    // parallel to `row_groups`; we can't write into `writer_states` here
    // because it is already borrowed mutably by `row_groups`. We merge
    // these back into `writer_states` after dropping the row groups.
    let mut service_names_per_output: Vec<HashSet<String>> =
        (0..num_storages).map(|_| HashSet::new()).collect();
    write_all_columns(
        handle,
        &mut row_groups,
        &mut service_names_per_output,
        &writer_index_view,
        decoders_state,
        aligned_sort_batches,
        sort_union_schema,
        &full_union_schema,
        merge_order,
        boundaries,
        destinations,
        &per_output_schemas,
    )?;

    // 6. Finish all row groups (drops the borrows on writers).
    for rg in row_groups {
        rg.finish().context("finishing row group")?;
    }

    // 7. Merge collected service names + close writers + build MergeOutputFiles.
    let mut outputs = Vec::with_capacity(writer_states.len());
    for (mut state, services) in writer_states
        .into_iter()
        .zip(service_names_per_output.into_iter())
    {
        state.service_names.extend(services);
        outputs.push(finalize_output_writer(state, &per_output_static)?);
    }
    Ok(outputs)
}

/// Static per-output state computed once from sort col data. Holds
/// the per-output sort-col-only batch (used for metadata extraction)
/// and the per-output schema-optimisation hints.
struct PerOutputStatic {
    /// Sort-cols-only batch in output sort order — used by row_keys /
    /// zonemap / metric_names / time_range extractors.
    sort_optimised: RecordBatch,
    row_keys_proto: Option<Vec<u8>>,
    zonemap_regexes: HashMap<String, String>,
    metric_names: HashSet<String>,
    time_range: crate::split::TimeRange,
    /// Number of rows that go into this output.
    num_rows: usize,
}

fn build_per_output_static(
    out_idx: usize,
    boundary: &Range<usize>,
    aligned_sort_batches: &[RecordBatch],
    sort_union_schema: &SchemaRef,
    merge_order: &[MergeRun],
    input_meta: &InputMetadata,
) -> Result<PerOutputStatic> {
    let runs = &merge_order[boundary.clone()];
    let sort_batch = apply_merge_permutation(aligned_sort_batches, sort_union_schema, runs)
        .with_context(|| format!("applying merge permutation for output {out_idx} sort cols"))?;
    let num_rows = sort_batch.num_rows();

    // MC-3 sort order on the sort-col-only batch (same check the
    // non-streaming engine does, just restricted to columns we have).
    verify_sort_order(&sort_batch, &input_meta.sort_fields);
    let sort_optimised = optimize_output_batch(&sort_batch);

    let row_keys_proto = row_keys::extract_row_keys(&input_meta.sort_fields, &sort_optimised)
        .with_context(|| format!("extracting row keys for output {out_idx}"))?
        .map(|rk| row_keys::encode_row_keys_proto(&rk));

    let zonemap_opts = ZonemapOptions::default();
    let zonemap_regexes =
        zonemap::extract_zonemap_regexes(&input_meta.sort_fields, &sort_optimised, &zonemap_opts)
            .with_context(|| format!("extracting zonemap regexes for output {out_idx}"))?;

    let metric_names = extract_metric_names(&sort_optimised)
        .with_context(|| format!("extracting metric names for output {out_idx}"))?;
    let time_range = extract_time_range(&sort_optimised)
        .with_context(|| format!("extracting time range for output {out_idx}"))?;

    Ok(PerOutputStatic {
        sort_optimised,
        row_keys_proto,
        zonemap_regexes,
        metric_names,
        time_range,
        num_rows,
    })
}

/// Build the full union schema across all inputs' arrow schemas
/// (NOT just sort cols). Reuses the same algorithm as
/// [`align_inputs_to_union_schema`] but takes pre-extracted arrow
/// schemas — phase 3 doesn't have full input batches.
fn build_full_union_schema_from_arrow_schemas(
    arrow_schemas: &[SchemaRef],
    sort_fields_str: &str,
) -> Result<(SchemaRef, ())> {
    // Build zero-row batches with the right schemas; that lets us
    // reuse `align_inputs_to_union_schema`'s field-merge / storage-
    // ordering logic unchanged.
    let empty_batches: Vec<RecordBatch> = arrow_schemas
        .iter()
        .map(|s| RecordBatch::new_empty(Arc::clone(s)))
        .collect();
    let (schema, _) = align_inputs_to_union_schema(&empty_batches, sort_fields_str)?;
    Ok((schema, ()))
}

/// Compute the per-output schema. For PR-6b.2 we use the
/// (string-normalised) union schema as the output schema directly —
/// fields stay Utf8/LargeUtf8 rather than being re-dict-encoded.
/// Reason: streaming-decoded input arrays come out of the page
/// decoder as plain `StringArray`/`BinaryArray` (not Dictionary), and
/// dict re-encoding per output page would add a per-page CPU cost we
/// don't want to take in the page-bounded path. Re-introducing
/// dict-encoded output strings can be done later by tracking
/// cardinality during the streaming pass — call site is here.
///
/// We do still want to drop columns that are all-null *for this
/// output* (e.g., a column only present in inputs that don't
/// contribute any rows to this output's range). The `sort_optimised`
/// batch has already discarded all-null sort fields; we mirror that
/// decision when building the per-output schema. Body cols are kept
/// unconditionally — tracking per-output body-col presence would
/// require pre-reading every body column for every output, which is
/// exactly the column-chunk-bounded buffering the streaming path
/// exists to avoid.
///
/// `sort_union_schema` is the union of every input's sort columns
/// (before per-output optimisation). It's the only way to tell
/// whether a given union-schema field is a sort field or a body
/// field — `sort_optimised.schema()` alone can't disambiguate because
/// it has dropped some sort fields by design. Without this
/// distinction the function falls into the trap of using
/// `full_union_schema.index_of(field.name())`, which is trivially
/// true for every iterated field, and the all-null drop never
/// happens.
fn derive_output_schema(
    full_union_schema: &SchemaRef,
    sort_union_schema: &SchemaRef,
    sort_optimised: &RecordBatch,
) -> Result<SchemaRef> {
    let sort_optimised_schema = sort_optimised.schema();
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(full_union_schema.fields().len());
    for field in full_union_schema.fields() {
        let is_sort_field = sort_union_schema.index_of(field.name()).is_ok();
        if is_sort_field {
            // Sort field: keep only if the per-output optimiser kept
            // it (i.e., not all-null for this output's rows).
            if sort_optimised_schema.index_of(field.name()).is_ok() {
                fields.push(Arc::clone(field));
            }
        } else {
            // Body field: always kept.
            fields.push(Arc::clone(field));
        }
    }
    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn open_output_writer(
    out_idx: usize,
    output_dir: &Path,
    schema: SchemaRef,
    static_meta: &PerOutputStatic,
    input_meta: &InputMetadata,
    writer_config: &crate::storage::ParquetWriterConfig,
) -> Result<OutputWriterStorage> {
    let output_prefix_len = input_meta.rg_partition_prefix_len;
    let kv_entries = build_merge_kv_metadata(
        input_meta,
        &static_meta.row_keys_proto,
        &static_meta.zonemap_regexes,
        output_prefix_len,
    );
    let sorting_cols = build_sorting_columns(&static_meta.sort_optimised, &input_meta.sort_fields)?;
    let sort_field_names = resolve_sort_field_names(&input_meta.sort_fields)?;

    let props = writer_config.to_writer_properties_with_metadata(
        &schema,
        sorting_cols,
        Some(kv_entries),
        &sort_field_names,
    );

    let output_filename = format!("merge_output_{}.parquet", Ulid::new());
    let output_path = output_dir.join(&output_filename);
    let file = std::fs::File::create(&output_path)
        .with_context(|| format!("creating output file: {}", output_path.display()))?;
    let writer = StreamingParquetWriter::try_new(file, Arc::clone(&schema), props)
        .with_context(|| format!("opening streaming writer for output {out_idx}"))?;

    Ok(OutputWriterStorage {
        output_idx: out_idx,
        output_path,
        writer,
        service_names: HashSet::new(),
        num_rows: static_meta.num_rows,
    })
}

/// Index view used inside the col loop to find the writer's
/// `output_idx` and `num_rows` without needing a mutable borrow on
/// `writer_states` (which is already mutably borrowed by `row_groups`).
fn writer_states_index_view(writer_states: &[OutputWriterStorage]) -> Vec<(usize, usize)> {
    writer_states
        .iter()
        .map(|s| (s.output_idx, s.num_rows))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn write_all_columns(
    handle: &Handle,
    row_groups: &mut [crate::storage::streaming_writer::RowGroupBuilder<'_, std::fs::File>],
    service_names_per_output: &mut [HashSet<String>],
    writer_index_view: &[(usize, usize)],
    decoders_state: &mut [InputDecoderState],
    aligned_sort_batches: &[RecordBatch],
    sort_union_schema: &SchemaRef,
    full_union_schema: &SchemaRef,
    merge_order: &[MergeRun],
    boundaries: &[Range<usize>],
    destinations: &InputRowDestinations,
    per_output_schemas: &[SchemaRef],
) -> Result<()> {
    // Iterate cols in the **full union schema** order. The union
    // covers every column that appears in ANY output. For each col K
    // and each output:
    //   - If output's schema includes col K: write col K's data (sort col → from buffer, body col →
    //     from decoder).
    //   - Else: skip — that output dropped col K as all-null for its row range; the next output's
    //     col K still gets written.
    //
    // It's tempting to drive from one per-output schema, since all
    // per-output schemas share the same column ordering as a
    // subsequence. But two outputs may drop *different* all-null
    // fields and end up with the same field count — then picking
    // either misses a field the other output still needs, and the
    // writer for the latter output writes subsequent columns into
    // the wrong slot. The full union schema is the only choice that
    // covers every column every output may need, in the canonical
    // storage order.

    // For each full-union-schema col K:
    for parent_col_idx in 0..full_union_schema.fields().len() {
        let parent_field = full_union_schema.field(parent_col_idx);
        let parent_name = parent_field.name();

        // Is this a sort col (in memory) or a body col (streamed)?
        let is_sort_col = sort_union_schema.index_of(parent_name).is_ok();

        if is_sort_col {
            write_sort_col_for_all_outputs(
                row_groups,
                writer_index_view,
                parent_name,
                aligned_sort_batches,
                sort_union_schema,
                merge_order,
                boundaries,
                destinations,
                per_output_schemas,
            )?;
        } else {
            write_body_col_for_all_outputs(
                handle,
                row_groups,
                service_names_per_output,
                writer_index_view,
                decoders_state,
                parent_name,
                destinations,
                per_output_schemas,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_sort_col_for_all_outputs(
    row_groups: &mut [crate::storage::streaming_writer::RowGroupBuilder<'_, std::fs::File>],
    writer_index_view: &[(usize, usize)],
    col_name: &str,
    aligned_sort_batches: &[RecordBatch],
    sort_union_schema: &SchemaRef,
    merge_order: &[MergeRun],
    boundaries: &[Range<usize>],
    destinations: &InputRowDestinations,
    per_output_schemas: &[SchemaRef],
) -> Result<()> {
    let _ = sort_union_schema;

    let mut storage_idx = 0;
    for (out_idx, boundary) in boundaries.iter().enumerate() {
        if destinations.rows_per_output[out_idx] == 0 {
            continue;
        }
        debug_assert_eq!(writer_index_view[storage_idx].0, out_idx);

        // Drop this col if the output's schema doesn't include it.
        let out_schema = &per_output_schemas[out_idx];
        if out_schema.index_of(col_name).is_err() {
            storage_idx += 1;
            continue;
        }

        let runs = &merge_order[boundary.clone()];
        let arrays = build_sort_col_pages_for_output(col_name, aligned_sort_batches, runs)?;
        row_groups[storage_idx]
            .write_next_column_arrays(arrays.into_iter())
            .with_context(|| format!("writing sort col '{col_name}' to output {out_idx}"))?;
        storage_idx += 1;
    }
    Ok(())
}

/// Build per-output-page arrays for one sort col. The col is already
/// in memory across all inputs (`aligned_sort_batches`); for this
/// output we walk its merge runs and split the take result into
/// `OUTPUT_PAGE_ROWS`-sized chunks.
fn build_sort_col_pages_for_output(
    col_name: &str,
    aligned_sort_batches: &[RecordBatch],
    runs: &[MergeRun],
) -> Result<Vec<ArrayRef>> {
    // Collect references to each input's column array.
    let mut input_arrays: Vec<&dyn Array> = Vec::with_capacity(aligned_sort_batches.len());
    for batch in aligned_sort_batches {
        let idx = batch.schema().index_of(col_name).map_err(|_| {
            anyhow!("input is missing sort col '{col_name}' that the union schema expected",)
        })?;
        input_arrays.push(batch.column(idx).as_ref());
    }

    let mut indices: Vec<(usize, usize)> =
        Vec::with_capacity(runs.iter().map(|r| r.row_count).sum());
    for run in runs {
        for r in 0..run.row_count {
            indices.push((run.input_index, run.start_row + r));
        }
    }

    // Split into OUTPUT_PAGE_ROWS-sized chunks; each chunk → one
    // arrow::interleave call → one ArrayRef.
    let mut pages = Vec::with_capacity(indices.len().div_ceil(OUTPUT_PAGE_ROWS));
    for chunk in indices.chunks(OUTPUT_PAGE_ROWS) {
        let arr = interleave(&input_arrays, chunk)
            .with_context(|| format!("interleaving sort col '{col_name}' pages"))?;
        pages.push(arr);
    }
    Ok(pages)
}

#[allow(clippy::too_many_arguments)]
fn write_body_col_for_all_outputs(
    handle: &Handle,
    row_groups: &mut [crate::storage::streaming_writer::RowGroupBuilder<'_, std::fs::File>],
    service_names_per_output: &mut [HashSet<String>],
    writer_index_view: &[(usize, usize)],
    decoders_state: &mut [InputDecoderState],
    col_name: &str,
    destinations: &InputRowDestinations,
    per_output_schemas: &[SchemaRef],
) -> Result<()> {
    // Find this col's per-input parquet leaf index (one per input).
    // Inputs whose schema doesn't have this col OR which have zero
    // row groups (legal — phase 0 explicitly accepts empty inputs and
    // returns an empty sort batch for them) contribute null rows and
    // don't advance their decoder for this col. Looking up
    // `row_group(0)` on a zero-RG input would panic, so guard up
    // front.
    let mut input_col_indices: Vec<Option<usize>> = Vec::with_capacity(decoders_state.len());
    for state in decoders_state.iter() {
        if state.metadata.num_row_groups() == 0 {
            input_col_indices.push(None);
            continue;
        }
        match state.arrow_schema.index_of(col_name) {
            Ok(idx) => input_col_indices.push(Some(idx)),
            Err(_) => input_col_indices.push(None),
        }
    }

    // Reset each input's body-col cache + cursor at the start of this
    // column. The persistent `StreamDecoder` retains its per-(rg, col)
    // state for *every* column it has touched so the next page from
    // this column has the correct `row_start`; only the cached pages
    // (which belong to the previous column) need to be discarded.
    for state in decoders_state.iter_mut() {
        state.reset_body_col_state();
    }

    // Track service names while streaming the service col.
    let track_service = col_name == "service";

    // For each output sequentially: build output pages, feed to writer
    // one page at a time. We must NOT collect the whole column into a
    // Vec — that would defeat the page-bounded merge path and scale
    // memory with column-chunk size on production splits. Instead we
    // hand `write_next_column_arrays` a streaming iterator that
    // captures the first error in a side cell so the writer stops as
    // soon as assembly fails.
    let mut storage_idx = 0;
    for (out_idx, &row_count) in destinations.rows_per_output.iter().enumerate() {
        if row_count == 0 {
            continue;
        }
        debug_assert_eq!(writer_index_view[storage_idx].0, out_idx);

        let out_schema = &per_output_schemas[out_idx];
        if out_schema.index_of(col_name).is_err() {
            storage_idx += 1;
            continue;
        }
        let out_field_idx = out_schema.index_of(col_name)?;
        let out_field = out_schema.field(out_field_idx);

        let assembler = BodyColOutputPageAssembler::new(
            handle,
            decoders_state,
            &input_col_indices,
            destinations,
            out_idx,
            col_name,
            out_field,
        );

        let mut error_slot: Option<anyhow::Error> = None;
        let service_collector: Option<&mut HashSet<String>> = if track_service {
            Some(&mut service_names_per_output[storage_idx])
        } else {
            None
        };

        let stream_iter = StreamingBodyColIter {
            inner: assembler.into_iter(),
            error_slot: &mut error_slot,
            service_collector,
        };

        let write_result = row_groups[storage_idx].write_next_column_arrays(stream_iter);

        // Assembly errors are reported via `error_slot`; surface them
        // first because a downstream write error is usually a
        // consequence (the writer stops on `None` and reports a
        // row-count mismatch otherwise).
        if let Some(err) = error_slot {
            return Err(err)
                .with_context(|| format!("assembling body col '{col_name}' for output {out_idx}"));
        }
        write_result
            .with_context(|| format!("writing body col '{col_name}' to output {out_idx}"))?;
        storage_idx += 1;
    }

    Ok(())
}

/// Adapts a `Result<ArrayRef>` page assembler into the
/// `Iterator<Item = ArrayRef>` shape `write_next_column_arrays` expects.
/// The first assembly error is captured in `error_slot` and iteration
/// ends; the caller MUST check the slot after the writer returns. If
/// `service_collector` is `Some`, every yielded page is scanned for
/// service names and added to the set; collection failures also stop
/// the iterator and populate `error_slot`.
struct StreamingBodyColIter<'a, I> {
    inner: I,
    error_slot: &'a mut Option<anyhow::Error>,
    service_collector: Option<&'a mut HashSet<String>>,
}

impl<I> Iterator for StreamingBodyColIter<'_, I>
where I: Iterator<Item = Result<ArrayRef>>
{
    type Item = ArrayRef;

    fn next(&mut self) -> Option<ArrayRef> {
        if self.error_slot.is_some() {
            return None;
        }
        match self.inner.next() {
            Some(Ok(arr)) => {
                if let Some(out) = self.service_collector.as_deref_mut()
                    && let Err(e) = collect_service_names_from_page(arr.as_ref(), out)
                {
                    *self.error_slot = Some(e);
                    return None;
                }
                Some(arr)
            }
            Some(Err(e)) => {
                *self.error_slot = Some(e);
                None
            }
            None => None,
        }
    }
}

/// Per-page service name collector. Used during the streaming write
/// of the "service" body col to populate per-output service_names.
fn collect_service_names_from_page(arr: &dyn Array, out: &mut HashSet<String>) -> Result<()> {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int8Type, Int16Type, Int32Type, Int64Type};

    fn extend_from_strings(strings: &arrow::array::StringArray, out: &mut HashSet<String>) {
        for i in 0..strings.len() {
            if strings.is_valid(i) {
                out.insert(strings.value(i).to_string());
            }
        }
    }

    match arr.data_type() {
        DataType::Utf8 => {
            let strings = arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray for service col page"))?;
            extend_from_strings(strings, out);
        }
        DataType::LargeUtf8 => {
            let strings = arr
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| anyhow!("expected LargeStringArray for service col page"))?;
            for i in 0..strings.len() {
                if strings.is_valid(i) {
                    out.insert(strings.value(i).to_string());
                }
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            // Extract the dictionary's values that are referenced by
            // valid (non-null) keys.
            match key_type.as_ref() {
                DataType::Int8 => {
                    let dict = arr.as_dictionary::<Int8Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int16 => {
                    let dict = arr.as_dictionary::<Int16Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    let dict = arr.as_dictionary::<Int32Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                DataType::Int64 => {
                    let dict = arr.as_dictionary::<Int64Type>();
                    if let Some(strings) = dict
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        for i in 0..dict.len() {
                            if dict.is_valid(i) {
                                let key = dict.keys().value(i) as usize;
                                if key < strings.len() && strings.is_valid(key) {
                                    out.insert(strings.value(key).to_string());
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {
            // Skip non-string types — service col is expected to be
            // string-like; if it isn't, just don't collect names.
        }
    }
    Ok(())
}

// ============================================================================
// Body col output page assembler — the page-bounded streaming core
// ============================================================================

/// Assembles output pages for one (output_idx, body_col) by:
/// 1. Walking the destinations table forward through this output's row range, accumulating
///    `(input_idx, input_row)` index pairs.
/// 2. When the index buffer hits `OUTPUT_PAGE_ROWS`, advancing each contributing input's decoder
///    until its decoded pages cover the needed input rows, then calling
///    `arrow::compute::interleave`.
/// 3. Emitting one `ArrayRef` per iter step until the row range is exhausted; then `Ok(None)`.
///
/// Memory per `next()` call: one in-progress output page (P rows) +
/// up to ~2 in-flight decoded pages per input (kept until all their
/// rows are consumed). Bounded by page sizes, not column-chunk sizes.
///
/// **Cross-output state**: the per-input `body_col_page_cache` and
/// `body_col_cursor` live on [`InputDecoderState`], not the assembler.
/// They must persist across all outputs that consume rows from the
/// active body column — a page whose row range straddles two outputs
/// would otherwise be dropped when the first output's assembler ends,
/// even though the stream has already advanced past it and the next
/// output still needs rows from inside that page.
struct BodyColOutputPageAssembler<'a> {
    handle: &'a Handle,
    decoders_state: &'a mut [InputDecoderState],
    input_col_indices: &'a [Option<usize>],
    destinations: &'a InputRowDestinations,
    out_idx: usize,
    col_name: &'a str,
    out_field: &'a Field,
    /// Total rows written so far for this output's col.
    rows_emitted: usize,
    /// Total rows expected = destinations.rows_per_output[out_idx].
    expected_rows: usize,
    /// EOF flag (returns None on subsequent calls once true).
    done: bool,
}

impl<'a> BodyColOutputPageAssembler<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        handle: &'a Handle,
        decoders_state: &'a mut [InputDecoderState],
        input_col_indices: &'a [Option<usize>],
        destinations: &'a InputRowDestinations,
        out_idx: usize,
        col_name: &'a str,
        out_field: &'a Field,
    ) -> Self {
        Self {
            handle,
            decoders_state,
            input_col_indices,
            destinations,
            out_idx,
            col_name,
            out_field,
            rows_emitted: 0,
            expected_rows: destinations.rows_per_output[out_idx],
            done: false,
        }
    }

    fn into_iter(self) -> BodyColOutputPageIter<'a> {
        BodyColOutputPageIter { inner: self }
    }
}

struct BodyColOutputPageIter<'a> {
    inner: BodyColOutputPageAssembler<'a>,
}

impl Iterator for BodyColOutputPageIter<'_> {
    type Item = Result<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.done || self.inner.rows_emitted >= self.inner.expected_rows {
            self.inner.done = true;
            return None;
        }
        match assemble_one_output_page(&mut self.inner) {
            Ok(Some(arr)) => Some(Ok(arr)),
            Ok(None) => {
                self.inner.done = true;
                None
            }
            Err(e) => {
                self.inner.done = true;
                Some(Err(e))
            }
        }
    }
}

fn assemble_one_output_page(s: &mut BodyColOutputPageAssembler) -> Result<Option<ArrayRef>> {
    let remaining = s.expected_rows - s.rows_emitted;
    if remaining == 0 {
        return Ok(None);
    }
    let page_size = remaining.min(OUTPUT_PAGE_ROWS);

    // Walk this output's row positions and figure out which (input, input_row)
    // contributes each one. We use the per-input destinations table: for
    // input i, find the next input_row whose destination is (out_idx, *).
    // Since `destinations.per_input[i]` is in input order and outputs are
    // strictly increasing by sort key, the rows that go to this output are
    // a contiguous slice in input i's row order.
    //
    // For each output position 0..page_size, we need (input_idx, input_row).
    // Walk input cursors and pick the next row going to this output.

    // Collect (input_idx, input_row) indices for this output page.
    let mut indices_per_input: Vec<Vec<usize>> = vec![Vec::new(); s.decoders_state.len()];
    let mut interleave_indices: Vec<(usize, usize)> = Vec::with_capacity(page_size);
    let mut total_picked = 0usize;

    while total_picked < page_size {
        // Look across all inputs for the next contribution to this output.
        // Per the merge order, within each input the rows assigned to this
        // output are a contiguous slice; once we've advanced cursor past
        // them, no more rows from this input contribute. We collect ALL
        // rows from one input up to a per-input limit determined by the
        // merge order, but the simplest correct approach is to walk in
        // merge-order globally. We don't have the merge order indexed by
        // output here, so re-derive by scanning the destinations table.
        //
        // Better: pre-compute per-output, per-input row ranges. Each input
        // contributes a contiguous half-open range `[lo_i..hi_i)` to this
        // output (possibly empty). We could compute these ranges once and
        // reuse. For now, lazy approach: scan forward from cursor on each
        // input, picking the next row that maps to (out_idx, *).
        //
        // The ORDER in which we pick across inputs must match the merge
        // plan's output position. We have output positions in destinations:
        // `destinations.per_input[i][r] = Some((out_idx, pos))`. The merged
        // output picks rows in order of increasing `pos`.
        //
        // For one output page, the positions we want are
        // `s.rows_emitted..s.rows_emitted + page_size`. For each position
        // p in that range, find (input_idx, input_row) such that
        // destinations.per_input[input_idx][input_row] == Some((out_idx, p)).
        let target_pos = s.rows_emitted + total_picked;
        let mut found = false;
        for (input_idx, dests) in s.destinations.per_input.iter().enumerate() {
            let cursor = s.decoders_state[input_idx].body_col_cursor;
            for (input_row, dest) in dests.iter().enumerate().skip(cursor) {
                match dest {
                    Some((o, p)) if *o == s.out_idx => {
                        if *p == target_pos {
                            interleave_indices.push((input_idx, input_row));
                            indices_per_input[input_idx].push(input_row);
                            // Don't advance the cursor past this row yet —
                            // we may need rows from input i in this page
                            // with positions ahead. We bump it after the
                            // whole page is collected.
                            found = true;
                            break;
                        }
                    }
                    _ => {}
                }
                if found {
                    break;
                }
            }
            if found {
                break;
            }
        }
        if !found {
            // Shouldn't happen — every output position should be reachable.
            bail!(
                "merge plan inconsistency: output {} position {target_pos} not found in any input",
                s.out_idx,
            );
        }
        total_picked += 1;
    }

    // Now ensure each input's decoder has decoded pages covering all
    // `indices_per_input[i]` rows. Advance decoders as needed.
    for (input_idx, input_rows) in indices_per_input.iter().enumerate() {
        if input_rows.is_empty() {
            continue;
        }
        let col_parquet_idx = match s.input_col_indices[input_idx] {
            Some(c) => c,
            None => {
                // This input lacks this col entirely — null contributions.
                // We'll handle null-filling in the interleave step below.
                continue;
            }
        };
        let max_needed_row = *input_rows.iter().max().expect("non-empty");
        fill_page_cache_to_row(
            s.handle,
            &mut s.decoders_state[input_idx],
            col_parquet_idx,
            max_needed_row,
        )?;
    }

    // Build the per-(input, row) value array by:
    //   1. Concatenating each input's cached pages into one ArrayRef (they cover a contiguous input
    //      row range from cache_start to cursor_max).
    //   2. Computing local indices = input_row - cache_start.
    //   3. Calling arrow::compute::interleave across N input arrays.
    //
    // For inputs without this col, we substitute a single null page of the
    // out_field's type.
    let mut input_array_refs: Vec<ArrayRef> = Vec::with_capacity(s.decoders_state.len());
    let mut input_cache_starts: Vec<usize> = Vec::with_capacity(s.decoders_state.len());

    for input_idx in 0..s.decoders_state.len() {
        match s.input_col_indices[input_idx] {
            Some(_) => {
                let pages = &s.decoders_state[input_idx].body_col_page_cache;
                if pages.is_empty() {
                    // No pages decoded for this input (no rows from this input go to this output).
                    // Use a zero-row placeholder; we won't index into it.
                    input_array_refs.push(new_null_array(s.out_field.data_type(), 0));
                    input_cache_starts.push(0);
                } else {
                    let cache_start = pages[0].row_start;
                    let arrays: Vec<&dyn Array> = pages.iter().map(|p| p.array.as_ref()).collect();
                    let concatenated = arrow::compute::concat(&arrays).with_context(|| {
                        format!(
                            "concatenating cached pages for input {input_idx} col '{}'",
                            s.col_name,
                        )
                    })?;
                    input_array_refs.push(concatenated);
                    input_cache_starts.push(cache_start);
                }
            }
            None => {
                // Null-fill array of the right length. The max needed local
                // index from this input is the largest index we'd reference;
                // since we don't actually reference rows from this input (we'd
                // need an alternate "null contribution" mechanism), we leave
                // it as a 1-row null array and route indices to position 0.
                let null_arr = new_null_array(s.out_field.data_type(), 1);
                input_array_refs.push(null_arr);
                input_cache_starts.push(0);
            }
        }
    }

    let interleave_local: Vec<(usize, usize)> = interleave_indices
        .iter()
        .map(|&(i_idx, i_row)| match s.input_col_indices[i_idx] {
            Some(_) => (i_idx, i_row - input_cache_starts[i_idx]),
            None => (i_idx, 0),
        })
        .collect();

    let array_refs_ref: Vec<&dyn Array> = input_array_refs.iter().map(|a| a.as_ref()).collect();
    let assembled = interleave(&array_refs_ref, &interleave_local).with_context(|| {
        format!(
            "interleaving body col '{}' for output {}",
            s.col_name, s.out_idx,
        )
    })?;

    // Bump input cursors past rows we just consumed and drop pages
    // whose rows are fully consumed. Both the cursor and the cache
    // live on InputDecoderState so they persist across outputs that
    // share this column.
    for (input_idx, input_rows) in indices_per_input.iter().enumerate() {
        if input_rows.is_empty() {
            continue;
        }
        let max_row = *input_rows.iter().max().expect("non-empty");
        let state = &mut s.decoders_state[input_idx];
        state.body_col_cursor = max_row + 1;

        // Drop pages whose last row is < cursor.
        if s.input_col_indices[input_idx].is_some() {
            let pages = &mut state.body_col_page_cache;
            while let Some(front) = pages.first() {
                let front_end = front.row_start + front.array.len();
                if front_end <= state.body_col_cursor {
                    pages.remove(0);
                } else {
                    break;
                }
            }
        }
    }

    s.rows_emitted += page_size;
    Ok(Some(assembled))
}

/// Pull pages from the input's persistent decoder via `block_on` until
/// the cached pages for `col_parquet_idx` cover up through `target_row`
/// (inclusive). Stops as soon as the latest cached page ends past
/// `target_row`. The function's effect on the world is *adding pages
/// to the cache* — it does not skip data and does not consume any
/// rows on its own.
///
/// The decoder MUST be the long-lived [`InputDecoderState::decoder`]:
/// it preserves the per-(rg, col) `rows_decoded` counter so successive
/// `DecodedPage::row_start` values are absolute input row indices,
/// not page-local zeros. Likewise, the cache lives on the state so
/// pages whose row range spans an output boundary survive into the
/// next output's assembler.
fn fill_page_cache_to_row(
    handle: &Handle,
    state: &mut InputDecoderState,
    col_parquet_idx: usize,
    target_row: usize,
) -> Result<()> {
    // If cache already covers target_row, nothing to do.
    if let Some(last) = state.body_col_page_cache.last() {
        let last_end = last.row_start + last.array.len();
        if target_row < last_end {
            return Ok(());
        }
    }

    loop {
        let decoded = handle
            .block_on(state.decoder.decode_next_page())
            .context("decoding body col page")?;
        let page = match decoded {
            Some(p) => p,
            None => bail!(
                "stream EOF while advancing to row {target_row} for parquet col {col_parquet_idx}",
            ),
        };
        if page.col_idx != col_parquet_idx {
            bail!(
                "expected col {col_parquet_idx} page, got col {} — column ordering violated",
                page.col_idx,
            );
        }
        let end = page.row_start + page.array.len();
        state.body_col_page_cache.push(page);
        record_body_col_page_cache_len(state.body_col_page_cache.len());
        if target_row < end {
            return Ok(());
        }
    }
}

fn finalize_output_writer(
    state: OutputWriterStorage,
    per_output_static: &[PerOutputStatic],
) -> Result<MergeOutputFile> {
    let OutputWriterStorage {
        output_idx,
        output_path,
        writer,
        mut service_names,
        num_rows,
    } = state;

    let _metadata = writer
        .close()
        .with_context(|| format!("closing writer for output {output_idx}"))?;

    let size_bytes = std::fs::metadata(&output_path)
        .with_context(|| format!("stat output file: {}", output_path.display()))?
        .len();

    let static_meta = &per_output_static[output_idx];

    // If `service` is a sort column for this schema, it took the
    // sort-col write path and `service_names` (populated by the body-
    // col `track_service` branch) never saw it. Fold in the names
    // from the per-output sort batch so the `TAG_SERVICE` low-
    // cardinality metadata stays accurate regardless of which path
    // wrote the column.
    if let Ok(service_col_idx) = static_meta.sort_optimised.schema().index_of("service") {
        collect_service_names_from_page(
            static_meta.sort_optimised.column(service_col_idx).as_ref(),
            &mut service_names,
        )
        .with_context(|| {
            format!("collecting service names from sort col for output {output_idx}")
        })?;
    }

    let mut low_cardinality_tags: HashMap<String, HashSet<String>> = HashMap::new();
    if !service_names.is_empty() {
        low_cardinality_tags.insert(TAG_SERVICE.to_string(), service_names);
    }

    Ok(MergeOutputFile {
        path: output_path,
        num_rows,
        num_row_groups: 1,
        size_bytes,
        row_keys_proto: static_meta.row_keys_proto.clone(),
        zonemap_regexes: static_meta.zonemap_regexes.clone(),
        metric_names: static_meta.metric_names.clone(),
        time_range: static_meta.time_range,
        low_cardinality_tags,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, DictionaryArray, Float64Array, Int64Array, StringArray, UInt8Array,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::TempDir;
    use tokio::io::AsyncRead;

    use super::*;
    use crate::storage::page_decoder::StreamDecoder;
    use crate::storage::streaming_reader::{RemoteByteSource, StreamingParquetReader};
    use crate::storage::{Compression, ParquetWriterConfig};

    // -------- Fixtures --------

    /// Build a sorted metrics RecordBatch with `num_rows` rows in
    /// the **storage column order**: sort cols (metric_name, timestamp_secs)
    /// → sorted_series → remaining body cols lexicographic
    /// (metric_type, service, timeseries_id, value). All rows share
    /// the single metric_name "cpu.usage". `sorted_series` is monotonic
    /// from `start_series_idx`. `service` carries nulls every 5th row.
    fn make_sorted_batch(num_rows: usize, start_series_idx: u64) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            // sort cols (in sort schema order)
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            // sorted_series marker
            Field::new("sorted_series", DataType::Binary, false),
            // body cols lexicographic
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("service", dict_type, true),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let metric_keys: Vec<i32> = (0..num_rows as i32).map(|_| 0).collect();
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .expect("test dict array"),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows as u64)
            .map(|i| 1_700_000_000 + (num_rows as u64 - i))
            .collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tsids: Vec<i64> = (0..num_rows as i64).map(|i| 1000 + i).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32)
            .map(|i| if i % 5 == 0 { None } else { Some(i % 3) })
            .collect();
        let svc_values = StringArray::from(vec!["api", "db", "cache"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(svc_values),
            )
            .expect("test dict array"),
        );
        let mut series_bytes: Vec<Vec<u8>> = Vec::with_capacity(num_rows);
        for i in 0..num_rows as u64 {
            let id = start_series_idx + i;
            series_bytes.push(id.to_be_bytes().to_vec());
        }
        let series_refs: Vec<&[u8]> = series_bytes.iter().map(|v| v.as_slice()).collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(series_refs));

        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                timestamp_secs,
                sorted_series,
                metric_type,
                service,
                timeseries_id,
                value,
            ],
        )
        .expect("test batch")
    }

    /// Write a fixture parquet file with the standard `qh.*` KVs that the
    /// streaming merge engine validates.
    fn write_input_parquet(batches: &[RecordBatch], extra_kvs: &[(&str, &str)]) -> Bytes {
        let schema = batches[0].schema();
        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            ..ParquetWriterConfig::default()
        };
        let sort_fields = "metric_name|-timestamp_secs/V2";
        let sort_field_names = vec!["metric_name".to_string(), "timestamp_secs".to_string()];
        let mut kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                sort_fields.to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
        ];
        for (k, v) in extra_kvs {
            kvs.push(KeyValue::new(k.to_string(), v.to_string()));
        }
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: schema.index_of("metric_name").expect("test schema") as i32,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: schema.index_of("timestamp_secs").expect("test schema") as i32,
                descending: true,
                nulls_first: false,
            },
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            sorting_cols,
            Some(kvs),
            &sort_field_names,
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("arrow writer");
        for b in batches {
            writer.write(b).expect("test write");
        }
        writer.close().expect("test close");
        Bytes::from(buf)
    }

    // -------- In-memory byte source --------

    struct InMemorySource {
        bytes: Bytes,
    }

    #[async_trait::async_trait]
    impl RemoteByteSource for InMemorySource {
        async fn file_size(&self, _path: &std::path::Path) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        async fn get_slice(
            &self,
            _path: &std::path::Path,
            range: std::ops::Range<u64>,
        ) -> std::io::Result<Bytes> {
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
        async fn get_slice_stream(
            &self,
            _path: &std::path::Path,
            range: std::ops::Range<u64>,
        ) -> std::io::Result<Box<dyn AsyncRead + Send + Unpin>> {
            let slice = self.bytes.slice(range.start as usize..range.end as usize);
            Ok(Box::new(std::io::Cursor::new(slice.to_vec())))
        }
    }

    async fn open_stream(bytes: Bytes) -> Box<dyn ColumnPageStream> {
        let source = Arc::new(InMemorySource { bytes });
        let reader = StreamingParquetReader::try_open(source, PathBuf::from("test.parquet"))
            .await
            .expect("open reader");
        Box::new(reader)
    }

    /// Read an output parquet file back into a single concatenated RecordBatch.
    fn read_output_to_record_batch(path: &Path) -> RecordBatch {
        let bytes = std::fs::read(path).expect("read output");
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            Bytes::from(bytes),
        )
        .expect("read output builder");
        let schema = builder.schema().clone();
        let reader = builder.build().expect("read output build");
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("read output");
        if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            arrow::compute::concat_batches(&schema, &batches).expect("concat")
        }
    }

    fn merge_config(num_outputs: usize) -> MergeConfig {
        MergeConfig {
            num_outputs,
            writer_config: ParquetWriterConfig {
                compression: Compression::Snappy,
                ..ParquetWriterConfig::default()
            },
        }
    }

    // -------- Tests --------

    /// Two inputs → one output: row count and sort order preserved.
    #[tokio::test]
    async fn test_two_inputs_simple_merge() {
        let batch_a = make_sorted_batch(50, 0);
        let batch_b = make_sorted_batch(50, 50);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let bytes_b = write_input_parquet(std::slice::from_ref(&batch_b), &[]);

        let inputs: Vec<Box<dyn ColumnPageStream>> =
            vec![open_stream(bytes_a).await, open_stream(bytes_b).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, 100);

        let merged = read_output_to_record_batch(&outputs[0].path);
        assert_eq!(merged.num_rows(), 100);
        let ss_array = merged.column(merged.schema().index_of("sorted_series").expect("col"));
        let ss = ss_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary");
        for i in 0..ss_array.len().saturating_sub(1) {
            assert!(
                ss.value(i) <= ss.value(i + 1),
                "row {i}: sorted_series not ascending",
            );
        }
    }

    /// Single-metric_name input + num_outputs=1 → output is single row group.
    #[tokio::test]
    async fn test_output_is_single_row_group() {
        let batch_a = make_sorted_batch(200, 0);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes_a).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);

        let bytes = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes)).expect("ser reader");
        assert_eq!(
            reader.metadata().num_row_groups(),
            1,
            "single-metric_name single-output merge must produce single row group",
        );
    }

    /// N inputs → M outputs: total row count preserved (MC-1).
    #[tokio::test]
    async fn test_total_rows_preserved() {
        let batch_a = make_sorted_batch(75, 0);
        let batch_b = make_sorted_batch(50, 100);
        let batch_c = make_sorted_batch(25, 200);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let bytes_b = write_input_parquet(std::slice::from_ref(&batch_b), &[]);
        let bytes_c = write_input_parquet(std::slice::from_ref(&batch_c), &[]);

        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![
            open_stream(bytes_a).await,
            open_stream(bytes_b).await,
            open_stream(bytes_c).await,
        ];
        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(2))
            .await
            .expect("merge");

        let total: usize = outputs.iter().map(|o| o.num_rows).sum();
        assert_eq!(total, 150);
    }

    /// Sort schema mismatch across inputs is rejected.
    #[tokio::test]
    async fn test_sort_schema_mismatch_rejected() {
        let batch_a = make_sorted_batch(20, 0);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            ..ParquetWriterConfig::default()
        };
        let kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "service|-timestamp_secs/V2".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &batch_a.schema(),
            Vec::new(),
            Some(kvs),
            &["service".to_string(), "timestamp_secs".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch_a.schema(), Some(props)).expect("arrow writer");
        writer.write(&batch_a).expect("write");
        writer.close().expect("close");
        let bytes_b = Bytes::from(buf);

        let inputs: Vec<Box<dyn ColumnPageStream>> =
            vec![open_stream(bytes_a).await, open_stream(bytes_b).await];
        let tmp = TempDir::new().expect("tmpdir");
        let err = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect_err("must reject mismatched sort schema");
        let s = err.to_string();
        assert!(
            s.contains("sort schema mismatch"),
            "expected 'sort schema mismatch', got: {s}",
        );
    }

    /// qh.* KV metadata is propagated to the output; num_merge_ops increments.
    #[tokio::test]
    async fn test_kv_metadata_propagated_to_output() {
        let batch_a = make_sorted_batch(40, 0);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes_a).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        let bytes = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes)).expect("ser reader");
        let kvs = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        let find = |k: &str| -> Option<String> {
            kvs.iter()
                .find(|kv| kv.key == k)
                .and_then(|kv| kv.value.clone())
        };
        assert_eq!(
            find(PARQUET_META_SORT_FIELDS).as_deref(),
            Some("metric_name|-timestamp_secs/V2"),
        );
        assert_eq!(
            find(PARQUET_META_WINDOW_START).as_deref(),
            Some("1700000000")
        );
        assert_eq!(find(PARQUET_META_WINDOW_DURATION).as_deref(), Some("60"));
        assert_eq!(
            find(PARQUET_META_NUM_MERGE_OPS).as_deref(),
            Some("1"),
            "num_merge_ops must increment by 1 over input's max",
        );
    }

    /// All-empty inputs produce no output.
    #[tokio::test]
    async fn test_all_empty_inputs_no_output() {
        let empty = make_sorted_batch(0, 0);
        let bytes = write_input_parquet(std::slice::from_ref(&empty), &[]);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert!(outputs.is_empty());
    }

    /// The streaming engine's output can be drained back via the new
    /// page-bounded decoder. End-to-end sanity check.
    #[tokio::test]
    async fn test_output_drainable_by_stream_decoder() {
        let batch_a = make_sorted_batch(40, 0);
        let batch_b = make_sorted_batch(40, 40);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let bytes_b = write_input_parquet(std::slice::from_ref(&batch_b), &[]);

        let inputs: Vec<Box<dyn ColumnPageStream>> =
            vec![open_stream(bytes_a).await, open_stream(bytes_b).await];
        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");

        let bytes = std::fs::read(&outputs[0].path).expect("read");
        let mut output_stream = open_stream(Bytes::from(bytes)).await;
        let mut decoder = StreamDecoder::new(&mut *output_stream);
        let mut total_decoded = 0usize;
        while let Some(page) = decoder.decode_next_page().await.expect("decode") {
            // Count only sort col 0 (col_idx 0) pages to get a row count.
            if page.col_idx == 0 {
                total_decoded += page.array.len();
            }
        }
        assert_eq!(total_decoded, 80);
    }

    /// Page-bounded contract sanity: with a row group large enough to
    /// require many parquet pages per col, body col writes go through
    /// the page-by-page assembler instead of materialising column
    /// chunks. We can't directly observe peak memory from a test, but
    /// we *can* assert that the merge completes correctly with input
    /// data whose body cols span many pages, and that the output is
    /// itself multi-page (no whole-column buffering happened on the
    /// output side either).
    #[tokio::test]
    async fn test_body_col_streams_many_pages_per_column_chunk() {
        // Force multiple pages per column chunk by setting a small
        // data_page_row_count_limit. With 8000 rows and a 1000-row
        // page limit, the output value col chunk must span ≥ 8 pages.
        let batch = make_sorted_batch(8000, 0);
        let bytes = write_input_parquet(std::slice::from_ref(&batch), &[]);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let writer_config = ParquetWriterConfig {
            compression: Compression::Snappy,
            data_page_row_count_limit: 1000,
            ..ParquetWriterConfig::default()
        };
        let config = MergeConfig {
            num_outputs: 1,
            writer_config,
        };

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &config)
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, 8000);

        // Verify the output is itself multi-page-per-column (which is
        // what page-bounded writing should produce, given the default
        // data_page_size). Read via the page-bounded decoder and count
        // pages for the value column.
        let out_bytes = std::fs::read(&outputs[0].path).expect("read");
        let mut output_stream = open_stream(Bytes::from(out_bytes)).await;
        // Find the "value" col index in the output's arrow schema BEFORE
        // borrowing output_stream mutably for the decoder.
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
            output_stream.metadata().file_metadata().schema_descr(),
            None,
        )
        .expect("arrow schema");
        let value_col_idx = arrow_schema.index_of("value").expect("value col");
        let mut decoder = StreamDecoder::new(&mut *output_stream);

        let mut value_pages = 0;
        while let Some(page) = decoder.decode_next_page().await.expect("decode") {
            if page.col_idx == value_col_idx {
                value_pages += 1;
            }
        }
        assert!(
            value_pages >= 2,
            "expected output 'value' col to span multiple pages (got {value_pages}); body col \
             writes should respect data_page_size",
        );
    }

    /// Regression for Codex P1 on PR-6409: a zero-row-group input
    /// must not panic the body-column path. Phase 0 explicitly accepts
    /// empty inputs (returning a zero-row sort batch), so the body-col
    /// loop has to defend against `row_group(0)` lookups on inputs
    /// with `num_row_groups() == 0`.
    #[tokio::test]
    async fn test_zero_row_input_mixed_with_non_empty() {
        let empty = make_sorted_batch(0, 0);
        let populated = make_sorted_batch(50, 0);
        let bytes_empty = write_input_parquet(std::slice::from_ref(&empty), &[]);
        let bytes_populated = write_input_parquet(std::slice::from_ref(&populated), &[]);

        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![
            open_stream(bytes_empty).await,
            open_stream(bytes_populated).await,
        ];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge with mixed empty + populated inputs");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, 50);

        let merged = read_output_to_record_batch(&outputs[0].path);
        let value = merged
            .column(merged.schema().index_of("value").expect("value col"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        // All 50 values from the populated input should round-trip in
        // input row order (timestamps descend in input row order to
        // match the sort key).
        for i in 0..50 {
            assert!(
                (value.value(i) - i as f64).abs() < 1e-9,
                "row {i}: expected {i}, got {}",
                value.value(i),
            );
        }
    }

    /// Regression for Codex P2 on PR-6409: `derive_output_schema`
    /// must drop sort fields that `optimize_output_batch` discarded as
    /// all-null for a given output. Before the fix the check was
    /// `sort_optimised.has(name) || full_union.has(name)`, where the
    /// second disjunct is trivially true for every iterated field, so
    /// all-null sort columns were never dropped. We exercise the
    /// helper directly with a synthetic union + sort-optimised pair so
    /// the regression doesn't depend on the merge plan creating an
    /// all-null sort col (which requires a multi-input fixture with
    /// divergent sort col presence).
    #[test]
    fn test_derive_output_schema_drops_all_null_sort_field() {
        // Union covers sort fields {metric_name, env, timestamp_secs}
        // plus body field {value}. `env` is a sort field declared in
        // the sort union but `optimize_output_batch` dropped it from
        // this output's `sort_optimised` (all-null for this output's
        // rows). Body field `value` is NOT in the sort union — it
        // must be preserved unconditionally.
        let full_union = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("env", DataType::Utf8, true),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let sort_union = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("env", DataType::Utf8, true),
            Field::new("timestamp_secs", DataType::UInt64, false),
        ]));
        // sort_optimised has dropped `env` (all-null for this output).
        let sort_optimised_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
        ]));
        let sort_optimised = RecordBatch::try_new(
            sort_optimised_schema,
            vec![
                Arc::new(StringArray::from(vec!["cpu.usage"; 4])),
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4])),
            ],
        )
        .expect("test batch");

        let derived = derive_output_schema(&full_union, &sort_union, &sort_optimised).expect("ok");
        let names: Vec<&str> = derived.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["metric_name", "timestamp_secs", "value"],
            "all-null sort field 'env' must be dropped; body field 'value' preserved",
        );

        // Direct contrast: with the pre-fix logic (every field kept)
        // the result would have included 'env'. Asserting the negative
        // form makes the regression unambiguous.
        assert!(
            derived.index_of("env").is_err(),
            "'env' must not appear in derived output schema",
        );
    }

    /// Write a fixture parquet file where each body column is forced
    /// to span multiple parquet data pages by pinning a small
    /// `data_page_row_count_limit`. The merge engine must read those
    /// pages back via a single persistent `StreamDecoder` per input —
    /// reconstructing the decoder for each `fill_page_cache_to_row`
    /// call (the pre-fix behaviour) would reset the per-column
    /// `rows_decoded` counter, making `DecodedPage::row_start` reset
    /// to zero on every page after the first.
    fn write_input_parquet_with_small_pages(
        batches: &[RecordBatch],
        data_page_row_count_limit: usize,
    ) -> Bytes {
        let schema = batches[0].schema();
        // Lower `write_batch_size` and `data_page_size` so the arrow
        // writer actually flushes pages at the row-count boundary.
        // With the defaults (`write_batch_size = 64 KiB`,
        // `data_page_size = 1 MiB`) the byte-size threshold doesn't
        // trip for our small fixtures and the writer emits one page
        // per column regardless of `data_page_row_count_limit`.
        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            data_page_row_count_limit,
            data_page_size: data_page_row_count_limit * 16,
            write_batch_size: data_page_row_count_limit,
            ..ParquetWriterConfig::default()
        };
        let sort_fields = "metric_name|-timestamp_secs/V2";
        let sort_field_names = vec!["metric_name".to_string(), "timestamp_secs".to_string()];
        let kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                sort_fields.to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
        ];
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: schema.index_of("metric_name").expect("test schema") as i32,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: schema.index_of("timestamp_secs").expect("test schema") as i32,
                descending: true,
                nulls_first: false,
            },
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            sorting_cols,
            Some(kvs),
            &sort_field_names,
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("arrow writer");
        for b in batches {
            writer.write(b).expect("test write");
        }
        writer.close().expect("test close");
        Bytes::from(buf)
    }

    /// Regression for Codex P1 on PR-6409: when a body column spans
    /// multiple input pages, every page-fetch must come from the same
    /// long-lived `StreamDecoder` so its per-column `rows_decoded`
    /// counter keeps producing absolute row offsets. Before the fix,
    /// each `fill_page_cache_to_row` call instantiated a fresh decoder
    /// whose counter started at zero — the *second* decoded page
    /// reported `row_start = 0` and the page cache's
    /// `(input_row - cache_start)` indexing landed on the wrong rows
    /// (or panicked on out-of-bounds).
    #[tokio::test]
    async fn test_body_col_multi_input_page_preserves_row_start() {
        // The bug only surfaces when `assemble_one_output_page` is
        // called more than once per output (so `fill_page_cache_to_row`
        // is invoked repeatedly with a non-empty cache). That means we
        // need more than `OUTPUT_PAGE_ROWS` (=1024) total input rows
        // for a single output. 2500 rows × 50-row input pages =
        // 50 body-col pages per column chunk; three output pages of
        // 1024+1024+452 each trigger a separate decoder advance.
        let total_rows = 2500;
        let batch = make_sorted_batch(total_rows, 0);
        let bytes = write_input_parquet_with_small_pages(std::slice::from_ref(&batch), 50);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, total_rows);

        let merged = read_output_to_record_batch(&outputs[0].path);
        let value_idx = merged.schema().index_of("value").expect("value col");
        let value = merged
            .column(value_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col is Float64");

        // `make_sorted_batch` fills `value` with `i as f64` (see the
        // fixture). Timestamps descend in input row order, matching the
        // sort key (timestamp_secs DESC), so the merge with a single
        // input is the identity permutation.
        for i in 0..total_rows {
            let expected = i as f64;
            let got = value.value(i);
            assert!(
                (expected - got).abs() < 1e-9,
                "row {i}: expected value {expected}, got {got} — body col page cache reported \
                 wrong row_start",
            );
        }
    }

    /// Regression for Codex P1 on PR-6409 (the multi-output half): when
    /// a body column is consumed by more than one output, the per-input
    /// page cache and decoder must outlive each output's assembler. The
    /// stream cannot be rewound, so dropping a partially-consumed page
    /// when output K ends would leave output K+1 unable to read rows
    /// that physically live inside that same page.
    #[tokio::test]
    async fn test_body_col_cache_persists_across_outputs() {
        // Two metric names so the engine splits the merge into two
        // outputs at the metric_name boundary. Each input has 200
        // rows of cpu.usage then 200 rows of memory.used — small
        // 50-row pages mean some pages span the boundary.
        let schema = make_sorted_batch(0, 0).schema();
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let keys: Vec<i32> = (0..400).map(|i| if i < 200 { 0 } else { 1 }).collect();
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(keys),
                Arc::new(metric_values),
            )
            .expect("dict"),
        );

        // Timestamps descend within each metric_name run so the file
        // is sorted by (metric_name ASC, timestamp DESC) — matching
        // the sort schema declared in write_input_parquet_with_small_pages.
        let timestamps: Vec<u64> = (0..400)
            .map(|i| {
                let run_pos = if i < 200 { i } else { i - 200 };
                1_700_000_000u64 + (199 - run_pos as u64)
            })
            .collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));

        let series: Vec<Vec<u8>> = (0..400u64).map(|i| i.to_be_bytes().to_vec()).collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
            series.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        ));

        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![1u8; 400]));
        let service_keys: Vec<i32> = (0..400i32).map(|_| 0).collect();
        let service_values = StringArray::from(vec!["svc-a"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(service_keys),
                Arc::new(service_values),
            )
            .expect("svc dict"),
        );
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from((0..400i64).collect::<Vec<_>>()));
        let value: ArrayRef = Arc::new(Float64Array::from(
            (0..400).map(|i| i as f64 * 0.5).collect::<Vec<_>>(),
        ));
        // Confirm the schema we hand-build still matches make_sorted_batch's:
        let _ = dict_type;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                metric_name,
                timestamp_secs,
                sorted_series,
                metric_type,
                service,
                timeseries_id,
                value,
            ],
        )
        .expect("test batch");

        let bytes = write_input_parquet_with_small_pages(std::slice::from_ref(&batch), 50);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(2))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 2, "two metric names → two outputs");

        let total: usize = outputs.iter().map(|o| o.num_rows).sum();
        assert_eq!(total, 400);

        // Concatenate the two outputs' value columns in output order
        // and verify every original value is present. The merge is
        // sort-stable, so values within each output appear in input
        // row order (timestamps descend within each metric run).
        let mut seen_values: HashSet<u64> = HashSet::new();
        for out in &outputs {
            let merged = read_output_to_record_batch(&out.path);
            let v = merged
                .column(merged.schema().index_of("value").expect("value col"))
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64");
            for i in 0..v.len() {
                // Encode as integer bits to dedupe; original values
                // are i * 0.5 for i in 0..400, all distinct.
                seen_values.insert(v.value(i).to_bits());
            }
        }
        assert_eq!(
            seen_values.len(),
            400,
            "every input value should round-trip through the merge across both outputs",
        );
    }

    /// Multi-RG input is rejected (PR-6b.2 simplification).
    #[tokio::test]
    async fn test_multi_rg_input_rejected() {
        // Force a 2-RG file by writing two batches with row_group_size = 1
        // small enough to trip RG rollover.
        let batch_a = make_sorted_batch(50, 0);
        let batch_b = make_sorted_batch(50, 50);

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            row_group_size: 50, // force one RG per 50-row batch
            ..ParquetWriterConfig::default()
        };
        let kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "metric_name|-timestamp_secs/V2".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &batch_a.schema(),
            Vec::new(),
            Some(kvs),
            &["metric_name".to_string(), "timestamp_secs".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch_a.schema(), Some(props)).expect("arrow writer");
        writer.write(&batch_a).expect("write");
        writer.write(&batch_b).expect("write");
        writer.close().expect("close");
        let bytes = Bytes::from(buf);

        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];
        let tmp = TempDir::new().expect("tmpdir");
        let err = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect_err("multi-RG input must be rejected");
        let s = err.to_string();
        assert!(
            s.contains("single-row-group"),
            "expected 'single-row-group' error, got: {s}",
        );
    }

    // ============================================================================
    // MC-2 round-trip: every parquet physical type the decoder supports must
    // survive the streaming merge unchanged.
    // ============================================================================

    /// Build a batch covering every primitive type the page decoder
    /// supports, plus byte arrays, dictionary encoding, and list types.
    /// Each row's `sorted_series` key uniquely identifies the row so
    /// callers can build a `(key → tuple)` map for output comparison.
    fn make_typed_round_trip_batch(num_rows: usize, key_offset: u64) -> RecordBatch {
        use arrow::array::{
            BooleanArray, Float32Array, Int8Array, Int16Array, Int32Array, LargeBinaryArray,
            ListArray, UInt16Array, UInt32Array,
        };
        use arrow::buffer::OffsetBuffer;

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        // Body cols MUST be in lexicographic order — that's the
        // storage convention the streaming engine assumes when
        // iterating columns. Inputs that ship body cols in a
        // different order trip "column ordering violated" mid-merge.
        let schema = Arc::new(ArrowSchema::new(vec![
            // sort cols
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            // body cols, all nullable, in lexicographic order. Null
            // every 7th row to exercise the null-mask path.
            Field::new("body_bool", DataType::Boolean, true),
            Field::new("body_dict", dict_type, true),
            Field::new("body_float32", DataType::Float32, true),
            Field::new("body_float64", DataType::Float64, true),
            Field::new("body_int16", DataType::Int16, true),
            Field::new("body_int32", DataType::Int32, true),
            Field::new("body_int64", DataType::Int64, true),
            Field::new("body_int8", DataType::Int8, true),
            Field::new("body_largebinary", DataType::LargeBinary, true),
            // `List<Float64>` covers production-shaped histogram bucket
            // columns. Outer + inner both non-nullable to match the
            // decoder's PR-6a.2 contract.
            Field::new(
                "body_list_f64",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                false,
            ),
            Field::new("body_string", DataType::Utf8, true),
            Field::new("body_uint16", DataType::UInt16, true),
            Field::new("body_uint32", DataType::UInt32, true),
            Field::new("body_uint64", DataType::UInt64, true),
            Field::new("body_uint8", DataType::UInt8, true),
        ]));

        let is_null = |i: usize| i.is_multiple_of(7);

        let metric_keys: Vec<i32> = vec![0; num_rows];
        let metric_values = StringArray::from(vec!["cpu.usage"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .expect("metric dict"),
        );

        // Timestamps DESC within the run so the input is pre-sorted on
        // (metric_name ASC, timestamp DESC) per the sort schema.
        let timestamps: Vec<u64> = (0..num_rows as u64)
            .map(|i| 1_700_000_000 + (num_rows as u64 - i))
            .collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));

        let key_bytes: Vec<Vec<u8>> = (0..num_rows as u64)
            .map(|i| (key_offset + i).to_be_bytes().to_vec())
            .collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
            key_bytes.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        ));

        // Primitive value generators chosen to span each type's range
        // including signed/unsigned boundaries.
        let mk_opt = |i: usize, v: i64| if is_null(i) { None } else { Some(v) };
        let body_int8: ArrayRef = Arc::new(Int8Array::from(
            (0..num_rows)
                .map(|i| mk_opt(i, (i as i64 % 251) - 125).map(|v| v as i8))
                .collect::<Vec<_>>(),
        ));
        let body_uint8: ArrayRef = Arc::new(UInt8Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some((i % 255) as u8)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_int16: ArrayRef = Arc::new(Int16Array::from(
            (0..num_rows)
                .map(|i| mk_opt(i, (i as i64 % 30001) - 15000).map(|v| v as i16))
                .collect::<Vec<_>>(),
        ));
        let body_uint16: ArrayRef = Arc::new(UInt16Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some((i % 60000) as u16)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_int32: ArrayRef = Arc::new(Int32Array::from(
            (0..num_rows)
                .map(|i| {
                    mk_opt(
                        i,
                        i as i64 * 0x100_0000i64 - i64::from(i32::MIN.unsigned_abs() / 2),
                    )
                    .map(|v| v as i32)
                })
                .collect::<Vec<_>>(),
        ));
        let body_uint32: ArrayRef = Arc::new(UInt32Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some((i as u32).wrapping_mul(0xDEAD_BEEF))
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_int64: ArrayRef = Arc::new(Int64Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some((i as i64).wrapping_mul(0x0123_4567_89AB_CDEF))
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_uint64: ArrayRef = Arc::new(UInt64Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some((i as u64).wrapping_mul(0xFEDC_BA98_7654_3210))
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_float32: ArrayRef = Arc::new(Float32Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some(i as f32 * 0.25 - 100.0)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_float64: ArrayRef = Arc::new(Float64Array::from(
            (0..num_rows)
                .map(|i| {
                    if is_null(i) {
                        None
                    } else {
                        Some(i as f64 * 0.5 - 1e6)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let body_bool: ArrayRef = Arc::new(BooleanArray::from(
            (0..num_rows)
                .map(|i| if is_null(i) { None } else { Some(i % 3 == 0) })
                .collect::<Vec<_>>(),
        ));

        let body_string_vals: Vec<Option<String>> = (0..num_rows)
            .map(|i| {
                if is_null(i) {
                    None
                } else {
                    Some(format!("row-{i:08}-payload"))
                }
            })
            .collect();
        let body_string: ArrayRef = Arc::new(StringArray::from(body_string_vals));

        let body_largebinary_vals: Vec<Option<Vec<u8>>> = (0..num_rows)
            .map(|i| {
                if is_null(i) {
                    None
                } else {
                    Some(vec![(i & 0xFF) as u8; 1 + (i % 5)])
                }
            })
            .collect();
        let body_largebinary: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(
            body_largebinary_vals
                .iter()
                .map(|opt| opt.as_deref())
                .collect(),
        ));

        // Dict body col cycles through a small set so the dict-encoding
        // path is exercised end-to-end.
        let dict_pool = ["api", "db", "cache", "auth", "billing"];
        let dict_keys: Vec<Option<i32>> = (0..num_rows as i32)
            .map(|i| {
                if is_null(i as usize) {
                    None
                } else {
                    Some(i % (dict_pool.len() as i32))
                }
            })
            .collect();
        let body_dict: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(dict_keys),
                Arc::new(StringArray::from(dict_pool.to_vec())),
            )
            .expect("body_dict"),
        );

        // List<f64>: row i has a list of length (i % 4) with f64
        // values derived from i and j. Outer + inner non-null (the
        // decoder's PR-6a.2 list path requires both). Empty lists are
        // still exercised on rows where (i % 4) == 0.
        let mut list_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
        let mut list_values: Vec<f64> = Vec::new();
        list_offsets.push(0);
        for i in 0..num_rows {
            for j in 0..(i % 4) {
                list_values.push((i * 10 + j) as f64 + 0.125);
            }
            list_offsets.push(list_values.len() as i32);
        }
        let list_inner_field = Arc::new(Field::new("item", DataType::Float64, false));
        let list_inner: ArrayRef = Arc::new(Float64Array::from(list_values));
        let body_list_f64: ArrayRef = Arc::new(ListArray::new(
            list_inner_field,
            OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(list_offsets)),
            list_inner,
            None,
        ));

        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                timestamp_secs,
                sorted_series,
                body_bool,
                body_dict,
                body_float32,
                body_float64,
                body_int16,
                body_int32,
                body_int64,
                body_int8,
                body_largebinary,
                body_list_f64,
                body_string,
                body_uint16,
                body_uint32,
                body_uint64,
                body_uint8,
            ],
        )
        .expect("typed round-trip batch")
    }

    /// Render a single row's body-col cell to a comparable string.
    /// Used by the round-trip test to compare logical values across
    /// the merge. MC-2 (row contents preservation) is about logical
    /// values, not internal storage layout: `Dictionary<i32, Utf8>`,
    /// `Utf8`, and `LargeUtf8` carrying the same string are the same
    /// row content; similarly for `Binary` / `LargeBinary`. The
    /// streaming engine normalizes string-flavoured types to `Utf8`
    /// via `normalize_type` in the union schema, and parquet has only
    /// one byte-array physical type so `LargeBinary` round-trips as
    /// `Binary`. Both transformations are storage-encoding changes,
    /// not value changes — the comparison must see them as equal.
    fn render_cell(arr: &dyn arrow::array::Array, row: usize) -> String {
        use arrow::array::AsArray;
        use arrow::datatypes::Int32Type as DictKeyInt32;

        if !arr.is_valid(row) {
            return "<null>".to_string();
        }
        match arr.data_type() {
            DataType::Int8 => format!(
                "i8:{}",
                arr.as_primitive::<arrow::datatypes::Int8Type>().value(row)
            ),
            DataType::Int16 => format!(
                "i16:{}",
                arr.as_primitive::<arrow::datatypes::Int16Type>().value(row)
            ),
            DataType::Int32 => format!(
                "i32:{}",
                arr.as_primitive::<arrow::datatypes::Int32Type>().value(row)
            ),
            DataType::Int64 => format!(
                "i64:{}",
                arr.as_primitive::<arrow::datatypes::Int64Type>().value(row)
            ),
            DataType::UInt8 => format!(
                "u8:{}",
                arr.as_primitive::<arrow::datatypes::UInt8Type>().value(row)
            ),
            DataType::UInt16 => format!(
                "u16:{}",
                arr.as_primitive::<arrow::datatypes::UInt16Type>()
                    .value(row)
            ),
            DataType::UInt32 => format!(
                "u32:{}",
                arr.as_primitive::<arrow::datatypes::UInt32Type>()
                    .value(row)
            ),
            DataType::UInt64 => format!(
                "u64:{}",
                arr.as_primitive::<arrow::datatypes::UInt64Type>()
                    .value(row)
            ),
            DataType::Float32 => {
                format!(
                    "f32:{:#x}",
                    arr.as_primitive::<arrow::datatypes::Float32Type>()
                        .value(row)
                        .to_bits()
                )
            }
            DataType::Float64 => {
                format!(
                    "f64:{:#x}",
                    arr.as_primitive::<arrow::datatypes::Float64Type>()
                        .value(row)
                        .to_bits()
                )
            }
            DataType::Boolean => format!("bool:{}", arr.as_boolean().value(row)),
            // String flavours collapse to one rendering — Dict<i32,
            // Utf8>, Utf8, LargeUtf8 are interchangeable by value.
            DataType::Utf8 => format!("str:{}", arr.as_string::<i32>().value(row)),
            DataType::LargeUtf8 => format!("str:{}", arr.as_string::<i64>().value(row)),
            DataType::Dictionary(_, _) => {
                let d = arr.as_dictionary::<DictKeyInt32>();
                let key = d.keys().value(row);
                let values = d
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("dict values Utf8");
                format!("str:{}", values.value(key as usize))
            }
            // Byte arrays collapse similarly — parquet has only one
            // BYTE_ARRAY physical type.
            DataType::Binary => format!("bin:{:?}", arr.as_binary::<i32>().value(row)),
            DataType::LargeBinary => format!("bin:{:?}", arr.as_binary::<i64>().value(row)),
            DataType::List(_) => {
                let list = arr.as_list::<i32>();
                let inner = list.value(row);
                let inner_f64 = inner.as_primitive::<arrow::datatypes::Float64Type>();
                let cells: Vec<String> = (0..inner_f64.len())
                    .map(|j| {
                        if inner_f64.is_valid(j) {
                            format!("{:#x}", inner_f64.value(j).to_bits())
                        } else {
                            "null".to_string()
                        }
                    })
                    .collect();
                format!("list_f64:[{}]", cells.join(","))
            }
            DataType::LargeList(_) => {
                let list = arr.as_list::<i64>();
                let inner = list.value(row);
                let inner_f64 = inner.as_primitive::<arrow::datatypes::Float64Type>();
                let cells: Vec<String> = (0..inner_f64.len())
                    .map(|j| {
                        if inner_f64.is_valid(j) {
                            format!("{:#x}", inner_f64.value(j).to_bits())
                        } else {
                            "null".to_string()
                        }
                    })
                    .collect();
                format!("ll_f64:[{}]", cells.join(","))
            }
            other => panic!("unexpected body-col data type in round-trip: {other:?}"),
        }
    }

    /// MC-2: row contents do not change during compaction. Build two
    /// inputs that together cover every parquet physical type the
    /// decoder supports, merge them via the streaming engine, then
    /// build a `(sorted_series_key → rendered tuple)` map from both
    /// inputs and from the output. The two maps must be byte-equal —
    /// no row added, removed, or mutated. Catches silent type-
    /// dispatch bugs (the class that the recent List<Float64>
    /// flattening regression was in).
    #[tokio::test]
    async fn test_mc2_all_types_round_trip_through_streaming_merge() {
        let batch_a = make_typed_round_trip_batch(120, 0);
        let batch_b = make_typed_round_trip_batch(120, 10_000);
        let bytes_a = write_input_parquet(std::slice::from_ref(&batch_a), &[]);
        let bytes_b = write_input_parquet(std::slice::from_ref(&batch_b), &[]);

        let inputs: Vec<Box<dyn ColumnPageStream>> =
            vec![open_stream(bytes_a).await, open_stream(bytes_b).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, 240);

        // Build the expected (sorted_series → rendered tuple) map from
        // both inputs. Body cols are everything past `sorted_series`.
        let mut expected: HashMap<Vec<u8>, String> = HashMap::new();
        for batch in [&batch_a, &batch_b] {
            let series_idx = batch.schema().index_of("sorted_series").expect("series");
            let series_col = batch
                .column(series_idx)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("series Binary");
            let body_indices: Vec<usize> =
                (series_idx + 1..batch.schema().fields().len()).collect();
            for row in 0..batch.num_rows() {
                let mut tuple = String::new();
                for (n, &col_idx) in body_indices.iter().enumerate() {
                    if n > 0 {
                        tuple.push('|');
                    }
                    tuple.push_str(&render_cell(batch.column(col_idx).as_ref(), row));
                }
                let key = series_col.value(row).to_vec();
                let prior = expected.insert(key.clone(), tuple);
                assert!(
                    prior.is_none(),
                    "input batches share a sorted_series key {key:?} — fixture bug",
                );
            }
        }
        assert_eq!(expected.len(), 240);

        // Build the observed map from the merged output. Note arrow
        // type coercions: Utf8 may come back as Dictionary because of
        // per-output schema optimisation, and Int32-keyed Dict may
        // come back with a different key width. Cast both sides to
        // Utf8 / Float64 / etc. via the same `render_cell` helper so
        // the comparison is type-insensitive.
        let merged = read_output_to_record_batch(&outputs[0].path);
        let merged_schema = merged.schema();
        let series_idx = merged_schema.index_of("sorted_series").expect("series");
        let series_col = merged
            .column(series_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("merged series Binary");

        // Map each body-col name in the inputs to its column index in
        // the merged output (positions can shift if output schema
        // dropped an all-null sort col).
        let input_body_cols: Vec<String> = batch_a
            .schema()
            .fields()
            .iter()
            .skip(batch_a.schema().index_of("sorted_series").unwrap() + 1)
            .map(|f| f.name().clone())
            .collect();
        let merged_body_indices: Vec<usize> = input_body_cols
            .iter()
            .map(|name| {
                merged_schema.index_of(name).unwrap_or_else(|_| {
                    panic!(
                        "merged output is missing body col '{name}' — MC-4 column union violated"
                    )
                })
            })
            .collect();

        let mut observed: HashMap<Vec<u8>, String> = HashMap::with_capacity(merged.num_rows());
        for row in 0..merged.num_rows() {
            let mut tuple = String::new();
            for (n, &col_idx) in merged_body_indices.iter().enumerate() {
                if n > 0 {
                    tuple.push('|');
                }
                tuple.push_str(&render_cell(merged.column(col_idx).as_ref(), row));
            }
            let key = series_col.value(row).to_vec();
            let prior = observed.insert(key.clone(), tuple);
            assert!(
                prior.is_none(),
                "merged output has duplicate sorted_series key {key:?} — MC-1 violated",
            );
        }

        // MC-1: same set of keys.
        assert_eq!(
            observed.len(),
            expected.len(),
            "row count mismatch input vs output",
        );
        for (key, want) in &expected {
            let got = observed.get(key).unwrap_or_else(|| {
                panic!(
                    "merged output is missing input key {:?} (first body cell expected: {})",
                    key,
                    want.split('|').next().unwrap_or("?")
                )
            });
            assert_eq!(
                got, want,
                "body-col tuple mismatch for sorted_series {key:?}: got {got}, want {want}",
            );
        }
    }

    // ============================================================================
    // MS-7: page-cache bounded-memory contract. The streaming engine's
    // raison d'être is that body-col memory stays bounded by ~constant
    // (page size × small) regardless of how big the input column gets.
    // Concretely, `body_col_page_cache` length per input must stay ≤ a
    // small constant — never scale with row count.
    // ============================================================================

    /// Build a fixture that forces many input body-col pages with a
    /// pinned `data_page_row_count_limit`, then merge it through the
    /// streaming engine and read back the peak cache length. Used by
    /// the MS-7 test below across multiple sizes.
    async fn merge_and_observe_peak_page_cache(num_rows: usize, page_rows: usize) -> usize {
        use std::sync::atomic::Ordering;

        PEAK_BODY_COL_PAGE_CACHE_LEN.store(0, Ordering::Relaxed);

        let batch = make_sorted_batch(num_rows, 0);
        let bytes = write_input_parquet_with_small_pages(std::slice::from_ref(&batch), page_rows);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, num_rows);

        PEAK_BODY_COL_PAGE_CACHE_LEN.load(Ordering::Relaxed)
    }

    /// MS-7: peak `body_col_page_cache` length is bounded by the
    /// ratio of output-page rows to input-page rows — NOT by input
    /// column size. Each `assemble_one_output_page` call must read
    /// enough input pages to cover `OUTPUT_PAGE_ROWS` rows, then
    /// evict everything below the new cursor. So the peak per advance
    /// loop is bounded by `ceil(OUTPUT_PAGE_ROWS / page_rows) + small
    /// slack` regardless of how many output pages we produce.
    ///
    /// Without the per-output-page eviction, the peak would scale with
    /// the total number of input pages — which scales with input size.
    /// A regression that dropped the eviction loop would push the peak
    /// past the ceiling for the 30 000-row fixture but not the 3 000-
    /// row fixture, breaking both assertions below.
    #[tokio::test]
    async fn test_ms7_body_col_page_cache_bounded_regardless_of_input_size() {
        const PAGE_ROWS: usize = 50;
        // ceil(1024 / 50) = 21 in-flight pages needed for one output
        // page. Allow 3 slack: decoder lookahead, transient between
        // push and check, dict-page-as-data-page corner cases.
        const MAX_RESIDENT_PAGES: usize = 24;

        let peak_small = merge_and_observe_peak_page_cache(300, PAGE_ROWS).await;
        let peak_medium = merge_and_observe_peak_page_cache(3_000, PAGE_ROWS).await;
        let peak_large = merge_and_observe_peak_page_cache(30_000, PAGE_ROWS).await;

        // 300-row fixture has only 6 input pages, so its peak can't
        // exceed 6; verifying that the assembler doesn't somehow
        // accumulate ghost entries past the input itself.
        assert!(
            peak_small <= 6 + 1,
            "300-row fixture: peak cache len {peak_small} > 7",
        );
        // Medium / large fixtures share the same OUTPUT_PAGE_ROWS /
        // PAGE_ROWS ratio, so they must share the same peak ceiling.
        assert!(
            peak_medium <= MAX_RESIDENT_PAGES,
            "3 000-row fixture: peak cache len {peak_medium} > {MAX_RESIDENT_PAGES}",
        );
        assert!(
            peak_large <= MAX_RESIDENT_PAGES,
            "30 000-row fixture: peak cache len {peak_large} > {MAX_RESIDENT_PAGES} — body col \
             write is no longer page-bounded; likely buffering whole column chunks",
        );

        // The headline MS-7 claim: peak DOES NOT grow proportionally
        // with input size. Going from 3 000 to 30 000 rows multiplies
        // total input pages by 10, but peak resident cache should
        // stay essentially flat. Allow a 2-page slack for transients.
        let growth = peak_large.saturating_sub(peak_medium);
        assert!(
            growth <= 2,
            "peak grows with input size: medium={peak_medium}, large={peak_large} — 10× more \
             input pages produced {growth} more resident pages, body-col path is not page-bounded",
        );
    }

    // ============================================================================
    // Heterogeneous-output regressions (Codex P2 batch on PR-6409)
    // ============================================================================

    /// Regression for "Use the full union schema when driving column
    /// writes": two outputs with the **same field count** that drop
    /// *different* all-null sort fields. The previous
    /// `build_parent_union_schema` heuristic picked the first such
    /// schema and used it to drive the column loop — silently
    /// skipping any column it dropped, even if another output still
    /// needed it. The fix drives iteration from `full_union_schema`
    /// directly.
    ///
    /// Setup: two inputs declaring an extended sort schema
    /// `metric_name|tag_a|tag_b|-timestamp_secs/V2`. Input A has only
    /// `tag_a` populated, input B has only `tag_b` populated. Merge
    /// with `num_outputs=2` so each input's rows land in its own
    /// output. After the per-output optimiser, output 0's schema
    /// drops `tag_b` (all-null) and output 1's schema drops `tag_a`
    /// — same field count, different dropped fields. Both outputs
    /// must still write their kept tag column.
    #[tokio::test]
    async fn test_heterogeneous_dropped_fields_drive_from_full_union_schema() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        // Schema for input A: metric_name, tag_a (non-null), tag_b
        // absent. Schema for input B: metric_name, tag_b (non-null),
        // tag_a absent. The merge engine's union schema makes both
        // tag fields nullable in the combined view.
        let mk_schema = |with_a: bool, with_b: bool| -> SchemaRef {
            let mut fields = vec![
                Field::new("metric_name", dict_type.clone(), false),
                Field::new("timestamp_secs", DataType::UInt64, false),
                Field::new("sorted_series", DataType::Binary, false),
            ];
            // Body cols in lexicographic order.
            if with_a {
                fields.push(Field::new("tag_a", DataType::Utf8, false));
            }
            if with_b {
                fields.push(Field::new("tag_b", DataType::Utf8, false));
            }
            fields.push(Field::new("value", DataType::Float64, false));
            Arc::new(ArrowSchema::new(fields))
        };

        let make_batch = |metric: &str,
                          schema: SchemaRef,
                          tag_a_val: Option<&str>,
                          tag_b_val: Option<&str>,
                          row_count: usize,
                          base_series: u64|
         -> RecordBatch {
            let metric_keys: Vec<i32> = vec![0; row_count];
            let metric_values = StringArray::from(vec![metric]);
            let metric_name: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    arrow::array::Int32Array::from(metric_keys),
                    Arc::new(metric_values),
                )
                .expect("dict"),
            );
            let timestamps: Vec<u64> = (0..row_count as u64)
                .map(|i| 1_700_000_000 + (row_count as u64 - i))
                .collect();
            let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
            let series: Vec<Vec<u8>> = (0..row_count as u64)
                .map(|i| (base_series + i).to_be_bytes().to_vec())
                .collect();
            let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
                series.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
            ));
            let value: ArrayRef = Arc::new(Float64Array::from(
                (0..row_count).map(|i| i as f64).collect::<Vec<_>>(),
            ));
            let mut columns: Vec<ArrayRef> = vec![metric_name, timestamp_secs, sorted_series];
            if let Some(v) = tag_a_val {
                columns.push(Arc::new(StringArray::from(vec![v; row_count])) as ArrayRef);
            }
            if let Some(v) = tag_b_val {
                columns.push(Arc::new(StringArray::from(vec![v; row_count])) as ArrayRef);
            }
            columns.push(value);
            RecordBatch::try_new(schema, columns).expect("batch")
        };

        // Input A has tag_a populated, sort key "aaa.metric" (sorts
        // before "zzz.metric"). Input B has tag_b populated, sort key
        // "zzz.metric". With num_outputs=2 the merge splits at the
        // metric boundary so each input's rows land in its own output.
        let schema_a = mk_schema(true, false);
        let schema_b = mk_schema(false, true);
        let batch_a = make_batch("aaa.metric", schema_a, Some("alpha"), None, 12, 0);
        let batch_b = make_batch("zzz.metric", schema_b, None, Some("beta"), 12, 1000);

        let sort_fields_str = "metric_name|tag_a|tag_b|-timestamp_secs/V2";
        let make_input_bytes = |batch: &RecordBatch| -> Bytes {
            let cfg = ParquetWriterConfig {
                compression: Compression::Snappy,
                ..ParquetWriterConfig::default()
            };
            let kvs = vec![
                KeyValue::new(
                    PARQUET_META_SORT_FIELDS.to_string(),
                    sort_fields_str.to_string(),
                ),
                KeyValue::new(
                    PARQUET_META_WINDOW_START.to_string(),
                    "1700000000".to_string(),
                ),
                KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
                KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
            ];
            let props: WriterProperties = cfg.to_writer_properties_with_metadata(
                &batch.schema(),
                Vec::new(),
                Some(kvs),
                &[
                    "metric_name".to_string(),
                    "tag_a".to_string(),
                    "tag_b".to_string(),
                    "timestamp_secs".to_string(),
                ],
            );
            let mut buf: Vec<u8> = Vec::new();
            let mut writer =
                ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).expect("arrow writer");
            writer.write(batch).expect("write");
            writer.close().expect("close");
            Bytes::from(buf)
        };
        let bytes_a = make_input_bytes(&batch_a);
        let bytes_b = make_input_bytes(&batch_b);

        let inputs: Vec<Box<dyn ColumnPageStream>> =
            vec![open_stream(bytes_a).await, open_stream(bytes_b).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(2))
            .await
            .expect("merge with heterogeneous dropped fields");
        assert_eq!(outputs.len(), 2, "two metric names → two outputs");

        // The output whose rows came from input A must carry tag_a
        // values; the output whose rows came from input B must carry
        // tag_b values. Before the fix, one of them was silently
        // missing its kept tag column because the parent driver
        // skipped it.
        let mut saw_alpha = false;
        let mut saw_beta = false;
        for out in &outputs {
            let merged = read_output_to_record_batch(&out.path);
            let schema = merged.schema();
            if let Ok(tag_a_idx) = schema.index_of("tag_a") {
                let col = merged.column(tag_a_idx);
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let s = render_cell(col.as_ref(), i);
                        if s.ends_with("alpha") {
                            saw_alpha = true;
                        }
                    }
                }
            }
            if let Ok(tag_b_idx) = schema.index_of("tag_b") {
                let col = merged.column(tag_b_idx);
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let s = render_cell(col.as_ref(), i);
                        if s.ends_with("beta") {
                            saw_beta = true;
                        }
                    }
                }
            }
        }
        assert!(
            saw_alpha,
            "expected to find tag_a='alpha' in some output — the full-union-schema-driven column \
             loop must visit tag_a even when another output dropped it",
        );
        assert!(
            saw_beta,
            "expected to find tag_b='beta' in some output — the full-union-schema-driven column \
             loop must visit tag_b even when another output dropped it",
        );
    }

    /// Regression for "Preserve service tags when service is a sort
    /// column". If the sort schema places `service` in the sort key
    /// (e.g., `metric_name|service|...`), the streaming engine writes
    /// it via the sort-col path and the body-col `track_service`
    /// branch never runs — so `MergeOutputFile.low_cardinality_tags`
    /// historically came back empty even though every row in the
    /// output has a service value. The fix folds in service names
    /// from the per-output sort batch at finalize time.
    #[tokio::test]
    async fn test_service_as_sort_column_still_populates_low_cardinality_tags() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            // Sort cols in sort schema order: metric_name, service,
            // timestamp_secs (timestamp comes last per the sort
            // validator's requirement).
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("service", DataType::Utf8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let row_count = 30usize;
        let metric_keys: Vec<i32> = vec![0; row_count];
        let metric_values = StringArray::from(vec!["cpu.usage"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .expect("dict"),
        );
        // service grouped in ASC order so the input is genuinely
        // sorted by (metric ASC, service ASC, timestamp DESC) and the
        // engine's MC-3 sort verifier accepts it.
        let mut services_sorted: Vec<&str> = Vec::with_capacity(row_count);
        for s in ["api", "cache", "db"] {
            for _ in 0..(row_count / 3) {
                services_sorted.push(s);
            }
        }
        let service: ArrayRef = Arc::new(StringArray::from(services_sorted));
        let timestamps: Vec<u64> = (0..row_count as u64)
            .map(|i| 1_700_000_000 + (row_count as u64 - (i % 10)))
            .collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let series_bytes: Vec<Vec<u8>> = (0..row_count as u64)
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
            series_bytes
                .iter()
                .map(|v| v.as_slice())
                .collect::<Vec<_>>(),
        ));
        let value: ArrayRef = Arc::new(Float64Array::from(
            (0..row_count).map(|i| i as f64).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![metric_name, service, timestamp_secs, sorted_series, value],
        )
        .expect("batch");

        let sort_fields_str = "metric_name|service|-timestamp_secs/V2";
        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            ..ParquetWriterConfig::default()
        };
        let kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                sort_fields_str.to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            Vec::new(),
            Some(kvs),
            &[
                "metric_name".to_string(),
                "service".to_string(),
                "timestamp_secs".to_string(),
            ],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(Bytes::from(buf)).await];
        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        let svc_tags = outputs[0].low_cardinality_tags.get(TAG_SERVICE).expect(
            "MergeOutputFile.low_cardinality_tags must contain TAG_SERVICE even when service is a \
             sort column",
        );
        let mut got: Vec<String> = svc_tags.iter().cloned().collect();
        got.sort();
        assert_eq!(
            got,
            vec!["api".to_string(), "cache".to_string(), "db".to_string()],
            "service-name set must cover every distinct value in the sort col",
        );
    }
}
