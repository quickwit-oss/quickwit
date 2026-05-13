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
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use parquet::file::metadata::ParquetMetaData;
use tokio::runtime::Handle;
use tracing::info;

use super::merge_order::{MergeRun, compute_merge_order};
use super::schema::align_inputs_to_union_schema;
use super::writer::{apply_merge_permutation, verify_sort_order};
use super::{InputMetadata, MergeConfig, MergeOutputFile};

mod body_assembler;
mod output;
pub(crate) mod region_grouping;

use body_assembler::{BodyColOutputPageAssembler, StreamingBodyColIter};
use output::{
    OutputAccumulator, OutputWriterStorage, finalize_output, open_output_writer_for_streaming,
};
use region_grouping::{
    Region, extract_regions_from_metadata, split_region_at_sorted_series,
    validate_region_order_matches_physical_rg_order,
};

use crate::sort_fields::{
    equivalent_schemas_for_compaction, is_timestamp_column_name, parse_sort_fields,
};
use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::storage::page_decoder::{DecodedPage, StreamDecoder};
use crate::storage::{
    ColumnPageStream, PARQUET_META_NUM_MERGE_OPS, PARQUET_META_RG_PARTITION_PREFIX_LEN,
    PARQUET_META_SORT_FIELDS, PARQUET_META_WINDOW_DURATION, PARQUET_META_WINDOW_START,
};

/// Output page size in rows for body-col assembly. Each call to the
/// sync iterator passed to [`write_next_column_arrays`] yields one
/// `ArrayRef` of up to this many rows; the parquet writer flushes
/// physical pages independently as encoded bytes cross
/// `data_page_size_limit`. 1024 keeps assembled arrays small enough
/// to bound per-output memory but large enough to amortise per-page
/// fixed costs.
///
/// [`write_next_column_arrays`]: crate::storage::streaming_writer::RowGroupBuilder::write_next_column_arrays
pub(crate) const OUTPUT_PAGE_ROWS: usize = 1024;

/// Test-only peak observed length of any input's `body_col_page_cache`
/// since the last reset. Used by the MS-7 page-bounded-memory test to
/// assert that the cache stays bounded by a small constant regardless
/// of input column size. Set unconditionally inside the merge so the
/// invariant is observable in any test build; reset on each merge entry.
#[cfg(test)]
pub(crate) static PEAK_BODY_COL_PAGE_CACHE_LEN: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn record_body_col_page_cache_len(len: usize) {
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
pub(crate) fn record_body_col_page_cache_len(_len: usize) {}

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

    let input_meta = extract_and_validate_input_metadata(&inputs)?;

    // Reject legacy multi-RG inputs (`rg_partition_prefix_len == 0`
    // AND any input has >1 row group). These have no alignment claim,
    // so RG boundaries are arbitrary row counts that may split a
    // single sort-key value across two RGs. The streaming engine
    // cannot determine merge regions without column-chunk-bounded
    // buffering; such inputs must go through PR-5's
    // `LegacyMultiRGAdapter`, which presents them as one synthetic
    // single-RG stream.
    if input_meta.rg_partition_prefix_len == 0 {
        for (idx, stream) in inputs.iter().enumerate() {
            let num_rgs = stream.metadata().num_row_groups();
            if num_rgs > 1 {
                bail!(
                    "legacy multi-RG inputs (rg_partition_prefix_len=0) must go through the PR-5 \
                     adapter — input {idx} has {num_rgs} row groups with no alignment claim"
                );
            }
        }
    }

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

        // Pre-compute regions from RG metadata. With prefix_len >= 1
        // each region is one sort-prefix value across inputs (each
        // contributing input has exactly one RG in that region). With
        // prefix_len == 0 (validated single-RG above) there is one
        // region covering all inputs — we'll subdivide it below by
        // walking the merge order to find first-sort-col transitions.
        let regions = extract_regions_from_metadata(&decoders_state, &input_meta)?;

        if regions.is_empty() {
            info!("all inputs empty, producing no output");
            return Ok(Vec::new());
        }

        // MS-2: validate that the BTreeMap-driven region order
        // agrees with each input's physical RG order. The streaming
        // engine reads inputs sequentially — it cannot rewind. If
        // region K's contributing RG for input I is physically AFTER
        // a later region L's contributing RG for the same input, the
        // engine would crash mid-merge with "page from rg X while
        // draining rg Y". This typically means the input was sorted
        // in the opposite direction from what the sort schema
        // declares (e.g., metric_name written DESC on disk but the
        // sort schema says ASC). Reject upfront with a clear error.
        validate_region_order_matches_physical_rg_order(&regions, decoders_state.len())?;

        let total_rows: usize = regions.iter().map(|r| r.total_rows()).sum();
        let target_per_output = (total_rows.div_ceil(num_outputs)).max(1);

        info!(
            total_rows,
            num_regions = regions.len(),
            num_outputs,
            "streaming merge regions computed"
        );

        // Build the union schema once across all inputs' arrow schemas.
        let arrow_schemas: Vec<SchemaRef> = decoders_state
            .iter()
            .map(|s| Arc::clone(&s.arrow_schema))
            .collect();
        let union_schema =
            build_full_union_schema_from_arrow_schemas(&arrow_schemas, &input_meta.sort_fields)?;

        // Region processing loop. For each top-level region we may
        // need to subdivide it across multiple output files so we
        // honor `num_outputs` even when the input layout doesn't
        // give us enough region boundaries (e.g., one giant
        // `metric_name` with prefix_len=0). Splitting happens at
        // `sorted_series` transitions inside the region's merge
        // order — never inside a single sorted_series run.
        //
        // Memory: between top-level regions we reset every input's
        // per-col page cache + cursor, because pages from one RG
        // would have row_start values that collide with the next
        // RG's row-index space. Sub-regions of one top-level region
        // share an RG, so the cache survives across sub-region
        // boundaries — that's what lets the col write loop re-read
        // an earlier col's pages for a later sub-region.
        let mut outputs: Vec<MergeOutputFile> = Vec::new();
        let mut current_writer: Option<OutputWriterStorage> = None;
        let mut current_accumulator: Option<OutputAccumulator> = None;
        let mut current_output_idx: usize = 0;
        let mut current_output_rows: usize = 0;

        for (region_idx, region) in regions.iter().enumerate() {
            if region_idx > 0 {
                for state in decoders_state.iter_mut() {
                    state.reset_all_body_col_state();
                }
            }

            // Decide whether we need to split this region across
            // multiple outputs. Two conditions must both hold: the
            // region's rows would push the current output past the
            // target AND there is an unused output to advance to.
            // When splitting kicks in we have to pre-drain this
            // region's sort cols so we can compute the merge order
            // and find sorted_series transitions; if no split is
            // needed we let `process_region` drain internally to
            // preserve the existing per-region memory bound.
            let outputs_remaining = num_outputs - current_output_idx;
            let remaining_in_current = target_per_output.saturating_sub(current_output_rows);
            let needs_split = outputs_remaining > 1 && region.total_rows() > remaining_in_current;

            let prefetched: Option<Vec<RecordBatch>> = if needs_split {
                Some(drain_and_align_region(
                    &handle,
                    &mut decoders_state,
                    region,
                    &input_meta.sort_fields,
                )?)
            } else {
                None
            };

            let sub_regions: Vec<Region> = if let Some(ref prefetched_batches) = prefetched {
                let merge_order = compute_merge_order(prefetched_batches, &input_meta.sort_fields)?;
                split_region_at_sorted_series(
                    region,
                    &merge_order,
                    prefetched_batches,
                    remaining_in_current,
                    target_per_output,
                    outputs_remaining,
                )?
            } else {
                vec![region.clone()]
            };

            for sub_region in &sub_regions {
                let sub_rows = sub_region.total_rows();

                // Advance to the next output file BEFORE this sub-
                // region if the current output already met the
                // target and we still have outputs to fill. The
                // very first sub-region is exempt so we don't open
                // an empty writer.
                if current_writer.is_some()
                    && current_output_rows >= target_per_output
                    && current_output_idx + 1 < num_outputs
                {
                    if let (Some(w), Some(acc)) =
                        (current_writer.take(), current_accumulator.take())
                    {
                        outputs.push(finalize_output(w, acc, &input_meta)?);
                    }
                    current_output_idx += 1;
                    current_output_rows = 0;
                }

                if current_writer.is_none() {
                    let writer = open_output_writer_for_streaming(
                        current_output_idx,
                        &output_dir,
                        &union_schema,
                        &input_meta,
                        &writer_config,
                    )?;
                    current_writer = Some(writer);
                    current_accumulator = Some(OutputAccumulator::new(current_output_idx));
                }

                process_region(
                    &handle,
                    &mut decoders_state,
                    current_writer
                        .as_mut()
                        .expect("writer opened above for this sub-region"),
                    current_accumulator
                        .as_mut()
                        .expect("accumulator opened above for this sub-region"),
                    sub_region,
                    &union_schema,
                    &input_meta,
                    prefetched.as_deref(),
                )?;
                current_output_rows += sub_rows;
            }
        }

        // Close the last writer.
        if let (Some(w), Some(acc)) = (current_writer.take(), current_accumulator.take()) {
            outputs.push(finalize_output(w, acc, &input_meta)?);
        }

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
pub(crate) struct InputDecoderState {
    pub(crate) decoder: StreamDecoder<'static>,
    pub(crate) metadata: Arc<ParquetMetaData>,
    /// Arrow schema of this input (from parquet → arrow conversion).
    pub(crate) arrow_schema: SchemaRef,
    /// Per-input, per-parquet-col page cache. Pages emitted by the
    /// decoder are stored under their actual `col_idx`, not the col
    /// the caller was asking about — this lets `advance_decoder_to_row`
    /// pull pages of col B during a col A advance (Husky storage
    /// order: col A pages all stream before col B pages within an RG,
    /// so to get any col B pages we may have to consume leftover col
    /// A pages first). The synthesized-prefix path relies on this:
    /// adjacent regions sharing one RG re-read col A from this cache
    /// when region 2 starts, and the cache has already been filled in
    /// by region 1's reads.
    ///
    /// Pages are evicted (per col) when the col's cursor advances
    /// past their last row.
    pub(crate) body_col_page_caches: HashMap<usize, Vec<DecodedPage>>,
    /// Per-parquet-col cursor: the next unconsumed input row for that
    /// col. Advances monotonically as outputs are written; survives
    /// across regions of the same input so the body col assembler can
    /// resume mid-RG for synthesized regions.
    pub(crate) body_col_cursors: HashMap<usize, usize>,
}

impl InputDecoderState {
    /// Position the cursor for `col_idx` to `start_row` and drop any
    /// cached pages strictly below it. Used at the start of each
    /// (region, body col) write: for whole-RG regions this is a no-op
    /// (cursor was already 0 and cache empty); for synthesized
    /// regions that share an RG with earlier regions, this skips the
    /// rows the previous region consumed without clearing the rest of
    /// the cache.
    pub(crate) fn set_body_col_cursor(&mut self, col_idx: usize, start_row: usize) {
        self.body_col_cursors.insert(col_idx, start_row);
        if let Some(pages) = self.body_col_page_caches.get_mut(&col_idx) {
            while let Some(front) = pages.first() {
                let front_end = front.row_start + front.array.len();
                if front_end <= start_row {
                    pages.remove(0);
                } else {
                    break;
                }
            }
        }
    }

    /// Look up the cached pages for the given col, returning an empty
    /// slice if none have been decoded yet.
    pub(crate) fn body_col_cache(&self, col_idx: usize) -> &[DecodedPage] {
        match self.body_col_page_caches.get(&col_idx) {
            Some(v) => v.as_slice(),
            None => &[],
        }
    }

    /// Mutable handle to the cache for `col_idx`, creating an empty
    /// entry if none exists.
    pub(crate) fn body_col_cache_mut(&mut self, col_idx: usize) -> &mut Vec<DecodedPage> {
        self.body_col_page_caches.entry(col_idx).or_default()
    }

    /// Current cursor for `col_idx`; defaults to 0 if untouched.
    pub(crate) fn body_col_cursor(&self, col_idx: usize) -> usize {
        self.body_col_cursors.get(&col_idx).copied().unwrap_or(0)
    }

    /// Sum of cached pages across all cols. Used by the MS-7 peak-
    /// length probe in tests.
    pub(crate) fn body_col_caches_total_len(&self) -> usize {
        self.body_col_page_caches.values().map(|v| v.len()).sum()
    }

    /// Clear every per-col page cache and every per-col cursor.
    /// Called between top-level regions: each region typically uses a
    /// different input RG, and the per-col page `row_start` values
    /// reported by the decoder are RG-local, so pages cached for RG K
    /// would conflict with RG K+1's row-index space. Sub-regions of a
    /// single top-level region share an RG and MUST NOT trigger this
    /// reset — they rely on the cache surviving from one sub-region
    /// to the next so an earlier-col read whose stream-tail spans
    /// later sub-regions stays available.
    pub(crate) fn reset_all_body_col_state(&mut self) {
        self.body_col_page_caches.clear();
        self.body_col_cursors.clear();
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
            body_col_page_caches: HashMap::new(),
            body_col_cursors: HashMap::new(),
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

    Ok(InputMetadata {
        sort_fields,
        window_start_secs: consensus_window_start.unwrap_or(None),
        window_duration_secs: consensus_window_duration.unwrap_or(0),
        num_merge_ops: max_merge_ops + 1,
        rg_partition_prefix_len: consensus_prefix_len.unwrap_or(0),
    })
}

/// Drain a region's contributing inputs' sort cols and align them to
/// the union sort schema. The result has one entry per global input
/// index (zero-row placeholders for non-contributing inputs), which
/// is what `split_region_at_sorted_series` and `process_region` both
/// expect.
///
/// Used by the main loop when a region needs to be sub-divided
/// across multiple output files — splitting needs the merge order,
/// which needs the drained sort cols. For regions that fit in a
/// single output we skip this and let `process_region` drain
/// internally so the per-region memory cost stays bounded by what
/// the writer actively consumes.
fn drain_and_align_region(
    handle: &Handle,
    decoders_state: &mut [InputDecoderState],
    region: &Region,
    sort_fields_str: &str,
) -> Result<Vec<RecordBatch>> {
    let num_inputs = decoders_state.len();
    let mut sort_col_batches: Vec<Option<RecordBatch>> = (0..num_inputs).map(|_| None).collect();
    for c in &region.contributing {
        let batch = drain_sort_cols_one_input(
            handle,
            &mut decoders_state[c.input_idx],
            sort_fields_str,
            c.input_idx,
            c.rg_idx,
        )?;
        if batch.num_columns() > 0 && batch.schema().index_of(SORTED_SERIES_COLUMN).is_err() {
            bail!(
                "input {} rg {} is missing the '{}' column required for merge",
                c.input_idx,
                c.rg_idx,
                SORTED_SERIES_COLUMN,
            );
        }
        sort_col_batches[c.input_idx] = Some(batch);
    }

    let mut sort_batch_vec: Vec<RecordBatch> = Vec::with_capacity(num_inputs);
    for (idx, slot) in sort_col_batches.into_iter().enumerate() {
        let batch = match slot {
            Some(b) => b,
            None => empty_sort_col_record_batch(&decoders_state[idx], sort_fields_str)?,
        };
        sort_batch_vec.push(batch);
    }

    let (_sort_union_schema, aligned_sort_batches) =
        align_inputs_to_union_schema(&sort_batch_vec, sort_fields_str)?;
    Ok(aligned_sort_batches)
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
    // Single-region path: drain RG 0 of each input. Used by the
    // single-region streaming path (one region covering all inputs;
    // applies when all inputs are single-RG OR `rg_partition_prefix_len
    // == 0` with one synthetic adapter-presented RG per input).
    let mut batches = Vec::with_capacity(decoders_state.len());
    for (idx, state) in decoders_state.iter_mut().enumerate() {
        let batch = drain_sort_cols_one_input(handle, state, sort_fields_str, idx, 0)?;
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

/// Process one merge region: obtain the contributing inputs' sort col
/// batches (drain them fresh from the streaming decoder when
/// `prefetched_sort_batches` is `None`; otherwise slice the
/// pre-drained batches by each contribution's row range — used by the
/// synthesized prefix-region path), compute the region's merge order,
/// open a new output RG in the writer, write all cols (sort cols via
/// interleave, body cols via the page-bounded assembler), close the
/// RG, accumulate per-output static metadata.
///
/// When `prefetched_sort_batches` is provided, each contribution's
/// `start_row` determines where the body col assembler starts inside
/// the contributing input's RG. The body col stream is shared with
/// adjacent synthesized regions of the same input, so its cache /
/// cursor must advance monotonically — see `reset_body_col_state`.
#[allow(clippy::too_many_arguments)]
fn process_region(
    handle: &Handle,
    decoders_state: &mut [InputDecoderState],
    writer_state: &mut OutputWriterStorage,
    accumulator: &mut OutputAccumulator,
    region: &Region,
    union_schema: &SchemaRef,
    input_meta: &InputMetadata,
    prefetched_sort_batches: Option<&[RecordBatch]>,
) -> Result<()> {
    // 1. Obtain sort col batches for this region's contributing inputs. When prefetched batches are
    //    supplied (synthesized path), slice them by the contribution's row range so a single RG
    //    drained once can feed multiple adjacent regions. Otherwise drain a fresh whole-RG batch
    //    from the streaming decoder. The result is indexed BY GLOBAL INPUT INDEX, with zero-row
    //    placeholders for non-contributing inputs so the BodyColOutputPageAssembler sees a uniform
    //    input-count.
    let num_inputs = decoders_state.len();
    let mut sort_col_batches: Vec<Option<RecordBatch>> = (0..num_inputs).map(|_| None).collect();
    for c in &region.contributing {
        let input_idx = c.input_idx;
        let rg_idx = c.rg_idx;
        let batch = match prefetched_sort_batches {
            Some(prefetched) => prefetched[input_idx].slice(c.start_row, c.num_rows),
            None => drain_sort_cols_one_input(
                handle,
                &mut decoders_state[input_idx],
                &input_meta.sort_fields,
                input_idx,
                rg_idx,
            )?,
        };
        if batch.num_columns() > 0 && batch.schema().index_of(SORTED_SERIES_COLUMN).is_err() {
            bail!(
                "input {input_idx} rg {rg_idx} is missing the '{}' column required for merge",
                SORTED_SERIES_COLUMN,
            );
        }
        sort_col_batches[input_idx] = Some(batch);
    }

    // Materialise into a `Vec<RecordBatch>` per input. Non-contributing
    // inputs get zero-row placeholders with the input's sort col schema
    // so `compute_merge_order` and the body col assembler see uniform
    // shapes.
    let mut sort_batch_vec: Vec<RecordBatch> = Vec::with_capacity(num_inputs);
    for (idx, slot) in sort_col_batches.into_iter().enumerate() {
        let batch = match slot {
            Some(b) => b,
            None => empty_sort_col_record_batch(&decoders_state[idx], &input_meta.sort_fields)?,
        };
        sort_batch_vec.push(batch);
    }

    // Per-input cursor offsets to feed to the body col reset hook
    // below. For whole-RG regions this is 0 everywhere; for
    // synthesized regions it is `c.start_row` of the contributing
    // input so the body col assembler walks rows starting at the
    // region's first input row instead of restarting at 0.
    let mut input_start_rows: Vec<usize> = vec![0; num_inputs];
    for c in &region.contributing {
        input_start_rows[c.input_idx] = c.start_row;
    }

    // 2. Align to union sort schema for the merge-order comparator.
    let (sort_union_schema, aligned_sort_batches) =
        align_inputs_to_union_schema(&sort_batch_vec, &input_meta.sort_fields)?;

    // 3. Compute merge order for this region.
    let merge_order = compute_merge_order(&aligned_sort_batches, &input_meta.sort_fields)?;
    let region_rows: usize = merge_order.iter().map(|r| r.row_count).sum();
    if region_rows == 0 {
        return Ok(());
    }

    // 4. Apply the merge permutation to the sort col batches to get the region's sorted sort-col
    //    batch. This will be appended to the output accumulator; also used to compute take indices
    //    for the body col assembler.
    let region_sort_batch =
        apply_merge_permutation(&aligned_sort_batches, &sort_union_schema, &merge_order)
            .context("applying merge permutation for region sort cols")?;

    // MC-3: verify the region's output is sorted.
    verify_sort_order(&region_sort_batch, &input_meta.sort_fields);

    // 5. Build per-region destinations: maps (input_idx, input_row) → (output_idx=0,
    //    position_in_region). The body col assembler walks this to find which (input, row)
    //    contributes each output position.
    //
    //    The destinations array is indexed by row *within the sort
    //    batch* — which for whole-RG regions equals "row within the
    //    RG" and for synthesized regions equals "row within the
    //    region's slice". In both cases that index lines up with what
    //    the body col decoder's `row_start` reports for the current
    //    RG, modulo the per-input `start_row` offset added below.
    let mut destinations: Vec<Vec<Option<(usize, usize)>>> = aligned_sort_batches
        .iter()
        .enumerate()
        .map(|(idx, b)| {
            // For the synthesized path the body col assembler walks
            // absolute input rows; pad the destinations array so the
            // index space matches what the page decoder reports.
            vec![None; input_start_rows[idx] + b.num_rows()]
        })
        .collect();
    let mut pos = 0usize;
    for run in &merge_order {
        for r in 0..run.row_count {
            let absolute_row = input_start_rows[run.input_index] + run.start_row + r;
            destinations[run.input_index][absolute_row] = Some((0, pos));
            pos += 1;
        }
    }
    let region_destinations = InputRowDestinations {
        per_input: destinations,
        rows_per_output: vec![region_rows],
    };

    // 6. Open a new output RG and write all cols in union schema order.
    let mut row_group = writer_state.writer.start_row_group().with_context(|| {
        format!(
            "opening row group for output {} region",
            writer_state.output_idx,
        )
    })?;
    writer_state.num_row_groups += 1;

    for (col_idx, field) in union_schema.fields().iter().enumerate() {
        let col_name = field.name();
        if sort_union_schema.index_of(col_name).is_ok() {
            // Sort col: take from the already-built region_sort_batch.
            let arrays = build_sort_col_pages_from_sorted_batch(&region_sort_batch, col_name)?;
            row_group
                .write_next_column_arrays(arrays.into_iter())
                .with_context(|| {
                    format!(
                        "writing sort col '{col_name}' (col_idx {col_idx}) to output {}",
                        writer_state.output_idx,
                    )
                })?;
        } else {
            // Body col: stream via the page-bounded assembler. Resolve
            // each input's parquet col_idx for this union-schema col
            // first, then position the per-col cursor at the region's
            // `start_row` for that input. For whole-RG regions
            // `start_row == 0` (no-op for first region); for synthesized
            // regions sharing an RG with earlier regions the cursor
            // jumps past already-emitted rows so we don't re-emit them.
            // The decoder itself is never reset — its per-(rg, col)
            // `rows_decoded` counters and queued pages must survive so
            // subsequent decode calls return correct row offsets.
            let mut input_col_indices: Vec<Option<usize>> = Vec::with_capacity(num_inputs);
            for state in decoders_state.iter() {
                input_col_indices.push(state.arrow_schema.index_of(col_name).ok());
            }
            for (idx, state) in decoders_state.iter_mut().enumerate() {
                if let Some(col_parquet_idx) = input_col_indices[idx] {
                    state.set_body_col_cursor(col_parquet_idx, input_start_rows[idx]);
                }
            }

            let track_service = col_name == "service";

            let assembler = BodyColOutputPageAssembler::new(
                handle,
                decoders_state,
                &input_col_indices,
                &region_destinations,
                0, // out_idx is always 0 within a single-region call
                col_name,
                field.as_ref(),
            );

            // Feed pages one at a time into `write_next_column_arrays`
            // via the streaming iterator: it surfaces assembly errors
            // through `error_slot` so memory stays bounded by output-
            // page size instead of column-chunk size.
            let mut error_slot: Option<anyhow::Error> = None;
            let service_collector: Option<&mut HashSet<String>> = if track_service {
                Some(&mut accumulator.service_names)
            } else {
                None
            };
            let stream_iter = StreamingBodyColIter {
                inner: assembler.into_iter(),
                error_slot: &mut error_slot,
                service_collector,
            };
            let write_result = row_group.write_next_column_arrays(stream_iter);

            if let Some(err) = error_slot {
                return Err(err).with_context(|| {
                    format!(
                        "assembling body col '{col_name}' for output {} region",
                        writer_state.output_idx,
                    )
                });
            }
            write_result.with_context(|| {
                format!(
                    "writing body col '{col_name}' to output {} region",
                    writer_state.output_idx,
                )
            })?;
        }
    }

    row_group.finish().with_context(|| {
        format!(
            "finishing region row group for output {}",
            writer_state.output_idx
        )
    })?;

    // 7. Accumulate this region's contribution to the output.
    accumulator.append_sort_batch(region_sort_batch)?;
    accumulator.num_rows += region_rows;

    Ok(())
}

/// Helper for sort col writes within a region: split the region's
/// already-sorted sort col into page-sized chunks for
/// `write_next_column_arrays`.
fn build_sort_col_pages_from_sorted_batch(
    sorted_batch: &RecordBatch,
    col_name: &str,
) -> Result<Vec<ArrayRef>> {
    let col_idx = sorted_batch
        .schema()
        .index_of(col_name)
        .with_context(|| format!("missing sort col '{col_name}' in region sorted batch"))?;
    let col = sorted_batch.column(col_idx);
    let total_rows = col.len();
    let mut pages = Vec::with_capacity(total_rows.div_ceil(OUTPUT_PAGE_ROWS));
    let mut start = 0;
    while start < total_rows {
        let len = (total_rows - start).min(OUTPUT_PAGE_ROWS);
        pages.push(col.slice(start, len));
        start += len;
    }
    Ok(pages)
}

fn drain_sort_cols_one_input(
    handle: &Handle,
    state: &mut InputDecoderState,
    sort_fields_str: &str,
    input_idx: usize,
    expected_rg_idx: usize,
) -> Result<RecordBatch> {
    if state.metadata.num_row_groups() == 0 || expected_rg_idx >= state.metadata.num_row_groups() {
        // No rows to drain at this RG. Return a zero-row batch with the
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
    let rg_meta = state.metadata.row_group(expected_rg_idx);
    let mut target_rows_per_col: HashMap<usize, usize> = HashMap::new();
    for &col_idx in sort_col_parquet_indices.keys() {
        target_rows_per_col.insert(col_idx, rg_meta.column(col_idx).num_values() as usize);
    }

    // Drain pages into per-col buffers until all sort cols are fully
    // decoded for this RG. With Husky storage ordering, sort col pages
    // come before any body col page within the row group, so we should
    // never see a non-sort page while sort cols are incomplete.
    let mut per_col_pages: HashMap<usize, Vec<ArrayRef>> = HashMap::new();
    let mut rows_done_per_col: HashMap<usize, usize> =
        sort_col_parquet_indices.keys().map(|&i| (i, 0)).collect();
    let mut sort_cols_finished = 0usize;
    let sort_col_target = sort_col_parquet_indices.len();

    while sort_cols_finished < sort_col_target {
        let decoded = handle
            .block_on(state.decoder.decode_next_page())
            .with_context(|| {
                format!("decoding sort col page (input {input_idx}, rg {expected_rg_idx})")
            })?;
        let page = match decoded {
            Some(p) => p,
            None => bail!(
                "stream ended before sort cols fully drained for input {input_idx} rg \
                 {expected_rg_idx}: {sort_cols_finished}/{sort_col_target} cols complete",
            ),
        };

        if !sort_col_parquet_indices.contains_key(&page.col_idx) {
            bail!(
                "input {input_idx} returned a non-sort page (col {}) before all sort cols were \
                 drained for rg {expected_rg_idx} — this violates Husky storage ordering",
                page.col_idx,
            );
        }
        if page.rg_idx != expected_rg_idx {
            bail!(
                "input {input_idx} returned a page from rg {} while draining sort cols of rg \
                 {expected_rg_idx}",
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
pub(crate) struct InputRowDestinations {
    /// One Vec per input. Length = input's sort-col row count.
    pub(crate) per_input: Vec<Vec<Option<(usize, usize)>>>,
    /// Total rows per output index (cumulative writer "expected" rows).
    pub(crate) rows_per_output: Vec<usize>,
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
        for run in runs {
            for r in 0..run.row_count {
                let input_row = run.start_row + r;
                per_input[run.input_index][input_row] = Some((out_idx, rows_per_output[out_idx]));
                rows_per_output[out_idx] += 1;
            }
        }
    }

    InputRowDestinations {
        per_input,
        rows_per_output,
    }
}

// ============================================================================
// Obsolete PR-6b.2 multi-output-parallel helpers (deleted in PR-6c.2's
// per-region restructure). The functions below are no longer used —
// per-region processing in `process_region` is the new path.
// ============================================================================

/// Build the full union schema across all inputs' arrow schemas
/// (NOT just sort cols). Reuses the same algorithm as
/// [`align_inputs_to_union_schema`] but takes pre-extracted arrow
/// schemas — phase 3 doesn't have full input batches.
fn build_full_union_schema_from_arrow_schemas(
    arrow_schemas: &[SchemaRef],
    sort_fields_str: &str,
) -> Result<SchemaRef> {
    // Build zero-row batches with the right schemas; that lets us
    // reuse `align_inputs_to_union_schema`'s field-merge / storage-
    // ordering logic unchanged.
    let empty_batches: Vec<RecordBatch> = arrow_schemas
        .iter()
        .map(|s| RecordBatch::new_empty(Arc::clone(s)))
        .collect();
    let (schema, _) = align_inputs_to_union_schema(&empty_batches, sort_fields_str)?;
    Ok(schema)
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
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::TempDir;
    use tokio::io::AsyncRead;

    use super::region_grouping::{
        assert_unique_rg_prefix_keys, encode_byte_array_prefix, extract_rg_composite_prefix_key,
        find_prefix_parquet_col_indices, invert_for_descending,
    };
    use super::*;
    use crate::split::TAG_SERVICE;
    use crate::storage::page_decoder::StreamDecoder;
    use crate::storage::streaming_reader::{RemoteByteSource, StreamingParquetReader};
    use crate::storage::{
        Compression, PARQUET_META_ROW_KEYS, PARQUET_META_ZONEMAP_REGEXES, ParquetWriterConfig,
    };

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

    /// Multi-RG metric-aligned input (`prefix_len >= 1`) is accepted
    /// and produces multi-RG output: one output RG per input metric_name
    /// region.
    #[tokio::test]
    async fn test_multi_rg_metric_aligned_input_produces_multi_rg_output() {
        // Build a fixture with 2 metric_names → 2 RGs each holding one
        // metric_name. Use `prefix_len = 1` to declare metric_name
        // alignment.
        let bytes = make_two_metric_aligned_input();
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge multi-RG metric-aligned input");
        assert_eq!(outputs.len(), 1, "expected one output file");
        assert_eq!(outputs[0].num_rows, 60, "30 + 30 rows");

        let out_bytes = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(out_bytes)).expect("ser");
        assert_eq!(
            reader.metadata().num_row_groups(),
            2,
            "multi-RG metric-aligned input must produce multi-RG output (one RG per metric_name \
             region)",
        );

        // `MergeOutputFile.num_row_groups` must agree with the file
        // on disk. Before the fix it was hard-coded to 1, so this
        // assertion caught the regression on a multi-region output.
        assert_eq!(
            outputs[0].num_row_groups, 2,
            "MergeOutputFile.num_row_groups should match physical row group count",
        );

        // F2 chunk-level verification: confirm each output RG actually
        // carries a single distinct metric_name (PA-1 + PA-3 read
        // straight off the column-chunk statistics).
        assert_unique_rg_prefix_keys(
            reader.metadata(),
            "metric_name|-timestamp_secs/V2",
            1,
            "test_multi_rg_metric_aligned_input_produces_multi_rg_output output",
        )
        .expect("streaming engine output must satisfy PA-1 + PA-3 on metric_name");
    }

    /// Regression for Codex P2 on PR-6410: a streaming merge output
    /// whose multi-region content lands in a single file must report
    /// `MergeOutputFile.num_row_groups` consistent with the parquet
    /// footer. Two regions assigned to one output should yield
    /// `num_row_groups = 2`.
    #[tokio::test]
    async fn test_streaming_output_num_row_groups_matches_footer() {
        let bytes = make_two_metric_aligned_input();
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");

        let bytes_out = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
        assert_eq!(
            outputs[0].num_row_groups,
            reader.metadata().num_row_groups(),
            "MergeOutputFile.num_row_groups must match the physical RG count",
        );
    }

    /// Regression for Codex P2 on PR-6410: `qh.row_keys` and
    /// `qh.zonemap_regexes` must be written into the on-disk Parquet
    /// footer for every streaming output. Per-output values come from
    /// the rows that landed in that output specifically — merging
    /// eliminates key overlap between outputs, so this metadata can't
    /// be carried over from inputs.
    #[tokio::test]
    async fn test_streaming_output_kv_footer_contains_row_keys_and_zonemap() {
        let bytes = make_two_metric_aligned_input();
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");

        let bytes_out = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
        let kvs: Vec<(String, Option<String>)> = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .map(|v| {
                v.iter()
                    .map(|kv| (kv.key.clone(), kv.value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let find = |k: &str| kvs.iter().find(|(key, _)| key == k).map(|(_, v)| v.clone());

        // row_keys: streaming merge derives them from THIS output's
        // sort cols, and the in-memory `MergeOutputFile` records the
        // proto bytes. The base64-encoded KV in the footer must
        // round-trip to the same bytes.
        assert!(
            outputs[0].row_keys_proto.is_some(),
            "expected per-output row_keys_proto in MergeOutputFile",
        );
        let row_keys_kv = find(PARQUET_META_ROW_KEYS).flatten().expect(
            "qh.row_keys missing from streaming output footer — appended-after-write KV metadata \
             did not survive close",
        );
        let decoded = BASE64.decode(row_keys_kv).expect("base64 decode");
        assert_eq!(
            &decoded,
            outputs[0].row_keys_proto.as_ref().unwrap(),
            "footer row_keys bytes must equal MergeOutputFile.row_keys_proto",
        );

        // zonemap_regexes: footer carries a JSON-encoded map.
        // metric_name alignment + multiple metric_names → non-empty.
        assert!(
            !outputs[0].zonemap_regexes.is_empty(),
            "expected non-empty zonemap_regexes for multi-metric output",
        );
        let zonemap_kv = find(PARQUET_META_ZONEMAP_REGEXES)
            .flatten()
            .expect("qh.zonemap_regexes missing from streaming output footer");
        let parsed: HashMap<String, String> =
            serde_json::from_str(&zonemap_kv).expect("zonemap JSON parse");
        assert_eq!(
            parsed, outputs[0].zonemap_regexes,
            "footer zonemap must equal MergeOutputFile.zonemap_regexes",
        );
    }

    /// Composite-key extraction with two byte-array prefix columns:
    /// `(metric_name, service)` ASC/ASC. Two RGs with the same
    /// metric_name but different services must produce distinct
    /// composite keys, with byte-lex order matching the natural
    /// `(metric_name, service)` sort order.
    #[test]
    fn test_extract_rg_composite_prefix_key_two_byte_array_cols() {
        let bytes =
            make_prefix_len_two_input(&[("cpu.usage", "host-a"), ("cpu.usage", "host-b")], 10);
        let reader = SerializedFileReader::new(bytes).expect("ser");
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 2);

        let prefix_cols = find_prefix_parquet_col_indices(
            metadata,
            "metric_name|service|-timestamp_secs/V2",
            2,
            0,
        )
        .expect("resolve");
        let key_rg0 = extract_rg_composite_prefix_key(metadata, 0, &prefix_cols, 0).expect("rg0");
        let key_rg1 = extract_rg_composite_prefix_key(metadata, 1, &prefix_cols, 0).expect("rg1");
        assert_ne!(
            key_rg0, key_rg1,
            "different services → different composite keys"
        );
        // metric_name is equal across RGs; ordering must come from
        // service ('host-a' < 'host-b' lex), so key_rg0 < key_rg1.
        assert!(
            key_rg0 < key_rg1,
            "composite key for ('cpu.usage', 'host-a') must lex-sort before ('cpu.usage', \
             'host-b')",
        );

        // Encoded representation: each byte-array column is the raw
        // bytes (no null bytes in these test values, so escape-pass
        // is a no-op) followed by a `0x00 0x00` terminator,
        // concatenated. "cpu.usage" → 9 bytes + 2-byte terminator;
        // "host-a" → 6 bytes + 2-byte terminator. Total = 19 bytes.
        assert_eq!(&key_rg0[0..9], b"cpu.usage");
        assert_eq!(&key_rg0[9..11], &[0x00, 0x00]);
        assert_eq!(&key_rg0[11..17], b"host-a");
        assert_eq!(&key_rg0[17..19], &[0x00, 0x00]);
        assert_eq!(key_rg0.len(), 19);
    }

    /// Regression for Codex P1 on PR-6410 (positive coverage of the
    /// fix): `rg_partition_prefix_len = 2` groups RGs by the
    /// composite (metric_name, service) value, producing one output
    /// row group per (metric_name, service) pair. Two RGs that share
    /// metric_name but differ in service must NOT be folded into one
    /// region.
    #[tokio::test]
    async fn test_streaming_merge_with_prefix_len_two() {
        let bytes = make_prefix_len_two_input(
            &[
                ("cpu.usage", "host-a"),
                ("cpu.usage", "host-b"),
                ("memory.used", "host-a"),
            ],
            20,
        );
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge with prefix_len = 2");
        assert_eq!(outputs.len(), 1, "expected one output file");
        assert_eq!(outputs[0].num_rows, 60, "20 × 3 input RGs = 60 rows");

        let bytes_out = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
        // Three distinct (metric_name, service) tuples → three output
        // row groups. With prefix_len truncated to 1 (the pre-fix
        // bug) the two cpu.usage RGs would have folded into one
        // region and only two output RGs would be written.
        assert_eq!(
            reader.metadata().num_row_groups(),
            3,
            "three distinct (metric_name, service) pairs must produce three output RGs",
        );
        assert_eq!(outputs[0].num_row_groups, 3);

        // F2 chunk-level verification: counting RGs and stamping a KV
        // is not enough — the OUTPUT's row groups must actually be
        // aligned on the composite (metric_name, service) prefix.
        // `assert_unique_rg_prefix_keys` enforces PA-1 (intra-RG
        // constancy) + PA-3 (inter-RG uniqueness) by reading the
        // chunk-level statistics.
        assert_unique_rg_prefix_keys(
            reader.metadata(),
            "metric_name|service|-timestamp_secs/V2",
            2,
            "test_streaming_merge_with_prefix_len_two output",
        )
        .expect("streaming engine output must satisfy PA-1 + PA-3 on the prefix columns");
    }

    /// Regression for Codex finding #1 on PR-6410: when one input
    /// file has two RGs sharing the same composite prefix key, the
    /// streaming engine must reject up-front. Without the check,
    /// `process_region` keys `sort_col_batches` by input_idx, so the
    /// second RG silently overwrites the first while
    /// `Region::total_rows` keeps counting both — rows would be
    /// dropped and the body-col / sort-col mapping would be off by a
    /// full RG.
    ///
    /// The fixture passes `("cpu.usage", "host-a")` twice, producing
    /// an input with two RGs that have identical (metric_name,
    /// service) statistics. The merge must bail with a clear error
    /// pointing at the duplicate.
    #[tokio::test]
    async fn test_streaming_merge_rejects_duplicate_prefix_rgs_in_one_input() {
        let bytes = make_prefix_len_two_input(
            &[
                ("cpu.usage", "host-a"),
                ("cpu.usage", "host-a"), // duplicate prefix key
                ("memory.used", "host-a"),
            ],
            20,
        );
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let err = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect_err("must reject input with duplicate prefix RGs");
        let s = err.to_string();
        assert!(
            s.contains("sharing a prefix key with an earlier RG"),
            "expected duplicate-prefix error, got: {s}",
        );
        assert!(
            s.contains("input 0"),
            "error should identify which input is bad, got: {s}",
        );
    }

    /// Build a single-RG fixture with multiple metric_names sorted
    /// by metric_name then timestamp. The file has
    /// `rg_partition_prefix_len = 0` (legacy) so the streaming merge
    /// synthesizes prefix-aligned regions during the merge.
    ///
    /// `metrics` = list of (metric_name, num_rows). Rows are emitted
    /// in order so the resulting batch is already sorted by
    /// metric_name; each row gets a unique sorted_series identifier
    /// derived from its position so the k-way merge has a well-
    /// defined order even when other tag dimensions are degenerate.
    fn make_multi_metric_single_rg_input(metrics: &[(&str, usize)]) -> Bytes {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("service", dict_type, true),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let total_rows: usize = metrics.iter().map(|(_, n)| *n).sum();
        let mut metric_keys: Vec<i32> = Vec::with_capacity(total_rows);
        let mut metric_values_vec: Vec<&str> = Vec::with_capacity(metrics.len());
        let mut timestamps: Vec<u64> = Vec::with_capacity(total_rows);
        let mut series_bytes: Vec<Vec<u8>> = Vec::with_capacity(total_rows);
        let mut tsids: Vec<i64> = Vec::with_capacity(total_rows);
        let mut values: Vec<f64> = Vec::with_capacity(total_rows);
        let mut row_idx: u64 = 0;
        for (metric_idx, (name, num)) in metrics.iter().enumerate() {
            metric_values_vec.push(name);
            // -timestamp_secs/V2 in the sort schema means timestamps
            // DESC within a metric.
            for r in 0..*num {
                metric_keys.push(metric_idx as i32);
                timestamps.push(1_700_000_000 + ((*num - r) as u64));
                series_bytes.push(row_idx.to_be_bytes().to_vec());
                tsids.push(1000 + row_idx as i64);
                values.push(row_idx as f64);
                row_idx += 1;
            }
        }
        let metric_names_arr = StringArray::from(metric_values_vec);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_names_arr),
            )
            .expect("dict array"),
        );
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let series_refs: Vec<&[u8]> = series_bytes.iter().map(|v| v.as_slice()).collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(series_refs));
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; total_rows]));
        let service: ArrayRef = {
            let svc_keys: Vec<Option<i32>> = (0..total_rows as i32).map(|i| Some(i % 3)).collect();
            let svc_values = StringArray::from(vec!["api", "db", "cache"]);
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    arrow::array::Int32Array::from(svc_keys),
                    Arc::new(svc_values),
                )
                .expect("svc dict"),
            )
        };
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        let value: ArrayRef = Arc::new(Float64Array::from(values));

        let batch = RecordBatch::try_new(
            schema.clone(),
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
        .expect("batch");

        // Write as a single-RG legacy file (prefix_len absent → 0).
        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            ..ParquetWriterConfig::default()
        };
        let sort_fields = "metric_name|-timestamp_secs/V2";
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
        let sort_field_names = vec!["metric_name".to_string(), "timestamp_secs".to_string()];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            Vec::new(),
            Some(kvs),
            &sort_field_names,
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("arrow writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        Bytes::from(buf)
    }

    /// Regression for Codex P2 on PR-6410: prefix_len=0 inputs +
    /// `num_outputs > 1` previously folded into a single oversized
    /// output because the region-to-output assigner only split at
    /// region boundaries and prefix_len=0 produces exactly one region.
    /// The engine now subdivides the single region at sorted_series
    /// transitions so it can honor `num_outputs`. The output inherits
    /// the input's `rg_partition_prefix_len` (= 0 here) — the engine
    /// does not declare a prefix it can't unconditionally guarantee.
    #[tokio::test]
    async fn test_prefix_len_zero_multi_output_splits_at_sorted_series() {
        // 6 distinct metric_names × 50 rows = 300 rows total.
        // num_outputs = 3 → target 100 rows/output. Splits land at
        // sorted_series transitions (each row has a unique
        // sorted_series in this fixture).
        let metrics = [
            ("aaa.alpha", 50usize),
            ("bbb.beta", 50),
            ("ccc.gamma", 50),
            ("ddd.delta", 50),
            ("eee.epsilon", 50),
            ("fff.zeta", 50),
        ];
        let bytes = make_multi_metric_single_rg_input(&metrics);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(3))
            .await
            .expect("merge");

        assert_eq!(
            outputs.len(),
            3,
            "prefix_len=0 input + num_outputs=3 must produce 3 output files; got {} ({} rows \
             total)",
            outputs.len(),
            outputs.iter().map(|o| o.num_rows).sum::<usize>(),
        );
        let total: usize = outputs.iter().map(|o| o.num_rows).sum();
        assert_eq!(total, 300, "rows preserved (MC-1)");

        for out in &outputs {
            let bytes_out = std::fs::read(&out.path).expect("read");
            let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
            // Inherits input's prefix_len = 0 → KV absent.
            let prefix_kv = reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .and_then(|kvs| {
                    kvs.iter()
                        .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
                        .and_then(|k| k.value.clone())
                });
            assert!(
                prefix_kv.is_none(),
                "prefix_len=0 input must produce prefix_len=0 output (KV absent); got \
                 {prefix_kv:?}",
            );
            // Each sub-region produces one output RG.
            assert_eq!(
                reader.metadata().num_row_groups(),
                1,
                "expected one output RG per sub-region; got {}",
                reader.metadata().num_row_groups(),
            );
        }
    }

    /// Giant single-metric input: prefix_len=0, only one metric_name,
    /// so there are NO metric_name transitions in the merge order.
    /// Splitting must still honor `num_outputs` by breaking at
    /// sorted_series transitions inside the single metric. Confirms
    /// the engine does not require prefix-value transitions to
    /// honor the requested output count.
    #[tokio::test]
    async fn test_prefix_len_zero_giant_single_metric_splits_into_multiple_outputs() {
        let metrics = [("cpu.usage", 200usize)];
        let bytes = make_multi_metric_single_rg_input(&metrics);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(2))
            .await
            .expect("merge");

        assert_eq!(
            outputs.len(),
            2,
            "one metric × num_outputs=2 must still split (at sorted_series boundaries); got {}",
            outputs.len(),
        );
        let total: usize = outputs.iter().map(|o| o.num_rows).sum();
        assert_eq!(total, 200, "rows preserved");
        // Each output is balanced near 100 rows.
        for out in &outputs {
            assert!(
                out.num_rows > 0 && out.num_rows <= 200,
                "output rows = {}",
                out.num_rows
            );
        }
    }

    /// Prefix_len=0 + num_outputs=1: the whole region fits in one
    /// output, no splitting needed. `process_region` drains
    /// internally (no pre-drain) and produces a single output RG.
    #[tokio::test]
    async fn test_prefix_len_zero_single_output_is_single_rg() {
        let metrics = [
            ("cpu.usage", 40usize),
            ("memory.used", 40),
            ("net.bytes", 40),
        ];
        let bytes = make_multi_metric_single_rg_input(&metrics);
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge");
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows, 120);

        let bytes_out = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
        assert_eq!(
            reader.metadata().num_row_groups(),
            1,
            "single output, no split → single RG (engine does not synthesize prefix alignment \
             when inputs declare prefix_len=0)",
        );
        let prefix_kv = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
                    .and_then(|k| k.value.clone())
            });
        assert!(
            prefix_kv.is_none(),
            "output must inherit input's prefix_len = 0 (KV absent); got {prefix_kv:?}",
        );
    }

    /// Build a multi-RG fixture where each RG has a single
    /// (metric_name, service) tuple. `rg_partition_prefix_len = 2`
    /// declares the alignment.
    fn make_prefix_len_two_input(rgs: &[(&str, &str)], rows_per_rg: usize) -> Bytes {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service", DataType::Utf8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let make_batch = |metric: &str, service: &str, start_series: u64| -> RecordBatch {
            let metric_name: ArrayRef = Arc::new(StringArray::from(vec![metric; rows_per_rg]));
            let svc: ArrayRef = Arc::new(StringArray::from(vec![service; rows_per_rg]));
            let timestamps: Vec<u64> = (0..rows_per_rg as u64).map(|i| 1_700_000_000 + i).collect();
            let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
            let series: Vec<Vec<u8>> = (0..rows_per_rg as u64)
                .map(|i| (start_series + i).to_be_bytes().to_vec())
                .collect();
            let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
                series.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
            ));
            let value: ArrayRef = Arc::new(Float64Array::from(
                (0..rows_per_rg).map(|i| i as f64).collect::<Vec<_>>(),
            ));
            RecordBatch::try_new(
                schema.clone(),
                vec![metric_name, svc, timestamp_secs, sorted_series, value],
            )
            .expect("batch")
        };

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            row_group_size: rows_per_rg, // one RG per batch
            ..ParquetWriterConfig::default()
        };
        let kvs = vec![
            KeyValue::new(
                PARQUET_META_SORT_FIELDS.to_string(),
                "metric_name|service|-timestamp_secs/V2".to_string(),
            ),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "2".to_string(),
            ),
        ];
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            sorting_cols,
            Some(kvs),
            &["metric_name".to_string(), "service".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).expect("arrow writer");
        for (i, (metric, service)) in rgs.iter().enumerate() {
            let batch = make_batch(metric, service, (i as u64) * rows_per_rg as u64);
            writer.write(&batch).expect("write");
        }
        writer.close().expect("close");
        Bytes::from(buf)
    }

    /// Unit test on `invert_for_descending`: byte-complement reverses
    /// the lex order of two byte strings, so a DESC-encoded value's
    /// lex position equals its reversed sort position. Covers the
    /// equal-length case and the variable-length case (where the
    /// escape encoding's `0x00 0x00` terminator does the work).
    #[test]
    fn test_invert_for_descending_reverses_lex_order() {
        // Same length: per-byte order is reversed.
        let a = encode_byte_array_prefix(b"host-a");
        let b = encode_byte_array_prefix(b"host-b");
        assert!(a < b, "ASC encoding: 'host-a' < 'host-b'");
        let a_desc = invert_for_descending(&a);
        let b_desc = invert_for_descending(&b);
        assert!(
            b_desc < a_desc,
            "DESC encoding must reverse order: 'host-b' DESC < 'host-a' DESC",
        );

        // Variable length: "ab" vs "abc". Under escape encoding the
        // terminator `0x00 0x00` after "ab" makes its third byte 0x00,
        // which sorts before 'c' (0x63), so "ab" < "abc"; DESC must
        // reverse to put the longer first.
        let ab = encode_byte_array_prefix(b"ab");
        let abc = encode_byte_array_prefix(b"abc");
        assert!(ab < abc, "ASC: 'ab' < 'abc'");
        let ab_desc = invert_for_descending(&ab);
        let abc_desc = invert_for_descending(&abc);
        assert!(
            abc_desc < ab_desc,
            "DESC: 'abc' DESC < 'ab' DESC (so iteration yields 'abc' before 'ab')",
        );
    }

    /// Regression for Codex finding #2 on PR-6410: the previous
    /// 4-byte length-prefix encoding made the encoded `"b"` (length=1)
    /// sort BEFORE the encoded `"aa"` (length=2), violating the
    /// declared ASC sort order on the byte-array column. Escape
    /// encoding fixes this by emitting raw bytes with a 0x00 0x00
    /// terminator — so `"aa"` ([0x61, 0x61, 0x00, 0x00]) correctly
    /// sorts before `"b"` ([0x62, 0x00, 0x00]).
    #[test]
    fn test_byte_array_prefix_preserves_lex_order_across_lengths() {
        let aa = encode_byte_array_prefix(b"aa");
        let b = encode_byte_array_prefix(b"b");
        assert!(
            aa < b,
            "ASC: 'aa' must encode lex-before 'b' (was broken under length-prefix encoding). got \
             aa={aa:?} b={b:?}",
        );

        // Empty value sorts before any non-empty value sharing no prefix.
        let empty = encode_byte_array_prefix(b"");
        assert!(empty < aa, "ASC: '' must encode lex-before 'aa'");

        // Same prefix, shorter sorts before longer (key property for
        // composite keys with multiple variable-length columns).
        let a = encode_byte_array_prefix(b"a");
        let ab = encode_byte_array_prefix(b"ab");
        assert!(a < ab, "ASC: 'a' must encode lex-before 'ab'");

        // Null bytes inside the value are escaped, preserving order
        // around the terminator.
        let null_inside = encode_byte_array_prefix(b"\x00a");
        let a_alone = encode_byte_array_prefix(b"a");
        assert!(
            null_inside < a_alone,
            "ASC: '\\0a' (encoded as 0x00 0x01 0x61 0x00 0x00) must sort before 'a' (encoded as \
             0x61 0x00 0x00)",
        );
    }

    /// End-to-end regression for DESC prefix columns. Three RGs with
    /// the same metric_name (ASC) and distinct `env` values; sort
    /// schema declares env DESC. The input file must itself be
    /// DESC-sorted on env (RGs in physical order staging → prod →
    /// dev) because the streaming engine processes each input's RGs
    /// in physical order; the BTreeMap-driven region order is the
    /// thing the composite key + `invert_for_descending` controls,
    /// and it must agree with the input's physical RG order for the
    /// engine to drain sort cols in lockstep with the body cols.
    ///
    /// Regions must therefore come out in DESC order on env:
    /// staging → prod → dev. Without the `invert_for_descending`
    /// step the BTreeMap would emit dev → prod → staging, which
    /// would disagree with the physical RG order and the engine
    /// would crash with "page from rg 0 while draining sort cols of
    /// rg 2".
    #[tokio::test]
    async fn test_streaming_merge_with_desc_prefix_col() {
        let bytes = make_prefix_len_two_input_with_directions(
            // (metric, env, marker_value). Each RG's body `value`
            // column is filled with `marker_value` so we can identify
            // which RG produced each output row group. Order is
            // env-DESC physical: staging, prod, dev.
            &[
                ("cpu.usage", "staging", 3.0),
                ("cpu.usage", "prod", 2.0),
                ("cpu.usage", "dev", 1.0),
            ],
            20,
            /* env_descending */ true,
        );
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let outputs = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect("merge with env DESC");
        assert_eq!(outputs.len(), 1);

        // Read the output and inspect the per-RG metric values: each
        // RG should be filled with a single marker, and the RG order
        // must be staging (3.0) → prod (2.0) → dev (1.0).
        let bytes_out = std::fs::read(&outputs[0].path).expect("read");
        let reader = SerializedFileReader::new(Bytes::from(bytes_out)).expect("ser");
        let meta = reader.metadata();
        assert_eq!(
            meta.num_row_groups(),
            3,
            "three distinct env values → three RGs"
        );

        let merged = read_output_to_record_batch(&outputs[0].path);
        let value = merged
            .column(merged.schema().index_of("value").expect("value"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        // RG 0 spans rows 0..20, RG 1 spans 20..40, RG 2 spans 40..60.
        // Within each RG the marker_value is constant.
        let first_block = value.value(0);
        let second_block = value.value(20);
        let third_block = value.value(40);
        assert!(
            (first_block - 3.0).abs() < 1e-9,
            "first output RG should be 'staging' (marker 3.0), got {first_block}",
        );
        assert!(
            (second_block - 2.0).abs() < 1e-9,
            "second output RG should be 'prod' (marker 2.0), got {second_block}",
        );
        assert!(
            (third_block - 1.0).abs() < 1e-9,
            "third output RG should be 'dev' (marker 1.0), got {third_block}",
        );

        // F2 chunk-level verification: each output RG must be aligned
        // on (metric_name, -env). PA-1 + PA-3 read from chunk stats.
        assert_unique_rg_prefix_keys(
            reader.metadata(),
            "metric_name|-env|-timestamp_secs/V2",
            2,
            "test_streaming_merge_with_desc_prefix_col output",
        )
        .expect("DESC prefix output must satisfy PA-1 + PA-3");
    }

    /// Regression for the composite-key encoding when ASC and DESC
    /// columns are interleaved. metric_name ASC + env DESC: composite
    /// keys for ("cpu.usage", "dev") and ("cpu.usage", "prod") must
    /// put 'prod' before 'dev' (because prod > dev in ASC lex, so
    /// prod's DESC encoding sorts smaller).
    #[test]
    fn test_extract_rg_composite_prefix_key_mixed_directions() {
        let bytes = make_prefix_len_two_input_with_directions(
            &[("cpu.usage", "dev", 0.0), ("cpu.usage", "prod", 0.0)],
            5,
            /* env_descending */ true,
        );
        let reader = SerializedFileReader::new(Bytes::from(bytes.to_vec())).expect("ser");
        let metadata = reader.metadata();
        let prefix_cols =
            find_prefix_parquet_col_indices(metadata, "metric_name|-env|-timestamp_secs/V2", 2, 0)
                .expect("resolve");
        // Sanity: the second prefix column must be flagged DESC.
        assert!(
            prefix_cols[1]
                .as_ref()
                .expect("env present in this fixture")
                .descending,
            "env must be parsed as DESC from sort schema",
        );

        let key_dev = extract_rg_composite_prefix_key(metadata, 0, &prefix_cols, 0).expect("dev");
        let key_prod = extract_rg_composite_prefix_key(metadata, 1, &prefix_cols, 0).expect("prod");
        // metric_name is the same; env differs. DESC on env means
        // 'prod' (larger lex) should encode to LESS-THAN 'dev', so
        // the BTreeMap iterates prod first.
        assert!(
            key_prod < key_dev,
            "with env DESC, composite key for 'prod' must lex-sort before 'dev'",
        );
    }

    /// MS-2: a file whose physical RG order disagrees with the
    /// composite-key encoding's derived order must be rejected
    /// upfront, not crash mid-merge. Construct an input that declares
    /// env DESC but physically writes RGs in ASC env order — the
    /// BTreeMap region iteration will visit RG 2 (env DESC = "dev",
    /// largest in DESC encoding ... wait, no — DESC means largest
    /// first, so env "staging" should be first ...).
    ///
    /// Concretely: RGs written physically as `[dev, prod, staging]`
    /// with sort direction declared DESC. DESC iteration order is
    /// `[staging (RG 2), prod (RG 1), dev (RG 0)]`. The first region
    /// the engine would try to drain is RG 2, but the input stream
    /// reaches RG 0 first. MS-2 must reject this at
    /// `extract_regions_from_metadata` time.
    #[tokio::test]
    async fn test_ms2_region_order_disagrees_with_physical_rg_order_rejected() {
        let bytes = make_prefix_len_two_input_with_directions(
            // Physical order ASC on env: dev, prod, staging. But the
            // sort schema below declares env DESC.
            &[
                ("cpu.usage", "dev", 1.0),
                ("cpu.usage", "prod", 2.0),
                ("cpu.usage", "staging", 3.0),
            ],
            10,
            /* env_descending */ true,
        );
        let inputs: Vec<Box<dyn ColumnPageStream>> = vec![open_stream(bytes).await];

        let tmp = TempDir::new().expect("tmpdir");
        let err = streaming_merge_sorted_parquet_files(inputs, tmp.path(), &merge_config(1))
            .await
            .expect_err("region order vs physical RG order mismatch must be rejected");
        let s = err.to_string();
        assert!(
            s.contains("disagrees with input") && s.contains("physical row order"),
            "expected MS-2 rejection message, got: {s}",
        );
    }

    /// Build a multi-RG fixture whose second sort col can be flagged
    /// DESC. Sort schema written into KV metadata: either
    /// `metric_name|env|-timestamp_secs/V2` or
    /// `metric_name|-env|-timestamp_secs/V2`.
    fn make_prefix_len_two_input_with_directions(
        rgs: &[(&str, &str, f64)],
        rows_per_rg: usize,
        env_descending: bool,
    ) -> Bytes {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("env", DataType::Utf8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let make_batch = |metric: &str, env: &str, marker: f64, start_series: u64| -> RecordBatch {
            let metric_name: ArrayRef = Arc::new(StringArray::from(vec![metric; rows_per_rg]));
            let env_arr: ArrayRef = Arc::new(StringArray::from(vec![env; rows_per_rg]));
            let timestamps: Vec<u64> = (0..rows_per_rg as u64)
                // Timestamps DESC within the RG to match the DESC sort.
                .map(|i| 1_700_000_000 + (rows_per_rg as u64 - i))
                .collect();
            let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
            let series: Vec<Vec<u8>> = (0..rows_per_rg as u64)
                .map(|i| (start_series + i).to_be_bytes().to_vec())
                .collect();
            let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
                series.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
            ));
            let value: ArrayRef = Arc::new(Float64Array::from(vec![marker; rows_per_rg]));
            RecordBatch::try_new(
                schema.clone(),
                vec![metric_name, env_arr, timestamp_secs, sorted_series, value],
            )
            .expect("batch")
        };

        let env_token = if env_descending { "-env" } else { "env" };
        let sort_fields = format!("metric_name|{env_token}|-timestamp_secs/V2");

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            row_group_size: rows_per_rg,
            ..ParquetWriterConfig::default()
        };
        let kvs = vec![
            KeyValue::new(PARQUET_META_SORT_FIELDS.to_string(), sort_fields),
            KeyValue::new(
                PARQUET_META_WINDOW_START.to_string(),
                "1700000000".to_string(),
            ),
            KeyValue::new(PARQUET_META_WINDOW_DURATION.to_string(), "60".to_string()),
            KeyValue::new(PARQUET_META_NUM_MERGE_OPS.to_string(), "0".to_string()),
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "2".to_string(),
            ),
        ];
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: 1,
                descending: env_descending,
                nulls_first: false,
            },
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            sorting_cols,
            Some(kvs),
            &["metric_name".to_string(), "env".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).expect("arrow writer");
        for (i, (metric, env, marker)) in rgs.iter().enumerate() {
            let batch = make_batch(metric, env, *marker, (i as u64) * rows_per_rg as u64);
            writer.write(&batch).expect("write");
        }
        writer.close().expect("close");
        Bytes::from(buf)
    }

    /// `extract_aligned_prefix_value` rejects an RG whose prefix
    /// column has `min != max` — those RGs are not actually aligned
    /// on the prefix value and grouping them into one region would be
    /// silently wrong.
    #[test]
    fn test_composite_key_rejects_non_aligned_rg() {
        // A single RG whose `metric_name` carries two distinct values:
        // min ("cpu.usage") != max ("memory.used"). The composite-key
        // extractor must refuse to mint a key for this RG.
        let bytes = make_misaligned_metric_name_input(&[("cpu.usage", "memory.used")], 20);
        let reader = SerializedFileReader::new(bytes).expect("ser");
        let metadata = reader.metadata();
        let prefix_cols =
            find_prefix_parquet_col_indices(metadata, "metric_name|-timestamp_secs/V2", 1, 0)
                .expect("resolve");
        let err = extract_rg_composite_prefix_key(metadata, 0, &prefix_cols, 0)
            .expect_err("RG with min != max on prefix col must be rejected");
        let s = err.to_string();
        assert!(
            s.contains("NOT prefix-aligned"),
            "expected misalignment error, got: {s}",
        );
    }

    /// Build a single-RG fixture whose `metric_name` column contains
    /// two distinct values within the same RG. Used to exercise the
    /// `min != max` rejection path in
    /// `extract_aligned_prefix_value`.
    fn make_misaligned_metric_name_input(names: &[(&str, &str)], rows_per_run: usize) -> Bytes {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let total = rows_per_run * 2 * names.len();
        let mut metric_values: Vec<&str> = Vec::with_capacity(total);
        for (a, b) in names {
            for _ in 0..rows_per_run {
                metric_values.push(*a);
            }
            for _ in 0..rows_per_run {
                metric_values.push(*b);
            }
        }
        let metric_name: ArrayRef = Arc::new(StringArray::from(metric_values));
        let timestamps: Vec<u64> = (0..total as u64).map(|i| 1_700_000_000 + i).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let series: Vec<Vec<u8>> = (0..total as u64)
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        let sorted_series: ArrayRef = Arc::new(BinaryArray::from(
            series.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        ));
        let value: ArrayRef = Arc::new(Float64Array::from(
            (0..total).map(|i| i as f64).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![metric_name, timestamp_secs, sorted_series, value],
        )
        .expect("batch");

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
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
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "1".to_string(),
            ),
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            Vec::new(),
            Some(kvs),
            &["metric_name".to_string(), "timestamp_secs".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).expect("arrow writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        Bytes::from(buf)
    }

    /// Build a parquet fixture with TWO row groups, each containing
    /// rows of one distinct metric_name. RG 0 = "cpu.usage" × 30 rows,
    /// RG 1 = "memory.used" × 30 rows. `rg_partition_prefix_len = 1`
    /// declares metric_name alignment.
    fn make_two_metric_aligned_input() -> Bytes {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("sorted_series", DataType::Binary, false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("service", dict_type, true),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let make_batch = |metric_key: i32, start_series: u64, rows: usize| -> RecordBatch {
            let metric_keys: Vec<i32> = vec![metric_key; rows];
            let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
            let metric_name: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    arrow::array::Int32Array::from(metric_keys),
                    Arc::new(metric_values),
                )
                .expect("dict"),
            );
            let timestamps: Vec<u64> = (0..rows as u64)
                .map(|i| 1_700_000_000 + (rows as u64 - i))
                .collect();
            let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
            let mut series_bytes: Vec<Vec<u8>> = Vec::with_capacity(rows);
            for i in 0..rows as u64 {
                series_bytes.push((start_series + i).to_be_bytes().to_vec());
            }
            let series_refs: Vec<&[u8]> = series_bytes.iter().map(|v| v.as_slice()).collect();
            let sorted_series: ArrayRef = Arc::new(BinaryArray::from(series_refs));
            let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; rows]));
            let svc_values = StringArray::from(vec!["api", "db", "cache"]);
            let svc_keys: Vec<Option<i32>> = (0..rows as i32)
                .map(|i| if i % 5 == 0 { None } else { Some(i % 3) })
                .collect();
            let service: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    arrow::array::Int32Array::from(svc_keys),
                    Arc::new(svc_values),
                )
                .expect("dict"),
            );
            let tsids: Vec<i64> = (0..rows as i64).map(|i| 1000 + i).collect();
            let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
            let values: Vec<f64> = (0..rows).map(|i| i as f64).collect();
            let value: ArrayRef = Arc::new(Float64Array::from(values));

            RecordBatch::try_new(
                schema.clone(),
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
            .expect("batch")
        };

        let batch_cpu = make_batch(0, 0, 30);
        let batch_mem = make_batch(1, 100, 30);

        let cfg = ParquetWriterConfig {
            compression: Compression::Snappy,
            row_group_size: 30, // one RG per metric_name
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
            // `prefix_len = 1` declares metric_name alignment.
            KeyValue::new(
                PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
                "1".to_string(),
            ),
        ];
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: 1,
                descending: true,
                nulls_first: false,
            },
        ];
        let props: WriterProperties = cfg.to_writer_properties_with_metadata(
            &schema,
            sorting_cols,
            Some(kvs),
            &["metric_name".to_string(), "timestamp_secs".to_string()],
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("arrow writer");
        writer.write(&batch_cpu).expect("write cpu");
        writer.write(&batch_mem).expect("write mem");
        writer.close().expect("close");
        Bytes::from(buf)
    }

    /// Regression for Codex P1 on PR-6409 (the empty-input half): a
    /// zero-row-group input mixed with a populated one must not panic
    /// the body-column path. Phase 0 already accepts empty inputs;
    /// PR-6c.2's per-region engine only iterates `region.contributing`
    /// inputs for body cols, but verify directly so any future change
    /// that broadens the iteration is caught.
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
        for i in 0..50 {
            assert!(
                (value.value(i) - i as f64).abs() < 1e-9,
                "row {i}: expected {i}, got {}",
                value.value(i),
            );
        }
    }

    /// Write a fixture parquet file where each body column is forced
    /// to span multiple parquet data pages by pinning a small
    /// `data_page_row_count_limit`. The merge engine must read those
    /// pages back via a single persistent `StreamDecoder` per input —
    /// reconstructing the decoder for each `advance_decoder_to_row`
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
    /// each `advance_decoder_to_row` call instantiated a fresh decoder
    /// whose counter started at zero — the *second* decoded page
    /// reported `row_start = 0` and the page cache's
    /// `(input_row - cache_start)` indexing landed on the wrong rows
    /// (or panicked on out-of-bounds).
    #[tokio::test]
    async fn test_body_col_multi_input_page_preserves_row_start() {
        // The bug only surfaces when `assemble_one_output_page` is
        // called more than once per output (so `advance_decoder_to_row`
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

    /// Legacy multi-RG input (prefix_len=0, num_RGs>1) is rejected —
    /// these must route through PR-5's `LegacyMultiRGAdapter`.
    #[tokio::test]
    async fn test_legacy_multi_rg_input_rejected() {
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
            .expect_err("legacy multi-RG input must be rejected");
        let s = err.to_string();
        assert!(
            s.contains("legacy multi-RG") || s.contains("PR-5 adapter"),
            "expected legacy multi-RG rejection, got: {s}",
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
