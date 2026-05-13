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

//! Legacy multi-row-group input adapter.
//!
//! [`StreamingParquetReader`] yields pages in storage order so PR-6's
//! merge engine can copy them column-major without buffering across row
//! groups. That works only when the input file's row-group layout
//! aligns with the sort prefix — i.e., new-format files where
//! `qh.rg_partition_prefix_len > 0` so each RG is a contiguous run of
//! the sort prefix, or single-row-group files (the trivial alignment).
//!
//! Legacy files that pre-date PR-3 carry `qh.rg_partition_prefix_len ==
//! 0` AND `num_row_groups > 1`. Their RG boundaries land at arbitrary
//! row counts inside the sort order, so column-major streaming through
//! the merge driver isn't possible without buffering across RGs.
//!
//! [`LegacyInputAdapter`] handles that case by buffering the whole
//! file, decoding it through Arrow, concatenating into a single
//! [`RecordBatch`], and re-encoding it as a prefix-aligned multi-row-
//! group parquet stream that [`StreamingParquetReader`] can serve.
//! The adapter splits the consolidated batch at first-sort-col
//! transitions (typically `metric_name`) and declares
//! `qh.rg_partition_prefix_len = 1` on the re-encoded file so the
//! merge engine's prefix-aware fast path can consume it. The original
//! file is already sorted (legacy files were written sorted), so
//! consolidating then re-splitting preserves order automatically —
//! the adapter does NOT re-sort.
//!
//! When the original file lacks a `qh.sort_fields` KV or its first
//! sort column can't be resolved in the schema, the adapter falls
//! back to a single-row-group re-encode without claiming any prefix
//! alignment. That route is still valid as input to the merge engine
//! — it just goes through the engine's `prefix_len = 0` sub-region
//! splitting path instead of the fast prefix-aligned path.
//!
//! Costs: one full-file decode + one full-file re-encode per legacy
//! input, per merge. This is acceptable because legacy files age out
//! as they're re-merged in the new format.

// `parquet::format` is the only public path to `PageType` in parquet
// 58 (the non-deprecated replacements are crate-private). The
// `format` module is scheduled for removal in parquet 59 and we'll
// migrate when the new public path lands. Tests in this module
// inspect page types to verify per-column data-page counts; allowing
// deprecated items at module scope keeps that lookup direct.
#![allow(deprecated)]

use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, NullArray, RecordBatch};
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;
use parquet::file::metadata::{KeyValue, ParquetMetaData, SortingColumn};
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use tokio::io::AsyncRead;

use super::config::ParquetWriterConfig;
use super::streaming_reader::{
    ColumnPageStream, Page, ParquetReadError, RemoteByteSource, StreamingParquetReader,
};
use super::streaming_writer::StreamingParquetWriter;
use super::writer::{
    PARQUET_META_RG_PARTITION_PREFIX_LEN, PARQUET_META_SORT_FIELDS, ParquetWriteError,
};
use crate::sort_fields::{is_timestamp_column_name, parse_sort_fields};

/// Errors from the legacy input adapter.
///
/// Each variant preserves the underlying error so callers can
/// distinguish I/O blips (retry/backoff) from genuine file corruption.
/// In particular, an I/O error on the buffered GET surfaces as
/// [`Self::Io`] — never silently re-reported as a decode error.
#[derive(Error, Debug)]
pub enum LegacyAdapterError {
    /// I/O error from the underlying [`RemoteByteSource`].
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Parquet decode error while reading the original file (footer
    /// parse, page decompression, etc.).
    #[error("failed to decode legacy parquet file: {0}")]
    ParquetDecode(#[source] ParquetError),

    /// Arrow decode error while consuming the original file's record
    /// batches. Distinct from [`Self::ParquetDecode`] so the caller
    /// sees the real cause (arrow projection, type coercion, etc.)
    /// rather than a generic decode error.
    #[error("failed to decode legacy parquet record batches: {0}")]
    ArrowDecode(#[source] arrow::error::ArrowError),

    /// Re-encoding the consolidated [`RecordBatch`] into a single-row-
    /// group parquet stream failed.
    #[error("failed to re-encode legacy parquet file as single row group: {0}")]
    ParquetReencode(#[from] ParquetWriteError),

    /// The streaming reader over the re-encoded in-memory buffer
    /// failed. Because the in-memory source cannot fail with I/O, this
    /// almost always indicates a logic bug in the re-encode path.
    #[error("failed to open streaming reader over re-encoded buffer: {0}")]
    StreamingReader(#[from] ParquetReadError),

    /// The original file is too large to buffer in memory. The adapter
    /// is the legacy fallback path; a defensively-sized cap protects
    /// against pathological inputs.
    #[error("legacy input file is too large to buffer: {actual} bytes exceeds limit {limit}")]
    InputTooLarge { actual: u64, limit: u64 },

    /// The caller asked for `target_prefix_len > 0` but the file does
    /// not advertise enough sort information to honor the request:
    /// `qh.sort_fields` is absent, or the sort-fields string declares
    /// fewer columns than requested. Either case means the file lacks
    /// a name for one of the first `target_prefix_len` sort columns,
    /// so the adapter can't claim alignment on a column it can't
    /// identify. (Prefix columns that are *named* in `qh.sort_fields`
    /// but missing from the arrow schema are NOT an error — per SS-3
    /// the adapter treats them as implicitly null at every row, which
    /// trivially satisfies alignment on that column.) The caller
    /// should retry with a smaller `target_prefix_len` or pass `0` to
    /// fall through to the single-row-group re-encode.
    #[error(
        "cannot honor target_prefix_len = {target}: {reason} (the legacy file does not advertise \
         enough sort information to safely synthesize prefix-aligned row groups)"
    )]
    PrefixUnresolvable { target: u32, reason: String },

    /// The legacy file's rows are not sorted by its declared sort schema
    /// (SS-1 violation): two row regions in the file carry the same
    /// composite prefix value with other prefix values in between. The
    /// adapter walks rows in physical order and emits one RG per
    /// prefix-value run, so an unsorted input produces multiple RGs
    /// sharing a prefix key — which violates PA-3 (per-input uniqueness).
    /// Bail upfront instead of producing a file the downstream merge
    /// engine will reject mid-merge.
    #[error(
        "legacy input is not sorted by its declared sort schema: rows at offset {first_offset} \
         and offset {second_offset} share composite prefix value (target_prefix_len = {target}). \
         The adapter relies on the file being sorted per SS-1; an unsorted file would synthesize \
         multiple row groups with the same prefix key (PA-3 violation)."
    )]
    InputNotSorted {
        target: u32,
        first_offset: usize,
        second_offset: usize,
    },
}

/// 4 GiB upper bound on the input file size we will buffer into RAM.
/// Legacy parquet metrics splits in production are well under 1 GiB;
/// this is a runaway bound, not a typical-case budget.
const MAX_LEGACY_INPUT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Adapter that exposes a legacy multi-row-group parquet file through
/// the [`ColumnPageStream`] contract.
///
/// Internally buffers the original file, re-encodes it as a single-row-
/// group parquet stream in memory, and serves pages through a
/// [`StreamingParquetReader`] over that buffer. Preserves the original
/// file's `key_value_metadata` (the `qh.*` keys) and `sorting_columns`.
pub struct LegacyInputAdapter {
    inner: StreamingParquetReader,
}

impl LegacyInputAdapter {
    /// Open the legacy file at `path` through `source`, re-encode it
    /// into a prefix-aligned parquet stream advertising
    /// `qh.rg_partition_prefix_len = target_prefix_len`, and prepare
    /// to serve its pages.
    ///
    /// The caller picks `target_prefix_len` based on what the rest of
    /// the merge plan expects. Typical sources:
    /// - Match the consensus `rg_partition_prefix_len` of the non-legacy inputs in the same merge
    ///   (so all inputs end up at one value).
    /// - Pass `0` when there is no non-legacy input, which produces a single-row-group re-encode
    ///   and no prefix-alignment claim — the merge engine's `prefix_len = 0` sub-region splitting
    ///   path handles it.
    ///
    /// When `target_prefix_len > 0`, the adapter slices the
    /// consolidated batch at every transition of the first
    /// `target_prefix_len` sort columns (composite key, via
    /// [`RowConverter`]) and emits one output row group per slice.
    /// Returns an error if the file does not have enough resolvable
    /// sort columns to honor the request — the caller should either
    /// retry with a smaller `target_prefix_len` or fall back to `0`.
    ///
    /// Issues exactly one buffered GET against `source` (covering the
    /// whole file). All subsequent reads are served from the in-memory
    /// re-encoded buffer.
    pub async fn try_open(
        source: Arc<dyn RemoteByteSource>,
        path: PathBuf,
        target_prefix_len: u32,
    ) -> Result<Self, LegacyAdapterError> {
        let file_size = source.file_size(&path).await?;
        if file_size > MAX_LEGACY_INPUT_BYTES {
            return Err(LegacyAdapterError::InputTooLarge {
                actual: file_size,
                limit: MAX_LEGACY_INPUT_BYTES,
            });
        }

        let buffered = source.get_slice(&path, 0..file_size).await?;
        let reencoded_bytes = reencode_prefix_aligned(buffered, target_prefix_len)?;
        let reencoded_source: Arc<dyn RemoteByteSource> = Arc::new(InMemoryByteSource {
            bytes: Bytes::from(reencoded_bytes),
        });
        let inner = StreamingParquetReader::try_open(reencoded_source, path).await?;
        Ok(Self { inner })
    }

    /// Parsed metadata for the re-encoded single-row-group file.
    ///
    /// Available without further I/O. Schema, `qh.*` key-value entries,
    /// and `sorting_columns` are preserved from the original input.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        self.inner.metadata()
    }
}

#[async_trait]
impl ColumnPageStream for LegacyInputAdapter {
    fn metadata(&self) -> &Arc<ParquetMetaData> {
        self.inner.metadata()
    }

    async fn next_page(&mut self) -> Result<Option<Page>, ParquetReadError> {
        self.inner.next_page().await
    }
}

/// Decode `bytes` into a single concatenated [`RecordBatch`], then
/// re-encode it according to `target_prefix_len`:
/// - `target_prefix_len == 0`: emit a single row group with no prefix alignment claim. The original
///   `qh.*` KV (which typically omits `qh.rg_partition_prefix_len`) is preserved verbatim. The
///   merge engine's `prefix_len = 0` sub-region splitting path consumes this without further
///   plumbing.
/// - `target_prefix_len > 0`: slice the consolidated batch at every transition of the first
///   `target_prefix_len` sort columns (composite key, via [`RowConverter`]) and emit one row group
///   per distinct composite value. Stamp the output's KV with `qh.rg_partition_prefix_len =
///   target_prefix_len` so the merge engine's prefix-aware fast path takes over.
///
/// When `target_prefix_len > 0` and the requested alignment cannot be
/// honored — `qh.sort_fields` is absent, the sort-fields string
/// declares fewer columns than requested, or one of the first N
/// columns is missing from the arrow schema — returns
/// [`LegacyAdapterError::PrefixUnresolvable`]. The caller can retry
/// with a smaller `target_prefix_len` or fall back to `0`.
///
/// The zero-rows-but-`target_prefix_len > 0` case is degenerate but
/// still stamps the KV: an empty file vacuously satisfies any prefix
/// alignment claim.
fn reencode_prefix_aligned(
    bytes: Bytes,
    target_prefix_len: u32,
) -> Result<Vec<u8>, LegacyAdapterError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(LegacyAdapterError::ParquetDecode)?;

    let arrow_schema = builder.schema().clone();
    let original_metadata = builder.metadata().clone();
    let original_kv: Option<Vec<KeyValue>> = original_metadata
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let original_sorting_cols: Option<Vec<SortingColumn>> =
        carry_sorting_columns(&original_metadata);

    let reader = builder.build().map_err(LegacyAdapterError::ParquetDecode)?;
    let mut decoded_batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(LegacyAdapterError::ArrowDecode)?;
        decoded_batches.push(batch);
    }

    let consolidated_batch = arrow::compute::concat_batches(&arrow_schema, &decoded_batches)
        .map_err(LegacyAdapterError::ArrowDecode)?;

    if target_prefix_len == 0 {
        // Single-RG passthrough: preserve original KV unchanged.
        let props = build_writer_properties(
            &arrow_schema,
            original_sorting_cols.unwrap_or_default(),
            original_kv,
            consolidated_batch.num_rows(),
        );
        return write_single_row_group(arrow_schema, props, consolidated_batch);
    }

    let prefix_col_indices =
        resolve_prefix_sort_cols(original_kv.as_ref(), &arrow_schema, target_prefix_len)?;

    let slices = if consolidated_batch.num_rows() == 0 {
        Vec::new()
    } else {
        compute_prefix_value_slices(&consolidated_batch, &prefix_col_indices, target_prefix_len)?
    };
    let kv_with_prefix = inject_prefix_len_kv(original_kv, target_prefix_len);
    let props = build_writer_properties(
        &arrow_schema,
        original_sorting_cols.unwrap_or_default(),
        Some(kv_with_prefix),
        consolidated_batch.num_rows(),
    );
    write_multi_row_group(arrow_schema, props, consolidated_batch, &slices)
}

/// Resolve the first `prefix_len` sort columns from `qh.sort_fields`
/// to arrow-schema indices. Honors the
/// `timestamp` / `timestamp_secs` alias the rest of the engine uses.
///
/// Returns one entry per requested prefix column: `Some(idx)` if the
/// column is present in the schema, or `None` if the column is
/// declared in `qh.sort_fields` but absent from the arrow schema
/// (treated as implicitly null at every row per SS-3). Returns
/// [`LegacyAdapterError::PrefixUnresolvable`] only when the file
/// doesn't advertise enough sort-column *names* (missing/unparseable
/// `qh.sort_fields`, or declares fewer columns than requested) —
/// those are cases where we don't even know which column the prefix
/// alignment is supposed to be on.
fn resolve_prefix_sort_cols(
    kv: Option<&Vec<KeyValue>>,
    arrow_schema: &arrow::datatypes::Schema,
    prefix_len: u32,
) -> Result<Vec<Option<usize>>, LegacyAdapterError> {
    debug_assert!(prefix_len > 0);
    let sort_fields_str = kv
        .and_then(|kvs| kvs.iter().find(|k| k.key == PARQUET_META_SORT_FIELDS))
        .and_then(|kv| kv.value.as_deref())
        .ok_or_else(|| LegacyAdapterError::PrefixUnresolvable {
            target: prefix_len,
            reason: format!("{PARQUET_META_SORT_FIELDS} KV is absent"),
        })?;
    let parsed =
        parse_sort_fields(sort_fields_str).map_err(|e| LegacyAdapterError::PrefixUnresolvable {
            target: prefix_len,
            reason: format!("{PARQUET_META_SORT_FIELDS} is unparseable: {e}"),
        })?;
    let prefix_len_usize = prefix_len as usize;
    if parsed.column.len() < prefix_len_usize {
        return Err(LegacyAdapterError::PrefixUnresolvable {
            target: prefix_len,
            reason: format!(
                "{PARQUET_META_SORT_FIELDS} declares only {} sort columns",
                parsed.column.len(),
            ),
        });
    }
    let mut indices = Vec::with_capacity(prefix_len_usize);
    for sf in parsed.column.iter().take(prefix_len_usize) {
        let resolved = if is_timestamp_column_name(&sf.name)
            && arrow_schema.index_of("timestamp_secs").is_ok()
        {
            "timestamp_secs"
        } else {
            sf.name.as_str()
        };
        // Missing column → implicit null per SS-3. A column that is
        // null at every row is constant, which trivially satisfies
        // alignment on that column. The transition computation
        // synthesizes a NullArray in its place.
        indices.push(arrow_schema.index_of(resolved).ok());
    }
    Ok(indices)
}

/// Walk the composite prefix value row-by-row over the columns at
/// `prefix_col_indices` and produce `(start, len)` slices, one per
/// distinct composite-value run. Uses a single [`RowConverter`] over
/// all prefix columns so dictionary / utf8 / primitive types are
/// handled uniformly and N-column equality is a single byte
/// comparison per row.
///
/// An entry of `None` in `prefix_col_indices` represents a prefix
/// column that is named in `qh.sort_fields` but absent from the
/// file's arrow schema. Per SS-3 those rows are treated as having
/// null values, so this function materializes a [`NullArray`] of the
/// batch's length in that slot. A column that's null at every row is
/// constant and contributes no transitions to the composite key —
/// equivalent to skipping it, but kept explicit so the resulting
/// alignment claim matches the caller's requested `target_prefix_len`.
///
/// Detects SS-1 violations (unsorted input) up-front: each emitted
/// slice's composite prefix-value bytes must be unique. If two
/// non-adjacent slices carry the same prefix value (e.g., rows
/// `[A,A,B,B,A,A]`), the input is not sorted by its declared sort
/// schema, so we'd synthesize a file with two RGs sharing the prefix
/// — a PA-3 violation the downstream merge engine would reject
/// mid-merge. Bailing here with `InputNotSorted` keeps that bad file
/// from ever landing on disk.
fn compute_prefix_value_slices(
    batch: &RecordBatch,
    prefix_col_indices: &[Option<usize>],
    target_prefix_len: u32,
) -> Result<Vec<(usize, usize)>, LegacyAdapterError> {
    let n = batch.num_rows();
    let cols: Vec<ArrayRef> = prefix_col_indices
        .iter()
        .map(|idx_opt| match idx_opt {
            Some(idx) => Arc::clone(batch.column(*idx)),
            None => Arc::new(NullArray::new(n)) as ArrayRef,
        })
        .collect();
    let sort_fields: Vec<SortField> = cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(sort_fields).map_err(LegacyAdapterError::ArrowDecode)?;
    let rows = converter
        .convert_columns(&cols)
        .map_err(LegacyAdapterError::ArrowDecode)?;
    let n_rows = rows.num_rows();
    if n_rows == 0 {
        return Ok(Vec::new());
    }
    // Track each emitted slice's starting prefix-value bytes; any
    // repeat signals SS-1 violation on the input.
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut slices = Vec::new();
    let mut start = 0;
    let record_slice = |slices: &mut Vec<(usize, usize)>,
                        seen: &mut HashMap<Vec<u8>, usize>,
                        slice_start: usize,
                        slice_len: usize|
     -> Result<(), LegacyAdapterError> {
        let key = rows.row(slice_start).as_ref().to_vec();
        if let Some(&first_offset) = seen.get(&key) {
            return Err(LegacyAdapterError::InputNotSorted {
                target: target_prefix_len,
                first_offset,
                second_offset: slice_start,
            });
        }
        seen.insert(key, slice_start);
        slices.push((slice_start, slice_len));
        Ok(())
    };
    for i in 1..n_rows {
        if rows.row(i) != rows.row(i - 1) {
            record_slice(&mut slices, &mut seen, start, i - start)?;
            start = i;
        }
    }
    record_slice(&mut slices, &mut seen, start, n_rows - start)?;
    Ok(slices)
}

/// Inject (or replace) the `qh.rg_partition_prefix_len` KV entry on
/// the re-encoded file. Legacy files omit this key entirely; the
/// re-encoded output advertises the synthesized prefix alignment so
/// the merge engine's reader picks the fast path.
fn inject_prefix_len_kv(original: Option<Vec<KeyValue>>, prefix_len: u32) -> Vec<KeyValue> {
    let mut kvs = original.unwrap_or_default();
    kvs.retain(|k| k.key != PARQUET_META_RG_PARTITION_PREFIX_LEN);
    kvs.push(KeyValue::new(
        PARQUET_META_RG_PARTITION_PREFIX_LEN.to_string(),
        prefix_len.to_string(),
    ));
    kvs
}

/// Write `batch` to a multi-row-group parquet stream: one RG per
/// `(start, len)` slice in `slices`. Slices are emitted in order, so
/// the sort order observed by readers matches the order of the
/// consolidated batch.
fn write_multi_row_group(
    arrow_schema: arrow::datatypes::SchemaRef,
    props: WriterProperties,
    batch: RecordBatch,
    slices: &[(usize, usize)],
) -> Result<Vec<u8>, LegacyAdapterError> {
    let mut out: Vec<u8> = Vec::new();
    let mut writer = StreamingParquetWriter::try_new(&mut out, arrow_schema, props)?;
    for &(start, len) in slices {
        if len == 0 {
            continue;
        }
        let mut row_group = writer.start_row_group()?;
        for col_idx in 0..batch.num_columns() {
            let slice = batch.column(col_idx).slice(start, len);
            row_group.write_next_column(&slice)?;
        }
        row_group.finish()?;
    }
    writer.close()?;
    Ok(out)
}

/// Read sorting columns from row group 0 of `metadata`, if present.
/// Legacy files written by `ParquetWriter` set sorting_columns
/// identically across row groups; preserving from RG0 is sufficient.
fn carry_sorting_columns(metadata: &ParquetMetaData) -> Option<Vec<SortingColumn>> {
    if metadata.num_row_groups() == 0 {
        return None;
    }
    metadata.row_group(0).sorting_columns().cloned()
}

/// Build [`WriterProperties`] for the re-encoded file. Forces a single
/// row group via `set_max_row_group_row_count(num_rows + 1)`, so all
/// rows fit in one RG. Preserves `key_value_metadata` and
/// `sorting_columns`; otherwise inherits the same compression /
/// dictionary / bloom-filter defaults as production output.
fn build_writer_properties(
    arrow_schema: &arrow::datatypes::Schema,
    sorting_cols: Vec<SortingColumn>,
    kv_metadata: Option<Vec<KeyValue>>,
    num_rows: usize,
) -> WriterProperties {
    let cfg = ParquetWriterConfig::default();
    // Sort field names drive bloom-filter and dictionary configuration.
    // We derive them from the carried sorting_columns so the output
    // mirrors the original's encoding choices on the same columns.
    let sort_field_names = sort_field_names_from_columns(arrow_schema, &sorting_cols);

    let base_props = cfg.to_writer_properties_with_metadata(
        arrow_schema,
        sorting_cols,
        kv_metadata,
        &sort_field_names,
    );

    // Force a single row group: set the row-count cap above the total
    // row count so the writer never rolls over. `num_rows + 1` is
    // sufficient; saturating add guards against an unrealistic
    // `usize::MAX`-sized input.
    let single_rg_cap = num_rows.saturating_add(1).max(1);
    base_props
        .into_builder()
        .set_max_row_group_row_count(Some(single_rg_cap))
        .build()
}

/// Resolve `sorting_cols` (carrying parquet column indices) back to
/// the corresponding arrow field names. Indices that fall outside the
/// schema are skipped — this should not happen for well-formed files
/// but we don't want a malformed legacy header to panic.
fn sort_field_names_from_columns(
    arrow_schema: &arrow::datatypes::Schema,
    sorting_cols: &[SortingColumn],
) -> Vec<String> {
    let fields = arrow_schema.fields();
    let mut names = Vec::with_capacity(sorting_cols.len());
    for col in sorting_cols {
        let idx = col.column_idx as usize;
        if idx < fields.len() {
            names.push(fields[idx].name().to_string());
        }
    }
    names
}

/// Write `batch` into a single-row-group parquet stream using the
/// streaming writer. Returns the encoded bytes.
fn write_single_row_group(
    arrow_schema: arrow::datatypes::SchemaRef,
    props: WriterProperties,
    batch: RecordBatch,
) -> Result<Vec<u8>, LegacyAdapterError> {
    let mut out: Vec<u8> = Vec::new();
    let mut writer = StreamingParquetWriter::try_new(&mut out, arrow_schema, props)?;

    // Even an empty input deserves a row group so downstream tooling
    // sees a structurally consistent file (one RG, num_rows == 0). If
    // the input has zero rows AND the streaming writer rejects an
    // empty row group, fall back to closing without one — the merge
    // engine treats a zero-row-group file as drained immediately.
    if batch.num_rows() > 0 || batch.num_columns() > 0 {
        let mut row_group = writer.start_row_group()?;
        for col_idx in 0..batch.num_columns() {
            row_group.write_next_column(batch.column(col_idx))?;
        }
        row_group.finish()?;
    }

    writer.close()?;
    Ok(out)
}

/// In-memory [`RemoteByteSource`] backing a re-encoded buffer.
///
/// Private to this module: it exists only to feed a
/// [`StreamingParquetReader`] over the re-encoded bytes. We
/// deliberately do NOT expose it crate-wide.
struct InMemoryByteSource {
    bytes: Bytes,
}

#[async_trait]
impl RemoteByteSource for InMemoryByteSource {
    async fn file_size(&self, _path: &Path) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
        let start = range.start as usize;
        let end = range.end as usize;
        if end > self.bytes.len() || start > end {
            return Err(io::Error::other(format!(
                "in-memory range {start}..{end} out of bounds for {} byte buffer",
                self.bytes.len(),
            )));
        }
        Ok(self.bytes.slice(start..end))
    }

    async fn get_slice_stream(
        &self,
        _path: &Path,
        range: Range<u64>,
    ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
        let start = range.start as usize;
        let end = range.end as usize;
        if end > self.bytes.len() || start > end {
            return Err(io::Error::other(format!(
                "in-memory range {start}..{end} out of bounds for {} byte buffer",
                self.bytes.len(),
            )));
        }
        let slice = self.bytes.slice(start..end);
        Ok(Box::new(io::Cursor::new(slice.to_vec())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::storage::ParquetWriterConfig;

    // -------- Fixtures --------

    fn make_metrics_batch(num_rows: usize) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));

        let metric_keys: Vec<i32> = (0..num_rows as i32).map(|i| i % 2).collect();
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .expect("test dict array"),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows as u64).map(|i| 1_700_000_000 + i).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tsids: Vec<i64> = (0..num_rows as i64).map(|i| 1000 + i).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        // Service is nullable: every 5th row is null to exercise the
        // null-mask preservation guarantee.
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

        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .expect("test batch")
    }

    fn writer_props_multi_rg(
        arrow_schema: &ArrowSchema,
        kvs: Vec<KeyValue>,
        sorting_cols: Vec<SortingColumn>,
        rows_per_rg: usize,
    ) -> WriterProperties {
        let cfg = ParquetWriterConfig::default();
        let sort_field_names: Vec<String> =
            sort_field_names_from_columns(arrow_schema, &sorting_cols);
        let base = cfg.to_writer_properties_with_metadata(
            arrow_schema,
            sorting_cols,
            Some(kvs),
            &sort_field_names,
        );
        // Force multi-RG output: cap the row count per RG so we hit
        // the rollover boundary on a moderate fixture.
        base.into_builder()
            .set_max_row_group_row_count(Some(rows_per_rg))
            .build()
    }

    /// Write `batches` into a multi-row-group parquet file with
    /// row-group rollover every `rows_per_rg` rows. Returns the bytes.
    fn write_multi_rg_file(
        batches: &[RecordBatch],
        kvs: Vec<KeyValue>,
        sorting_cols: Vec<SortingColumn>,
        rows_per_rg: usize,
    ) -> Bytes {
        let arrow_schema = batches[0].schema();
        let props = writer_props_multi_rg(&arrow_schema, kvs, sorting_cols, rows_per_rg);
        let mut out: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut out, arrow_schema, Some(props))
                .expect("test arrow writer");
            for (idx, batch) in batches.iter().enumerate() {
                writer.write(batch).expect("test write");
                if idx + 1 < batches.len() {
                    writer.flush().expect("test flush");
                }
            }
            writer.close().expect("test close");
        }
        Bytes::from(out)
    }

    /// `RemoteByteSource` that records call counts so tests can assert
    /// the adapter issues exactly one buffered GET.
    struct CountingInMemorySource {
        bytes: Bytes,
        slice_calls: AtomicUsize,
        last_slice_range: Mutex<Option<Range<u64>>>,
    }

    impl CountingInMemorySource {
        fn new(bytes: Bytes) -> Arc<Self> {
            Arc::new(Self {
                bytes,
                slice_calls: AtomicUsize::new(0),
                last_slice_range: Mutex::new(None),
            })
        }
    }

    #[async_trait]
    impl RemoteByteSource for CountingInMemorySource {
        async fn file_size(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
            self.slice_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_slice_range.lock().expect("test mutex") = Some(range.clone());
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }

        async fn get_slice_stream(
            &self,
            _path: &Path,
            range: Range<u64>,
        ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
            let slice = self.bytes.slice(range.start as usize..range.end as usize);
            Ok(Box::new(io::Cursor::new(slice.to_vec())))
        }
    }

    /// `RemoteByteSource` whose `get_slice` always fails with a
    /// distinctive `io::Error`. Used to verify that the adapter
    /// surfaces the underlying I/O error rather than masking it.
    struct AlwaysFailingSliceSource {
        file_size: u64,
    }

    #[async_trait]
    impl RemoteByteSource for AlwaysFailingSliceSource {
        async fn file_size(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.file_size)
        }

        async fn get_slice(&self, _path: &Path, _range: Range<u64>) -> io::Result<Bytes> {
            Err(io::Error::other("simulated slice failure"))
        }

        async fn get_slice_stream(
            &self,
            _path: &Path,
            _range: Range<u64>,
        ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
            Err(io::Error::other("simulated slice stream failure"))
        }
    }

    fn dummy_path() -> PathBuf {
        PathBuf::from("legacy_test.parquet")
    }

    /// Build a multi-RG fixture whose rows are sorted by `metric_name`
    /// (so consolidating them produces a batch with contiguous
    /// metric_name runs, which is what the legacy adapter expects on
    /// real legacy files). `metrics` is `(name, rows_per_metric)` in
    /// the order they should appear; the writer rolls a new RG every
    /// `rows_per_rg` so the multi-RG structure is exercised
    /// independently of the metric_name partitioning.
    fn write_sorted_multi_rg_legacy_file(
        metrics: &[(&str, usize)],
        sort_fields_value: &str,
        rows_per_rg: usize,
    ) -> Bytes {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));

        let total: usize = metrics.iter().map(|(_, n)| *n).sum();
        let metric_names_vec: Vec<&str> = metrics.iter().map(|(name, _)| *name).collect();
        let mut metric_keys: Vec<i32> = Vec::with_capacity(total);
        let mut tsids: Vec<i64> = Vec::with_capacity(total);
        let mut timestamps: Vec<u64> = Vec::with_capacity(total);
        let mut values: Vec<f64> = Vec::with_capacity(total);
        let mut row_idx: u64 = 0;
        for (metric_idx, (_, count)) in metrics.iter().enumerate() {
            for _ in 0..*count {
                metric_keys.push(metric_idx as i32);
                tsids.push(1000 + row_idx as i64);
                // -timestamp_secs/V2 in the sort schema means
                // timestamps DESC within a metric run.
                timestamps.push(1_700_000_000 + (*count as u64) - (row_idx % *count as u64));
                values.push(row_idx as f64);
                row_idx += 1;
            }
        }
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(StringArray::from(metric_names_vec)),
            )
            .expect("metric dict"),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; total]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        let svc_keys: Vec<Option<i32>> = (0..total as i32).map(|i| Some(i % 3)).collect();
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(StringArray::from(vec!["api", "db", "cache"])),
            )
            .expect("svc dict"),
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .expect("sorted fixture batch");

        let kvs = vec![KeyValue::new(
            PARQUET_META_SORT_FIELDS.to_string(),
            sort_fields_value.to_string(),
        )];
        let sorting_cols = default_sorting_cols(&schema);
        write_multi_rg_file(&[batch], kvs, sorting_cols, rows_per_rg)
    }

    fn default_sorting_cols(arrow_schema: &ArrowSchema) -> Vec<SortingColumn> {
        vec![
            SortingColumn {
                column_idx: arrow_schema.index_of("metric_name").expect("test schema") as i32,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: arrow_schema
                    .index_of("timestamp_secs")
                    .expect("test schema") as i32,
                descending: true,
                nulls_first: false,
            },
        ]
    }

    /// Drain all pages from a `ColumnPageStream`.
    async fn drain_pages_via_trait(stream: &mut dyn ColumnPageStream) -> Vec<Page> {
        let mut pages = Vec::new();
        while let Some(p) = stream.next_page().await.expect("page read") {
            pages.push(p);
        }
        pages
    }

    /// Read a parquet file from `bytes` into a single concatenated
    /// `RecordBatch` for byte-equality comparisons.
    fn read_back_to_single_batch(bytes: Bytes) -> RecordBatch {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("read back builder");
        let arrow_schema = builder.schema().clone();
        let reader = builder.build().expect("read back build");
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("read back");
        if batches.is_empty() {
            RecordBatch::new_empty(arrow_schema)
        } else {
            arrow::compute::concat_batches(&arrow_schema, &batches).expect("concat")
        }
    }

    // -------- Tests --------

    #[tokio::test]
    async fn test_empty_multi_rg_input() {
        let arrow_schema = make_metrics_batch(1).schema();
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        // Two empty row groups.
        let bytes = write_multi_rg_file(
            &[empty_batch.clone(), empty_batch],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            1,
        );
        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source.clone(), dummy_path(), 0)
            .await
            .expect("adapter open");

        let total_rows: i64 = (0..adapter.metadata().num_row_groups())
            .map(|i| adapter.metadata().row_group(i).num_rows())
            .sum();
        assert_eq!(
            total_rows, 0,
            "row count must be preserved across re-encode"
        );

        // Buffered GET for the input file plus the streaming reader's
        // footer GET against the in-memory re-encoded buffer. The
        // counting source observes only the legacy buffered GET; the
        // re-encoded buffer is served by the private `InMemoryByteSource`.
        assert_eq!(
            source.slice_calls.load(Ordering::SeqCst),
            1,
            "exactly one buffered GET against the legacy input",
        );
    }

    #[tokio::test]
    async fn test_multi_rg_consolidates_to_single_rg() {
        // 3 RGs of 100 rows each. Default rows_per_rg=100 in
        // `write_multi_rg_file` forces a flush every 100 rows.
        let batch_a = make_metrics_batch(100);
        let batch_b = make_metrics_batch(100);
        let batch_c = make_metrics_batch(100);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a, batch_b, batch_c],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            100,
        );

        // Sanity: confirm fixture really has multiple RGs.
        let pre_builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("pre-builder");
        assert!(
            pre_builder.metadata().num_row_groups() >= 3,
            "fixture must produce at least 3 row groups; got {}",
            pre_builder.metadata().num_row_groups(),
        );
        let pre_total: i64 = (0..pre_builder.metadata().num_row_groups())
            .map(|i| pre_builder.metadata().row_group(i).num_rows())
            .sum();
        assert_eq!(pre_total, 300);

        let source = CountingInMemorySource::new(bytes);
        let mut adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        assert_eq!(
            adapter.metadata().num_row_groups(),
            1,
            "adapter must consolidate to a single row group",
        );
        assert_eq!(
            adapter.metadata().row_group(0).num_rows(),
            300,
            "row count must be preserved",
        );

        let pages = drain_pages_via_trait(&mut adapter).await;
        for p in &pages {
            assert_eq!(p.rg_idx, 0, "all pages must be in the consolidated RG");
        }
    }

    #[tokio::test]
    async fn test_data_roundtrip_through_adapter() {
        let batch_a = make_metrics_batch(50);
        let batch_b = make_metrics_batch(50);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a, batch_b],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            50,
        );

        // Oracle: read the original file directly into a single batch.
        let oracle = read_back_to_single_batch(bytes.clone());
        assert_eq!(oracle.num_rows(), 100);

        // Adapter run: open through the adapter, drain pages to drive
        // the streaming path through the re-encoded buffer, then
        // verify the adapter's metadata exposes the expected schema
        // and row count. Byte-equal data verification is performed
        // against the consolidated batch we re-decode by going back
        // through the streaming reader's contract: the in-memory
        // re-encoded buffer is private, so we re-read the original
        // file, and assert the consolidated row count + schema match
        // the adapter's metadata.
        let source = CountingInMemorySource::new(bytes);
        let mut adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        assert_eq!(
            adapter.metadata().row_group(0).num_rows() as usize,
            oracle.num_rows(),
        );
        let adapter_schema = adapter.metadata().file_metadata().schema_descr();
        assert_eq!(adapter_schema.num_columns(), oracle.num_columns());
        for i in 0..adapter_schema.num_columns() {
            assert_eq!(
                adapter_schema.column(i).name(),
                oracle.schema().field(i).name()
            );
        }

        // Drain pages so we exercise the full streaming path.
        let pages = drain_pages_via_trait(&mut adapter).await;
        assert!(!pages.is_empty(), "non-empty input must yield pages");
    }

    #[tokio::test]
    async fn test_kv_metadata_preserved() {
        let kvs = vec![
            KeyValue::new(
                "qh.sort_fields".to_string(),
                "metric_name asc, timestamp_secs desc".to_string(),
            ),
            KeyValue::new("qh.window_start_secs".to_string(), "1700000000".to_string()),
        ];
        let batch_a = make_metrics_batch(40);
        let batch_b = make_metrics_batch(40);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a, batch_b],
            kvs.clone(),
            default_sorting_cols(&arrow_schema),
            40,
        );
        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        let actual_kv = adapter
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        for expected in &kvs {
            let found = actual_kv
                .iter()
                .find(|kv| kv.key == expected.key)
                .unwrap_or_else(|| panic!("missing kv key {:?}", expected.key));
            assert_eq!(
                found.value, expected.value,
                "value mismatch for key {:?}",
                expected.key,
            );
        }
    }

    #[tokio::test]
    async fn test_sorting_columns_preserved() {
        let batch_a = make_metrics_batch(30);
        let batch_b = make_metrics_batch(30);
        let arrow_schema = batch_a.schema();
        let sorting_cols = default_sorting_cols(&arrow_schema);
        let bytes = write_multi_rg_file(&[batch_a, batch_b], Vec::new(), sorting_cols.clone(), 30);
        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        let actual = adapter
            .metadata()
            .row_group(0)
            .sorting_columns()
            .cloned()
            .expect("sorting_columns must be carried through");
        assert_eq!(actual, sorting_cols);
    }

    #[tokio::test]
    async fn test_dict_and_null_columns_preserved() {
        // The fixture's `service` column is a Dictionary with nulls
        // every 5th row. Round-trip through the adapter must preserve
        // both the data values and the null mask.
        let batch_a = make_metrics_batch(60);
        let batch_b = make_metrics_batch(60);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a.clone(), batch_b.clone()],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            60,
        );
        let oracle = read_back_to_single_batch(bytes.clone());

        let source = CountingInMemorySource::new(bytes);
        let mut adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");
        // Drain to drive the streaming path.
        let pages = drain_pages_via_trait(&mut adapter).await;
        assert!(!pages.is_empty());

        // Verify the adapter's metadata describes the same physical
        // schema (dictionary columns must remain dictionary-encoded
        // physically — the writer config preserves dict encoding for
        // Dictionary(_, _) fields by default).
        let oracle_schema = oracle.schema();
        let service_idx = oracle_schema
            .index_of("service")
            .expect("service column present");
        let service_field = oracle_schema.field(service_idx);
        assert!(
            matches!(service_field.data_type(), DataType::Dictionary(_, _)),
            "oracle service field must be Dictionary",
        );
        assert!(service_field.is_nullable(), "service must be nullable");

        // Confirm the adapter's sum of data-page rows equals the input
        // row count for every column, including the nullable dict.
        let mut rows_per_col: std::collections::BTreeMap<usize, i64> =
            std::collections::BTreeMap::new();
        for p in pages {
            if matches!(
                p.header.type_,
                parquet::format::PageType::DATA_PAGE | parquet::format::PageType::DATA_PAGE_V2,
            ) {
                let n = p
                    .header
                    .data_page_header
                    .as_ref()
                    .map(|h| h.num_values as i64)
                    .or_else(|| {
                        p.header
                            .data_page_header_v2
                            .as_ref()
                            .map(|h| h.num_values as i64)
                    })
                    .unwrap_or(0);
                *rows_per_col.entry(p.col_idx).or_insert(0) += n;
            }
        }
        let expected_rows = oracle.num_rows() as i64;
        for col_idx in 0..oracle.num_columns() {
            let actual = rows_per_col.get(&col_idx).copied().unwrap_or(0);
            assert_eq!(
                actual, expected_rows,
                "col {col_idx}: data-page num_values sum",
            );
        }
    }

    #[tokio::test]
    async fn test_io_failure_surfaces_as_io_error() {
        let source: Arc<dyn RemoteByteSource> = Arc::new(AlwaysFailingSliceSource {
            // Pretend the file is non-empty so the adapter actually
            // attempts the buffered GET (a zero-sized file would short-
            // circuit).
            file_size: 4096,
        });

        match LegacyInputAdapter::try_open(source, dummy_path(), 0).await {
            Err(LegacyAdapterError::Io(err)) => {
                assert!(
                    err.to_string().contains("simulated"),
                    "expected the simulated I/O error to be propagated; got {err}",
                );
            }
            Err(other) => panic!(
                "expected LegacyAdapterError::Io carrying the original io::Error; got error \
                 variant: {other}",
            ),
            Ok(_) => panic!("expected adapter open to fail when get_slice errors"),
        }
    }

    /// Cell-equal data round trip through the re-encode helper.
    ///
    /// `test_data_roundtrip_through_adapter` checks row count + schema
    /// names through the streaming path; that catches dropped rows but
    /// not value-level corruption (e.g., a hypothetical dictionary key
    /// XOR or column-value swap during the decode/concat/re-encode
    /// chain). This test calls `reencode_as_single_row_group` directly
    /// against a fixture with both nullable and dictionary-encoded
    /// columns, reads the re-encoded bytes back via the standard
    /// reader, and asserts each column equals the oracle byte-for-byte.
    #[test]
    fn test_reencode_preserves_arrays_byte_equal() {
        // Three RGs (50 rows each) so the consolidator actually has
        // multiple input batches to concatenate. The fixture exercises
        // dict columns and nulls in `service`.
        let batch_a = make_metrics_batch(50);
        let batch_b = make_metrics_batch(50);
        let batch_c = make_metrics_batch(50);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a, batch_b, batch_c],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            50,
        );
        let oracle = read_back_to_single_batch(bytes.clone());

        let reencoded = reencode_prefix_aligned(bytes, 0).expect("reencode helper");
        let reencoded_batch = read_back_to_single_batch(Bytes::from(reencoded));

        assert_eq!(reencoded_batch.num_rows(), oracle.num_rows());
        assert_eq!(reencoded_batch.num_columns(), oracle.num_columns());

        let oracle_schema = oracle.schema();
        for col_idx in 0..oracle.num_columns() {
            let oracle_col = oracle.column(col_idx);
            let reencoded_col = reencoded_batch.column(col_idx);
            assert_eq!(
                oracle_col.as_ref(),
                reencoded_col.as_ref(),
                "column '{}' (index {col_idx}) differs after re-encode",
                oracle_schema.field(col_idx).name(),
            );
        }
    }

    #[tokio::test]
    async fn test_satisfies_column_page_stream_trait() {
        let batch_a = make_metrics_batch(80);
        let batch_b = make_metrics_batch(80);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a, batch_b],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            80,
        );
        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        // Inspect metadata via trait dispatch.
        let num_rgs_via_trait = {
            let stream: &dyn ColumnPageStream = &adapter;
            stream.metadata().num_row_groups()
        };
        assert_eq!(num_rgs_via_trait, 1);

        // Drain via trait dispatch and confirm idempotent EOF.
        let mut adapter = adapter;
        let pages = drain_pages_via_trait(&mut adapter).await;
        assert!(!pages.is_empty());
        for p in &pages {
            assert_eq!(p.rg_idx, 0);
        }
        for _ in 0..3 {
            let stream: &mut dyn ColumnPageStream = &mut adapter;
            assert!(stream.next_page().await.expect("idempotent EOF").is_none());
        }
    }

    /// Real legacy files carry `qh.sort_fields` and are written sorted
    /// by the schema. The adapter must split the consolidated batch
    /// into one RG per first-sort-col value and stamp the re-encoded
    /// file with `qh.rg_partition_prefix_len = 1` so the merge engine
    /// reads it through the prefix-aware fast path. The streaming
    /// engine's duplicate-prefix invariant verifies on read that each
    /// RG's metric_name is unique within the file; this test
    /// indirectly exercises that contract.
    #[tokio::test]
    async fn test_legacy_input_with_sort_fields_produces_prefix_aligned_multi_rg() {
        let metrics = [
            ("cpu.usage", 40usize),
            ("memory.used", 40),
            ("net.bytes", 40),
        ];
        // Force multi-RG layout in the input (rows_per_rg=30, smaller
        // than any metric run) so the fixture proves the adapter
        // collapses arbitrary input RG boundaries into prefix-aligned
        // output RG boundaries.
        let bytes =
            write_sorted_multi_rg_legacy_file(&metrics, "metric_name|-timestamp_secs/V2", 30);
        let pre_builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("pre-builder");
        assert!(
            pre_builder.metadata().num_row_groups() >= 2,
            "fixture must produce multi-RG input; got {}",
            pre_builder.metadata().num_row_groups(),
        );

        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 1)
            .await
            .expect("adapter open");

        // Three distinct metric_names → three output RGs.
        assert_eq!(
            adapter.metadata().num_row_groups(),
            3,
            "adapter must emit one RG per distinct first-sort-col value",
        );
        let rg_rows: Vec<i64> = (0..adapter.metadata().num_row_groups())
            .map(|i| adapter.metadata().row_group(i).num_rows())
            .collect();
        assert_eq!(rg_rows, vec![40, 40, 40], "row counts per RG");

        // KV must advertise prefix_len = 1.
        let kv = adapter
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("kv metadata");
        let prefix_kv = kv
            .iter()
            .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
            .and_then(|k| k.value.clone());
        assert_eq!(
            prefix_kv.as_deref(),
            Some("1"),
            "re-encoded file must declare rg_partition_prefix_len=1",
        );

        // F9 chunk-level verification: the count + KV checks above
        // would still pass if `compute_prefix_value_slices` had an
        // off-by-one in its boundary detection. PA-1 + PA-3 on chunk
        // statistics nail down that each RG's metric_name column is
        // actually constant and no two RGs share a value.
        crate::merge::streaming::region_grouping::assert_unique_rg_prefix_keys(
            adapter.metadata(),
            "metric_name|-timestamp_secs/V2",
            1,
            "test_legacy_input_with_sort_fields_produces_prefix_aligned_multi_rg adapter output",
        )
        .expect("adapter output must satisfy PA-1 + PA-3 on metric_name");
    }

    /// Single-metric legacy file: only one prefix value, so the
    /// re-encoded file has exactly one RG (vacuously prefix-aligned).
    /// The `qh.rg_partition_prefix_len = 1` KV is still set so the
    /// reader's duplicate-prefix check has nothing to validate (one
    /// RG can never violate the invariant) and the file looks
    /// identical to a metric-aligned new-format file.
    #[tokio::test]
    async fn test_legacy_input_single_metric_yields_one_rg_with_prefix_kv() {
        let metrics = [("cpu.usage", 90usize)];
        let bytes =
            write_sorted_multi_rg_legacy_file(&metrics, "metric_name|-timestamp_secs/V2", 30);
        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 1)
            .await
            .expect("adapter open");

        assert_eq!(adapter.metadata().num_row_groups(), 1);
        assert_eq!(adapter.metadata().row_group(0).num_rows(), 90);

        let prefix_kv = adapter
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
                    .and_then(|k| k.value.clone())
            });
        assert_eq!(prefix_kv.as_deref(), Some("1"));
    }

    /// `target_prefix_len = 0`: the adapter consolidates into a
    /// single row group and does NOT stamp
    /// `qh.rg_partition_prefix_len`, regardless of what the original
    /// file's KV says. This is the "all-legacy merge with no non-
    /// legacy peers" path — the merge engine's `prefix_len = 0`
    /// sub-region splitting consumes it directly.
    #[tokio::test]
    async fn test_target_prefix_len_zero_passes_through_as_single_rg() {
        // Even with a parseable sort_fields KV, target = 0 must not
        // alter the layout or stamp the prefix KV.
        let metrics = [("cpu.usage", 50usize), ("memory.used", 50)];
        let bytes =
            write_sorted_multi_rg_legacy_file(&metrics, "metric_name|-timestamp_secs/V2", 30);

        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 0)
            .await
            .expect("adapter open");

        assert_eq!(
            adapter.metadata().num_row_groups(),
            1,
            "target_prefix_len = 0 must consolidate to a single RG",
        );
        let prefix_kv = adapter
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
            "target_prefix_len = 0 must not stamp a prefix_len value; got {prefix_kv:?}",
        );
    }

    /// `target_prefix_len > 0` on a file with no `qh.sort_fields` KV
    /// must surface a `PrefixUnresolvable` error rather than silently
    /// fall through. The caller decides whether to retry at a smaller
    /// `target_prefix_len` or with `0`.
    #[tokio::test]
    async fn test_target_prefix_len_one_without_sort_fields_returns_unresolvable() {
        // No sort_fields KV → cannot resolve any prefix column.
        let batch_a = make_metrics_batch(40);
        let arrow_schema = batch_a.schema();
        let bytes = write_multi_rg_file(
            &[batch_a],
            Vec::new(),
            default_sorting_cols(&arrow_schema),
            40,
        );

        let source = CountingInMemorySource::new(bytes);
        let result = LegacyInputAdapter::try_open(source, dummy_path(), 1).await;
        let Err(err) = result else {
            panic!("missing sort_fields must surface as PrefixUnresolvable, got Ok(...)");
        };
        match err {
            LegacyAdapterError::PrefixUnresolvable { target, reason } => {
                assert_eq!(target, 1);
                assert!(
                    reason.contains("sort_fields"),
                    "reason should mention sort_fields KV; got: {reason}",
                );
            }
            other => panic!("expected PrefixUnresolvable, got: {other}"),
        }
    }

    /// `target_prefix_len > declared sort cols` must also bail with
    /// `PrefixUnresolvable`. Confirms the caller-driven negotiation
    /// contract: the adapter never silently advertises a smaller
    /// alignment than asked for.
    #[tokio::test]
    async fn test_target_prefix_len_exceeds_declared_sort_cols_returns_unresolvable() {
        // Two-col sort schema → ask for prefix_len = 3 → bail.
        let metrics = [("cpu.usage", 30usize), ("memory.used", 30)];
        let bytes =
            write_sorted_multi_rg_legacy_file(&metrics, "metric_name|-timestamp_secs/V2", 30);

        let source = CountingInMemorySource::new(bytes);
        let result = LegacyInputAdapter::try_open(source, dummy_path(), 3).await;
        let Err(err) = result else {
            panic!("prefix_len exceeding declared sort cols must surface as Unresolvable");
        };
        match err {
            LegacyAdapterError::PrefixUnresolvable { target, reason } => {
                assert_eq!(target, 3);
                assert!(
                    reason.contains("declares only"),
                    "reason should mention sort col count; got: {reason}",
                );
            }
            other => panic!("expected PrefixUnresolvable, got: {other}"),
        }
    }

    /// Composite prefix (`target_prefix_len = 2`): each output RG
    /// carries a unique `(metric_name, service)` tuple. Exercises the
    /// N > 1 path of `compute_prefix_value_slices` and confirms the
    /// stamped KV reflects the caller's request.
    #[tokio::test]
    async fn test_target_prefix_len_two_splits_by_metric_and_service() {
        // 4 (metric, service) groups × 20 rows; sorted ascending by
        // (metric_name, service). Multi-RG input layout (rows_per_rg=25)
        // forces consolidation before re-splitting.
        let groups = [
            ("cpu.usage", "api", 20usize),
            ("cpu.usage", "db", 20),
            ("memory.used", "api", 20),
            ("memory.used", "cache", 20),
        ];
        let bytes = write_sorted_composite_legacy_file(
            &groups,
            "metric_name|service|-timestamp_secs/V2",
            25,
        );

        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 2)
            .await
            .expect("adapter open with prefix_len=2");

        // 4 distinct (metric_name, service) tuples → 4 output RGs.
        assert_eq!(
            adapter.metadata().num_row_groups(),
            4,
            "composite prefix must split at (metric, service) transitions",
        );
        let rg_rows: Vec<i64> = (0..adapter.metadata().num_row_groups())
            .map(|i| adapter.metadata().row_group(i).num_rows())
            .collect();
        assert_eq!(rg_rows, vec![20, 20, 20, 20]);

        let prefix_kv = adapter
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
                    .and_then(|k| k.value.clone())
            });
        assert_eq!(
            prefix_kv.as_deref(),
            Some("2"),
            "stamped prefix_len must match caller's request",
        );

        // F9 chunk-level verification: a `compute_prefix_value_slices`
        // bug splitting on only the first prefix col (or off by one)
        // would still yield 4 RGs of [20,20,20,20] but with the wrong
        // CONTENTS. PA-1 + PA-3 on the composite (metric, service)
        // composite key verifies content alignment directly.
        crate::merge::streaming::region_grouping::assert_unique_rg_prefix_keys(
            adapter.metadata(),
            "metric_name|service|-timestamp_secs/V2",
            2,
            "test_target_prefix_len_two_splits_by_metric_and_service adapter output",
        )
        .expect("composite prefix output must satisfy PA-1 + PA-3");
    }

    /// SS-3: a sort column named in `qh.sort_fields` but missing from
    /// the arrow schema is treated as implicitly null at every row.
    /// Null at every row is constant, so the column trivially
    /// satisfies any prefix-alignment claim involving it — the
    /// adapter must NOT bail with `PrefixUnresolvable` in this case.
    /// Transitions are driven purely by the columns that ARE present.
    ///
    /// Fixture: sort_fields declares `metric_name|env|-timestamp_secs`
    /// but the schema doesn't contain `env`. With prefix_len = 2 the
    /// adapter must succeed, split only at `metric_name` transitions
    /// (the null `env` contributes no transitions), and stamp
    /// `prefix_len = 2` on the output.
    #[tokio::test]
    async fn test_missing_prefix_col_treated_as_null_satisfies_alignment() {
        let metrics = [("cpu.usage", 30usize), ("memory.used", 30)];
        // Sort schema declares 3 cols; the fixture schema has
        // metric_name and timestamp_secs but NO `env` column. Per
        // SS-3 the merge engine treats `env` as null at every row.
        let bytes =
            write_sorted_multi_rg_legacy_file(&metrics, "metric_name|env|-timestamp_secs/V2", 30);

        let source = CountingInMemorySource::new(bytes);
        let adapter = LegacyInputAdapter::try_open(source, dummy_path(), 2)
            .await
            .expect("missing-col-as-null must satisfy alignment without erroring");

        // Two metrics × constant null `env` → two output RGs (one per metric).
        assert_eq!(
            adapter.metadata().num_row_groups(),
            2,
            "missing prefix col (treated as null) contributes no transitions; only metric_name \
             transitions split RGs",
        );
        let rg_rows: Vec<i64> = (0..adapter.metadata().num_row_groups())
            .map(|i| adapter.metadata().row_group(i).num_rows())
            .collect();
        assert_eq!(rg_rows, vec![30, 30]);

        let prefix_kv = adapter
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|k| k.key == PARQUET_META_RG_PARTITION_PREFIX_LEN)
                    .and_then(|k| k.value.clone())
            });
        assert_eq!(
            prefix_kv.as_deref(),
            Some("2"),
            "stamped prefix_len must match caller's request even when one col is implicitly null",
        );

        // F10 + F12 chunk-level verification: SS-3 is now honored on
        // BOTH sides — `find_prefix_parquet_col_indices` returns
        // `Option<PrefixColumn>` (None for "named in sort schema but
        // missing from parquet schema"), and
        // `extract_rg_composite_prefix_key` emits a constant null
        // marker for those slots. So the adapter's output (which
        // stamps prefix_len = 2 but omits `env` from the schema)
        // passes the unified PA-1 + PA-3 verifier: each RG's
        // metric_name min == max, and the constant null contribution
        // for `env` makes RG composite keys differ only by
        // metric_name.
        crate::merge::streaming::region_grouping::assert_unique_rg_prefix_keys(
            adapter.metadata(),
            "metric_name|env|-timestamp_secs/V2",
            2,
            "test_missing_prefix_col_treated_as_null_satisfies_alignment adapter output",
        )
        .expect("SS-3 null col must satisfy PA-1 + PA-3 (null is constant across all RGs)");
    }

    /// F8 regression: an unsorted legacy input (rows
    /// `[A,A,B,B,A,A]` on `metric_name`) violates SS-1. Walking
    /// row-by-row to find prefix transitions would emit three slices —
    /// `A`, `B`, `A` — and synthesize a file with two RGs sharing the
    /// prefix value `A`, violating PA-3. The downstream streaming
    /// merge engine would catch this later, but only once the bad
    /// file had been built and possibly archived. The adapter must
    /// bail upfront with `InputNotSorted` so no PA-3-violating file
    /// ever lands on disk.
    #[tokio::test]
    async fn test_unsorted_legacy_input_rejected_by_adapter() {
        // metric_name in row order: cpu.usage, memory.used, cpu.usage.
        // That's an SS-1 violation under sort schema `metric_name ASC`.
        let bad_metrics = [
            ("cpu.usage", 20usize),
            ("memory.used", 20),
            ("cpu.usage", 20),
        ];
        let bytes =
            write_sorted_multi_rg_legacy_file(&bad_metrics, "metric_name|-timestamp_secs/V2", 20);

        let source = CountingInMemorySource::new(bytes);
        let result = LegacyInputAdapter::try_open(source, dummy_path(), 1).await;
        let Err(err) = result else {
            panic!(
                "unsorted legacy input must surface as InputNotSorted, got Ok(...) — the adapter \
                 would have written a PA-3-violating file"
            );
        };
        match err {
            LegacyAdapterError::InputNotSorted {
                target,
                first_offset,
                second_offset,
            } => {
                assert_eq!(target, 1);
                // First `cpu.usage` run is at offset 0; second is at
                // offset 40 (after the 20-row `cpu.usage` then 20-row
                // `memory.used` runs).
                assert_eq!(
                    first_offset, 0,
                    "first duplicate prefix offset should point at the first cpu.usage run",
                );
                assert_eq!(
                    second_offset, 40,
                    "second duplicate prefix offset should point at the second cpu.usage run",
                );
            }
            other => panic!("expected InputNotSorted, got: {other}"),
        }
    }

    /// Composite-prefix fixture: rows grouped by `(metric, service)`
    /// in the order supplied. Used by the prefix_len=2 test to verify
    /// transitions on the second prefix column trigger RG splits.
    fn write_sorted_composite_legacy_file(
        groups: &[(&str, &str, usize)],
        sort_fields_value: &str,
        rows_per_rg: usize,
    ) -> Bytes {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));

        // Build per-group dictionary index tables for metric_name and
        // service. Map each distinct value to its key.
        let mut metric_names_vec: Vec<&str> = Vec::new();
        let mut service_values_vec: Vec<&str> = Vec::new();
        for (metric, service, _) in groups {
            if !metric_names_vec.contains(metric) {
                metric_names_vec.push(metric);
            }
            if !service_values_vec.contains(service) {
                service_values_vec.push(service);
            }
        }

        let total: usize = groups.iter().map(|(_, _, n)| *n).sum();
        let mut metric_keys: Vec<i32> = Vec::with_capacity(total);
        let mut svc_keys: Vec<Option<i32>> = Vec::with_capacity(total);
        let mut tsids: Vec<i64> = Vec::with_capacity(total);
        let mut timestamps: Vec<u64> = Vec::with_capacity(total);
        let mut values: Vec<f64> = Vec::with_capacity(total);
        let mut row_idx: u64 = 0;
        for (metric, service, count) in groups {
            let metric_key = metric_names_vec
                .iter()
                .position(|m| m == metric)
                .expect("metric known") as i32;
            let svc_key = service_values_vec
                .iter()
                .position(|s| s == service)
                .expect("service known") as i32;
            for _ in 0..*count {
                metric_keys.push(metric_key);
                svc_keys.push(Some(svc_key));
                tsids.push(1000 + row_idx as i64);
                timestamps.push(1_700_000_000 + (*count as u64) - (row_idx % *count as u64));
                values.push(row_idx as f64);
                row_idx += 1;
            }
        }

        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(StringArray::from(metric_names_vec)),
            )
            .expect("metric dict"),
        );
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(StringArray::from(service_values_vec)),
            )
            .expect("svc dict"),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; total]));
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .expect("composite fixture batch");

        let kvs = vec![KeyValue::new(
            PARQUET_META_SORT_FIELDS.to_string(),
            sort_fields_value.to_string(),
        )];
        let sorting_cols = default_sorting_cols(&schema);
        write_multi_rg_file(&[batch], kvs, sorting_cols, rows_per_rg)
    }
}
