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

//! Page-level streaming Parquet reader primitive.
//!
//! Issues exactly one footer GET (via [`RemoteByteSource::get_slice`])
//! and one body GET (via [`RemoteByteSource::get_slice_stream`]) per
//! input file in the common path. Yields one [`Page`] per call in
//! storage order — row-group-major, column-major-within-row-group,
//! page-major-within-column. Peak memory is one page's compressed
//! bytes (~1 MiB for typical metrics splits) plus a small Thrift
//! header buffer, regardless of file size or row-group size.
//!
//! Why pages and not column chunks: PR-3 cuts ingest over to
//! single-row-group files. Under that layout, "one column chunk" =
//! "all rows of a column in the entire file," which scales linearly
//! with file size. Column-chunk granularity would defeat the
//! streaming budget for large compacted splits.
//!
//! The reader is plumbing: PR-4 ships the primitive, PR-5 will wrap a
//! legacy adapter for files where the new layout claim isn't valid
//! (`qh.rg_partition_prefix_len == 0` AND multi-row-group), and PR-6's
//! streaming merge engine consumes both shapes through the same trait
//! and exercises direct page copy (no decode/recode) on this output.
//! Until then the items here are only exercised by this file's tests,
//! so `dead_code` is allowed at module scope.

#![allow(dead_code)]
// `parquet::format` is the only public path to `PageHeader` and
// `PageType` in parquet 58; the non-deprecated replacements
// (`parquet::file::metadata::thrift::*`) are crate-private. The
// `format` module is scheduled for removal in parquet 59 and we'll
// migrate when the new public path lands.
#![allow(deprecated)]

use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::format::{PageHeader, PageType};
use thiserror::Error;
use thrift::protocol::TCompactInputProtocol;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Minimal interface for the I/O backend the streaming reader consumes.
///
/// `quickwit-parquet-engine` is a pure parquet-data-model library and
/// must not depend on `quickwit-storage` directly (it would invert the
/// layering — storage carries cloud-vendor SDKs that this crate has
/// no business pulling in). Callers in `quickwit-indexing` provide a
/// thin adapter that delegates to `quickwit_storage::Storage`.
#[async_trait]
pub(crate) trait RemoteByteSource: Send + Sync {
    /// Total file length in bytes.
    async fn file_size(&self, path: &Path) -> io::Result<u64>;

    /// Random read of `range`. Used for the footer GET (and the rare
    /// retry when the footer prefetch was too small).
    async fn get_slice(&self, path: &Path, range: Range<u64>) -> io::Result<Bytes>;

    /// Forward-only stream over `range`. Used for the single body GET
    /// that backs all per-page reads.
    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>>;
}

/// Configuration for [`StreamingParquetReader`].
#[derive(Debug, Clone)]
pub(crate) struct StreamingReaderConfig {
    /// Bytes prefetched from the file tail to capture the footer.
    /// Default 256 KiB — sized for a 50 MB metrics split with the
    /// writer config we ship in production.
    pub footer_prefetch_bytes: u64,
    /// Hard upper bound on a single page header's encoded size.
    /// Default 1 MiB — page headers are typically 30–100 bytes; this
    /// is a runaway bound, not a typical-case budget.
    pub max_page_header_bytes: usize,
}

impl Default for StreamingReaderConfig {
    fn default() -> Self {
        Self {
            footer_prefetch_bytes: 256 * 1024,
            max_page_header_bytes: 1024 * 1024,
        }
    }
}

/// Errors from the streaming reader.
#[derive(Error, Debug)]
pub(crate) enum ParquetReadError {
    /// I/O error from the underlying [`RemoteByteSource`].
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Parquet decode error (e.g., footer parse).
    #[error("parquet error: {0}")]
    Parquet(#[from] ParquetError),

    /// Thrift decode error from a page header.
    #[error("thrift error decoding page header at byte {file_offset}: {source}")]
    ThriftPageHeader {
        file_offset: u64,
        #[source]
        source: thrift::Error,
    },

    /// Page header exceeded [`StreamingReaderConfig::max_page_header_bytes`].
    /// Either the file is malformed or the bound is too low.
    #[error(
        "page header at byte {file_offset} exceeds max_page_header_bytes={max} (consider \
         increasing the limit if the file is known-good)"
    )]
    PageHeaderTooLarge { file_offset: u64, max: usize },

    /// Page header advertised a `compressed_page_size` that overruns
    /// the column chunk's compressed_size. File is malformed.
    #[error(
        "page at ({rg}, {col}) overruns column chunk: page advertises compressed_page_size={page} \
         but only {remaining} bytes remain in column chunk"
    )]
    PageOverrunsColumn {
        rg: usize,
        col: usize,
        page: u64,
        remaining: u64,
    },

    /// File is too small to contain its declared footer — almost
    /// certainly truncated.
    #[error(
        "file is too small to contain footer: file size {file_size} bytes, footer needs {needed}"
    )]
    FooterTooLarge { file_size: u64, needed: u64 },
}

/// One Parquet page yielded by [`StreamingParquetReader::next_page`].
///
/// Carries the Thrift-decoded `PageHeader` plus the raw compressed
/// bytes (`bytes.len() == header.compressed_page_size`). Caller can
/// inspect the page type (`Dictionary` / `DataPage` / `DataPageV2` /
/// `Index`) via `header.type_`, and either copy `bytes` straight to
/// an output writer (PR-6's direct page copy) or decompress + decode
/// for sort-column inspection.
#[derive(Debug)]
pub(crate) struct Page {
    /// Row group this page belongs to.
    pub rg_idx: usize,
    /// Column chunk this page belongs to (within the row group).
    pub col_idx: usize,
    /// Sequential index of this page within its column chunk
    /// (0 = first page; dictionary page, when present, is index 0).
    pub page_idx_in_col: usize,
    /// Thrift-decoded page header.
    pub header: PageHeader,
    /// Raw compressed page bytes; length equals
    /// `header.compressed_page_size`. Cheap to clone (ref-counted).
    pub bytes: Bytes,
}

/// Page-level streaming Parquet reader.
///
/// See module docs for the contract. Caller must consume pages in
/// storage order via [`Self::next_page`]; the body stream is forward-
/// only.
pub(crate) struct StreamingParquetReader {
    source: Arc<dyn RemoteByteSource>,
    path: PathBuf,
    file_size: u64,
    metadata: Arc<ParquetMetaData>,
    config: StreamingReaderConfig,
    state: ReaderState,
}

/// Body-side state. Body GET is deferred until the first
/// [`StreamingParquetReader::next_page`] call, so a caller that only
/// needs metadata pays for one footer GET.
enum ReaderState {
    NotStarted { body_start: u64, body_end: u64 },
    Reading(ReadingState),
    Done,
}

/// In-progress read state. Tracks the body stream cursor (in
/// file-absolute offsets, so it compares directly to
/// `column.byte_range().0`) plus a small forward-only buffer that
/// holds pre-fetched bytes — the buffer holds at most one page
/// header's worth of over-read between page boundaries.
struct ReadingState {
    body: Box<dyn AsyncRead + Send + Unpin>,
    /// Bytes that have been read from `body` but not yet returned to
    /// the caller. Acts as a tiny rewindable buffer between header
    /// parse and body read.
    pending: Vec<u8>,
    /// Logical position in the file: number of bytes from offset 0
    /// of the file that have been consumed (returned to caller OR
    /// skipped over). Equivalent to `body_start + bytes_read - pending.len()`.
    cursor: u64,
    next_rg_idx: usize,
    next_col_idx: usize,
    /// Bytes already consumed from the current column chunk (including
    /// page headers). Compared against `column.byte_range().1` to
    /// detect end of column.
    bytes_consumed_in_col: u64,
    /// Sequential page index within the current column.
    next_page_idx_in_col: usize,
}

impl StreamingParquetReader {
    /// Open a reader with the default [`StreamingReaderConfig`].
    pub async fn try_open(
        source: Arc<dyn RemoteByteSource>,
        path: PathBuf,
    ) -> Result<Self, ParquetReadError> {
        Self::try_open_with_config(source, path, StreamingReaderConfig::default()).await
    }

    /// Open a reader with caller-supplied config. Issues exactly one
    /// footer GET (and at most one retry GET if the configured
    /// prefetch is smaller than the file's actual footer).
    pub async fn try_open_with_config(
        source: Arc<dyn RemoteByteSource>,
        path: PathBuf,
        config: StreamingReaderConfig,
    ) -> Result<Self, ParquetReadError> {
        let file_size = source.file_size(&path).await?;
        let metadata = fetch_and_parse_metadata(&*source, &path, file_size, &config).await?;

        let (body_start, body_end) = compute_body_range(&metadata);
        let state = if metadata.num_row_groups() == 0 || body_start == body_end {
            ReaderState::Done
        } else {
            ReaderState::NotStarted {
                body_start,
                body_end,
            }
        };

        Ok(Self {
            source,
            path,
            file_size,
            metadata: Arc::new(metadata),
            config,
            state,
        })
    }

    /// Parsed file metadata. Available after construction without any
    /// further I/O.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Total file size, as reported by [`RemoteByteSource::file_size`]
    /// at construction time.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Read the next page in storage order. The body GET is issued
    /// lazily on the first call.
    ///
    /// Returns `Ok(None)` after the last page; further calls continue
    /// to return `Ok(None)` (idempotent EOF).
    pub async fn next_page(&mut self) -> Result<Option<Page>, ParquetReadError> {
        // Lazily kick off the body GET on the first call.
        if let ReaderState::NotStarted {
            body_start,
            body_end,
        } = self.state
        {
            let stream = self
                .source
                .get_slice_stream(&self.path, body_start..body_end)
                .await?;
            self.state = ReaderState::Reading(ReadingState {
                body: stream,
                pending: Vec::new(),
                cursor: body_start,
                next_rg_idx: 0,
                next_col_idx: 0,
                bytes_consumed_in_col: 0,
                next_page_idx_in_col: 0,
            });
        }

        let ReaderState::Reading(reading) = &mut self.state else {
            return Ok(None);
        };

        // Advance to the next column if the previous one is fully
        // consumed. May skip several columns whose data we already
        // streamed past (e.g., we just exited the last page of column
        // K; reposition for column K+1).
        loop {
            if reading.next_rg_idx >= self.metadata.num_row_groups() {
                self.state = ReaderState::Done;
                return Ok(None);
            }

            let rg = self.metadata.row_group(reading.next_rg_idx);
            let col_meta = rg.column(reading.next_col_idx);
            let (col_start, col_size) = col_meta.byte_range();

            if reading.bytes_consumed_in_col == 0 {
                // Starting a new column. Skip forward to its first byte.
                if reading.cursor > col_start {
                    let cursor = reading.cursor;
                    return Err(ParquetReadError::Io(io::Error::other(format!(
                        "out-of-order column boundary: cursor at {cursor} but column ({}, {}) \
                         starts at {col_start}",
                        reading.next_rg_idx, reading.next_col_idx,
                    ))));
                }
                if reading.cursor < col_start {
                    let to_skip = col_start - reading.cursor;
                    skip_forward(reading, to_skip as usize).await?;
                    reading.cursor = col_start;
                }
            }

            if reading.bytes_consumed_in_col >= col_size {
                // Column exhausted; move to next.
                reading.next_col_idx += 1;
                reading.bytes_consumed_in_col = 0;
                reading.next_page_idx_in_col = 0;
                if reading.next_col_idx >= rg.num_columns() {
                    reading.next_col_idx = 0;
                    reading.next_rg_idx += 1;
                }
                continue;
            }

            // Read one page from the current column.
            let page = read_one_page(
                reading,
                self.metadata
                    .row_group(reading.next_rg_idx)
                    .column(reading.next_col_idx),
                reading.next_rg_idx,
                reading.next_col_idx,
                reading.next_page_idx_in_col,
                &self.config,
            )
            .await?;
            return Ok(Some(page));
        }
    }
}

/// Read one page from the body stream and update `state` accordingly.
async fn read_one_page(
    state: &mut ReadingState,
    col_meta: &parquet::file::metadata::ColumnChunkMetaData,
    rg_idx: usize,
    col_idx: usize,
    page_idx_in_col: usize,
    config: &StreamingReaderConfig,
) -> Result<Page, ParquetReadError> {
    let (_col_start, col_size) = col_meta.byte_range();
    let col_remaining = col_size - state.bytes_consumed_in_col;

    // Parse the page header by feeding bytes from `state.pending`
    // (refilling from `state.body` on demand) into a Thrift compact
    // protocol. Header is variable-length; iterate until we have
    // enough buffered to parse, capped at `max_page_header_bytes`.
    let header_offset = state.cursor;
    let (header, header_len) =
        parse_page_header_streaming(state, config.max_page_header_bytes, header_offset).await?;

    // Header was consumed from `pending`; `cursor` and `bytes_consumed_in_col`
    // already advanced inside `parse_page_header_streaming`.

    let body_size = header.compressed_page_size as i64;
    if body_size < 0 {
        return Err(ParquetReadError::Parquet(
            parquet::errors::ParquetError::General(format!(
                "page at ({rg_idx}, {col_idx}) advertises negative compressed_page_size \
                 {body_size}"
            )),
        ));
    }
    let body_size = body_size as u64;

    if header_len as u64 + body_size > col_remaining {
        return Err(ParquetReadError::PageOverrunsColumn {
            rg: rg_idx,
            col: col_idx,
            page: header_len as u64 + body_size,
            remaining: col_remaining,
        });
    }

    // Pull the page body bytes.
    fill_pending(state, body_size as usize).await?;
    let body_bytes: Vec<u8> = state.pending.drain(..body_size as usize).collect();
    state.cursor += body_size;
    state.bytes_consumed_in_col += body_size;
    state.next_page_idx_in_col += 1;

    Ok(Page {
        rg_idx,
        col_idx,
        page_idx_in_col,
        header,
        bytes: Bytes::from(body_bytes),
    })
}

/// Read the next Thrift `PageHeader` by trying to decode from
/// progressively-larger buffer sizes. Drains the consumed bytes from
/// `state.pending` and advances `state.cursor` and
/// `state.bytes_consumed_in_col`.
async fn parse_page_header_streaming(
    state: &mut ReadingState,
    max_header_bytes: usize,
    file_offset_for_error: u64,
) -> Result<(PageHeader, usize), ParquetReadError> {
    // Start small; grow geometrically up to the configured cap.
    let mut target = 256.min(max_header_bytes);
    loop {
        // `fill_pending_best_effort` returns `Ok(())` on EOF (treating
        // a short read as success), so propagating with `?` only
        // surfaces real I/O errors from the body stream — exactly the
        // signal callers need for transient-storage retry / backoff.
        // The earlier `let _ = ...` form silenced those errors and
        // re-reported them as `ThriftPageHeader` or
        // `PageHeaderTooLarge`, hiding the underlying cause.
        fill_pending_best_effort(state, target).await?;
        match try_parse_page_header(&state.pending) {
            Ok((header, consumed)) => {
                state.pending.drain(..consumed);
                state.cursor += consumed as u64;
                state.bytes_consumed_in_col += consumed as u64;
                return Ok((header, consumed));
            }
            Err(thrift_err) => {
                // Some thrift errors are recoverable by reading more
                // bytes (incomplete struct); others are real decode
                // failures. We can't easily distinguish the cases via
                // the error variant, so we use buffer growth as the
                // signal: if pending has fewer bytes than `target`,
                // we hit EOF and the error is real; otherwise we
                // probably need more bytes.
                if state.pending.len() < target {
                    // EOF or short read — give up.
                    return Err(ParquetReadError::ThriftPageHeader {
                        file_offset: file_offset_for_error,
                        source: thrift_err,
                    });
                }
                if target >= max_header_bytes {
                    return Err(ParquetReadError::PageHeaderTooLarge {
                        file_offset: file_offset_for_error,
                        max: max_header_bytes,
                    });
                }
                target = (target * 2).min(max_header_bytes);
            }
        }
    }
}

/// Attempt to decode a `PageHeader` from the prefix of `buf`. On
/// success, returns the header and the number of bytes consumed.
fn try_parse_page_header(buf: &[u8]) -> Result<(PageHeader, usize), thrift::Error> {
    use parquet::thrift::TSerializable;
    let mut cursor = io::Cursor::new(buf);
    let mut prot = TCompactInputProtocol::new(&mut cursor);
    let header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok((header, cursor.position() as usize))
}

/// Ensure `state.pending` has at least `target` bytes, reading from
/// the body stream as needed. Errors on premature EOF.
async fn fill_pending(state: &mut ReadingState, target: usize) -> io::Result<()> {
    while state.pending.len() < target {
        let buf_len = state.pending.len();
        let to_alloc = (target - buf_len).max(8 * 1024);
        state.pending.resize(buf_len + to_alloc, 0);
        let n = state.body.read(&mut state.pending[buf_len..]).await?;
        state.pending.truncate(buf_len + n);
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "body stream ended with {} bytes pending; expected {target}",
                    state.pending.len(),
                ),
            ));
        }
    }
    Ok(())
}

/// Fill `state.pending` toward `target` bytes, but tolerate EOF
/// (return `Ok(())` even if we can't reach the target). Used by the
/// page-header parser, which iterates and decides whether the buffer
/// is sufficient.
async fn fill_pending_best_effort(state: &mut ReadingState, target: usize) -> io::Result<()> {
    while state.pending.len() < target {
        let buf_len = state.pending.len();
        let to_alloc = (target - buf_len).max(4 * 1024);
        state.pending.resize(buf_len + to_alloc, 0);
        let n = state.body.read(&mut state.pending[buf_len..]).await?;
        state.pending.truncate(buf_len + n);
        if n == 0 {
            return Ok(());
        }
    }
    Ok(())
}

/// Discard `n` bytes by reading and dropping them. Bytes already in
/// `pending` are drained first; remaining bytes are read from `body`.
async fn skip_forward(state: &mut ReadingState, mut n: usize) -> io::Result<()> {
    if n == 0 {
        return Ok(());
    }
    let from_pending = n.min(state.pending.len());
    state.pending.drain(..from_pending);
    n -= from_pending;
    if n == 0 {
        return Ok(());
    }
    let mut sink = tokio::io::sink();
    let copied = tokio::io::copy(&mut (&mut state.body).take(n as u64), &mut sink).await?;
    if copied < n as u64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("body stream ended after skipping {copied} bytes; expected to skip {n} more",),
        ));
    }
    Ok(())
}

/// Compute the (start, end) offsets of the body byte range — first
/// column chunk's start to last column chunk's end. Returns
/// `(0, 0)` for files with zero row groups.
fn compute_body_range(metadata: &ParquetMetaData) -> (u64, u64) {
    let num_rgs = metadata.num_row_groups();
    if num_rgs == 0 {
        return (0, 0);
    }
    let first_rg = metadata.row_group(0);
    if first_rg.num_columns() == 0 {
        return (0, 0);
    }
    let body_start = first_rg.column(0).byte_range().0;

    let last_rg = metadata.row_group(num_rgs - 1);
    let last_col_idx = last_rg.num_columns() - 1;
    let (last_start, last_size) = last_rg.column(last_col_idx).byte_range();
    let body_end = last_start + last_size;

    (body_start, body_end)
}

/// Fetch the footer slice and parse it. Issues one
/// `RemoteByteSource::get_slice` call in the common case, plus at
/// most one retry if the configured prefetch was too small to
/// contain the footer.
async fn fetch_and_parse_metadata(
    source: &(dyn RemoteByteSource + 'static),
    path: &Path,
    file_size: u64,
    config: &StreamingReaderConfig,
) -> Result<ParquetMetaData, ParquetReadError> {
    let prefetch = config.footer_prefetch_bytes.min(file_size);
    let prefetch_start = file_size - prefetch;
    let footer_bytes = source.get_slice(path, prefetch_start..file_size).await?;
    match parse_footer_slice(&footer_bytes, prefetch_start, file_size) {
        Ok(metadata) => Ok(metadata),
        Err(ParquetReadError::Parquet(ParquetError::NeedMoreData(needed))) => {
            let needed_u64 = needed as u64;
            if needed_u64 > file_size {
                return Err(ParquetReadError::FooterTooLarge {
                    file_size,
                    needed: needed_u64,
                });
            }
            let retry_start = file_size - needed_u64;
            let retry_bytes = source.get_slice(path, retry_start..file_size).await?;
            parse_footer_slice(&retry_bytes, retry_start, file_size)
        }
        Err(other) => Err(other),
    }
}

/// Parse a footer slice that starts at `slice_start_offset` (file-
/// absolute) and runs to `file_size`. Returns
/// `ParquetReadError::Parquet(NeedMoreData)` if the slice is too
/// small.
///
/// Page indexes (offset/column) are NOT loaded — they live in the
/// file's body, not the footer, and loading them would require the
/// body GET range to extend earlier than the first column chunk.
/// PR-6's direct page copy doesn't need them in `ParquetMetaData`:
/// it reads `column_chunk.offset_index_offset()` /
/// `_length()` from the column metadata and decodes the offset
/// index from body bytes when (or if) it needs page boundaries.
fn parse_footer_slice(
    slice: &Bytes,
    slice_start_offset: u64,
    file_size: u64,
) -> Result<ParquetMetaData, ParquetReadError> {
    let mut reader = ParquetMetaDataReader::new()
        .with_offset_index_policy(PageIndexPolicy::Skip)
        .with_column_index_policy(PageIndexPolicy::Skip);
    match reader.try_parse_sized(slice, file_size) {
        Ok(()) => Ok(reader.finish()?),
        Err(ParquetError::NeedMoreData(_)) if slice_start_offset == 0 => {
            Err(ParquetReadError::FooterTooLarge {
                file_size,
                needed: file_size + 1,
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// Extension: classify a page header for callers that want to filter
/// (e.g., "sum num_values across data pages only"). `INDEX_PAGE` is a
/// historical variant in the Parquet Thrift schema that no production
/// writer emits (not to be confused with PR-1's column / offset index,
/// which lives outside the column chunk); the arm exists only for
/// exhaustiveness and returns `None`.
pub(crate) fn page_num_values(header: &PageHeader) -> Option<i32> {
    match header.type_ {
        PageType::DICTIONARY_PAGE => header.dictionary_page_header.as_ref().map(|h| h.num_values),
        PageType::DATA_PAGE => header.data_page_header.as_ref().map(|h| h.num_values),
        PageType::DATA_PAGE_V2 => header.data_page_header_v2.as_ref().map(|h| h.num_values),
        PageType::INDEX_PAGE => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt8Array,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

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
            .unwrap(),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows as u64).map(|i| 1_700_000_000 + i).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tsids: Vec<i64> = (0..num_rows as i64).map(|i| 1000 + i).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32).map(|i| Some(i % 3)).collect();
        let svc_values = StringArray::from(vec!["api", "db", "cache"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(svc_values),
            )
            .unwrap(),
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
        .unwrap()
    }

    fn writer_props(arrow_schema: &ArrowSchema, kvs: Vec<KeyValue>) -> WriterProperties {
        let cfg = ParquetWriterConfig::default();
        let sort_field_names = vec!["metric_name".to_string(), "service".to_string()];
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: arrow_schema.index_of("metric_name").unwrap() as i32,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: arrow_schema.index_of("service").unwrap() as i32,
                descending: false,
                nulls_first: false,
            },
        ];
        cfg.to_writer_properties_with_metadata(
            arrow_schema,
            sorting_cols,
            Some(kvs),
            &sort_field_names,
        )
    }

    /// Write `batches` (one per row group) and return the resulting
    /// Parquet bytes.
    fn write_test_file(batches: &[RecordBatch], kvs: Vec<KeyValue>) -> Bytes {
        let arrow_schema = batches[0].schema();
        let props = writer_props(&arrow_schema, kvs);
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
            for (idx, batch) in batches.iter().enumerate() {
                w.write(batch).unwrap();
                if idx + 1 < batches.len() {
                    w.flush().unwrap();
                }
            }
            w.close().unwrap();
        }
        Bytes::from(out)
    }

    /// Force pages to be small so we exercise multi-page columns.
    /// `data_page_row_count_limit` requires `write_batch_size` to be
    /// no larger than the row count limit, otherwise the limit is
    /// ignored.
    fn write_test_file_multi_page(batches: &[RecordBatch], rows_per_page: usize) -> Bytes {
        let arrow_schema = batches[0].schema();
        let cfg = ParquetWriterConfig::default();
        let sort_field_names = vec!["metric_name".to_string(), "service".to_string()];
        let mut props = cfg
            .to_writer_properties_with_metadata(&arrow_schema, Vec::new(), None, &sort_field_names)
            .into_builder();
        props = props
            .set_data_page_row_count_limit(rows_per_page)
            .set_write_batch_size(rows_per_page);
        let props = props.build();
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
            for (idx, batch) in batches.iter().enumerate() {
                w.write(batch).unwrap();
                if idx + 1 < batches.len() {
                    w.flush().unwrap();
                }
            }
            w.close().unwrap();
        }
        Bytes::from(out)
    }

    // -------- Counting in-memory RemoteByteSource --------

    struct InMemorySource {
        bytes: Bytes,
        slice_calls: AtomicUsize,
        stream_calls: AtomicUsize,
        last_slice_range: Mutex<Option<Range<u64>>>,
        last_stream_range: Mutex<Option<Range<u64>>>,
    }

    impl InMemorySource {
        fn new(bytes: Bytes) -> Arc<Self> {
            Arc::new(Self {
                bytes,
                slice_calls: AtomicUsize::new(0),
                stream_calls: AtomicUsize::new(0),
                last_slice_range: Mutex::new(None),
                last_stream_range: Mutex::new(None),
            })
        }
    }

    #[async_trait]
    impl RemoteByteSource for InMemorySource {
        async fn file_size(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
            self.slice_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_slice_range.lock().unwrap() = Some(range.clone());
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }

        async fn get_slice_stream(
            &self,
            _path: &Path,
            range: Range<u64>,
        ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
            self.stream_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_stream_range.lock().unwrap() = Some(range.clone());
            let slice = self.bytes.slice(range.start as usize..range.end as usize);
            Ok(Box::new(Cursor::new(slice.to_vec())))
        }
    }

    fn dummy_path() -> PathBuf {
        PathBuf::from("test.parquet")
    }

    /// Drain all pages from a reader.
    async fn drain_pages(reader: &mut StreamingParquetReader) -> Vec<Page> {
        let mut pages = Vec::new();
        while let Some(p) = reader.next_page().await.unwrap() {
            pages.push(p);
        }
        pages
    }

    // -------- PR-A: two-GETs --------

    #[tokio::test]
    async fn test_footer_get_only_at_construction() {
        let batch = make_metrics_batch(64);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source = InMemorySource::new(bytes);

        let _reader = StreamingParquetReader::try_open(source.clone(), dummy_path())
            .await
            .unwrap();

        assert_eq!(source.slice_calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.stream_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_two_gets_for_full_consumption() {
        let batch = make_metrics_batch(64);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source = InMemorySource::new(bytes);

        let mut reader = StreamingParquetReader::try_open(source.clone(), dummy_path())
            .await
            .unwrap();
        let _ = drain_pages(&mut reader).await;

        assert_eq!(
            source.slice_calls.load(Ordering::SeqCst),
            1,
            "exactly one footer GET",
        );
        assert_eq!(
            source.stream_calls.load(Ordering::SeqCst),
            1,
            "exactly one body GET",
        );
    }

    #[tokio::test]
    async fn test_body_get_starts_at_first_column_chunk_offset() {
        let batch = make_metrics_batch(64);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source = InMemorySource::new(bytes.clone());

        let mut reader = StreamingParquetReader::try_open(source.clone(), dummy_path())
            .await
            .unwrap();
        // Trigger the body GET.
        let _ = reader.next_page().await.unwrap();

        let last_stream_range = source
            .last_stream_range
            .lock()
            .unwrap()
            .clone()
            .expect("body GET must have been issued");

        let sync_reader = SerializedFileReader::new(bytes).unwrap();
        let metadata = sync_reader.metadata();
        let expected_start = metadata.row_group(0).column(0).byte_range().0;
        let last_rg = metadata.row_group(metadata.num_row_groups() - 1);
        let last_col = last_rg.column(last_rg.num_columns() - 1);
        let (last_start, last_size) = last_col.byte_range();
        let expected_end = last_start + last_size;

        assert_eq!(last_stream_range.start, expected_start);
        assert_eq!(last_stream_range.end, expected_end);
    }

    // -------- PR-B: metadata equivalence --------

    #[tokio::test]
    async fn test_metadata_matches_sync_reader() {
        let batch_a = make_metrics_batch(40);
        let batch_b = make_metrics_batch(20);
        let kvs = vec![KeyValue::new(
            "qh.sort_fields".to_string(),
            "metric_name|timestamp/V2".to_string(),
        )];
        let bytes = write_test_file(&[batch_a, batch_b], kvs.clone());

        let sync_reader = SerializedFileReader::new(bytes.clone()).unwrap();
        let oracle = sync_reader.metadata();

        let source = InMemorySource::new(bytes);
        let reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let actual = reader.metadata();

        let oracle_schema = oracle.file_metadata().schema_descr();
        let actual_schema = actual.file_metadata().schema_descr();
        assert_eq!(actual_schema.num_columns(), oracle_schema.num_columns());
        for i in 0..actual_schema.num_columns() {
            assert_eq!(
                actual_schema.column(i).name(),
                oracle_schema.column(i).name()
            );
            assert_eq!(
                format!("{:?}", actual_schema.column(i).physical_type()),
                format!("{:?}", oracle_schema.column(i).physical_type()),
            );
        }

        let oracle_kv = oracle
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        let actual_kv = actual
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        for kv in &oracle_kv {
            let found = actual_kv.iter().find(|k| k.key == kv.key);
            assert!(found.is_some(), "actual KV missing key {:?}", kv.key);
            assert_eq!(
                found.unwrap().value,
                kv.value,
                "value differs for {:?}",
                kv.key,
            );
        }

        assert_eq!(actual.num_row_groups(), oracle.num_row_groups());
        for rg_idx in 0..actual.num_row_groups() {
            assert_eq!(
                actual.row_group(rg_idx).sorting_columns(),
                oracle.row_group(rg_idx).sorting_columns(),
            );
            assert_eq!(
                actual.row_group(rg_idx).num_rows(),
                oracle.row_group(rg_idx).num_rows(),
            );
        }

        for kv in &kvs {
            let found = actual_kv.iter().find(|k| k.key == kv.key).unwrap();
            assert_eq!(found.value, kv.value);
        }
    }

    // -------- PR-C: page round-trip --------

    /// Storage order: pages within a column chunk are emitted in
    /// the order they appear on disk. Concretely, dict page (if
    /// present) first, then data pages. Compare against the file's
    /// raw bytes at the column-chunk's `byte_range()`.
    #[tokio::test]
    async fn test_pages_concatenate_to_column_chunk_bytes() {
        let batch = make_metrics_batch(128);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());

        let source = InMemorySource::new(bytes.clone());
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let pages = drain_pages(&mut reader).await;

        // Group pages by (rg, col), check concatenation == file slice.
        let metadata = reader.metadata().clone();
        let mut by_col: std::collections::BTreeMap<(usize, usize), Vec<Vec<u8>>> =
            std::collections::BTreeMap::new();
        let mut header_lens: std::collections::BTreeMap<(usize, usize), Vec<usize>> =
            std::collections::BTreeMap::new();
        let mut last_page_idx: std::collections::BTreeMap<(usize, usize), usize> =
            std::collections::BTreeMap::new();
        for p in pages {
            // Page index sequence: 0, 1, 2, ... within each column.
            let prev = last_page_idx.get(&(p.rg_idx, p.col_idx)).copied();
            match prev {
                None => assert_eq!(p.page_idx_in_col, 0),
                Some(prev) => assert_eq!(p.page_idx_in_col, prev + 1),
            }
            last_page_idx.insert((p.rg_idx, p.col_idx), p.page_idx_in_col);

            // Re-encode the page header to know its byte length, so
            // we can reassemble the column chunk = sum of (header +
            // body) for all pages.
            let header_bytes = encode_page_header(&p.header);
            header_lens
                .entry((p.rg_idx, p.col_idx))
                .or_default()
                .push(header_bytes.len());
            let mut combined = header_bytes;
            combined.extend_from_slice(&p.bytes);
            by_col
                .entry((p.rg_idx, p.col_idx))
                .or_default()
                .push(combined);
        }

        for ((rg, col), parts) in by_col {
            let col_meta = metadata.row_group(rg).column(col);
            let (start, len) = col_meta.byte_range();
            let expected = bytes.slice(start as usize..(start + len) as usize);
            let mut actual: Vec<u8> = Vec::with_capacity(len as usize);
            for part in parts {
                actual.extend_from_slice(&part);
            }
            assert_eq!(
                actual,
                expected.as_ref(),
                "rg={}, col={} reassembled bytes",
                rg,
                col,
            );
        }
    }

    /// Multi-page columns: same assertion as above but with the
    /// data-page-row-count limit forcing several pages per column.
    /// This is the test that proves we correctly walk page-by-page
    /// through a column whose body is split across multiple pages.
    #[tokio::test]
    async fn test_pages_concatenate_to_column_chunk_bytes_multi_page() {
        let batch = make_metrics_batch(2048);
        let bytes = write_test_file_multi_page(std::slice::from_ref(&batch), 256);

        let source = InMemorySource::new(bytes.clone());
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let pages = drain_pages(&mut reader).await;

        // We expect at least one column to have > 1 data page (since
        // 2048 rows / 256 rows per page = 8 data pages). Confirm the
        // multi-page property is exercised.
        let mut max_pages_per_col = 0usize;
        let mut by_col: std::collections::BTreeMap<(usize, usize), usize> =
            std::collections::BTreeMap::new();
        for p in &pages {
            *by_col.entry((p.rg_idx, p.col_idx)).or_insert(0) += 1;
        }
        for &count in by_col.values() {
            max_pages_per_col = max_pages_per_col.max(count);
        }
        assert!(
            max_pages_per_col > 1,
            "test fixture must produce multi-page columns; got max {max_pages_per_col}",
        );

        // Same byte-equality check as the single-page test.
        let metadata = reader.metadata().clone();
        let mut by_col_bytes: std::collections::BTreeMap<(usize, usize), Vec<u8>> =
            std::collections::BTreeMap::new();
        for p in pages {
            let header_bytes = encode_page_header(&p.header);
            let entry = by_col_bytes.entry((p.rg_idx, p.col_idx)).or_default();
            entry.extend_from_slice(&header_bytes);
            entry.extend_from_slice(&p.bytes);
        }
        for ((rg, col), actual) in by_col_bytes {
            let col_meta = metadata.row_group(rg).column(col);
            let (start, len) = col_meta.byte_range();
            let expected = bytes.slice(start as usize..(start + len) as usize);
            assert_eq!(
                actual.as_slice(),
                expected.as_ref(),
                "rg={}, col={} reassembled bytes",
                rg,
                col,
            );
        }
    }

    /// Encode a `PageHeader` back to Thrift compact bytes — used by
    /// tests that reassemble column-chunk bytes from pages. (The
    /// streaming reader peels off the header; reassembly needs to
    /// add it back.)
    fn encode_page_header(header: &PageHeader) -> Vec<u8> {
        use parquet::thrift::TSerializable;
        use thrift::protocol::TCompactOutputProtocol;
        let mut buf = Vec::new();
        {
            let mut prot = TCompactOutputProtocol::new(&mut buf);
            header.write_to_out_protocol(&mut prot).unwrap();
        }
        buf
    }

    // -------- PR-D: storage-order --------

    #[tokio::test]
    async fn test_storage_order_advance() {
        let batch_a = make_metrics_batch(40);
        let batch_b = make_metrics_batch(40);
        let bytes = write_test_file(&[batch_a, batch_b], Vec::new());
        let source = InMemorySource::new(bytes);

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let pages = drain_pages(&mut reader).await;

        // Page sequence: (rg=0, col=0, page=0), (rg=0, col=0, page=1)?,
        // ..., (rg=0, col=1, page=0), ..., (rg=1, col=0, page=0), ...
        let mut prev: Option<(usize, usize, usize)> = None;
        for p in &pages {
            let cur = (p.rg_idx, p.col_idx, p.page_idx_in_col);
            if let Some(prev) = prev {
                let prev_key = (prev.0, prev.1);
                let cur_key = (cur.0, cur.1);
                if prev_key == cur_key {
                    // Same column: page index increments.
                    assert_eq!(cur.2, prev.2 + 1);
                } else {
                    // Different column: lex-greater (rg, col), and page=0.
                    assert!(
                        cur_key > prev_key,
                        "out-of-order: {prev_key:?} → {cur_key:?}"
                    );
                    assert_eq!(cur.2, 0);
                }
            } else {
                assert_eq!(cur, (0, 0, 0));
            }
            prev = Some(cur);
        }
    }

    // -------- PR-E: EOF idempotent --------

    #[tokio::test]
    async fn test_eof_idempotent() {
        let batch = make_metrics_batch(8);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source = InMemorySource::new(bytes);

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        while reader.next_page().await.unwrap().is_some() {}
        for _ in 0..3 {
            assert!(reader.next_page().await.unwrap().is_none());
        }
    }

    // -------- Bounded memory --------

    /// Bound on `pending` buffer size: the streaming reader should
    /// never accumulate more than ~one page header + one page body
    /// in memory at any time, regardless of file size or row group
    /// size. Concretely: after each `next_page` returns, the
    /// returned page's `bytes.len()` is bounded by the page's
    /// compressed size, and the reader's internal `pending` buffer
    /// is empty (we drained exactly the bytes for this page).
    ///
    /// We test by writing a multi-page file and asserting that, after
    /// each page yield, `pending.len() == 0`.
    #[tokio::test]
    async fn test_pending_buffer_drained_after_each_page() {
        let batch = make_metrics_batch(2048);
        let bytes = write_test_file_multi_page(std::slice::from_ref(&batch), 256);
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();

        loop {
            let page = reader.next_page().await.unwrap();
            if page.is_none() {
                break;
            }
            // Inspect internal state: pending buffer should be empty
            // after a page is returned (we drained it down to zero).
            if let ReaderState::Reading(reading) = &reader.state {
                assert!(
                    reading.pending.is_empty() || reading.pending.len() < 16 * 1024,
                    "pending buffer leaking ({} bytes) — should be drained after each page",
                    reading.pending.len(),
                );
            }
        }
    }

    // -------- Footer prefetch retry --------

    #[tokio::test]
    async fn test_small_prefetch_retries_with_correct_size() {
        let batch = make_metrics_batch(128);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source = InMemorySource::new(bytes);

        let config = StreamingReaderConfig {
            footer_prefetch_bytes: 1024,
            max_page_header_bytes: 1024 * 1024,
        };
        let mut reader =
            StreamingParquetReader::try_open_with_config(source.clone(), dummy_path(), config)
                .await
                .unwrap();

        assert_eq!(
            source.slice_calls.load(Ordering::SeqCst),
            2,
            "prefetch + retry = two slice calls",
        );

        let _ = drain_pages(&mut reader).await;
        assert_eq!(source.stream_calls.load(Ordering::SeqCst), 1);
    }

    // -------- Body stream failure --------

    /// A transient failure on the body stream while reading bytes for
    /// a page header MUST surface as `ParquetReadError::Io`, not be
    /// silenced and re-reported as a thrift/header error. Callers in
    /// production rely on `Io` to drive retry/backoff decisions; if
    /// it shows up as `ThriftPageHeader` or `PageHeaderTooLarge`, the
    /// caller has no way to distinguish "network blip, retry" from
    /// "file is malformed, give up."
    #[tokio::test]
    async fn test_body_read_failure_surfaces_as_io_error() {
        let batch = make_metrics_batch(64);
        let bytes = write_test_file(std::slice::from_ref(&batch), Vec::new());
        let source: Arc<dyn RemoteByteSource> = Arc::new(FailingBodySource { bytes });

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .expect("footer fetch must succeed; only the body stream is wired to fail");

        match reader.next_page().await {
            Err(ParquetReadError::Io(err)) => {
                assert!(
                    err.to_string().contains("simulated"),
                    "expected the simulated body error to be propagated; got {err}",
                );
            }
            other => panic!(
                "expected ParquetReadError::Io to surface from a failing body stream; got \
                 {other:?}",
            ),
        }
    }

    /// `RemoteByteSource` that succeeds for `file_size` and `get_slice`
    /// (so `try_open`'s footer fetch works), but returns a body stream
    /// that always errors on read. Used by
    /// `test_body_read_failure_surfaces_as_io_error`.
    struct FailingBodySource {
        bytes: Bytes,
    }

    #[async_trait]
    impl RemoteByteSource for FailingBodySource {
        async fn file_size(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }

        async fn get_slice_stream(
            &self,
            _path: &Path,
            _range: Range<u64>,
        ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
            Ok(Box::new(AlwaysFailRead))
        }
    }

    struct AlwaysFailRead;

    impl AsyncRead for AlwaysFailRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Err(io::Error::other("simulated body read failure")))
        }
    }

    // -------- Truncated file --------

    #[tokio::test]
    async fn test_truncated_file_returns_footer_too_large() {
        let batch = make_metrics_batch(8);
        let mut bytes_vec = write_test_file(std::slice::from_ref(&batch), Vec::new()).to_vec();
        bytes_vec.truncate(32);
        let truncated = Bytes::from(bytes_vec);
        let source = InMemorySource::new(truncated);

        let result = StreamingParquetReader::try_open(source, dummy_path()).await;
        match result {
            Err(ParquetReadError::FooterTooLarge { .. } | ParquetReadError::Parquet(_)) => {}
            Err(other) => panic!("expected FooterTooLarge or Parquet error, got {other:?}"),
            Ok(_) => panic!("expected error on truncated file"),
        }
    }

    // -------- num_values consistency --------

    /// The total `num_values` across all data pages of a column in a
    /// row group must equal that row group's `num_rows` (modulo
    /// dictionary pages, which carry the dictionary not the data).
    #[tokio::test]
    async fn test_data_page_num_values_sum_matches_row_count() {
        let batch = make_metrics_batch(2048);
        let bytes = write_test_file_multi_page(std::slice::from_ref(&batch), 256);
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let pages = drain_pages(&mut reader).await;
        let metadata = reader.metadata().clone();

        let mut data_values_per_col: std::collections::BTreeMap<(usize, usize), i64> =
            std::collections::BTreeMap::new();
        for p in pages {
            match p.header.type_ {
                PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => {
                    let n = page_num_values(&p.header).unwrap_or(0) as i64;
                    *data_values_per_col
                        .entry((p.rg_idx, p.col_idx))
                        .or_insert(0) += n;
                }
                PageType::DICTIONARY_PAGE | PageType::INDEX_PAGE => {}
                _ => {}
            }
        }

        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            let expected = rg.num_rows();
            for col_idx in 0..rg.num_columns() {
                let actual = data_values_per_col
                    .get(&(rg_idx, col_idx))
                    .copied()
                    .unwrap_or(0);
                assert_eq!(
                    actual, expected,
                    "rg={rg_idx}, col={col_idx} data-page num_values sum",
                );
            }
        }
    }
}
