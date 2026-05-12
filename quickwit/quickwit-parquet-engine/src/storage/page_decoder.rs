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

//! Page-stream → Arrow `ArrayRef` decoder, one input page at a time.
//!
//! Each [`StreamDecoder::decode_next_page`] call pulls one [`Page`] from
//! the underlying [`ColumnPageStream`] and (for data pages) emits a
//! [`DecodedPage`] carrying the Arrow array for that page's rows. Memory
//! is bounded by:
//!
//! - **One in-flight page** (compressed + decompressed bytes during the current decode).
//! - **One cached dictionary page** per (rg, col) when the column is dictionary-encoded — needed to
//!   decode subsequent data pages that reference it. Dict pages are typically small relative to
//!   data.
//! - **One [`ColumnReader`] per (rg, col)** holding small internal bookkeeping (level decoders,
//!   value decoder). The reader holds the current page during decode; we feed pages one at a time,
//!   so it never holds more than one data page at a time.
//!
//! The decoder does **not** buffer a row group, a column chunk, or any
//! materialised array beyond the one currently being emitted. PR-6b's
//! merge engine takes the emitted [`DecodedPage`]s in storage order
//! (row-group-major, column-major-within-rg, page-major-within-col),
//! consults sort columns to compute the local merge order for each RG,
//! and streams take-applied output pages directly into the writer.
//!
//! # How it works
//!
//! 1. Pull one [`Page`] from the stream. Skip `INDEX_PAGE` (never emitted by production writers;
//!    the variant exists in the Thrift schema for completeness).
//! 2. Look up (or initialise) per-(rg, col) state: a `PageQueue` that feeds a parquet-rs
//!    [`ColumnReader`] one page at a time, plus a counter tracking how many rows of this column
//!    we've decoded.
//! 3. Convert our [`Page`] (raw compressed bytes + parsed Thrift header) to parquet-rs's
//!    [`column::page::Page`] enum: decompress with [`create_codec`], translate `format::Encoding`
//!    (Thrift wrapper) to `basic::Encoding`, drop optional statistics (not needed for decoding
//!    values).
//! 4. Push the converted page onto the queue. If it was a dictionary or index page, loop back to
//!    step 1 — those don't yield rows.
//! 5. For a data page: ask the [`ColumnReader`] to decode exactly `header.num_values` records. The
//!    reader pulls the queued data page (plus the cached dict if not yet consumed), decodes values
//!    + def/rep levels into typed buffers, and returns the count.
//! 6. Build an `ArrayRef` from `(values, def_levels, rep_levels)` per the column's parquet physical
//!    type. Emit [`DecodedPage`].
//!
//! [`ColumnPageStream`]: super::streaming_reader::ColumnPageStream
//! [`Page`]: super::streaming_reader::Page
//! [`ColumnReader`]: parquet::column::reader::ColumnReader
//! [`column::page::Page`]: parquet::column::page::Page
//! [`create_codec`]: parquet::compression::create_codec

#![allow(dead_code)]
#![allow(deprecated)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use bytes::Bytes;
use parquet::basic::{Encoding as BasicEncoding, Type as PhysicalType};
use parquet::column::page::Page as ColumnPage;
use parquet::column::reader::{ColumnReader, get_column_reader};
use parquet::compression::{Codec, CodecOptions, create_codec};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType as ParquetDataType, DoubleType, FloatType,
    Int32Type, Int64Type,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::format::{PageHeader, PageType};
use parquet::schema::types::ColumnDescPtr;
use thiserror::Error;

use super::streaming_reader::{ColumnPageStream, Page, ParquetReadError};

/// Errors from [`StreamDecoder`].
///
/// Each variant carries the underlying source via `#[from]` / `#[source]`
/// so I/O failures from the page stream surface as
/// [`PageDecodeError::PageStream`] rather than being masked as decode
/// errors — the same policy [`ParquetReadError`] already enforces.
#[derive(Error, Debug)]
pub enum PageDecodeError {
    /// The underlying [`ColumnPageStream`] returned an error.
    #[error("page stream error: {0}")]
    PageStream(#[from] ParquetReadError),

    /// Parquet decode error from the column reader or page decompression.
    #[error("parquet decode error: {0}")]
    Parquet(#[from] ParquetError),

    /// Arrow array-construction error.
    #[error("arrow build error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A page header carried a field type we don't understand (e.g., an
    /// `Encoding` value the parquet spec didn't define when this code
    /// was written).
    #[error("unsupported encoding value {encoding} on page at ({rg_idx}, {col_idx})")]
    UnsupportedEncoding {
        rg_idx: usize,
        col_idx: usize,
        encoding: i32,
    },

    /// A column's parquet physical type pairs with an Arrow type we
    /// don't construct from raw values (e.g., decimals, FIXED_LEN_BYTE_ARRAY
    /// outside the supported set).
    #[error(
        "field '{name}' has unsupported parquet physical type / arrow type pairing: \
         physical={physical:?}, arrow={arrow:?}"
    )]
    UnsupportedColumnType {
        name: String,
        physical: PhysicalType,
        arrow: DataType,
    },
}

/// One decoded data page yielded by [`StreamDecoder::decode_next_page`].
#[derive(Debug)]
pub struct DecodedPage {
    /// Row group this page belongs to.
    pub rg_idx: usize,
    /// Column chunk this page belongs to (within the row group).
    pub col_idx: usize,
    /// Index of this data page within its column chunk (0-based,
    /// counting data pages only — dictionary pages do not increment).
    pub page_idx_in_col: usize,
    /// Cumulative row offset for `(rg_idx, col_idx)` *before* this
    /// page. Together with `array.len()` this gives the row range
    /// `row_start..row_start + array.len()` that this page covers,
    /// which PR-6b's merge engine uses to slice take indices per page.
    pub row_start: usize,
    /// Decoded Arrow array. Length equals the number of records this
    /// page contributes (i.e. `header.num_values` for the data page).
    pub array: ArrayRef,
}

/// Drains a [`ColumnPageStream`] one *page* at a time and emits Arrow
/// arrays. Caller drives via [`Self::decode_next_page`] until it returns
/// `Ok(None)` (idempotent EOF).
///
/// Memory is bounded by ~one in-flight page per decoder, plus one
/// cached dictionary page per (rg, col) for dictionary-encoded columns.
/// Does not buffer the row group.
pub struct StreamDecoder<'a> {
    stream: &'a mut dyn ColumnPageStream,
    metadata: Arc<ParquetMetaData>,
    columns: HashMap<(usize, usize), ColumnState>,
    eof: bool,
}

/// Per-(rg, col) state. Holds the [`ColumnReader`] that owns the
/// page-decoder pipeline, plus a handle to the `PageQueue` we push
/// converted pages into. The same `Arc<Mutex<...>>` queue lives both
/// here (so we can push) and inside the `Box<dyn PageReader>` the
/// `ColumnReader` consumes from (so it can pop).
struct ColumnState {
    queue: Arc<Mutex<VecDeque<ColumnPage>>>,
    reader: ColumnReader,
    rows_decoded: usize,
    next_data_page_idx: usize,
    field: Arc<Field>,
}

impl<'a> StreamDecoder<'a> {
    pub fn new(stream: &'a mut dyn ColumnPageStream) -> Self {
        let metadata = Arc::clone(stream.metadata());
        Self {
            stream,
            metadata,
            columns: HashMap::new(),
            eof: false,
        }
    }

    /// Pull and decode the next data page in storage order. Dictionary
    /// pages are absorbed silently (fed to the column reader for use by
    /// subsequent data pages). Returns `Ok(None)` at EOF.
    ///
    /// Maintains a **one-page lookahead** in the per-(rg, col) queues:
    /// after the current data page is queued, one more page is pulled
    /// from the stream and routed to its queue *before* `read_records`
    /// runs. This makes `PageQueueReader::peek_next_page` return
    /// accurate next-page metadata when parquet-rs's column reader
    /// calls `at_record_boundary()` — required for V1 data pages with
    /// repetition levels, where a list record can continue onto the
    /// next page and parquet-rs needs the next page's metadata to
    /// decide whether to flush partial rep-level state. Without the
    /// lookahead, peek returns `None` at every page end and parquet-rs
    /// treats it as the last page, which can split a list incorrectly.
    pub async fn decode_next_page(&mut self) -> Result<Option<DecodedPage>, PageDecodeError> {
        loop {
            // Prefer a state whose queue already has an unconsumed data
            // page (left over from a previous call's lookahead, or
            // queued during the loop below). At most one state has
            // an unconsumed queued data page at any time, since each
            // call pre-fetches exactly one page.
            if let Some(key) = self.find_state_with_queued_data_page() {
                return self.decode_state_head(key).await;
            }

            if self.eof {
                return Ok(None);
            }

            match self.stream.next_page().await? {
                Some(page) => self.route_page_to_queue(page)?,
                None => {
                    self.eof = true;
                    // Loop once more to flush any state that may have
                    // a queued data page from a prior call's lookahead.
                    if self.find_state_with_queued_data_page().is_none() {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Scan column states for one whose queue front contains an
    /// unconsumed data page. Returns the (rg, col) key, or `None`.
    fn find_state_with_queued_data_page(&self) -> Option<(usize, usize)> {
        for (&key, state) in self.columns.iter() {
            let q = state.queue.lock().expect("PageQueue mutex poisoned");
            if q.iter().any(|p| {
                matches!(
                    p,
                    ColumnPage::DataPage { .. } | ColumnPage::DataPageV2 { .. }
                )
            }) {
                return Some(key);
            }
        }
        None
    }

    /// Decode the front data page from `key`'s state's queue.
    /// Pre-fetches one more page from the stream (best effort) so
    /// `peek_next_page` returns accurate metadata when the column
    /// reader checks record boundaries.
    async fn decode_state_head(
        &mut self,
        key: (usize, usize),
    ) -> Result<Option<DecodedPage>, PageDecodeError> {
        // Pre-fetch one page from the stream before driving read_records.
        // If the pre-fetched page is for the same (rg, col) the column
        // reader is about to decode, it sits in the same queue so
        // `peek_next_page` returns real metadata (V1 record continuation
        // is handled correctly). If it's for a different (rg, col), it
        // goes into that state's queue — current state's `peek_next_page`
        // returns `None`, which is correct because this column chunk's
        // pages are now exhausted from the current state's perspective.
        if !self.eof {
            match self.stream.next_page().await? {
                Some(page) => self.route_page_to_queue(page)?,
                None => self.eof = true,
            }
        }

        // Read num_values from the front data page (skipping any
        // dictionary pages that may precede it in the queue).
        let state = self
            .columns
            .get(&key)
            .expect("caller's key must have a state");
        let num_values_in_page = {
            let q = state.queue.lock().expect("PageQueue mutex poisoned");
            q.iter()
                .find_map(|p| match p {
                    ColumnPage::DataPage { num_values, .. }
                    | ColumnPage::DataPageV2 { num_values, .. } => Some(*num_values as usize),
                    _ => None,
                })
                .expect("caller's key has a queued data page")
        };

        let state = self
            .columns
            .get_mut(&key)
            .expect("caller's key must have a state");
        let array = decode_one_data_page_into_array(state, num_values_in_page)?;
        let row_start = state.rows_decoded;
        let page_idx_in_col = state.next_data_page_idx;
        state.rows_decoded += array.len();
        state.next_data_page_idx += 1;

        Ok(Some(DecodedPage {
            rg_idx: key.0,
            col_idx: key.1,
            page_idx_in_col,
            row_start,
            array,
        }))
    }

    /// Convert a raw stream `Page` to a parquet-rs `ColumnPage` and
    /// push it onto the appropriate (rg, col) state's queue. Skips
    /// `INDEX_PAGE` defensively (no production writer emits it).
    fn route_page_to_queue(&mut self, page: Page) -> Result<(), PageDecodeError> {
        if page.header.type_ == PageType::INDEX_PAGE {
            return Ok(());
        }
        let key = (page.rg_idx, page.col_idx);
        if !self.columns.contains_key(&key) {
            let state = init_column_state(&self.metadata, key)?;
            self.columns.insert(key, state);
        }
        let col_meta = self.metadata.row_group(key.0).column(key.1);
        let physical = col_meta.column_type();
        let compression = col_meta.compression();
        let col_page = convert_page(&page, physical, compression, key)?;
        let state = self.columns.get_mut(&key).expect("just inserted above");
        state
            .queue
            .lock()
            .expect("PageQueue mutex poisoned")
            .push_back(col_page);
        Ok(())
    }

    /// File metadata. Schema, row-group layout, and KV `qh.*` metadata
    /// come from here.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }
}

// -------- Per-(rg, col) initialisation --------

fn init_column_state(
    metadata: &Arc<ParquetMetaData>,
    key: (usize, usize),
) -> Result<ColumnState, PageDecodeError> {
    let (rg_idx, col_idx) = key;
    let parquet_schema = metadata.file_metadata().schema_descr();
    let col_descr: ColumnDescPtr = parquet_schema.column(col_idx);

    // Compute the arrow Field for this column. We use
    // `parquet_to_arrow_schema` over the full schema and pick the
    // matching top-level field. Most columns will be flat (one leaf
    // per top-level field), so the col_idx is also the field idx.
    // Nested columns (List<primitive>) still have one top-level field,
    // matching one entry in the arrow schema.
    //
    // We deliberately pass `None` for kv_metadata so the
    // `ARROW:schema` hint is ignored — that hint reconstructs the
    // writer's original Dictionary<...> types, but the parquet
    // column reader decodes values back to their physical type (Utf8
    // / Binary for byte-array columns), so a `Dictionary` Arrow field
    // wouldn't match the array we produce. PR-6b's union schema
    // normalises strings to Utf8 anyway, and the streaming writer
    // re-applies dict encoding on output based on observed cardinality.
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(parquet_schema, None)?;
    let field = arrow_schema
        .fields()
        .get(col_idx)
        .ok_or_else(|| {
            PageDecodeError::Parquet(ParquetError::General(format!(
                "column index {col_idx} out of range for arrow schema (rg {rg_idx})",
            )))
        })?
        .clone();

    let queue: Arc<Mutex<VecDeque<ColumnPage>>> = Arc::new(Mutex::new(VecDeque::with_capacity(2)));
    let page_reader: Box<dyn parquet::column::page::PageReader> =
        Box::new(PageQueueReader::new(Arc::clone(&queue)));
    let reader = get_column_reader(col_descr, page_reader);

    Ok(ColumnState {
        queue,
        reader,
        rows_decoded: 0,
        next_data_page_idx: 0,
        field,
    })
}

// -------- PageReader over a shared queue --------

/// Implements [`parquet::column::page::PageReader`] over an
/// `Arc<Mutex<VecDeque<ColumnPage>>>`. The owning [`StreamDecoder`]
/// pushes converted pages into the queue; the reader pops them on
/// demand. When the queue is empty the reader returns `Ok(None)` —
/// the column reader interprets that as "no more pages for this column
/// chunk *right now*" and stops mid-decode. Since we always push exactly
/// one data page at a time and then drive the column reader to decode
/// `num_values` records (which the reader does in one swoop, draining
/// the page), the queue is empty between calls and refilled before the
/// next call.
struct PageQueueReader {
    queue: Arc<Mutex<VecDeque<ColumnPage>>>,
}

impl PageQueueReader {
    fn new(queue: Arc<Mutex<VecDeque<ColumnPage>>>) -> Self {
        Self { queue }
    }
}

impl Iterator for PageQueueReader {
    type Item = parquet::errors::Result<ColumnPage>;

    fn next(&mut self) -> Option<Self::Item> {
        let popped = self
            .queue
            .lock()
            .expect("PageQueue mutex poisoned")
            .pop_front();
        popped.map(Ok)
    }
}

impl parquet::column::page::PageReader for PageQueueReader {
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<ColumnPage>> {
        Ok(self
            .queue
            .lock()
            .expect("PageQueue mutex poisoned")
            .pop_front())
    }

    fn peek_next_page(
        &mut self,
    ) -> parquet::errors::Result<Option<parquet::column::page::PageMetadata>> {
        // Used by the rep-level decoder (`at_record_boundary`) to know
        // whether the next page begins a new record. We build the
        // metadata directly from the front of our queue.
        let guard = self.queue.lock().expect("PageQueue mutex poisoned");
        Ok(guard.front().map(page_metadata_from_column_page))
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        let mut guard = self.queue.lock().expect("PageQueue mutex poisoned");
        guard.pop_front();
        Ok(())
    }
}

/// Build a [`parquet::column::page::PageMetadata`] from a decoded
/// [`ColumnPage`]. Mirrors the shape of parquet-rs's
/// `TryFrom<&PageHeader> for PageMetadata` for the variants we use.
fn page_metadata_from_column_page(p: &ColumnPage) -> parquet::column::page::PageMetadata {
    match p {
        ColumnPage::DataPage { num_values, .. } => parquet::column::page::PageMetadata {
            num_rows: None,
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        ColumnPage::DataPageV2 {
            num_values,
            num_rows,
            ..
        } => parquet::column::page::PageMetadata {
            num_rows: Some(*num_rows as usize),
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        ColumnPage::DictionaryPage { .. } => parquet::column::page::PageMetadata {
            num_rows: None,
            num_levels: None,
            is_dict: true,
        },
    }
}

// -------- Page conversion (our format::Page → column::page::Page) --------

/// Convert our [`Page`] to parquet-rs's [`ColumnPage`] enum, decompressing
/// the body bytes where applicable.
fn convert_page(
    page: &Page,
    physical: PhysicalType,
    compression: parquet::basic::Compression,
    key: (usize, usize),
) -> Result<ColumnPage, PageDecodeError> {
    let header: &PageHeader = &page.header;
    let raw = &page.bytes;

    // For DATA_PAGE_V2, def + rep levels precede the value bytes and
    // are NOT compressed. Only the suffix beyond the levels is
    // compressed (and only when `is_compressed` is true).
    let mut levels_prefix_len = 0usize;
    let mut can_decompress = true;
    if let Some(v2) = header.data_page_header_v2.as_ref() {
        if v2.definition_levels_byte_length < 0 || v2.repetition_levels_byte_length < 0 {
            return Err(PageDecodeError::Parquet(ParquetError::General(format!(
                "DataPageV2 at ({}, {}) has negative level byte lengths",
                key.0, key.1,
            ))));
        }
        levels_prefix_len =
            (v2.definition_levels_byte_length + v2.repetition_levels_byte_length) as usize;
        can_decompress = v2.is_compressed.unwrap_or(true);
    }

    let body: Bytes = decompress_page_body(
        raw,
        compression,
        header.uncompressed_page_size as usize,
        levels_prefix_len,
        can_decompress,
    )?;

    let _ = physical; // currently unused — kept for future page-type-specific validation
    let _ = key;

    match header.type_ {
        PageType::DICTIONARY_PAGE => {
            let h = header.dictionary_page_header.as_ref().ok_or_else(|| {
                PageDecodeError::Parquet(ParquetError::General(
                    "dictionary page header missing".into(),
                ))
            })?;
            Ok(ColumnPage::DictionaryPage {
                buf: body,
                num_values: h.num_values as u32,
                encoding: format_encoding_to_basic(h.encoding, key)?,
                is_sorted: h.is_sorted.unwrap_or(false),
            })
        }
        PageType::DATA_PAGE => {
            let h = header.data_page_header.as_ref().ok_or_else(|| {
                PageDecodeError::Parquet(ParquetError::General("data page header missing".into()))
            })?;
            Ok(ColumnPage::DataPage {
                buf: body,
                num_values: h.num_values as u32,
                encoding: format_encoding_to_basic(h.encoding, key)?,
                def_level_encoding: format_encoding_to_basic(h.definition_level_encoding, key)?,
                rep_level_encoding: format_encoding_to_basic(h.repetition_level_encoding, key)?,
                statistics: None,
            })
        }
        PageType::DATA_PAGE_V2 => {
            let h = header.data_page_header_v2.as_ref().ok_or_else(|| {
                PageDecodeError::Parquet(ParquetError::General(
                    "data page v2 header missing".into(),
                ))
            })?;
            Ok(ColumnPage::DataPageV2 {
                buf: body,
                num_values: h.num_values as u32,
                encoding: format_encoding_to_basic(h.encoding, key)?,
                num_nulls: h.num_nulls as u32,
                num_rows: h.num_rows as u32,
                def_levels_byte_len: h.definition_levels_byte_length as u32,
                rep_levels_byte_len: h.repetition_levels_byte_length as u32,
                is_compressed: h.is_compressed.unwrap_or(true),
                statistics: None,
            })
        }
        other => Err(PageDecodeError::Parquet(ParquetError::General(format!(
            "unexpected page type {other:?} at ({}, {})",
            key.0, key.1,
        )))),
    }
}

fn decompress_page_body(
    raw: &Bytes,
    compression: parquet::basic::Compression,
    uncompressed_page_size: usize,
    levels_prefix_len: usize,
    can_decompress: bool,
) -> Result<Bytes, PageDecodeError> {
    if !can_decompress {
        // DataPageV2 with is_compressed=false: body is already plain.
        return Ok(raw.clone());
    }
    let codec_opt: Option<Box<dyn Codec>> = create_codec(compression, &CodecOptions::default())?;
    let mut codec: Box<dyn Codec> = match codec_opt {
        Some(c) => c,
        None => {
            // UNCOMPRESSED.
            return Ok(raw.clone());
        }
    };

    if levels_prefix_len > raw.len() || levels_prefix_len > uncompressed_page_size {
        return Err(PageDecodeError::Parquet(ParquetError::General(format!(
            "level prefix length {levels_prefix_len} exceeds page bounds",
        ))));
    }

    let mut out: Vec<u8> = Vec::with_capacity(uncompressed_page_size);
    out.extend_from_slice(&raw[..levels_prefix_len]);
    let values_uncompressed = uncompressed_page_size - levels_prefix_len;
    if values_uncompressed > 0 {
        codec.decompress(
            &raw[levels_prefix_len..],
            &mut out,
            Some(values_uncompressed),
        )?;
    }
    if out.len() != uncompressed_page_size {
        return Err(PageDecodeError::Parquet(ParquetError::General(format!(
            "decompressed size {} does not match uncompressed_page_size {}",
            out.len(),
            uncompressed_page_size,
        ))));
    }
    Ok(Bytes::from(out))
}

/// Translate the Thrift-wrapped `format::Encoding` (i32) to the
/// strongly-typed `basic::Encoding` parquet-rs uses for page decoding.
/// `parquet-rs` doesn't expose a public `From<i32>` so we mirror the
/// match here.
fn format_encoding_to_basic(
    encoding: parquet::format::Encoding,
    key: (usize, usize),
) -> Result<BasicEncoding, PageDecodeError> {
    let v = encoding.0;
    match v {
        0 => Ok(BasicEncoding::PLAIN),
        // 1 is GROUP_VAR_INT, deprecated and never written by arrow-rs.
        2 => Ok(BasicEncoding::PLAIN_DICTIONARY),
        3 => Ok(BasicEncoding::RLE),
        4 => Ok(BasicEncoding::BIT_PACKED),
        5 => Ok(BasicEncoding::DELTA_BINARY_PACKED),
        6 => Ok(BasicEncoding::DELTA_LENGTH_BYTE_ARRAY),
        7 => Ok(BasicEncoding::DELTA_BYTE_ARRAY),
        8 => Ok(BasicEncoding::RLE_DICTIONARY),
        9 => Ok(BasicEncoding::BYTE_STREAM_SPLIT),
        _ => Err(PageDecodeError::UnsupportedEncoding {
            rg_idx: key.0,
            col_idx: key.1,
            encoding: v,
        }),
    }
}

// -------- Decode one data page into an Arrow ArrayRef --------

const READ_BATCH: usize = 4096;

fn decode_one_data_page_into_array(
    state: &mut ColumnState,
    num_values: usize,
) -> Result<ArrayRef, PageDecodeError> {
    match &mut state.reader {
        ColumnReader::BoolColumnReader(r) => {
            let (records, defs, _reps, values) = read_typed::<BoolType>(r, num_values)?;
            build_bool_array(&state.field, records, &defs, &values)
        }
        ColumnReader::Int32ColumnReader(r) => {
            let (records, defs, reps, values) = read_typed::<Int32Type>(r, num_values)?;
            build_int32_array(&state.field, records, &defs, &reps, &values)
        }
        ColumnReader::Int64ColumnReader(r) => {
            let (records, defs, reps, values) = read_typed::<Int64Type>(r, num_values)?;
            build_int64_array(&state.field, records, &defs, &reps, &values)
        }
        ColumnReader::FloatColumnReader(r) => {
            let (records, defs, reps, values) = read_typed::<FloatType>(r, num_values)?;
            build_float32_array(&state.field, records, &defs, &reps, &values)
        }
        ColumnReader::DoubleColumnReader(r) => {
            let (records, defs, reps, values) = read_typed::<DoubleType>(r, num_values)?;
            build_float64_array(&state.field, records, &defs, &reps, &values)
        }
        ColumnReader::ByteArrayColumnReader(r) => {
            let (records, defs, _reps, values) = read_typed::<ByteArrayType>(r, num_values)?;
            build_byte_array(&state.field, records, &defs, &values)
        }
        ColumnReader::Int96ColumnReader(_) | ColumnReader::FixedLenByteArrayColumnReader(_) => {
            Err(PageDecodeError::UnsupportedColumnType {
                name: state.field.name().to_string(),
                physical: PhysicalType::INT96,
                arrow: state.field.data_type().clone(),
            })
        }
    }
}

/// Read up to `num_values` records out of one typed column reader,
/// returning `(records_read, def_levels, rep_levels, values)`. The reader
/// pulls pages from its `PageQueueReader`; since we push exactly one
/// data page (plus optional dictionary) before each call and the data
/// page advertises `num_values` records, this single call consumes the
/// queued page in full.
/// `(records_read, def_levels, rep_levels, values)`.
type ReadOutput<T> = (usize, Vec<i16>, Vec<i16>, Vec<<T as ParquetDataType>::T>);

fn read_typed<T>(
    reader: &mut parquet::column::reader::ColumnReaderImpl<T>,
    num_values: usize,
) -> Result<ReadOutput<T>, PageDecodeError>
where
    T: ParquetDataType,
    T::T: Default + Clone,
{
    let mut values: Vec<T::T> = Vec::with_capacity(num_values);
    let mut def_levels: Vec<i16> = Vec::new();
    let mut rep_levels: Vec<i16> = Vec::new();
    let mut total_records = 0usize;

    while total_records < num_values {
        let want = num_values - total_records;
        let (records, _values_read, _levels_read) = reader.read_records(
            want.min(READ_BATCH),
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )?;
        if records == 0 {
            break;
        }
        total_records += records;
    }
    Ok((total_records, def_levels, rep_levels, values))
}

// -------- Array builders, parquet physical type → arrow ArrayRef --------

fn null_buffer_from_defs(num_records: usize, defs: &[i16], max_def: i16) -> Option<NullBuffer> {
    if defs.is_empty() || max_def == 0 {
        return None;
    }
    let presence: Vec<bool> = defs
        .iter()
        .take(num_records)
        .map(|d| *d >= max_def)
        .collect();
    Some(NullBuffer::from(presence))
}

fn build_bool_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    values: &[bool],
) -> Result<ArrayRef, PageDecodeError> {
    let max_def = max_def_for(field);
    let arr = if defs.is_empty() || max_def == 0 {
        BooleanArray::from(values.to_vec())
    } else {
        let mut full: Vec<Option<bool>> = Vec::with_capacity(records);
        let mut val_idx = 0usize;
        for d in defs.iter().take(records) {
            if *d >= max_def {
                full.push(Some(values[val_idx]));
                val_idx += 1;
            } else {
                full.push(None);
            }
        }
        BooleanArray::from(full)
    };
    Ok(Arc::new(arr))
}

fn build_int32_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    reps: &[i16],
    values: &[i32],
) -> Result<ArrayRef, PageDecodeError> {
    if matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ) {
        return build_list_i32_array(field, defs, reps, values);
    }
    let max_def = max_def_for(field);
    let nulls = null_buffer_from_defs(records, defs, max_def);
    match field.data_type() {
        DataType::Int8 => {
            let nulls_clone = nulls.clone();
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as i8);
            Ok(Arc::new(arrow::array::Int8Array::new(scalars, nulls_clone)))
        }
        DataType::Int16 => {
            let nulls_clone = nulls.clone();
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as i16);
            Ok(Arc::new(Int16Array::new(scalars, nulls_clone)))
        }
        DataType::Int32 => {
            let nulls_clone = nulls.clone();
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v);
            Ok(Arc::new(Int32Array::new(scalars, nulls_clone)))
        }
        DataType::UInt8 => {
            let nulls_clone = nulls.clone();
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as u8);
            Ok(Arc::new(UInt8Array::new(scalars, nulls_clone)))
        }
        DataType::UInt16 => {
            let nulls_clone = nulls.clone();
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as u16);
            Ok(Arc::new(UInt16Array::new(scalars, nulls_clone)))
        }
        DataType::UInt32 => {
            let nulls_clone = nulls.clone();
            // Bit-reinterpret: parquet stores u32 as INT32 physical with
            // unsigned logical annotation. The on-wire i32 maps to u32
            // by reinterpreting bits.
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as u32);
            Ok(Arc::new(UInt32Array::new(scalars, nulls_clone)))
        }
        other => Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::INT32,
            arrow: other.clone(),
        }),
    }
}

fn build_int64_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    reps: &[i16],
    values: &[i64],
) -> Result<ArrayRef, PageDecodeError> {
    if matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ) {
        return build_list_i64_array(field, defs, reps, values);
    }
    let max_def = max_def_for(field);
    let nulls = null_buffer_from_defs(records, defs, max_def);
    match field.data_type() {
        DataType::Int64 => {
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v);
            Ok(Arc::new(Int64Array::new(scalars, nulls)))
        }
        DataType::UInt64 => {
            let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v as u64);
            Ok(Arc::new(UInt64Array::new(scalars, nulls)))
        }
        other => Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::INT64,
            arrow: other.clone(),
        }),
    }
}

fn build_float32_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    reps: &[i16],
    values: &[f32],
) -> Result<ArrayRef, PageDecodeError> {
    if matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ) {
        return build_list_f32_array(field, defs, reps, values);
    }
    let max_def = max_def_for(field);
    let nulls = null_buffer_from_defs(records, defs, max_def);
    let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v);
    Ok(Arc::new(Float32Array::new(scalars, nulls)))
}

fn build_float64_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    reps: &[i16],
    values: &[f64],
) -> Result<ArrayRef, PageDecodeError> {
    if matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ) {
        return build_list_f64_array(field, defs, reps, values);
    }
    let max_def = max_def_for(field);
    let nulls = null_buffer_from_defs(records, defs, max_def);
    let scalars = scalar_buffer_from_present(records, defs, max_def, values, |v| v);
    Ok(Arc::new(Float64Array::new(scalars, nulls)))
}

fn build_byte_array(
    field: &Field,
    records: usize,
    defs: &[i16],
    values: &[ByteArray],
) -> Result<ArrayRef, PageDecodeError> {
    let max_def = max_def_for(field);
    match field.data_type() {
        DataType::Utf8 => Ok(Arc::new(build_string_array(
            records, defs, max_def, values,
        )?)),
        DataType::LargeUtf8 => Ok(Arc::new(build_large_string_array(
            records, defs, max_def, values,
        )?)),
        DataType::Binary => Ok(Arc::new(build_binary_array(records, defs, max_def, values))),
        DataType::LargeBinary => Ok(Arc::new(build_large_binary_array(
            records, defs, max_def, values,
        ))),
        DataType::Dictionary(_, value_type) => {
            // Materialise as the value type (Utf8 / Binary); the merge
            // engine's union schema normalises strings to Utf8 anyway,
            // and the output writer re-applies dict encoding based on
            // observed cardinality. Decoding to a Dictionary array
            // directly would require synthesising keys; not needed.
            match value_type.as_ref() {
                DataType::Utf8 => Ok(Arc::new(build_string_array(
                    records, defs, max_def, values,
                )?)),
                DataType::LargeUtf8 => Ok(Arc::new(build_large_string_array(
                    records, defs, max_def, values,
                )?)),
                DataType::Binary => {
                    Ok(Arc::new(build_binary_array(records, defs, max_def, values)))
                }
                DataType::LargeBinary => Ok(Arc::new(build_large_binary_array(
                    records, defs, max_def, values,
                ))),
                _ => Err(PageDecodeError::UnsupportedColumnType {
                    name: field.name().to_string(),
                    physical: PhysicalType::BYTE_ARRAY,
                    arrow: field.data_type().clone(),
                }),
            }
        }
        other => Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::BYTE_ARRAY,
            arrow: other.clone(),
        }),
    }
}

fn build_string_array(
    records: usize,
    defs: &[i16],
    max_def: i16,
    values: &[ByteArray],
) -> Result<StringArray, PageDecodeError> {
    let mut builder = arrow::array::StringBuilder::with_capacity(records, records * 8);
    if defs.is_empty() || max_def == 0 {
        for v in values {
            let s = std::str::from_utf8(v.data())
                .map_err(|e| PageDecodeError::Parquet(ParquetError::General(e.to_string())))?;
            builder.append_value(s);
        }
    } else {
        let mut val_idx = 0usize;
        for d in defs.iter().take(records) {
            if *d >= max_def {
                let s = std::str::from_utf8(values[val_idx].data())
                    .map_err(|e| PageDecodeError::Parquet(ParquetError::General(e.to_string())))?;
                builder.append_value(s);
                val_idx += 1;
            } else {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}

fn build_large_string_array(
    records: usize,
    defs: &[i16],
    max_def: i16,
    values: &[ByteArray],
) -> Result<LargeStringArray, PageDecodeError> {
    let mut builder = arrow::array::LargeStringBuilder::with_capacity(records, records * 8);
    if defs.is_empty() || max_def == 0 {
        for v in values {
            let s = std::str::from_utf8(v.data())
                .map_err(|e| PageDecodeError::Parquet(ParquetError::General(e.to_string())))?;
            builder.append_value(s);
        }
    } else {
        let mut val_idx = 0usize;
        for d in defs.iter().take(records) {
            if *d >= max_def {
                let s = std::str::from_utf8(values[val_idx].data())
                    .map_err(|e| PageDecodeError::Parquet(ParquetError::General(e.to_string())))?;
                builder.append_value(s);
                val_idx += 1;
            } else {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}

fn build_binary_array(
    records: usize,
    defs: &[i16],
    max_def: i16,
    values: &[ByteArray],
) -> BinaryArray {
    let mut builder = arrow::array::BinaryBuilder::with_capacity(records, records * 8);
    if defs.is_empty() || max_def == 0 {
        for v in values {
            builder.append_value(v.data());
        }
    } else {
        let mut val_idx = 0usize;
        for d in defs.iter().take(records) {
            if *d >= max_def {
                builder.append_value(values[val_idx].data());
                val_idx += 1;
            } else {
                builder.append_null();
            }
        }
    }
    builder.finish()
}

fn build_large_binary_array(
    records: usize,
    defs: &[i16],
    max_def: i16,
    values: &[ByteArray],
) -> LargeBinaryArray {
    let mut builder = arrow::array::LargeBinaryBuilder::with_capacity(records, records * 8);
    if defs.is_empty() || max_def == 0 {
        for v in values {
            builder.append_value(v.data());
        }
    } else {
        let mut val_idx = 0usize;
        for d in defs.iter().take(records) {
            if *d >= max_def {
                builder.append_value(values[val_idx].data());
                val_idx += 1;
            } else {
                builder.append_null();
            }
        }
    }
    builder.finish()
}

fn scalar_buffer_from_present<T, U, F>(
    records: usize,
    defs: &[i16],
    max_def: i16,
    values: &[T],
    cast: F,
) -> ScalarBuffer<U>
where
    T: Copy,
    U: arrow::datatypes::ArrowNativeType,
    F: Fn(T) -> U,
{
    if defs.is_empty() || max_def == 0 {
        let casted: Vec<U> = values.iter().copied().map(&cast).collect();
        return ScalarBuffer::from(casted);
    }
    let mut out: Vec<U> = Vec::with_capacity(records);
    let mut val_idx = 0usize;
    for d in defs.iter().take(records) {
        if *d >= max_def {
            out.push(cast(values[val_idx]));
            val_idx += 1;
        } else {
            out.push(U::default());
        }
    }
    ScalarBuffer::from(out)
}

/// Compute the `ListArray` offsets vector from Dremel def/rep levels
/// for the `List<non-nullable primitive>` shape used by DDSketch
/// `keys` / `counts`. `max_def = 1`, `max_rep = 1`. Each entry with
/// `def == 1` is a present element; `def == 0` is an empty list slot
/// (no element). `rep == 0` starts a new row; `rep == 1` continues the
/// current list.
fn list_offsets_from_levels(defs: &[i16], reps: &[i16]) -> Vec<i64> {
    // Compute as `i64` so the same offset buffer can back either a
    // `ListArray` (truncated to i32 by `wrap_inner_in_list`) or a
    // `LargeListArray`. For our `max_def = 1, max_rep = 1` shape
    // there's no risk of overflow in either width.
    if defs.is_empty() {
        return vec![0];
    }
    let mut offsets: Vec<i64> = Vec::with_capacity(defs.len() + 1);
    offsets.push(0);
    let mut current_len: i64 = 0;
    for i in 0..defs.len() {
        if i > 0 && reps[i] == 0 {
            // Boundary between rows: close the previous row's list.
            offsets.push(current_len);
        }
        if defs[i] == 1 {
            current_len += 1;
        }
    }
    offsets.push(current_len);
    offsets
}

/// Build a `ListArray` over Int32-physical values (parquet inner type
/// Int8/Int16/Int32 or UInt8/UInt16/UInt32). Outer + inner must be
/// non-nullable.
fn build_list_i32_array(
    field: &Field,
    defs: &[i16],
    reps: &[i16],
    values: &[i32],
) -> Result<ArrayRef, PageDecodeError> {
    let inner_field = list_inner_field(field);
    if field.is_nullable() || inner_field.is_nullable() {
        return Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::INT32,
            arrow: field.data_type().clone(),
        });
    }
    let inner_array: ArrayRef = match inner_field.data_type() {
        DataType::Int8 => {
            let v: Vec<i8> = values.iter().map(|&x| x as i8).collect();
            Arc::new(arrow::array::Int8Array::from(v))
        }
        DataType::Int16 => {
            let v: Vec<i16> = values.iter().map(|&x| x as i16).collect();
            Arc::new(Int16Array::from(v))
        }
        DataType::Int32 => Arc::new(Int32Array::from(values.to_vec())),
        DataType::UInt8 => {
            let v: Vec<u8> = values.iter().map(|&x| x as u8).collect();
            Arc::new(UInt8Array::from(v))
        }
        DataType::UInt16 => {
            let v: Vec<u16> = values.iter().map(|&x| x as u16).collect();
            Arc::new(UInt16Array::from(v))
        }
        DataType::UInt32 => {
            // Bit-reinterpret cast preserves the unsigned-logical
            // round trip — same convention as the flat path.
            let v: Vec<u32> = values.iter().map(|&x| x as u32).collect();
            Arc::new(UInt32Array::from(v))
        }
        other => {
            return Err(PageDecodeError::UnsupportedColumnType {
                name: field.name().to_string(),
                physical: PhysicalType::INT32,
                arrow: other.clone(),
            });
        }
    };
    wrap_inner_in_list(field, inner_field, inner_array, defs, reps)
}

/// Build a `ListArray` over Int64-physical values (parquet inner type
/// Int64 or UInt64). Outer + inner must be non-nullable. DDSketch
/// `counts` is the primary use.
fn build_list_i64_array(
    field: &Field,
    defs: &[i16],
    reps: &[i16],
    values: &[i64],
) -> Result<ArrayRef, PageDecodeError> {
    let inner_field = list_inner_field(field);
    if field.is_nullable() || inner_field.is_nullable() {
        return Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::INT64,
            arrow: field.data_type().clone(),
        });
    }
    let inner_array: ArrayRef = match inner_field.data_type() {
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        DataType::UInt64 => {
            let v: Vec<u64> = values.iter().map(|&x| x as u64).collect();
            Arc::new(UInt64Array::from(v))
        }
        other => {
            return Err(PageDecodeError::UnsupportedColumnType {
                name: field.name().to_string(),
                physical: PhysicalType::INT64,
                arrow: other.clone(),
            });
        }
    };
    wrap_inner_in_list(field, inner_field, inner_array, defs, reps)
}

/// Build a `ListArray` / `LargeListArray` over `Float32` (parquet
/// inner type `Float32`). Outer + inner must be non-nullable.
fn build_list_f32_array(
    field: &Field,
    defs: &[i16],
    reps: &[i16],
    values: &[f32],
) -> Result<ArrayRef, PageDecodeError> {
    let inner_field = list_inner_field(field);
    if field.is_nullable() || inner_field.is_nullable() {
        return Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::FLOAT,
            arrow: field.data_type().clone(),
        });
    }
    let inner_array: ArrayRef = match inner_field.data_type() {
        DataType::Float32 => Arc::new(Float32Array::from(values.to_vec())),
        other => {
            return Err(PageDecodeError::UnsupportedColumnType {
                name: field.name().to_string(),
                physical: PhysicalType::FLOAT,
                arrow: other.clone(),
            });
        }
    };
    wrap_inner_in_list(field, inner_field, inner_array, defs, reps)
}

/// Build a `ListArray` / `LargeListArray` over `Float64` (parquet
/// inner type `Double`). Outer + inner must be non-nullable.
fn build_list_f64_array(
    field: &Field,
    defs: &[i16],
    reps: &[i16],
    values: &[f64],
) -> Result<ArrayRef, PageDecodeError> {
    let inner_field = list_inner_field(field);
    if field.is_nullable() || inner_field.is_nullable() {
        return Err(PageDecodeError::UnsupportedColumnType {
            name: field.name().to_string(),
            physical: PhysicalType::DOUBLE,
            arrow: field.data_type().clone(),
        });
    }
    let inner_array: ArrayRef = match inner_field.data_type() {
        DataType::Float64 => Arc::new(Float64Array::from(values.to_vec())),
        other => {
            return Err(PageDecodeError::UnsupportedColumnType {
                name: field.name().to_string(),
                physical: PhysicalType::DOUBLE,
                arrow: other.clone(),
            });
        }
    };
    wrap_inner_in_list(field, inner_field, inner_array, defs, reps)
}

/// Extract the inner field from a `List<T>` or `LargeList<T>`
/// outer field. Callers must already have checked the outer type.
fn list_inner_field(field: &Field) -> Arc<Field> {
    match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => Arc::clone(inner),
        _ => unreachable!("caller guards on List/LargeList"),
    }
}

/// Wrap a decoded inner array in `ListArray` (i32 offsets) or
/// `LargeListArray` (i64 offsets) according to the outer field's
/// `DataType`. This preserves the schema's list flavour through the
/// page decoder — `LargeList<T>` inputs round-trip to `LargeListArray`,
/// not `ListArray`, so downstream schema validation sees the right
/// type. Reps + defs are interpreted under `max_def = 1, max_rep = 1`
/// (the writer's contract for non-nullable outer + non-nullable inner
/// lists).
fn wrap_inner_in_list(
    field: &Field,
    inner_field: Arc<Field>,
    inner_array: ArrayRef,
    defs: &[i16],
    reps: &[i16],
) -> Result<ArrayRef, PageDecodeError> {
    let i64_offsets = list_offsets_from_levels(defs, reps);
    match field.data_type() {
        DataType::LargeList(_) => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(i64_offsets));
            Ok(Arc::new(LargeListArray::new(
                inner_field,
                offsets,
                inner_array,
                None,
            )))
        }
        DataType::List(_) => {
            let i32_offsets: Vec<i32> = i64_offsets.iter().map(|&o| o as i32).collect();
            let offsets = OffsetBuffer::new(ScalarBuffer::from(i32_offsets));
            Ok(Arc::new(ListArray::new(
                inner_field,
                offsets,
                inner_array,
                None,
            )))
        }
        _ => unreachable!("caller guards on List/LargeList"),
    }
}

fn max_def_for(field: &Field) -> i16 {
    if field.is_nullable() { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Range;
    use std::path::{Path, PathBuf};

    use arrow::array::{ArrayRef, DictionaryArray, RecordBatch};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{Field as ArrowField, Int32Type, Schema as ArrowSchema};
    use async_trait::async_trait;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tokio::io::AsyncRead;

    use super::*;
    use crate::storage::streaming_reader::{
        ColumnPageStream, RemoteByteSource, StreamingParquetReader,
    };

    fn make_metrics_batch(num_rows: usize) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("metric_name", dict_type.clone(), false),
            ArrowField::new("metric_type", DataType::UInt8, false),
            ArrowField::new("timestamp_secs", DataType::UInt64, false),
            ArrowField::new("value", DataType::Float64, false),
            ArrowField::new("timeseries_id", DataType::Int64, false),
            ArrowField::new("service", dict_type, true),
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
        // sprinkle nulls
        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32)
            .map(|i| if i % 5 == 0 { None } else { Some(i % 3) })
            .collect();
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

    fn write_parquet(
        batches: &[RecordBatch],
        page_row_limit: Option<usize>,
        rg_row_limit: Option<usize>,
        compression: Compression,
    ) -> Vec<u8> {
        let schema = batches[0].schema();
        let mut props_builder = WriterProperties::builder().set_compression(compression);
        if let Some(n) = page_row_limit {
            props_builder = props_builder.set_data_page_row_count_limit(n);
        }
        if let Some(n) = rg_row_limit {
            props_builder = props_builder.set_max_row_group_row_count(Some(n));
        }
        let props = props_builder.build();
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        for b in batches {
            writer.write(b).unwrap();
        }
        writer.close().unwrap();
        buf
    }

    #[derive(Clone)]
    struct InMemorySource {
        bytes: Bytes,
    }
    impl InMemorySource {
        fn new(bytes: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                bytes: Bytes::from(bytes),
            })
        }
    }
    #[async_trait]
    impl RemoteByteSource for InMemorySource {
        async fn file_size(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
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
    fn dummy_path() -> PathBuf {
        PathBuf::from("test.parquet")
    }

    fn read_canonical(bytes: &[u8]) -> RecordBatch {
        let cursor = Bytes::copy_from_slice(bytes);
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();
        let schema = builder.schema().clone();
        let reader = builder.build().unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            concat_batches(&schema, &batches).unwrap()
        }
    }

    /// Drain every DecodedPage from the decoder and assemble the
    /// resulting per-(rg, col) arrays back into a single RecordBatch by
    /// concatenation in storage order. This is what PR-6b would do if it
    /// wanted a full RecordBatch view; the decoder itself never
    /// materialises one.
    async fn drain_to_record_batch(reader: &mut StreamingParquetReader) -> RecordBatch {
        let metadata = Arc::clone(reader.metadata());
        let parquet_schema = metadata.file_metadata().schema_descr();
        // None for kv_metadata: skip the ARROW:schema hint so the
        // computed schema matches what the decoder actually produces
        // (Utf8 instead of Dictionary<Int32, Utf8>, etc.).
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(parquet_schema, None).unwrap();
        let num_cols = arrow_schema.fields().len();
        let num_rgs = metadata.num_row_groups();

        let mut per_col: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_cols];

        let mut decoder = StreamDecoder::new(reader as &mut dyn ColumnPageStream);
        while let Some(dp) = decoder.decode_next_page().await.unwrap() {
            per_col[dp.col_idx].push(dp.array);
        }

        let _ = num_rgs;
        let columns: Vec<ArrayRef> = per_col
            .into_iter()
            .map(|chunks| {
                let refs: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
                arrow::compute::concat(&refs).unwrap()
            })
            .collect();
        RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap()
    }

    #[tokio::test]
    async fn test_drain_single_rg_round_trip() {
        let batch = make_metrics_batch(64);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );
        let canonical = read_canonical(&bytes);

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let drained = drain_to_record_batch(&mut reader).await;

        // Compare per-column. Dict columns decode to their value type
        // (Utf8), so compare against the canonical's value-cast.
        for (col_idx, want_field) in canonical.schema().fields().iter().enumerate() {
            let want = canonical.column(col_idx);
            let got = drained.column(col_idx);
            assert_eq!(want.len(), got.len(), "col {col_idx} length mismatch",);
            // Cast the canonical to the decoded type for comparison.
            let want_cast = arrow::compute::cast(want, got.data_type()).unwrap();
            assert_eq!(
                want_cast.as_ref(),
                got.as_ref(),
                "col {col_idx} ({}) data mismatch",
                want_field.name(),
            );
        }
    }

    #[tokio::test]
    async fn test_drain_multi_rg_round_trip() {
        let batch = make_metrics_batch(300);
        let bytes = write_parquet(&[batch], None, Some(100), Compression::SNAPPY);
        let canonical = read_canonical(&bytes);

        let source = InMemorySource::new(bytes.clone());
        let sync_reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert!(
            sync_reader.metadata().num_row_groups() >= 2,
            "fixture must produce multi-RG",
        );

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let drained = drain_to_record_batch(&mut reader).await;
        for (col_idx, _) in canonical.schema().fields().iter().enumerate() {
            let want = canonical.column(col_idx);
            let got = drained.column(col_idx);
            assert_eq!(want.len(), got.len());
            let want_cast = arrow::compute::cast(want, got.data_type()).unwrap();
            assert_eq!(want_cast.as_ref(), got.as_ref(), "col {col_idx} mismatch");
        }
    }

    /// Each `decode_next_page` returns exactly one data page worth of
    /// rows — `row_start + array.len()` advances monotonically per
    /// (rg, col), with row_start = 0 at the start of each (rg, col).
    #[tokio::test]
    async fn test_decoded_page_row_indexing() {
        let batch = make_metrics_batch(2048);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            Some(256),
            None,
            Compression::SNAPPY,
        );

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);

        let mut per_col_cumulative: HashMap<(usize, usize), usize> = HashMap::new();
        let mut next_idx: HashMap<(usize, usize), usize> = HashMap::new();
        while let Some(dp) = decoder.decode_next_page().await.unwrap() {
            let key = (dp.rg_idx, dp.col_idx);
            let prior = per_col_cumulative.get(&key).copied().unwrap_or(0);
            assert_eq!(
                dp.row_start, prior,
                "row_start for ({}, {}) page {} should equal prior cumulative",
                dp.rg_idx, dp.col_idx, dp.page_idx_in_col,
            );
            per_col_cumulative.insert(key, prior + dp.array.len());

            let expected_idx = next_idx.get(&key).copied().unwrap_or(0);
            assert_eq!(dp.page_idx_in_col, expected_idx);
            next_idx.insert(key, expected_idx + 1);
        }
    }

    /// `decode_next_page` is idempotent at EOF.
    #[tokio::test]
    async fn test_eof_idempotent() {
        let batch = make_metrics_batch(32);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        while decoder.decode_next_page().await.unwrap().is_some() {}
        assert!(decoder.decode_next_page().await.unwrap().is_none());
        assert!(decoder.decode_next_page().await.unwrap().is_none());
    }

    /// Nullable columns: `service` has nulls every 5th row. The decoded
    /// page must surface those nulls — the null mask round-trips.
    #[tokio::test]
    async fn test_nullable_column_round_trip() {
        let batch = make_metrics_batch(50);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );
        let canonical = read_canonical(&bytes);
        let svc_idx = canonical.schema().index_of("service").unwrap();
        assert!(canonical.column(svc_idx).null_count() > 0);

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let drained = drain_to_record_batch(&mut reader).await;
        assert_eq!(
            drained.column(svc_idx).null_count(),
            canonical.column(svc_idx).null_count(),
        );
    }

    /// Compression codec round trips. The crate's parquet feature set
    /// is `arrow + snap + zstd` (see `quickwit/Cargo.toml`); LZ4 is
    /// intentionally not in scope.
    #[tokio::test]
    async fn test_compression_codecs() {
        for compression in [
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::ZSTD(parquet::basic::ZstdLevel::default()),
        ] {
            let batch = make_metrics_batch(64);
            let bytes = write_parquet(std::slice::from_ref(&batch), None, None, compression);
            let canonical = read_canonical(&bytes);
            let source = InMemorySource::new(bytes);
            let mut reader = StreamingParquetReader::try_open(source, dummy_path())
                .await
                .unwrap();
            let drained = drain_to_record_batch(&mut reader).await;
            for col_idx in 0..canonical.num_columns() {
                let want = canonical.column(col_idx);
                let got = drained.column(col_idx);
                assert_eq!(want.len(), got.len());
                let want_cast = arrow::compute::cast(want, got.data_type()).unwrap();
                assert_eq!(
                    want_cast.as_ref(),
                    got.as_ref(),
                    "compression {compression:?} col {col_idx} diverged",
                );
            }
        }
    }

    /// The decoder must not buffer the row group: across a long stream,
    /// the number of pages held in any single column's `PageQueue` at
    /// any instant stays ≤ 2 (at most a queued dictionary plus the
    /// current data page). This is a structural check of the page-
    /// bounded contract.
    #[tokio::test]
    async fn test_page_bounded_queue_depth() {
        let batch = make_metrics_batch(8192);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            Some(256),
            None,
            Compression::SNAPPY,
        );
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        while decoder.decode_next_page().await.unwrap().is_some() {
            for (_, state) in decoder.columns.iter() {
                let depth = state.queue.lock().unwrap().len();
                assert!(
                    depth <= 2,
                    "PageQueue depth {depth} exceeds page-bounded contract (≤2)",
                );
            }
        }
    }

    /// `List<UInt64>` (the DDSketch `counts` shape) round-trips with
    /// variable list lengths including the empty list and `u64::MAX`.
    /// This exercises the Dremel level → ListArray reconstruction in
    /// `build_list_i64_array`.
    #[tokio::test]
    async fn test_list_uint64_round_trip() {
        use arrow::array::ListBuilder;

        let item_field = Arc::new(ArrowField::new("item", DataType::UInt64, false));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "counts",
            DataType::List(Arc::clone(&item_field)),
            false,
        )]));

        let rows: Vec<Vec<u64>> = vec![
            vec![1, 2, 3],
            vec![],
            vec![42],
            vec![u64::MAX, 0, 0x8000_0000_0000_0000],
            vec![],
            vec![100],
        ];
        let mut builder = ListBuilder::new(arrow::array::UInt64Builder::new())
            .with_field(Arc::clone(&item_field));
        for row in &rows {
            for &v in row {
                builder.values().append_value(v);
            }
            builder.append(true);
        }
        let counts: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![counts]).unwrap();
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let dp = decoder
            .decode_next_page()
            .await
            .unwrap()
            .expect("at least one page");
        let got_list = dp
            .array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("counts must decode to ListArray");
        assert_eq!(got_list.len(), rows.len());
        for (row_idx, want) in rows.iter().enumerate() {
            let got = got_list.value(row_idx);
            let got_u64: Vec<u64> = got
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("inner must be UInt64Array")
                .values()
                .to_vec();
            assert_eq!(got_u64, *want, "row {row_idx} list mismatch");
        }
    }

    /// `List<Float64>` round-trips through the decoder as a `ListArray`
    /// with a `Float64Array` inner — NOT as a flat `Float64Array`. The
    /// type/row shape must match what the streaming writer advertises
    /// for `List<Float64>` columns. Regression test for the codex
    /// review on PR-6407.
    #[tokio::test]
    async fn test_list_float64_round_trip() {
        use arrow::array::ListBuilder;

        let item_field = Arc::new(ArrowField::new("item", DataType::Float64, false));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "samples",
            DataType::List(Arc::clone(&item_field)),
            false,
        )]));

        let rows: Vec<Vec<f64>> = vec![
            vec![1.0, 2.5, -7.5],
            vec![],
            vec![f64::MAX, f64::MIN, 0.0],
            vec![42.42],
        ];
        let mut builder = ListBuilder::new(arrow::array::Float64Builder::new())
            .with_field(Arc::clone(&item_field));
        for row in &rows {
            for &v in row {
                builder.values().append_value(v);
            }
            builder.append(true);
        }
        let samples: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![samples]).unwrap();
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let dp = decoder
            .decode_next_page()
            .await
            .unwrap()
            .expect("at least one page");
        let got_list = dp
            .array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("samples must decode to ListArray, not flat Float64Array");
        assert_eq!(got_list.len(), rows.len());
        for (row_idx, want) in rows.iter().enumerate() {
            let got = got_list.value(row_idx);
            let got_f64: Vec<f64> = got
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("inner must be Float64Array")
                .values()
                .to_vec();
            assert_eq!(got_f64, *want, "row {row_idx} list mismatch");
        }
    }

    /// `List<UInt64>` records that span multiple V1 pages are decoded
    /// without splitting. Regression test for the codex review on
    /// PR-6407: prior to the one-page lookahead in `decode_next_page`,
    /// `peek_next_page` returned `None` at every page boundary, which
    /// parquet-rs treats as "last page" — it would flush partial
    /// repetition-level state and emit incomplete records.
    ///
    /// We force the issue by writing a long list with a tiny
    /// `data_page_size_limit`, so parquet-rs splits the single
    /// list record across multiple V1 pages. With the lookahead in
    /// place, the column reader sees `peek_next_page = Some(_)` and
    /// continues consuming until the record completes.
    #[tokio::test]
    async fn test_list_record_spanning_pages_preserved() {
        use arrow::array::{ListBuilder, UInt64Array, UInt64Builder};

        let item_field = Arc::new(ArrowField::new("item", DataType::UInt64, false));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "counts",
            DataType::List(Arc::clone(&item_field)),
            false,
        )]));

        // Two records: a 50-element list (forced to span several pages
        // by the 20-byte page size limit) and a short 3-element list.
        let row_long: Vec<u64> = (0..50u64).collect();
        let row_short: Vec<u64> = vec![1, 2, 3];
        let mut builder =
            ListBuilder::new(UInt64Builder::new()).with_field(Arc::clone(&item_field));
        for v in &row_long {
            builder.values().append_value(*v);
        }
        builder.append(true);
        for v in &row_short {
            builder.values().append_value(*v);
        }
        builder.append(true);
        let counts: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![counts]).unwrap();

        // 20-byte page-size limit forces V1 pages to split the
        // 50-element list across several pages.
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_data_page_size_limit(20)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Decode all pages and accumulate the lists.
        let source = InMemorySource::new(buf);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let mut all_records: Vec<Vec<u64>> = Vec::new();
        while let Some(dp) = decoder.decode_next_page().await.unwrap() {
            let list = dp
                .array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("counts must decode to ListArray");
            for i in 0..list.len() {
                let inner = list.value(i);
                let u64_arr = inner
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("inner must be UInt64Array");
                all_records.push(u64_arr.values().to_vec());
            }
        }

        assert_eq!(
            all_records.len(),
            2,
            "must emit exactly two records, got {}: {all_records:?}",
            all_records.len()
        );
        assert_eq!(
            all_records[0], row_long,
            "first record (50 elements) must be preserved intact across page boundaries"
        );
        assert_eq!(all_records[1], row_short, "second record must be preserved",);
    }

    /// `wrap_inner_in_list` dispatches to `LargeListArray` (i64 offsets)
    /// when the outer field is `LargeList<>`, and to `ListArray` (i32
    /// offsets) when it's `List<>`. Regression for codex review on
    /// PR-6407: the builders accept either outer flavour but
    /// previously always constructed `ListArray`.
    ///
    /// Tested at the helper level rather than via parquet round-trip:
    /// `init_column_state` derives fields from
    /// `parquet_to_arrow_schema(_, None)`, which only produces
    /// `List<>` (parquet's native schema doesn't distinguish list
    /// offset widths). The `LargeList<>` branch is reachable only
    /// when callers construct fields directly, so we exercise it
    /// directly.
    #[test]
    fn test_wrap_inner_in_list_dispatches_on_outer_flavour() {
        use arrow::array::UInt64Array;

        let inner_field = Arc::new(ArrowField::new("item", DataType::UInt64, false));
        let inner_array: ArrayRef = Arc::new(UInt64Array::from(vec![1u64, 2, 3, 42, 100, 200]));
        // Three rows: [1,2,3], [42], [100,200] → defs/reps that
        // `list_offsets_from_levels` translates to offsets [0,3,4,6].
        let defs = vec![1, 1, 1, 1, 1, 1];
        let reps = vec![0, 1, 1, 0, 0, 1];

        // List<UInt64> path → ListArray with i32 offsets.
        let list_field = ArrowField::new(
            "list_field",
            DataType::List(Arc::clone(&inner_field)),
            false,
        );
        let got_list = wrap_inner_in_list(
            &list_field,
            Arc::clone(&inner_field),
            Arc::clone(&inner_array),
            &defs,
            &reps,
        )
        .expect("list dispatch");
        let list_arr = got_list
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list field must produce ListArray");
        assert_eq!(list_arr.len(), 3);
        assert_eq!(list_arr.value(0).len(), 3);
        assert_eq!(list_arr.value(1).len(), 1);
        assert_eq!(list_arr.value(2).len(), 2);

        // LargeList<UInt64> path → LargeListArray with i64 offsets.
        let large_field = ArrowField::new(
            "large_field",
            DataType::LargeList(Arc::clone(&inner_field)),
            false,
        );
        let got_large = wrap_inner_in_list(
            &large_field,
            Arc::clone(&inner_field),
            Arc::clone(&inner_array),
            &defs,
            &reps,
        )
        .expect("large list dispatch");
        let large_arr = got_large
            .as_any()
            .downcast_ref::<LargeListArray>()
            .expect("LargeList field must produce LargeListArray, not ListArray");
        assert_eq!(large_arr.len(), 3);
        assert_eq!(large_arr.value(0).len(), 3);
        assert_eq!(large_arr.value(1).len(), 1);
        assert_eq!(large_arr.value(2).len(), 2);
    }

    /// I/O failures from the page stream surface as
    /// `PageDecodeError::PageStream(ParquetReadError::Io(_))`.
    #[tokio::test]
    async fn test_io_failure_surfaces_as_page_stream_error() {
        struct FailingBodySource {
            footer: Bytes,
            file_size: u64,
        }
        #[async_trait]
        impl RemoteByteSource for FailingBodySource {
            async fn file_size(&self, _path: &Path) -> io::Result<u64> {
                Ok(self.file_size)
            }
            async fn get_slice(&self, _path: &Path, range: Range<u64>) -> io::Result<Bytes> {
                if range.start >= self.file_size - self.footer.len() as u64 {
                    let foot_start = self.file_size - self.footer.len() as u64;
                    let off = (range.start - foot_start) as usize;
                    let len = (range.end - range.start) as usize;
                    return Ok(self.footer.slice(off..off + len));
                }
                Err(io::Error::other("simulated body get failure"))
            }
            async fn get_slice_stream(
                &self,
                _path: &Path,
                _range: Range<u64>,
            ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
                Err(io::Error::other("simulated body stream failure"))
            }
        }
        let batch = make_metrics_batch(16);
        let bytes = write_parquet(
            std::slice::from_ref(&batch),
            None,
            None,
            Compression::SNAPPY,
        );
        let file_size = bytes.len() as u64;
        let source = Arc::new(FailingBodySource {
            footer: Bytes::from(bytes),
            file_size,
        });
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let err = decoder.decode_next_page().await.unwrap_err();
        match err {
            PageDecodeError::PageStream(ParquetReadError::Io(_)) => {}
            other => panic!("expected PageStream(Io), got {other:?}"),
        }
    }
}
