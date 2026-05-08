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

//! Page-stream → `RecordBatch` decoder, one row group at a time.
//!
//! Bridges PR-4's [`ColumnPageStream`] (raw compressed pages in storage
//! order) to arrow's standard `ParquetRecordBatchReaderBuilder` (decoded
//! arrays). Used by PR-6's streaming merge engine to drain inputs into
//! the form the row-major merge planner already understands, while
//! keeping per-RG memory bounded — the decoder reconstructs the byte
//! layout of one input row group at a time and discards it before
//! starting the next.
//!
//! # How it works
//!
//! Each [`Page`] carries `header_bytes` (raw Thrift-compact bytes) and
//! `bytes` (raw compressed body). Concatenating those two fields back-
//! to-back for every page in a column chunk reproduces the column
//! chunk's on-disk byte layout — *byte-exact*, not re-encoded — which
//! arrow's standard reader can decode through the public
//! [`ParquetRecordBatchReaderBuilder`] API. We allocate one buffer per
//! RG sized to `max(col_byte_range_end)`, place each column chunk at
//! its original offset, and ask the reader to read just that RG via
//! `with_row_groups`. Bytes outside known column-chunk ranges are
//! zero-padded; the standard reader only reads the byte ranges
//! advertised by metadata, so the padding is never inspected.
//!
//! # Why this avoids re-encoding
//!
//! Thrift-compact encoding is deterministic for a given struct value,
//! so we *could* re-encode parsed [`PageHeader`] structs and get
//! byte-equal output most of the time. But "most of the time" is not
//! a contract: encoder version drift inside the compactor would
//! silently corrupt outputs. Carrying the original header bytes
//! sidesteps the problem entirely.
//!
//! [`ColumnPageStream`]: super::streaming_reader::ColumnPageStream
//! [`Page`]: super::streaming_reader::Page
//! [`PageHeader`]: parquet::format::PageHeader

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
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

    /// Parquet decode error from the standard record-batch reader after
    /// reconstructing the column chunks.
    #[error("parquet decode error: {0}")]
    Parquet(#[from] ParquetError),

    /// Arrow decode error (e.g., concatenating multiple batches from
    /// the standard reader for one row group).
    #[error("arrow decode error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// The reconstructed column-chunk bytes don't match the size
    /// advertised by metadata. Means either the input file is
    /// inconsistent (unlikely from a valid producer) or the streaming
    /// reader produced incorrect [`Page::header_bytes`] / [`Page::bytes`]
    /// for the chunk.
    #[error(
        "reconstructed column chunk for ({rg_idx}, {col_idx}) is {actual} bytes but metadata \
         advertises byte_range size {expected}"
    )]
    ColumnChunkSizeMismatch {
        rg_idx: usize,
        col_idx: usize,
        expected: u64,
        actual: u64,
    },
}

/// Drains a [`ColumnPageStream`] one row group at a time, decoding each
/// row group into a `RecordBatch`.
///
/// # Caller contract
/// 1. [`Self::new`] over an open [`ColumnPageStream`].
/// 2. Loop on [`Self::next_rg`] until it returns `Ok(None)` (idempotent EOF). Each call yields one
///    row group's `RecordBatch`, in metadata order.
///
/// The decoder borrows the stream for its lifetime; the caller keeps
/// ownership so multiple decoders can be combined (e.g. one per merge
/// input) without nesting boxes.
pub struct StreamDecoder<'a> {
    stream: &'a mut dyn ColumnPageStream,
    /// First page of the next row group, held back from the previous
    /// `next_rg` call. Cleared once consumed at the start of the next
    /// call.
    pending_page: Option<Page>,
    /// Set after the underlying stream returns `Ok(None)` for the first
    /// time. Subsequent `next_rg` calls return `Ok(None)` directly.
    eof: bool,
}

impl<'a> StreamDecoder<'a> {
    /// Wrap an open page stream.
    pub fn new(stream: &'a mut dyn ColumnPageStream) -> Self {
        Self {
            stream,
            pending_page: None,
            eof: false,
        }
    }

    /// Read pages until the row-group index advances (or EOF), then
    /// decode the buffered pages into a `RecordBatch`.
    ///
    /// Returns `Ok(None)` after the last row group; further calls
    /// continue to return `Ok(None)` (idempotent EOF).
    pub async fn next_rg(&mut self) -> Result<Option<RecordBatch>, PageDecodeError> {
        if self.eof && self.pending_page.is_none() {
            return Ok(None);
        }

        let metadata: Arc<ParquetMetaData> = Arc::clone(self.stream.metadata());

        let mut pages_by_col: BTreeMap<usize, Vec<Page>> = BTreeMap::new();
        let mut current_rg_idx: Option<usize> = None;

        if let Some(p) = self.pending_page.take() {
            current_rg_idx = Some(p.rg_idx);
            pages_by_col.entry(p.col_idx).or_default().push(p);
        }

        if !self.eof {
            loop {
                let next = self.stream.next_page().await?;
                let page = match next {
                    Some(p) => p,
                    None => {
                        self.eof = true;
                        break;
                    }
                };
                match current_rg_idx {
                    None => {
                        current_rg_idx = Some(page.rg_idx);
                        pages_by_col.entry(page.col_idx).or_default().push(page);
                    }
                    Some(rg) if page.rg_idx == rg => {
                        pages_by_col.entry(page.col_idx).or_default().push(page);
                    }
                    Some(_) => {
                        self.pending_page = Some(page);
                        break;
                    }
                }
            }
        }

        let rg_idx = match current_rg_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let batch = decode_row_group(&metadata, rg_idx, pages_by_col)?;
        Ok(Some(batch))
    }
}

/// Decode one row group into a `RecordBatch` by reconstructing its
/// column-chunk byte layout in a buffer and feeding that buffer through
/// arrow's standard reader.
fn decode_row_group(
    metadata: &Arc<ParquetMetaData>,
    rg_idx: usize,
    pages_by_col: BTreeMap<usize, Vec<Page>>,
) -> Result<RecordBatch, PageDecodeError> {
    let rg = metadata.row_group(rg_idx);

    // Compute the buffer size we need to cover every column chunk in
    // this row group. `byte_range` returns `(start, size)` per column;
    // we allocate up to the max end so the column chunks land at
    // their original offsets.
    let mut max_end: u64 = 0;
    for col_idx in 0..rg.num_columns() {
        let (start, size) = rg.column(col_idx).byte_range();
        let end = start + size;
        if end > max_end {
            max_end = end;
        }
    }

    // Empty / zero-row row group: produce an empty batch with the
    // file's arrow schema rather than driving the parquet reader.
    if rg.num_columns() == 0 || max_end == 0 || rg.num_rows() == 0 {
        let arrow_meta =
            ArrowReaderMetadata::try_new(Arc::clone(metadata), ArrowReaderOptions::default())?;
        return Ok(RecordBatch::new_empty(arrow_meta.schema().clone()));
    }

    let mut buf = vec![0u8; max_end as usize];

    for (col_idx_ref, pages) in &pages_by_col {
        let col_idx = *col_idx_ref;
        let col_meta = rg.column(col_idx);
        let (col_start, col_size) = col_meta.byte_range();

        let chunk_bytes = reconstruct_column_chunk(pages);
        let actual = chunk_bytes.len() as u64;
        if actual != col_size {
            return Err(PageDecodeError::ColumnChunkSizeMismatch {
                rg_idx,
                col_idx,
                expected: col_size,
                actual,
            });
        }
        let start = col_start as usize;
        let end = start + chunk_bytes.len();
        buf[start..end].copy_from_slice(&chunk_bytes);
    }

    let bytes = Bytes::from(buf);

    let arrow_meta =
        ArrowReaderMetadata::try_new(Arc::clone(metadata), ArrowReaderOptions::default())?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, arrow_meta)
        .with_row_groups(vec![rg_idx]);
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    consolidate_batches(metadata, batches)
}

/// Concatenate `header_bytes ++ bytes` for every page in storage order.
fn reconstruct_column_chunk(pages: &[Page]) -> Vec<u8> {
    let total: usize = pages
        .iter()
        .map(|p| p.header_bytes.len() + p.bytes.len())
        .sum();
    let mut buf = Vec::with_capacity(total);
    for page in pages {
        buf.extend_from_slice(&page.header_bytes);
        buf.extend_from_slice(&page.bytes);
    }
    buf
}

/// Collapse the arrow reader's per-call output (which may emit several
/// `RecordBatch`es per row group at the configured batch size) into a
/// single `RecordBatch` per row group, matching the
/// [`StreamDecoder::next_rg`] contract.
fn consolidate_batches(
    metadata: &Arc<ParquetMetaData>,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, PageDecodeError> {
    if batches.is_empty() {
        let arrow_meta =
            ArrowReaderMetadata::try_new(Arc::clone(metadata), ArrowReaderOptions::default())?;
        return Ok(RecordBatch::new_empty(arrow_meta.schema().clone()));
    }
    if batches.len() == 1 {
        let mut iter = batches.into_iter();
        match iter.next() {
            Some(b) => return Ok(b),
            None => {
                return Err(PageDecodeError::Parquet(ParquetError::General(
                    "Vec::into_iter on len-1 vec yielded no element".to_string(),
                )));
            }
        }
    }
    let schema = batches[0].schema();
    Ok(arrow::compute::concat_batches(&schema, &batches)?)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Range;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use arrow::array::{
        ArrayRef, DictionaryArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt64Array,
    };
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
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

    // -------- Fixture --------

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
        // Sprinkle nulls into `service` to exercise null preservation.
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

    /// Write a parquet file with configurable page-row limit, RG row
    /// limit, and compression.
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

    // -------- In-memory byte source --------

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
            Ok(Box::new(std::io::Cursor::new(slice.to_vec())))
        }
    }

    fn dummy_path() -> PathBuf {
        PathBuf::from("test.parquet")
    }

    /// Read a parquet file synchronously to derive the canonical
    /// `RecordBatch` for comparison.
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

    /// Drain the decoder until EOF, returning all per-RG batches.
    async fn drain_all(decoder: &mut StreamDecoder<'_>) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = decoder.next_rg().await.unwrap() {
            out.push(b);
        }
        out
    }

    // -------- Tests --------

    /// Single-RG file: decoder yields exactly one batch and it equals
    /// the canonical decode of the file.
    #[tokio::test]
    async fn test_drain_single_rg_matches_canonical() {
        let batch = make_metrics_batch(64);
        let bytes = write_parquet(std::slice::from_ref(&batch), None, None, Compression::SNAPPY);
        let canonical = read_canonical(&bytes);

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();

        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let drained = drain_all(&mut decoder).await;
        assert_eq!(drained.len(), 1, "single-RG file yields one batch");
        assert_eq!(drained[0], canonical);
    }

    /// Multi-RG file: decoder yields one batch per RG; concatenation
    /// equals the canonical decode.
    #[tokio::test]
    async fn test_drain_multi_rg_yields_one_batch_per_rg() {
        let batch = make_metrics_batch(300);
        let bytes = write_parquet(&[batch], None, Some(100), Compression::SNAPPY);
        let canonical = read_canonical(&bytes);

        let source = InMemorySource::new(bytes.clone());
        let sync_reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        let expected_num_rgs = sync_reader.metadata().num_row_groups();
        assert!(
            expected_num_rgs >= 2,
            "fixture must produce multi-RG; got {expected_num_rgs}",
        );

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let drained = drain_all(&mut decoder).await;

        assert_eq!(drained.len(), expected_num_rgs);
        let schema = canonical.schema();
        let concatenated = concat_batches(&schema, &drained).unwrap();
        assert_eq!(concatenated, canonical);
    }

    /// Multi-page columns within a single RG decode correctly. Forces
    /// the reader to traverse multiple page headers per column.
    #[tokio::test]
    async fn test_drain_multi_page_column() {
        let batch = make_metrics_batch(2048);
        let bytes = write_parquet(&[batch], Some(256), None, Compression::SNAPPY);
        let canonical = read_canonical(&bytes);

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let drained = drain_all(&mut decoder).await;

        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0], canonical);
    }

    /// Dictionary-encoded columns survive the page-stream → batch
    /// round trip. Verified implicitly by full equality with the
    /// canonical decode (which preserves dict encoding).
    #[tokio::test]
    async fn test_drain_preserves_dict_columns() {
        let batch = make_metrics_batch(64);
        let bytes = write_parquet(&[batch], None, None, Compression::SNAPPY);
        let canonical = read_canonical(&bytes);
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();

        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let drained = drain_all(&mut decoder).await;
        assert_eq!(drained.len(), 1);
        // Confirm dict types survived the round trip by name+datatype
        // before relying on full equality.
        let drained_schema = drained[0].schema();
        for field in canonical.schema().fields() {
            let drained_field = drained_schema.field_with_name(field.name()).unwrap();
            assert_eq!(field.data_type(), drained_field.data_type());
        }
        assert_eq!(drained[0], canonical);
    }

    /// Null values in `service` survive the round trip (the fixture
    /// inserts nulls every 5th row).
    #[tokio::test]
    async fn test_drain_preserves_nulls() {
        let batch = make_metrics_batch(50);
        let bytes = write_parquet(&[batch], None, None, Compression::SNAPPY);
        let canonical = read_canonical(&bytes);
        let svc_idx = canonical.schema().index_of("service").unwrap();
        assert!(canonical.column(svc_idx).null_count() > 0);

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let drained = drain_all(&mut decoder).await;
        assert_eq!(drained.len(), 1);
        let drained_svc = drained[0].column(svc_idx);
        assert_eq!(
            drained_svc.null_count(),
            canonical.column(svc_idx).null_count(),
        );
        assert_eq!(drained[0], canonical);
    }

    /// Round-trip through every compression we support in production.
    /// The crate's `parquet` dependency is built with `snap` and
    /// `zstd` features (see `quickwit/Cargo.toml`); LZ4 is not enabled
    /// because Quickwit doesn't write LZ4-compressed parquet.
    #[tokio::test]
    async fn test_drain_supports_compression_codecs() {
        for compression in [
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::ZSTD(parquet::basic::ZstdLevel::default()),
        ] {
            let batch = make_metrics_batch(128);
            let bytes = write_parquet(&[batch], None, None, compression);
            let canonical = read_canonical(&bytes);
            let source = InMemorySource::new(bytes);
            let mut reader = StreamingParquetReader::try_open(source, dummy_path())
                .await
                .unwrap();
            let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
            let drained = drain_all(&mut decoder).await;
            assert_eq!(
                drained.len(),
                1,
                "single-RG fixture must produce 1 batch (compression={compression:?})",
            );
            assert_eq!(
                drained[0], canonical,
                "round trip diverged for compression={compression:?}",
            );
        }
    }

    /// `next_rg` is idempotent at EOF — repeated calls after the last
    /// row group keep returning `Ok(None)` instead of stalling or
    /// erroring.
    #[tokio::test]
    async fn test_eof_idempotent() {
        let batch = make_metrics_batch(32);
        let bytes = write_parquet(&[batch], None, None, Compression::SNAPPY);
        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);

        assert!(decoder.next_rg().await.unwrap().is_some());
        assert!(decoder.next_rg().await.unwrap().is_none());
        assert!(decoder.next_rg().await.unwrap().is_none());
        assert!(decoder.next_rg().await.unwrap().is_none());
    }

    /// Page-stream I/O failures surface as
    /// [`PageDecodeError::PageStream`] — they are NOT masked as
    /// parquet decode errors. Verifies the policy from CLAUDE.md
    /// (no silent error swallowing).
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
                // Footer reads succeed; that's how `try_open` parses
                // metadata. Body reads (start near 0) fail.
                if range.start >= self.file_size - self.footer.len() as u64 {
                    let foot_start = self.file_size - self.footer.len() as u64;
                    let off = (range.start - foot_start) as usize;
                    let len = (range.end - range.start) as usize;
                    return Ok(self.footer.slice(off..off + len));
                }
                Err(io::Error::other("simulated body GET failure"))
            }
            async fn get_slice_stream(
                &self,
                _path: &Path,
                _range: Range<u64>,
            ) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
                Err(io::Error::other("simulated body stream failure"))
            }
        }

        // Build a real file so the footer parses, then plug it into
        // a source that errors on body reads.
        let batch = make_metrics_batch(16);
        let bytes = write_parquet(&[batch], None, None, Compression::SNAPPY);
        let file_size = bytes.len() as u64;
        // Read the entire file as the "footer" buffer so any footer
        // GET (with retry) succeeds.
        let source = Arc::new(FailingBodySource {
            footer: Bytes::from(bytes),
            file_size,
        });

        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();
        let mut decoder = StreamDecoder::new(&mut reader as &mut dyn ColumnPageStream);
        let err = decoder.next_rg().await.unwrap_err();
        match err {
            PageDecodeError::PageStream(ParquetReadError::Io(_)) => {}
            other => panic!("expected PageStream(Io), got {other:?}"),
        }
    }

    /// The reconstructed column-chunk byte size must equal the size
    /// metadata advertises — proves byte-exact reconstruction
    /// (`header_bytes + bytes` per page) for production-shape inputs.
    #[tokio::test]
    async fn test_byte_exact_reconstruction() {
        let batch = make_metrics_batch(512);
        let bytes = write_parquet(&[batch], Some(64), Some(200), Compression::SNAPPY);
        let sync_reader = SerializedFileReader::new(Bytes::from(bytes.clone())).unwrap();
        let metadata = sync_reader.metadata();

        let source = InMemorySource::new(bytes);
        let mut reader = StreamingParquetReader::try_open(source, dummy_path())
            .await
            .unwrap();

        // Drain pages and verify per-(rg, col) reconstructed-chunk
        // sizes match metadata.
        let chunk_sizes: Mutex<BTreeMap<(usize, usize), u64>> = Mutex::new(BTreeMap::new());
        loop {
            let page = reader.next_page().await.unwrap();
            let p = match page {
                Some(p) => p,
                None => break,
            };
            let total = (p.header_bytes.len() + p.bytes.len()) as u64;
            *chunk_sizes
                .lock()
                .unwrap()
                .entry((p.rg_idx, p.col_idx))
                .or_insert(0) += total;
        }

        let chunk_sizes = chunk_sizes.into_inner().unwrap();
        for ((rg_idx, col_idx), reconstructed_size) in &chunk_sizes {
            let (_, expected) = metadata.row_group(*rg_idx).column(*col_idx).byte_range();
            assert_eq!(
                *reconstructed_size, expected,
                "rg={rg_idx} col={col_idx}: reconstructed {reconstructed_size} != metadata \
                 {expected}",
            );
        }
    }
}
