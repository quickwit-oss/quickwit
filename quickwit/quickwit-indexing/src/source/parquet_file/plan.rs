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

//! Shared row-group allocation for one local Parquet load.
//! Groups are claimed once, never checkpointed or retried; pipeline failure aborts the load.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, bail};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use quickwit_common::uri::{Protocol, Uri};

use super::source::record_batch_to_ndjson_docs;

/// Default number of rows decoded per Arrow record batch.
pub const DEFAULT_PARQUET_BATCH_NUM_ROWS: usize = 8192;

/// Shared file metadata and row-group cursor, injected via
/// [`ParquetSourceFactory`](super::ParquetSourceFactory).
pub struct ParquetLoadPlan {
    file_uri: Uri,
    filepath: PathBuf,
    arrow_metadata: ArrowReaderMetadata,
    /// Arrow docs mode: the file, memory-mapped once. Page reads are then slices of the page cache
    /// instead of `read` copies (~4% of the bulk load CPU).
    mmap_opt: Option<bytes::Bytes>,
    /// Sources send record batches and documents are built from Arrow columns
    /// (`QW_PARQUET_ARROW_DOCS`).
    arrow_docs: bool,
    batch_num_rows: usize,
    next_row_group_idx: AtomicUsize,
}

impl std::fmt::Debug for ParquetLoadPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ParquetLoadPlan")
            .field("file_uri", &self.file_uri)
            .field("num_row_groups", &self.num_row_groups())
            .field("batch_num_rows", &self.batch_num_rows)
            .finish()
    }
}

impl ParquetLoadPlan {
    /// Reads metadata and checks one row's JSON conversion. Performs blocking I/O.
    ///
    /// Experimental: `QW_PARQUET_ARROW_DOCS=true` builds documents straight from Arrow columns.
    pub fn try_new(file_uri: Uri, batch_num_rows: usize) -> anyhow::Result<Self> {
        let arrow_docs = quickwit_common::get_bool_from_env("QW_PARQUET_ARROW_DOCS", false);
        Self::try_new_with_arrow_docs(file_uri, batch_num_rows, arrow_docs)
    }

    /// [`Self::try_new`] with an explicit Arrow documents mode.
    pub fn try_new_with_arrow_docs(
        file_uri: Uri,
        batch_num_rows: usize,
        arrow_docs: bool,
    ) -> anyhow::Result<Self> {
        if file_uri.protocol() != Protocol::File {
            bail!("Parquet input only supports local files, got `{file_uri}`");
        }
        if batch_num_rows == 0 {
            bail!("Parquet batch number of rows must be strictly positive");
        }
        let filepath = file_uri
            .filepath()
            .context("failed to extract file path from URI")?
            .to_path_buf();
        let file = File::open(&filepath)
            .with_context(|| format!("failed to open file `{}`", filepath.display()))?;
        let mut arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())
            .with_context(|| format!("failed to read Parquet footer of `{file_uri}`"))?;
        if arrow_docs {
            // Decode strings as views: the reader then points into the decompressed pages
            // instead of copying every value into a new buffer (dictionary pages included).
            let view_schema = with_string_views(arrow_metadata.schema());
            if view_schema.as_ref() != arrow_metadata.schema().as_ref() {
                let options = ArrowReaderOptions::new().with_schema(view_schema);
                arrow_metadata =
                    ArrowReaderMetadata::try_new(arrow_metadata.metadata().clone(), options)
                        .with_context(|| {
                            format!("failed to read Parquet footer of `{file_uri}`")
                        })?;
            }
        }
        let mmap_opt = if arrow_docs {
            // SAFETY: the file is opened read-only and not expected to change during the load;
            // a concurrent truncation would make reads of the mapping fault.
            let mmap = unsafe { memmap2::Mmap::map(&file) }
                .with_context(|| format!("failed to memory-map `{}`", filepath.display()))?;
            Some(bytes::Bytes::from_owner(mmap))
        } else {
            None
        };
        let plan = Self {
            file_uri,
            filepath,
            arrow_metadata,
            mmap_opt,
            arrow_docs,
            batch_num_rows,
            next_row_group_idx: AtomicUsize::new(0),
        };
        plan.check_json_conversion()?;
        Ok(plan)
    }

    /// Checks JSON conversion before spawning pipelines.
    fn check_json_conversion(&self) -> anyhow::Result<()> {
        let row_groups = self.arrow_metadata.metadata().row_groups();
        let Some(row_group_idx) = row_groups
            .iter()
            .position(|row_group| row_group.num_rows() > 0)
        else {
            return Ok(());
        };
        let mut reader = self.build_row_group_reader(row_group_idx, Some(1))?;
        if let Some(record_batch) = reader.next() {
            record_batch_to_ndjson_docs(&record_batch?)
                .with_context(|| format!("unsupported Parquet schema in `{}`", self.file_uri))?;
        }
        Ok(())
    }

    fn row_group_reader_builder<R: parquet::file::reader::ChunkReader + 'static>(
        &self,
        reader: R,
        row_group_idx: usize,
    ) -> ParquetRecordBatchReaderBuilder<R> {
        ParquetRecordBatchReaderBuilder::new_with_metadata(reader, self.arrow_metadata.clone())
            .with_row_groups(vec![row_group_idx])
            .with_batch_size(self.batch_num_rows)
    }

    /// A reader for one row group, reading at most `limit_opt` rows.
    fn build_row_group_reader(
        &self,
        row_group_idx: usize,
        limit_opt: Option<usize>,
    ) -> anyhow::Result<ParquetRecordBatchReader> {
        let reader = if let Some(mmap) = &self.mmap_opt {
            let mut builder = self.row_group_reader_builder(mmap.clone(), row_group_idx);
            if let Some(limit) = limit_opt {
                builder = builder.with_limit(limit);
            }
            builder.build()?
        } else {
            let file = File::open(&self.filepath)
                .with_context(|| format!("failed to open file `{}`", self.filepath.display()))?;
            let mut builder = self.row_group_reader_builder(file, row_group_idx);
            if let Some(limit) = limit_opt {
                builder = builder.with_limit(limit);
            }
            builder.build()?
        };
        Ok(reader)
    }

    /// Claims the next row group, or returns `None` when all groups have been claimed.
    pub(super) fn next_row_group_reader(
        &self,
    ) -> anyhow::Result<Option<(usize, ParquetRecordBatchReader)>> {
        let row_group_idx = self.next_row_group_idx.fetch_add(1, Ordering::Relaxed);
        if row_group_idx >= self.num_row_groups() {
            return Ok(None);
        }
        let reader = self
            .build_row_group_reader(row_group_idx, None)
            .with_context(|| {
                format!(
                    "failed to read row group {row_group_idx} of `{}`",
                    self.file_uri
                )
            })?;
        Ok(Some((row_group_idx, reader)))
    }

    pub fn arrow_docs(&self) -> bool {
        self.arrow_docs
    }

    pub fn file_uri(&self) -> &Uri {
        &self.file_uri
    }

    pub fn num_rows(&self) -> u64 {
        self.arrow_metadata
            .metadata()
            .file_metadata()
            .num_rows()
            .max(0) as u64
    }

    pub fn batch_num_rows(&self) -> usize {
        self.batch_num_rows
    }

    pub fn num_row_groups(&self) -> usize {
        self.arrow_metadata.metadata().num_row_groups()
    }

    /// Uncompressed size of the file, as reported by the footer.
    pub fn num_uncompressed_bytes(&self) -> u64 {
        self.arrow_metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group| row_group.total_byte_size().max(0) as u64)
            .sum()
    }
}

/// `schema` with `Utf8` (also inside maps, lists and structs) replaced by `Utf8View`.
fn with_string_views(schema: &arrow_schema::SchemaRef) -> arrow_schema::SchemaRef {
    use arrow_schema::{DataType, Field, Fields, Schema};
    fn convert(data_type: &DataType) -> DataType {
        match data_type {
            DataType::Utf8 => DataType::Utf8View,
            DataType::Map(entries, sorted) => DataType::Map(convert_field(entries), *sorted),
            DataType::List(item) => DataType::List(convert_field(item)),
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(convert_field).collect::<Fields>())
            }
            other => other.clone(),
        }
    }
    fn convert_field(field: &std::sync::Arc<Field>) -> std::sync::Arc<Field> {
        std::sync::Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(convert(field.data_type())),
        )
    }
    let fields: Fields = schema.fields().iter().map(convert_field).collect();
    std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
