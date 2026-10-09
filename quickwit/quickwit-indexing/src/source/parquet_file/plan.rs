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
    pub fn try_new(file_uri: Uri, batch_num_rows: usize) -> anyhow::Result<Self> {
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
        if super::source::arrow_docs_enabled() {
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
        let plan = Self {
            file_uri,
            filepath,
            arrow_metadata,
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
        let mut reader = self
            .row_group_reader_builder(row_group_idx)?
            .with_limit(1)
            .build()?;
        if let Some(record_batch) = reader.next() {
            record_batch_to_ndjson_docs(&record_batch?)
                .with_context(|| format!("unsupported Parquet schema in `{}`", self.file_uri))?;
        }
        Ok(())
    }

    fn row_group_reader_builder(
        &self,
        row_group_idx: usize,
    ) -> anyhow::Result<ParquetRecordBatchReaderBuilder<File>> {
        let file = File::open(&self.filepath)
            .with_context(|| format!("failed to open file `{}`", self.filepath.display()))?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.arrow_metadata.clone())
                .with_row_groups(vec![row_group_idx])
                .with_batch_size(self.batch_num_rows);
        Ok(builder)
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
            .row_group_reader_builder(row_group_idx)?
            .build()
            .with_context(|| {
                format!(
                    "failed to read row group {row_group_idx} of `{}`",
                    self.file_uri
                )
            })?;
        Ok(Some((row_group_idx, reader)))
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
