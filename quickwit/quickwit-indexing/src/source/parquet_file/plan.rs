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

//! Load plan shared by the indexing pipelines of a Parquet bulk load.
//!
//! A load plan splits one local Parquet file into work units (row groups), handed out to the
//! pipelines on demand. A load is all or nothing: nothing is checkpointed, and the CLI aborts the
//! load as soon as one pipeline fails. A row group handed out is therefore never handed out again.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, bail};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use quickwit_common::uri::{Protocol, Uri};

use super::ndjson::record_batch_to_ndjson_docs;

/// Default number of rows decoded per Arrow record batch.
pub const DEFAULT_PARQUET_BATCH_NUM_ROWS: usize = 8192;

/// A plan to load one local Parquet file with several indexing pipelines.
///
/// The CLI creates the plan before spawning the pipelines and hands it to their sources through
/// a [`ParquetSourceFactory`](super::ParquetSourceFactory).
pub struct ParquetLoadPlan {
    file_uri: Uri,
    filepath: PathBuf,
    arrow_metadata: ArrowReaderMetadata,
    batch_num_rows: usize,
    /// Index of the next row group to hand out.
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
    /// Reads the footer of the Parquet file and checks that its rows can be converted to JSON.
    ///
    /// This function performs blocking IO.
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
        let arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())
            .with_context(|| format!("failed to read Parquet footer of `{file_uri}`"))?;
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

    /// Fails early if the rows of the file cannot be converted to JSON, instead of failing in
    /// every pipeline.
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

    /// Hands out the next row group and returns a reader over its rows, or `None` when all the
    /// row groups have been handed out.
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
