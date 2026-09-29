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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::{FileSourceParams, SourceInputFormat};
use quickwit_proto::metastore::SourceType;
use serde_json::json;

use super::ndjson::record_batch_to_ndjson_docs;
use super::plan::ParquetLoadPlan;
use crate::actors::DocProcessor;
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, Source, SourceContext, SourceFactory, SourceRuntime,
};

/// Creates the [`ParquetSource`]s of a Parquet load, which all read from the same plan.
///
/// It is registered for the `file` source type in the source loader of the CLI's indexing service
/// only: servers use the regular file source, so they can't run a Parquet load.
pub struct ParquetSourceFactory {
    plan: Arc<ParquetLoadPlan>,
}

impl ParquetSourceFactory {
    pub fn new(plan: Arc<ParquetLoadPlan>) -> Self {
        Self { plan }
    }
}

#[async_trait]
impl SourceFactory for ParquetSourceFactory {
    async fn create_source(
        &self,
        source_runtime: SourceRuntime,
    ) -> anyhow::Result<Box<dyn Source>> {
        let source_params: FileSourceParams =
            serde_json::from_value(source_runtime.source_config.params())?;
        let parquet_source =
            ParquetSource::try_new(self.plan.clone(), &source_runtime, source_params)?;
        Ok(Box::new(parquet_source))
    }
}

/// Reads row groups handed out by a shared [`ParquetLoadPlan`] and emits their rows as NDJSON
/// documents.
///
/// The emitted batches carry an empty checkpoint delta: a Parquet load is all or nothing, so a
/// source never resumes a previous load. If the pipeline restarts, the row groups handed out to
/// the failed source are lost; the CLI aborts the load in that case.
pub struct ParquetSource {
    plan: Arc<ParquetLoadPlan>,
    /// Row group being read. `None` between row groups, and while the reader is lent to the
    /// decoding task.
    current_opt: Option<(usize, ParquetRecordBatchReader)>,
    num_rows_emitted: u64,
    num_bytes_emitted: u64,
}

impl fmt::Debug for ParquetSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ParquetSource {{ file_uri: {} }}",
            self.plan.file_uri()
        )
    }
}

impl ParquetSource {
    /// Creates a source reading from `plan`. The source config must describe the plan's file, with
    /// the `json` input format.
    fn try_new(
        plan: Arc<ParquetLoadPlan>,
        source_runtime: &SourceRuntime,
        source_params: FileSourceParams,
    ) -> anyhow::Result<Self> {
        let FileSourceParams::Filepath(file_uri) = source_params else {
            bail!("a Parquet load reads a file path, not file notifications");
        };
        if &file_uri != plan.file_uri() {
            bail!(
                "the Parquet load plan reads `{}`, not `{file_uri}`",
                plan.file_uri()
            );
        }
        // The rows are emitted as JSON documents: any other input format would make the doc
        // processor misinterpret them.
        let input_format = source_runtime.source_config.input_format;
        if input_format != SourceInputFormat::Json {
            bail!("Parquet files require the `json` input format, got `{input_format:?}`");
        }
        Ok(Self {
            plan,
            current_opt: None,
            num_rows_emitted: 0,
            num_bytes_emitted: 0,
        })
    }

    /// Decodes one Arrow record batch and sends it to the doc processor, split into batches of at
    /// most `BATCH_NUM_BYTES_LIMIT` bytes.
    ///
    /// Returns `false` once all the row groups of the plan have been handed out and read.
    async fn emit_record_batch(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<bool, ActorExitStatus> {
        let current_opt = match self.current_opt.take() {
            Some(current) => Some(current),
            None => self.plan.next_row_group_reader()?,
        };
        let Some((row_group_idx, reader)) = current_opt else {
            return Ok(false);
        };
        let (reader, docs_res) = ctx.protect_future(decode_next_batch(reader)).await?;
        let Some(docs) = docs_res.with_context(|| {
            format!(
                "failed to decode row group {row_group_idx} of `{}`",
                self.plan.file_uri()
            )
        })?
        else {
            // The row group is exhausted. The next call moves on to the next row group.
            return Ok(true);
        };
        self.current_opt = Some((row_group_idx, reader));

        let mut batch_builder = BatchBuilder::new(SourceType::File);
        let num_docs = docs.len();
        for (doc_idx, doc) in docs.into_iter().enumerate() {
            batch_builder.add_doc(doc);
            let is_last_doc = doc_idx + 1 == num_docs;
            if !is_last_doc && batch_builder.num_bytes < BATCH_NUM_BYTES_LIMIT {
                continue;
            }
            self.num_rows_emitted += batch_builder.docs.len() as u64;
            self.num_bytes_emitted += batch_builder.num_bytes;
            let raw_doc_batch =
                std::mem::replace(&mut batch_builder, BatchBuilder::new(SourceType::File)).build();
            ctx.send_message(doc_processor_mailbox, raw_doc_batch)
                .await?;
        }
        Ok(true)
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let has_more = self.emit_record_batch(doc_processor_mailbox, ctx).await?;
        if !has_more {
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::ZERO)
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({
            "num_rows_emitted": self.num_rows_emitted,
            "num_bytes_emitted": self.num_bytes_emitted,
        })
    }
}

/// Decodes the next record batch on the blocking runtime: Parquet decoding and JSON encoding are
/// CPU-intensive, and sources run on the non-blocking runtime.
///
/// Returns the reader along with the documents of the batch, or `None` if the reader is
/// exhausted.
async fn decode_next_batch(
    mut reader: ParquetRecordBatchReader,
) -> anyhow::Result<(ParquetRecordBatchReader, anyhow::Result<Option<Vec<Bytes>>>)> {
    let join_handle = RuntimeType::Blocking
        .get_runtime_handle()
        .spawn(async move {
            let docs_res = match reader.next() {
                Some(Ok(record_batch)) => record_batch_to_ndjson_docs(&record_batch).map(Some),
                Some(Err(error)) => Err(anyhow::Error::from(error)),
                None => Ok(None),
            };
            (reader, docs_res)
        });
    join_handle
        .await
        .context("Parquet decoding task panicked or was cancelled")
}
