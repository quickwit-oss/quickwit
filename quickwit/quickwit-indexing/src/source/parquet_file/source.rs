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
use std::io::Write;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use anyhow::{Context, bail};
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowDictionaryKeyType, Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayAccessor, DictionaryArray, RecordBatch, downcast_dictionary_array};
use arrow_json::writer::{
    Encoder, EncoderFactory, EncoderOptions, LineDelimited, NullableEncoder, Writer, WriterBuilder,
    make_encoder,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use async_trait::async_trait;
use base64::display::Base64Display;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::{FileSourceParams, SourceInputFormat};
use quickwit_proto::metastore::SourceType;
use serde_json::json;

use super::plan::ParquetLoadPlan;
use crate::actors::DocProcessor;
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, Source, SourceContext, SourceFactory, SourceRuntime,
};

/// Creates sources sharing one load plan; registered as `file` only in the CLI's loader.
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

/// Emits row groups from a shared [`ParquetLoadPlan`] as JSON documents.
/// Checkpoints are empty: restarts lose claimed groups, so the CLI must abort the load.
pub struct ParquetSource {
    plan: Arc<ParquetLoadPlan>,
    /// Current row group; absent between groups and while borrowed by the decoding task.
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
    /// Requires the plan's file and the `json` input format.
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

    /// Emits one record batch in byte-limited messages (one document may exceed the limit).
    /// Returns `false` when this source has no remaining row groups.
    async fn emit_record_batch(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<bool, ActorExitStatus> {
        let next_batch = ctx
            .protect_future(decode_next_batch(
                self.plan.clone(),
                self.current_opt.take(),
            ))
            .await?;
        let Some((row_group_idx, reader, docs_opt)) = next_batch else {
            return Ok(false);
        };
        let Some(docs) = docs_opt else {
            // Advance to the next row group on the next call.
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

/// Acquires readers, decodes and JSON-encodes on the blocking runtime.
/// Outer `None` marks EOF; absent documents mark an exhausted row group.
pub(super) async fn decode_next_batch(
    plan: Arc<ParquetLoadPlan>,
    current_opt: Option<(usize, ParquetRecordBatchReader)>,
) -> anyhow::Result<Option<(usize, ParquetRecordBatchReader, Option<Vec<Bytes>>)>> {
    let join_handle = RuntimeType::Blocking
        .get_runtime_handle()
        .spawn(async move {
            let current_opt = match current_opt {
                Some(current) => Some(current),
                None => plan.next_row_group_reader()?,
            };
            let Some((row_group_idx, mut reader)) = current_opt else {
                return Ok(None);
            };
            let docs_res = match reader.next() {
                Some(Ok(record_batch)) => record_batch_to_ndjson_docs(&record_batch).map(Some),
                Some(Err(error)) => Err(anyhow::Error::from(error)),
                None => Ok(None),
            };
            let docs_opt = docs_res.with_context(|| {
                format!(
                    "failed to decode row group {row_group_idx} of `{}`",
                    plan.file_uri()
                )
            })?;
            Ok(Some((row_group_idx, reader, docs_opt)))
        });
    join_handle
        .await
        .context("Parquet decoding task panicked or was cancelled")?
}

/// Treat naive timestamps as UTC for RFC 3339 parsing; zoned timestamps keep their offset.
const NAIVE_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

fn new_ndjson_writer(
    buffer: Vec<u8>,
    encoder_factory: Arc<ParquetEncoderFactory>,
) -> Writer<Vec<u8>, LineDelimited> {
    WriterBuilder::new()
        .with_explicit_nulls(false)
        .with_timestamp_format(NAIVE_TIMESTAMP_FORMAT.to_string())
        .with_encoder_factory(encoder_factory)
        .build::<_, LineDelimited>(buffer)
}

struct Base64Encoder<B>(B);

impl<'a, B: ArrayAccessor<Item = &'a [u8]>> Encoder for Base64Encoder<B> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write!(
            out,
            "\"{}\"",
            Base64Display::new(self.0.value(idx), &BASE64_STANDARD)
        )
        .expect("writing to a Vec cannot fail");
    }
}

struct ParquetDictionaryEncoder<'a, K: ArrowDictionaryKeyType> {
    array: &'a DictionaryArray<K>,
    values: NullableEncoder<'a>,
}

impl<K: ArrowDictionaryKeyType> Encoder for ParquetDictionaryEncoder<'_, K> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let key = self.array.key(idx).expect("null keys are not encoded");
        // A valid key can reference a null value, including a struct/list with masked children.
        if self.values.is_null(key) {
            out.extend_from_slice(b"null");
        } else {
            self.values.encode(key, out);
        }
    }
}

struct CheckedFloatEncoder<'a, F> {
    inner: NullableEncoder<'a>,
    is_finite: F,
    field_name: &'a str,
    invalid_field: Arc<OnceLock<String>>,
}

impl<F: Fn(usize) -> bool> Encoder for CheckedFloatEncoder<'_, F> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        // Encoder cannot return errors; reject the batch without checking masked/unused values.
        if !(self.is_finite)(idx) {
            let _ = self.invalid_field.set(self.field_name.to_string());
        }
        self.inner.encode(idx, out);
    }
}

#[derive(Debug, Default)]
struct ParquetEncoderFactory {
    invalid_field: Arc<OnceLock<String>>,
}

// Primitive float formatting does not use the writer's timestamp/null options or custom factory.
static FLOAT_ENCODER_OPTIONS: LazyLock<EncoderOptions> = LazyLock::new(EncoderOptions::default);

impl EncoderFactory for ParquetEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        // Arrow invokes this hook recursively, including for dictionary values and list children.
        macro_rules! float_encoder {
            ($float_type:ty) => {{
                let array = array.as_primitive::<$float_type>();
                Box::new(CheckedFloatEncoder {
                    inner: make_encoder(field, array, &FLOAT_ENCODER_OPTIONS)?,
                    is_finite: move |idx| array.value(idx).is_finite(),
                    field_name: field.name(),
                    invalid_field: self.invalid_field.clone(),
                })
            }};
        }
        let encoder: Box<dyn Encoder> = match array.data_type() {
            DataType::Binary => Box::new(Base64Encoder(array.as_binary::<i32>())),
            DataType::LargeBinary => Box::new(Base64Encoder(array.as_binary::<i64>())),
            DataType::BinaryView => Box::new(Base64Encoder(array.as_binary_view())),
            DataType::FixedSizeBinary(_) => Box::new(Base64Encoder(array.as_fixed_size_binary())),
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                array => Box::new(ParquetDictionaryEncoder {
                    array,
                    values: make_encoder(field, array.values().as_ref(), options)?,
                }),
                _ => unreachable!("Arrow dictionary keys must be integers"),
            },
            DataType::Float16 => float_encoder!(Float16Type),
            DataType::Float32 => float_encoder!(Float32Type),
            DataType::Float64 => float_encoder!(Float64Type),
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.logical_nulls())))
    }
}

/// Encodes one JSON document per row; returned slices share a single buffer.
pub(super) fn record_batch_to_ndjson_docs(
    record_batch: &RecordBatch,
) -> anyhow::Result<Vec<Bytes>> {
    let num_rows = record_batch.num_rows();
    let encoder_factory = Arc::new(ParquetEncoderFactory::default());
    let mut writer = new_ndjson_writer(Vec::new(), encoder_factory.clone());
    writer
        .write(record_batch)
        .context("failed to encode Parquet rows as JSON")?;
    if let Some(field_name) = encoder_factory.invalid_field.get() {
        bail!("non-finite float in Parquet field `{field_name}` cannot be represented as JSON");
    }
    writer
        .finish()
        .context("failed to encode Parquet rows as JSON")?;
    let buffer = Bytes::from(writer.into_inner());

    // arrow-json escapes string newlines, so every raw newline separates documents.
    let mut docs = Vec::with_capacity(num_rows);
    let mut line_start = 0;
    for (newline_pos, _) in buffer
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
    {
        docs.push(buffer.slice(line_start..newline_pos));
        line_start = newline_pos + 1;
    }
    if line_start != buffer.len() {
        anyhow::bail!("JSON encoding of Parquet rows is missing a trailing newline");
    }
    if docs.len() != num_rows {
        anyhow::bail!(
            "JSON encoding of {num_rows} Parquet rows produced {} lines",
            docs.len()
        );
    }
    Ok(docs)
}
