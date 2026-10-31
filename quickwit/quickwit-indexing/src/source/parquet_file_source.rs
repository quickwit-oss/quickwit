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

//! Parallel local Parquet source for `quickwit tool local-ingest`.
//! See `docs/internals/parquet-bulk-load.md`.
//!
//! The sources of all pipelines share one [`ParquetSharedReader`]: they claim row groups one at a
//! time, decode them on the blocking runtime and emit JSON documents. Like the file source, the
//! checkpoint records the number of rows read in each row group, and sources resume from it.
//! Claimed row groups are not retried, so a pipeline failure must abort the load.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, Float16Type, Float32Type, Float64Type,
};
use arrow_array::{Array, ArrayAccessor, DictionaryArray, RecordBatch, downcast_dictionary_array};
use arrow_json::writer::{
    Encoder, EncoderFactory, EncoderOptions, LineDelimited, NullableEncoder, WriterBuilder,
    make_encoder,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use async_trait::async_trait;
use base64::display::Base64Display;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::{FileSourceParams, SourceInputFormat, SourceParams};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use serde_json::json;

use crate::actors::DocProcessor;
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, Source, SourceContext, SourceFactory, SourceRuntime,
};

/// Number of rows decoded per Arrow record batch.
const BATCH_NUM_ROWS: usize = 8192;

/// Treat naive timestamps as UTC for RFC 3339 parsing; zoned timestamps keep their offset.
const NAIVE_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// Checkpoint partition of a row group, positioned at its number of rows read.
fn row_group_partition_id(file_uri: &Uri, row_group_idx: usize) -> PartitionId {
    PartitionId::from(format!("{file_uri}#row-group-{row_group_idx}"))
}

/// Shared by the sources of all pipelines of a local Parquet load: holds the file metadata and
/// hands out each row group once, resuming after the rows recorded in the checkpoint.
pub struct ParquetSharedReader {
    file_uri: Uri,
    filepath: PathBuf,
    arrow_metadata: ArrowReaderMetadata,
    /// Top-level columns to read: the excluded columns are never decoded.
    projection: ProjectionMask,
    batch_num_rows: usize,
    next_row_group_idx: AtomicUsize,
}

impl ParquetSharedReader {
    /// Reads the footer and checks the JSON conversion of one row, without the top-level
    /// `excluded_columns`. Performs blocking I/O.
    pub fn try_new(file_uri: Uri, excluded_columns: &[String]) -> anyhow::Result<Self> {
        Self::try_new_with_batch_num_rows(file_uri, excluded_columns, BATCH_NUM_ROWS)
    }

    fn try_new_with_batch_num_rows(
        file_uri: Uri,
        excluded_columns: &[String],
        batch_num_rows: usize,
    ) -> anyhow::Result<Self> {
        if file_uri.protocol() != Protocol::File {
            bail!("Parquet input only supports local files, got `{file_uri}`");
        }
        let filepath = file_uri
            .filepath()
            .context("failed to extract file path from URI")?
            .to_path_buf();
        let file = File::open(&filepath)
            .with_context(|| format!("failed to open file `{}`", filepath.display()))?;
        let arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())
            .with_context(|| format!("failed to read Parquet footer of `{file_uri}`"))?;
        let projection = column_projection(&arrow_metadata, excluded_columns)?;
        let shared_reader = Self {
            file_uri,
            filepath,
            arrow_metadata,
            projection,
            batch_num_rows,
            next_row_group_idx: AtomicUsize::new(0),
        };
        // Reject an unsupported schema before spawning pipelines.
        let first_row_group_opt = (0..shared_reader.num_row_groups())
            .find(|&row_group_idx| shared_reader.row_group_num_rows(row_group_idx) > 0);
        if let Some(row_group_idx) = first_row_group_opt {
            let mut reader = shared_reader
                .row_group_reader_builder(row_group_idx)?
                .with_limit(1)
                .build()?;
            if let Some(record_batch) = reader.next() {
                record_batch_to_ndjson_docs(&record_batch?).with_context(|| {
                    format!("unsupported Parquet schema in `{}`", shared_reader.file_uri)
                })?;
            }
        }
        Ok(shared_reader)
    }

    fn row_group_reader_builder(
        &self,
        row_group_idx: usize,
    ) -> anyhow::Result<ParquetRecordBatchReaderBuilder<File>> {
        let file = File::open(&self.filepath)
            .with_context(|| format!("failed to open file `{}`", self.filepath.display()))?;
        Ok(
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.arrow_metadata.clone())
                .with_row_groups(vec![row_group_idx])
                .with_projection(self.projection.clone())
                .with_batch_size(self.batch_num_rows),
        )
    }

    /// Claims the next row group with rows missing from the checkpoint, and returns a reader
    /// starting after the rows already read. Returns `None` when all groups have been claimed.
    fn next_row_group_reader(
        &self,
        checkpoint: &SourceCheckpoint,
    ) -> anyhow::Result<Option<RowGroupReader>> {
        loop {
            let row_group_idx = self.next_row_group_idx.fetch_add(1, Ordering::Relaxed);
            if row_group_idx >= self.num_row_groups() {
                return Ok(None);
            }
            let next_row = self.num_checkpointed_rows(checkpoint, row_group_idx);
            if next_row == self.row_group_num_rows(row_group_idx) {
                continue;
            }
            let reader = self
                .row_group_reader_builder(row_group_idx)?
                .with_offset(next_row as usize)
                .build()
                .with_context(|| {
                    format!(
                        "failed to read row group {row_group_idx} of `{}`",
                        self.file_uri
                    )
                })?;
            return Ok(Some(RowGroupReader {
                row_group_idx,
                next_row,
                reader,
            }));
        }
    }

    /// Number of rows of the file recorded in the checkpoint.
    pub fn num_checkpointed_rows_total(&self, checkpoint: &SourceCheckpoint) -> u64 {
        (0..self.num_row_groups())
            .map(|row_group_idx| self.num_checkpointed_rows(checkpoint, row_group_idx))
            .sum()
    }

    fn num_checkpointed_rows(&self, checkpoint: &SourceCheckpoint, row_group_idx: usize) -> u64 {
        let partition_id = row_group_partition_id(&self.file_uri, row_group_idx);
        let num_rows = checkpoint
            .position_for_partition(&partition_id)
            .and_then(|position| position.as_u64())
            .unwrap_or(0);
        num_rows.min(self.row_group_num_rows(row_group_idx))
    }

    fn row_group_num_rows(&self, row_group_idx: usize) -> u64 {
        let row_group = self.arrow_metadata.metadata().row_group(row_group_idx);
        row_group.num_rows().max(0) as u64
    }

    pub fn num_rows(&self) -> u64 {
        let file_metadata = self.arrow_metadata.metadata().file_metadata();
        file_metadata.num_rows().max(0) as u64
    }

    pub fn num_row_groups(&self) -> usize {
        self.arrow_metadata.metadata().num_row_groups()
    }

    /// Uncompressed size of the file, as reported by the footer.
    pub fn num_uncompressed_bytes(&self) -> u64 {
        let row_groups = self.arrow_metadata.metadata().row_groups();
        let row_group_num_bytes = row_groups
            .iter()
            .map(|row_group| row_group.total_byte_size());
        row_group_num_bytes
            .map(|num_bytes| num_bytes.max(0) as u64)
            .sum()
    }
}

/// Selects the top-level columns that are not excluded. Unknown names are rejected rather than
/// ignored, so that a typo does not silently keep a column.
fn column_projection(
    arrow_metadata: &ArrowReaderMetadata,
    excluded_columns: &[String],
) -> anyhow::Result<ProjectionMask> {
    let fields = arrow_metadata.schema().fields();
    for excluded_column in excluded_columns {
        ensure!(
            fields.iter().any(|field| field.name() == excluded_column),
            "Parquet column `{excluded_column}` does not exist, available columns: {}",
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let selected_root_indices: Vec<usize> = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !excluded_columns.contains(field.name()))
        .map(|(root_idx, _)| root_idx)
        .collect();
    ensure!(
        !selected_root_indices.is_empty(),
        "all the Parquet columns are excluded"
    );
    Ok(ProjectionMask::roots(
        arrow_metadata.parquet_schema(),
        selected_root_indices,
    ))
}

/// Reader of a claimed row group.
struct RowGroupReader {
    row_group_idx: usize,
    /// Row of the row group that the reader emits next.
    next_row: u64,
    reader: ParquetRecordBatchReader,
}

/// Creates sources sharing one reader; registered as `file` only in the CLI's loader.
pub struct ParquetSourceFactory {
    shared_reader: Arc<ParquetSharedReader>,
}

impl ParquetSourceFactory {
    pub fn new(shared_reader: ParquetSharedReader) -> Self {
        Self {
            shared_reader: Arc::new(shared_reader),
        }
    }
}

#[async_trait]
impl SourceFactory for ParquetSourceFactory {
    async fn create_source(
        &self,
        source_runtime: SourceRuntime,
    ) -> anyhow::Result<Box<dyn Source>> {
        let source_config = &source_runtime.source_config;
        let reads_plan_file = matches!(
            &source_config.source_params,
            SourceParams::File(FileSourceParams::Filepath(file_uri))
                if file_uri == &self.shared_reader.file_uri
        );
        ensure!(
            reads_plan_file && source_config.input_format == SourceInputFormat::Json,
            "the Parquet source must read `{}` as `json` documents",
            self.shared_reader.file_uri
        );
        // Like the file source, resume after the rows recorded in the checkpoint.
        let checkpoint = source_runtime.fetch_checkpoint().await?;
        Ok(Box::new(ParquetSource {
            shared_reader: self.shared_reader.clone(),
            checkpoint: Arc::new(checkpoint),
            current_opt: None,
            num_rows_emitted: 0,
        }))
    }
}

/// Emits the row groups it claims from the shared reader as JSON documents.
pub struct ParquetSource {
    shared_reader: Arc<ParquetSharedReader>,
    /// Checkpoint when the source was created.
    checkpoint: Arc<SourceCheckpoint>,
    /// Current row group, taken while the blocking task owns the reader.
    current_opt: Option<RowGroupReader>,
    num_rows_emitted: u64,
}

#[async_trait]
impl Source for ParquetSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let next_batch = ctx
            .protect_future(decode_next_batch(
                self.shared_reader.clone(),
                self.checkpoint.clone(),
                self.current_opt.take(),
            ))
            .await?;
        let Some((mut current, docs)) = next_batch else {
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        };
        let partition_id =
            row_group_partition_id(&self.shared_reader.file_uri, current.row_group_idx);

        // Split between documents; a single document may exceed the byte limit.
        let mut batch_builder = BatchBuilder::new(SourceType::File);
        let num_docs = docs.len();
        for (doc_idx, doc) in docs.into_iter().enumerate() {
            batch_builder.add_doc(doc);
            let is_last_doc = doc_idx + 1 == num_docs;
            if !is_last_doc && batch_builder.num_bytes < BATCH_NUM_BYTES_LIMIT {
                continue;
            }
            let num_batch_rows = batch_builder.docs.len() as u64;
            let from_position = Position::offset(current.next_row);
            current.next_row += num_batch_rows;
            batch_builder
                .checkpoint_delta
                .record_partition_delta(
                    partition_id.clone(),
                    from_position,
                    Position::offset(current.next_row),
                )
                .context("failed to record the Parquet checkpoint")?;
            self.num_rows_emitted += num_batch_rows;
            let raw_doc_batch =
                std::mem::replace(&mut batch_builder, BatchBuilder::new(SourceType::File)).build();
            ctx.send_message(doc_processor_mailbox, raw_doc_batch)
                .await?;
        }
        self.current_opt = Some(current);
        Ok(Duration::ZERO)
    }

    fn name(&self) -> String {
        format!(
            "ParquetSource {{ file_uri: {} }}",
            self.shared_reader.file_uri
        )
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({ "num_rows_emitted": self.num_rows_emitted })
    }
}

/// Acquires readers, decodes and JSON-encodes on the blocking runtime.
/// Advances past exhausted row groups; `None` marks EOF.
async fn decode_next_batch(
    shared_reader: Arc<ParquetSharedReader>,
    checkpoint: Arc<SourceCheckpoint>,
    mut current_opt: Option<RowGroupReader>,
) -> anyhow::Result<Option<(RowGroupReader, Vec<Bytes>)>> {
    let join_handle = RuntimeType::Blocking
        .get_runtime_handle()
        .spawn(async move {
            loop {
                let current = match current_opt.take() {
                    Some(current) => Some(current),
                    None => shared_reader.next_row_group_reader(&checkpoint)?,
                };
                let Some(mut current) = current else {
                    return Ok(None);
                };
                let docs_res = match current.reader.next() {
                    Some(Ok(record_batch)) => record_batch_to_ndjson_docs(&record_batch),
                    Some(Err(error)) => Err(anyhow::Error::from(error)),
                    None => continue,
                };
                let docs = docs_res.with_context(|| {
                    format!(
                        "failed to decode row group {} of `{}`",
                        current.row_group_idx, shared_reader.file_uri
                    )
                })?;
                return Ok(Some((current, docs)));
            }
        });
    join_handle
        .await
        .context("Parquet decoding task panicked or was cancelled")?
}

/// Quickwit `bytes` fields expect base64, while arrow-json writes binary values in hex.
struct Base64Encoder<B>(B);

impl<'a, B: ArrayAccessor<Item = &'a [u8]>> Encoder for Base64Encoder<B> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = Base64Display::new(self.0.value(idx), &BASE64_STANDARD);
        write!(out, "\"{value}\"").expect("writing to a Vec cannot fail");
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
        // Parents usually skip them, since the dictionary is wrapped with its logical nulls.
        if self.values.is_null(key) {
            out.extend_from_slice(b"null");
        } else {
            self.values.encode(key, out);
        }
    }
}

/// arrow-json writes non-finite floats as `null`: reject them instead of losing values.
struct CheckedFloatEncoder<'a, F> {
    inner: NullableEncoder<'a>,
    is_finite: F,
    field_name: &'a str,
    invalid_field: Arc<OnceLock<String>>,
}

impl<F: Fn(usize) -> bool> Encoder for CheckedFloatEncoder<'_, F> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        // Encoder cannot return errors; only encoded (not masked or unused) values are checked.
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

// Without our factory, so that primitive float encoders do not recurse into it.
static FLOAT_ENCODER_OPTIONS: LazyLock<EncoderOptions> = LazyLock::new(EncoderOptions::default);

impl EncoderFactory for ParquetEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        // Arrow invokes this hook recursively, including for dictionary values and list children.
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
            DataType::Float16 => checked_float_encoder::<Float16Type>(
                field,
                array,
                |value| value.is_finite(),
                &self.invalid_field,
            )?,
            DataType::Float32 => checked_float_encoder::<Float32Type>(
                field,
                array,
                |value| value.is_finite(),
                &self.invalid_field,
            )?,
            DataType::Float64 => checked_float_encoder::<Float64Type>(
                field,
                array,
                |value| value.is_finite(),
                &self.invalid_field,
            )?,
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.logical_nulls())))
    }
}

/// `is_finite` is passed in because floats share no trait with an `is_finite` method.
fn checked_float_encoder<'a, T: ArrowPrimitiveType>(
    field: &'a FieldRef,
    array: &'a dyn Array,
    is_finite: fn(T::Native) -> bool,
    invalid_field: &Arc<OnceLock<String>>,
) -> Result<Box<dyn Encoder + 'a>, ArrowError> {
    let array = array.as_primitive::<T>();
    Ok(Box::new(CheckedFloatEncoder {
        inner: make_encoder(field, array, &FLOAT_ENCODER_OPTIONS)?,
        is_finite: move |idx| is_finite(array.value(idx)),
        field_name: field.name(),
        invalid_field: invalid_field.clone(),
    }))
}

/// Encodes one JSON document per row; returned slices share a single buffer.
fn record_batch_to_ndjson_docs(record_batch: &RecordBatch) -> anyhow::Result<Vec<Bytes>> {
    let encoder_factory = Arc::new(ParquetEncoderFactory::default());
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(false)
        .with_timestamp_format(NAIVE_TIMESTAMP_FORMAT.to_string())
        .with_encoder_factory(encoder_factory.clone())
        .build::<_, LineDelimited>(Vec::new());
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
    let mut docs = Vec::with_capacity(record_batch.num_rows());
    let mut line_start = 0;
    for (newline_pos, _) in buffer
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
    {
        docs.push(buffer.slice(line_start..newline_pos));
        line_start = newline_pos + 1;
    }
    ensure!(
        line_start == buffer.len() && docs.len() == record_batch.num_rows(),
        "JSON encoding of {} Parquet rows produced {} lines",
        record_batch.num_rows(),
        docs.len()
    );
    Ok(docs)
}

#[cfg(any(test, feature = "testsuite"))]
mod testsuite {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, RecordBatch};
    use arrow_json::ReaderBuilder;
    use arrow_json::reader::infer_json_schema_from_iterator;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Writes JSON as Parquet with an inferred schema and at most `row_group_num_rows` per group.
    pub fn write_json_docs_as_parquet_file(
        path: &Path,
        json_docs: &[serde_json::Value],
        row_group_num_rows: usize,
    ) -> anyhow::Result<()> {
        let schema = Arc::new(infer_json_schema_from_iterator(
            json_docs.iter().map(Ok::<_, arrow_schema::ArrowError>),
        )?);
        let mut decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(json_docs.len().max(1))
            .build_decoder()?;
        decoder.serialize(json_docs)?;
        let record_batch = decoder
            .flush()?
            .unwrap_or_else(|| RecordBatch::new_empty(schema));
        write_record_batch_as_parquet_file(path, &record_batch, row_group_num_rows)
    }

    /// Writes native float values, including values that JSON cannot represent, in a `value`
    /// column.
    pub fn write_f64_values_as_parquet_file(
        path: &Path,
        values: &[f64],
        row_group_num_rows: usize,
    ) -> anyhow::Result<()> {
        let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
        let record_batch = RecordBatch::try_from_iter([("value", array)])?;
        write_record_batch_as_parquet_file(path, &record_batch, row_group_num_rows)
    }

    pub(super) fn write_record_batch_as_parquet_file(
        path: &Path,
        record_batch: &RecordBatch,
        row_group_num_rows: usize,
    ) -> anyhow::Result<()> {
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_num_rows))
            .build();
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(properties))?;
        writer.write(record_batch)?;
        writer.close()?;
        Ok(())
    }
}

#[cfg(any(test, feature = "testsuite"))]
pub use testsuite::{write_f64_values_as_parquet_file, write_json_docs_as_parquet_file};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use arrow_array::types::{Float64Type, Int32Type};
    use arrow_array::{
        ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryViewArray, BooleanArray,
        FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array,
        Int32Array, Int64Array, LargeBinaryArray, ListArray, StringArray, StructArray,
        TimestampMillisecondArray,
    };
    use arrow_schema::Field;
    use quickwit_actors::{Command, Universe};
    use quickwit_config::SourceConfig;
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_proto::types::IndexUid;
    use serde_json::Value as JsonValue;

    use super::testsuite::write_record_batch_as_parquet_file;
    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::SourceActor;
    use crate::source::tests::SourceRuntimeBuilder;

    fn source_config(file_uri: &Uri, input_format: SourceInputFormat) -> SourceConfig {
        SourceConfig {
            source_id: "test-parquet-source".to_string(),
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::File(FileSourceParams::Filepath(file_uri.clone())),
            transform_config: None,
            input_format,
        }
    }

    /// Writes rows `{"id": i}` in row groups of `row_group_num_rows` rows.
    fn write_ids(dir: &std::path::Path, num_rows: usize, row_group_num_rows: usize) -> Uri {
        let path = dir.join("ids.parquet");
        let json_docs: Vec<JsonValue> = (0..num_rows).map(|id| json!({"id": id})).collect();
        write_json_docs_as_parquet_file(&path, &json_docs, row_group_num_rows).unwrap();
        Uri::from_str(path.to_str().unwrap()).unwrap()
    }

    /// Runs a source in batches of 3 rows from the checkpoint `checkpoint_delta`, and returns the
    /// emitted batches and the resulting checkpoint.
    async fn run_source(
        file_uri: &Uri,
        checkpoint_delta: SourceCheckpointDelta,
    ) -> (Vec<RawDocBatch>, SourceCheckpoint) {
        let shared_reader =
            ParquetSharedReader::try_new_with_batch_num_rows(file_uri.clone(), &[], 3).unwrap();
        let source_runtime = SourceRuntimeBuilder::new(
            IndexUid::new_with_random_ulid("test-index"),
            source_config(file_uri, SourceInputFormat::Json),
        )
        .with_mock_metastore(Some(checkpoint_delta.clone()))
        .build();
        let source = ParquetSourceFactory::new(shared_reader)
            .create_source(source_runtime)
            .await
            .unwrap();
        let universe = Universe::with_accelerated_time();
        let (mailbox, inbox) = universe.create_test_mailbox::<DocProcessor>();
        let (_, handle) = universe
            .spawn_builder()
            .spawn(SourceActor::new(source, mailbox));
        assert!(handle.join().await.0.is_success());
        universe.assert_quit().await;

        let mut messages = inbox.drain_for_test();
        let last_message = messages.pop().unwrap();
        assert!(matches!(
            last_message.downcast_ref::<Command>(),
            Some(Command::ExitWithSuccess)
        ));
        let batches: Vec<RawDocBatch> = messages
            .into_iter()
            .map(|message| *message.downcast::<RawDocBatch>().unwrap())
            .collect();
        let mut checkpoint = SourceCheckpoint::default();
        checkpoint.try_apply_delta(checkpoint_delta).unwrap();
        for batch in &batches {
            checkpoint
                .try_apply_delta(batch.checkpoint_delta.clone())
                .unwrap();
        }
        (batches, checkpoint)
    }

    fn ids(batches: &[RawDocBatch]) -> Vec<u64> {
        let docs = batches.iter().flat_map(|batch| batch.docs.iter());
        docs.map(|doc| {
            serde_json::from_slice::<JsonValue>(doc).unwrap()["id"]
                .as_u64()
                .unwrap()
        })
        .collect()
    }

    fn row_group_position(checkpoint: &SourceCheckpoint, file_uri: &Uri, idx: usize) -> u64 {
        let partition_id = row_group_partition_id(file_uri, idx);
        let position = checkpoint.position_for_partition(&partition_id).unwrap();
        position.as_u64().unwrap()
    }

    #[tokio::test]
    async fn test_parquet_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Row groups of 4, 4 and 2 rows, read in batches of 3 rows.
        let file_uri = write_ids(temp_dir.path(), 10, 4);
        let (batches, checkpoint) = run_source(&file_uri, SourceCheckpointDelta::default()).await;
        let batch_sizes: Vec<usize> = batches.iter().map(|batch| batch.docs.len()).collect();
        assert_eq!(batch_sizes, [3, 1, 3, 1, 2]);
        assert_eq!(ids(&batches), (0..10).collect::<Vec<u64>>());
        for (row_group_idx, num_rows) in [(0, 4), (1, 4), (2, 2)] {
            let position = row_group_position(&checkpoint, &file_uri, row_group_idx);
            assert_eq!(position, num_rows);
        }
    }

    #[tokio::test]
    async fn test_parquet_source_resumes_from_checkpoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_uri = write_ids(temp_dir.path(), 10, 4);
        // Row group 0 was fully read, row group 1 up to its third row, row group 2 not at all.
        let mut checkpoint_delta = SourceCheckpointDelta::default();
        for (row_group_idx, num_rows) in [(0, 4u64), (1, 3)] {
            let partition_id = row_group_partition_id(&file_uri, row_group_idx);
            let to_position = Position::offset(num_rows);
            checkpoint_delta
                .record_partition_delta(partition_id, Position::Beginning, to_position)
                .unwrap();
        }
        let (batches, checkpoint) = run_source(&file_uri, checkpoint_delta).await;
        assert_eq!(ids(&batches), [7, 8, 9]);
        assert_eq!(row_group_position(&checkpoint, &file_uri, 1), 4);
        assert_eq!(row_group_position(&checkpoint, &file_uri, 2), 2);

        let shared_reader = ParquetSharedReader::try_new(file_uri, &[]).unwrap();
        assert_eq!(shared_reader.num_checkpointed_rows_total(&checkpoint), 10);
    }

    #[test]
    fn test_parquet_excluded_columns_are_not_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("columns.parquet");
        let json_docs = [json!({"id": 1, "message": "hello", "extra": {"nested": 2}})];
        write_json_docs_as_parquet_file(&path, &json_docs, 10).unwrap();
        let file_uri = Uri::from_str(path.to_str().unwrap()).unwrap();

        let excluded_columns = ["extra".to_string(), "message".to_string()];
        let shared_reader =
            ParquetSharedReader::try_new(file_uri.clone(), &excluded_columns).unwrap();
        let mut current = shared_reader
            .next_row_group_reader(&SourceCheckpoint::default())
            .unwrap()
            .unwrap();
        let record_batch = current.reader.next().unwrap().unwrap();
        assert_eq!(to_json_docs(&record_batch), [json!({"id": 1})]);

        for (excluded_columns, expected_error) in [
            (vec!["unknown".to_string()], "does not exist"),
            (
                vec!["id".to_string(), "message".to_string(), "extra".to_string()],
                "all the Parquet columns are excluded",
            ),
        ] {
            let shared_reader_result =
                ParquetSharedReader::try_new(file_uri.clone(), &excluded_columns);
            let error = shared_reader_result.err().unwrap();
            assert!(error.to_string().contains(expected_error), "{error:#}");
        }
    }

    #[tokio::test]
    async fn test_parquet_source_rejects_other_inputs() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_uri = write_ids(temp_dir.path(), 3, 2);
        let other_file_uri = Uri::from_str("/other.parquet").unwrap();
        for (source_file_uri, input_format) in [
            (&other_file_uri, SourceInputFormat::Json),
            (&file_uri, SourceInputFormat::PlainText),
        ] {
            let shared_reader = ParquetSharedReader::try_new(file_uri.clone(), &[]).unwrap();
            let source_runtime = SourceRuntimeBuilder::new(
                IndexUid::new_with_random_ulid("test-index"),
                source_config(source_file_uri, input_format),
            )
            .with_mock_metastore(None)
            .build();
            let error = ParquetSourceFactory::new(shared_reader)
                .create_source(source_runtime)
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains("must read"), "{error:#}");
        }
        let shared_reader_result =
            ParquetSharedReader::try_new(Uri::for_test("s3://bucket/test.parquet"), &[]);
        let error = shared_reader_result.err().unwrap();
        assert!(error.to_string().contains("only supports local files"));
    }

    fn one_column_batch(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_from_iter([("value", array)]).unwrap()
    }

    fn to_json_docs(record_batch: &RecordBatch) -> Vec<JsonValue> {
        let docs = record_batch_to_ndjson_docs(record_batch).unwrap();
        let docs = docs.iter().map(|doc| serde_json::from_slice(doc).unwrap());
        docs.collect()
    }

    fn nested(array: ArrayRef, nulls: Option<Vec<bool>>) -> ArrayRef {
        let field = Arc::new(Field::new("inner", array.data_type().clone(), true));
        let nulls = nulls.map(Into::into);
        Arc::new(StructArray::new(vec![field].into(), vec![array], nulls))
    }

    fn dictionary(keys: Vec<Option<i32>>, values: ArrayRef) -> ArrayRef {
        let keys = Int32Array::from(keys);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
    }

    #[test]
    fn test_record_batch_to_ndjson_docs() {
        let flags = Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef;
        let record_batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "message",
                Arc::new(StringArray::from(vec![Some("hello\nworld"), None])),
            ),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None])),
            ),
            (
                "ts_utc",
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_000), None]).with_timezone("UTC"),
                ),
            ),
            ("nested", nested(flags, None)),
        ])
        .unwrap();
        assert_eq!(
            to_json_docs(&record_batch),
            [
                json!({
                    "id": 1,
                    "message": "hello\nworld",
                    "ts": "1970-01-01T00:00:01Z",
                    "ts_utc": "1970-01-01T00:00:01Z",
                    "nested": {"inner": true},
                }),
                json!({"id": 2, "nested": {}}),
            ]
        );
    }

    #[test]
    fn test_parquet_binary_values_are_base64() {
        let values = vec![Some(&[1, 2][..]), None];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from(values.clone())),
            Arc::new(LargeBinaryArray::from(values.clone())),
            Arc::new(BinaryViewArray::from(values.clone())),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 2)
                    .unwrap(),
            ),
            dictionary(
                vec![Some(0), None],
                Arc::new(BinaryArray::from(vec![&[1, 2][..]])),
            ),
        ];
        for array in arrays {
            let docs = to_json_docs(&one_column_batch(array));
            assert_eq!(docs, [json!({"value": "AQI="}), json!({})]);
        }
        // Parquet may decode binary columns with another Arrow type.
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("binary.parquet");
        let binary = Arc::new(BinaryArray::from(vec![&[1, 2][..]])) as ArrayRef;
        write_record_batch_as_parquet_file(&path, &one_column_batch(binary.clone()), 3).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let decoded_batch = reader.next().unwrap().unwrap();
        assert_eq!(to_json_docs(&decoded_batch), [json!({"value": "AQI="})]);
        // Nested in a dictionary, including empty values.
        let item_field = Arc::new(Field::new("item", binary.data_type().clone(), false));
        let empty = Arc::new(BinaryArray::from(vec![&[1, 2][..], &[][..]])) as ArrayRef;
        let list = Arc::new(FixedSizeListArray::try_new(item_field, 2, empty, None).unwrap());
        let docs = to_json_docs(&one_column_batch(nested(
            dictionary(vec![Some(0)], list),
            None,
        )));
        assert_eq!(docs, [json!({"value": {"inner": ["AQI=", ""]}})]);
    }

    #[test]
    fn test_parquet_rejects_nonfinite_floats() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let float16 = <Float16Type as ArrowPrimitiveType>::Native::from_f64(value);
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Float16Array::from(vec![Some(float16), None])),
                Arc::new(Float32Array::from(vec![Some(value as f32), None])),
                Arc::new(Float64Array::from(vec![Some(value), None])),
            ];
            for array in arrays {
                let dictionary = dictionary(vec![Some(0), Some(1)], array.clone());
                for candidate in [array, dictionary.clone(), nested(dictionary, None)] {
                    let record_batch = one_column_batch(candidate);
                    let error = record_batch_to_ndjson_docs(&record_batch).unwrap_err();
                    assert!(
                        format!("{error:#}").contains("non-finite float"),
                        "{error:#}"
                    );
                }
            }
            let list = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                vec![Some(1.0), Some(value)],
            )])) as ArrayRef;
            let error = record_batch_to_ndjson_docs(&one_column_batch(nested(list, None)));
            assert!(format!("{:#}", error.unwrap_err()).contains("non-finite float"));
        }
    }

    #[test]
    fn test_parquet_masked_nonfinite_floats_are_supported() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let floats = Arc::new(Float64Array::from(vec![value, 1.5])) as ArrayRef;
            // Masked by a null parent.
            let parent = nested(floats.clone(), Some(vec![false, true]));
            let expected = [json!({}), json!({"value": {"inner": 1.5}})];
            assert_eq!(to_json_docs(&one_column_batch(parent.clone())), expected);
            // Null value.
            let nullable = Arc::new(Float64Array::new(
                vec![value, 1.5].into(),
                Some(vec![false, true].into()),
            )) as ArrayRef;
            let expected = [json!({}), json!({"value": 1.5})];
            assert_eq!(to_json_docs(&one_column_batch(nullable)), expected);
            // Unused dictionary value, and null key.
            let unused = dictionary(vec![Some(1), None], floats);
            let expected = [json!({"value": 1.5}), json!({})];
            assert_eq!(to_json_docs(&one_column_batch(unused)), expected);
            // Valid key referencing a null value.
            let null_value = dictionary(vec![Some(0), Some(1)], parent);
            let expected = [json!({}), json!({"value": {"inner": 1.5}})];
            assert_eq!(to_json_docs(&one_column_batch(null_value)), expected);
        }
    }
}
