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

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema_from_iterator;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value as JsonValue;

/// Writes JSON as Parquet with an inferred schema and at most `row_group_num_rows` per group.
pub fn write_json_docs_as_parquet_file(
    path: &Path,
    json_docs: &[JsonValue],
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

/// Writes native float values, including values that JSON cannot represent, in a `value` column.
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
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(properties))?;
    writer.write(record_batch)?;
    writer.close()?;
    Ok(())
}
