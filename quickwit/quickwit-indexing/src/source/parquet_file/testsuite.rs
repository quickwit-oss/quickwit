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

use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema_from_iterator;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value as JsonValue;

/// Writes `json_docs` to a Parquet file in row groups of at most `row_group_num_rows` rows. The
/// Arrow schema is inferred from the documents.
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
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_num_rows))
        .build();
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    if let Some(record_batch) = decoder.flush()? {
        writer.write(&record_batch)?;
    }
    writer.close()?;
    Ok(())
}
