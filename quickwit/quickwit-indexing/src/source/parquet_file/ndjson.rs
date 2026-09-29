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

//! Conversion of Arrow record batches into NDJSON documents.

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_json::writer::{LineDelimited, Writer, WriterBuilder};
use bytes::Bytes;

/// Timestamps without a time zone are formatted as UTC so that they can be parsed by the default
/// `rfc3339` datetime input format. Timestamps with a time zone keep arrow-json's default
/// formatting, which already includes the offset.
const NAIVE_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

fn new_ndjson_writer(buffer: Vec<u8>) -> Writer<Vec<u8>, LineDelimited> {
    WriterBuilder::new()
        .with_explicit_nulls(false)
        .with_timestamp_format(NAIVE_TIMESTAMP_FORMAT.to_string())
        .build::<_, LineDelimited>(buffer)
}

/// Encodes each row of `record_batch` as a JSON object and returns one `Bytes` per row.
///
/// All the returned documents share the same underlying buffer.
pub(super) fn record_batch_to_ndjson_docs(
    record_batch: &RecordBatch,
) -> anyhow::Result<Vec<Bytes>> {
    let num_rows = record_batch.num_rows();
    let mut writer = new_ndjson_writer(Vec::new());
    writer
        .write(record_batch)
        .context("failed to encode Parquet rows as JSON")?;
    writer
        .finish()
        .context("failed to encode Parquet rows as JSON")?;
    let buffer = Bytes::from(writer.into_inner());

    // arrow-json escapes control characters in strings, so `\n` only occurs as a line separator.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Int64Array, StringArray, StructArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

    use super::*;

    #[test]
    fn test_record_batch_to_ndjson_docs() {
        let inner_fields = Fields::from(vec![Field::new("flag", DataType::Boolean, true)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new(
                "ts_utc",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("nested", DataType::Struct(inner_fields.clone()), true),
        ]));
        let nested = StructArray::new(
            inner_fields,
            vec![Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef],
            None,
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("hello\nworld"), None])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None]).with_timezone("UTC")),
            Arc::new(nested),
        ];
        let record_batch = RecordBatch::try_new(schema, columns).unwrap();
        let docs = record_batch_to_ndjson_docs(&record_batch).unwrap();
        assert_eq!(docs.len(), 2);

        let doc_0: serde_json::Value = serde_json::from_slice(&docs[0]).unwrap();
        assert_eq!(
            doc_0,
            serde_json::json!({
                "id": 1,
                "message": "hello\nworld",
                "ts": "1970-01-01T00:00:01Z",
                "ts_utc": "1970-01-01T00:00:01Z",
                "nested": {"flag": true},
            })
        );
        let doc_1: serde_json::Value = serde_json::from_slice(&docs[1]).unwrap();
        assert_eq!(doc_1, serde_json::json!({"id": 2, "nested": {}}));
    }
}
