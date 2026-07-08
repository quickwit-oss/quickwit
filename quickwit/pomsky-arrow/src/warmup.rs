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

//! Async pre-fetch of fast-field column bytes.
//!
//! For a storage-backed split (S3/GCS), tantivy's reads are synchronous and
//! cannot fetch from object storage mid-read — every byte a read touches must
//! already be resident in the directory cache. This module prefetches exactly
//! the fast-field columns a projection will read, so the subsequent
//! synchronous [`read_segment_columns`](crate::read_segment_columns) calls hit
//! memory.
//!
//! It warms only the *columnar* data implied by the projection. Warming the
//! inverted-index posting lists for the caller's filter query is the caller's
//! responsibility — this crate never sees the query.

use std::collections::BTreeSet;

use arrow::datatypes::SchemaRef;
use tantivy::Searcher;

use crate::error::{PomskyArrowError, Result};

const DOC_ID_FIELD_NAME: &str = "_doc_id";
const SEGMENT_ORD_FIELD_NAME: &str = "_segment_ord";

/// Pre-fetches the byte ranges of every fast-field column referenced by
/// `projected_schema`, across all segments of `searcher`.
///
/// Warming by fast-field name covers every coercion candidate at once:
/// several projected fields with the same name (e.g. one path read as `u64`,
/// `i64`, and `str`) collapse to a single fetch, and that fetch warms all
/// physical columns registered under the path regardless of type.
///
/// A projected column absent from a segment resolves to an empty handle list
/// and is skipped — consistent with `read_segment_columns` returning an
/// all-null array for a missing column. A genuine listing or read error
/// surfaces as an error rather than being swallowed.
pub async fn warm_up_fast_fields(searcher: &Searcher, projected_schema: &SchemaRef) -> Result<()> {
    let field_names = fast_field_names(projected_schema);
    if field_names.is_empty() {
        return Ok(());
    }

    for segment_reader in searcher.segment_readers() {
        let fast_fields = segment_reader.fast_fields();
        for &name in &field_names {
            let column_handles = fast_fields.list_dynamic_column_handles(name).await?;
            for handle in column_handles {
                handle
                    .file_slice()
                    .read_bytes_async()
                    .await
                    .map_err(|error| {
                        PomskyArrowError::Internal(format!(
                            "warmup: failed to pre-load fast field '{name}': {error}"
                        ))
                    })?;
            }
        }
    }
    Ok(())
}

/// The distinct fast-field names a projection reads, excluding the synthetic
/// `_doc_id` / `_segment_ord` columns. Duplicate field names collapse to one
/// entry so each column is warmed once.
fn fast_field_names(projected_schema: &SchemaRef) -> Vec<&str> {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for field in projected_schema.fields() {
        let name = field.name().as_str();
        if name == DOC_ID_FIELD_NAME || name == SEGMENT_ORD_FIELD_NAME {
            continue;
        }
        names.insert(name);
    }
    names.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use tantivy::schema::{FAST, SchemaBuilder};
    use tantivy::{Index, IndexWriter, TantivyDocument};

    use super::*;

    fn ram_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let id_field = builder.add_u64_field("id", FAST);
        let schema = builder.build();

        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_field, 42);
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        index
    }

    #[tokio::test]
    async fn warms_projected_columns_and_ignores_missing() {
        let index = ram_index();
        let searcher = index.reader().unwrap().searcher();

        let projected = Arc::new(Schema::new(vec![
            Field::new(DOC_ID_FIELD_NAME, DataType::UInt32, false),
            Field::new("id", DataType::UInt64, true),
            Field::new("missing", DataType::Float64, true),
        ]));

        // A present column warms; a missing one is skipped, not an error.
        warm_up_fast_fields(&searcher, &projected).await.unwrap();
    }

    #[tokio::test]
    async fn empty_projection_is_a_noop() {
        let index = ram_index();
        let searcher = index.reader().unwrap().searcher();

        let projected = Arc::new(Schema::new(vec![Field::new(
            DOC_ID_FIELD_NAME,
            DataType::UInt32,
            false,
        )]));
        warm_up_fast_fields(&searcher, &projected).await.unwrap();
    }

    #[test]
    fn field_names_skip_internal_and_dedup_by_field_name() {
        let projected = Arc::new(Schema::new(vec![
            Field::new(DOC_ID_FIELD_NAME, DataType::UInt32, false),
            Field::new(SEGMENT_ORD_FIELD_NAME, DataType::UInt32, false),
            Field::new("status", DataType::UInt64, true),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("service", DataType::Utf8, true),
        ]));

        let names = fast_field_names(&projected);
        assert_eq!(names, vec!["service", "status"]);
    }
}
