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

//! Golden tests: build a small in-memory tantivy split, read a projection for
//! a doc selection, and assert the Arrow values / nulls / types. Pure
//! `tantivy` + `arrow`, no infra.

use std::sync::Arc;

use arrow::array::{
    AsArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, UInt64Type};
use pomsky_arrow::{DocSelection, read_segment_columns};
use tantivy::schema::{FAST, SchemaBuilder, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};

/// Builds a 3-document in-memory index with one column of each common type and
/// returns the index so the caller can open a searcher.
fn build_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let id_field = builder.add_u64_field("id", FAST);
    let score_field = builder.add_i64_field("score", FAST);
    let price_field = builder.add_f64_field("price", FAST);
    let active_field = builder.add_bool_field("active", FAST);
    let name_field = builder.add_text_field("name", FAST | TEXT);
    let schema = builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    let rows: [(u64, i64, f64, bool, &str); 3] = [
        (10, -1, 1.5, true, "alpha"),
        (20, -2, 2.5, false, "beta"),
        (30, -3, 3.5, true, "alpha"),
    ];
    for (id, score, price, active, name) in rows {
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_field, id);
        doc.add_i64(score_field, score);
        doc.add_f64(price_field, price);
        doc.add_bool(active_field, active);
        doc.add_text(name_field, name);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();
    index
}

#[test]
fn reads_projection_for_explicit_doc_ids() {
    let index = build_index();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_reader = &searcher.segment_readers()[0];

    // Project every type, plus the synthetic _doc_id / _segment_ord columns.
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("_doc_id", DataType::UInt32, false),
        Field::new("_segment_ord", DataType::UInt32, false),
        Field::new("id", DataType::UInt64, true),
        Field::new("score", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    // Read docs 0 and 2 (skip 1), in that order.
    let doc_ids = [0u32, 2u32];
    let batch = read_segment_columns(
        segment_reader,
        &projected_schema,
        DocSelection::Ids(&doc_ids),
        7,
        None,
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), projected_schema);

    let doc_id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(doc_id_col.values(), &[0, 2]);

    let seg_ord_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(seg_ord_col.values(), &[7, 7]);

    let id_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(id_col.values(), &[10, 30]);

    let score_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(score_col.values(), &[-1, -3]);

    let price_col = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(price_col.values(), &[1.5, 3.5]);

    let active_col = batch
        .column(5)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(active_col.value(0));
    assert!(active_col.value(1));

    // Dictionary-encoded string column: both rows are "alpha".
    let name_col = batch.column(6).as_dictionary::<Int32Type>();
    let values = name_col
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let key0 = name_col.keys().value(0) as usize;
    let key1 = name_col.keys().value(1) as usize;
    assert_eq!(values.value(key0), "alpha");
    assert_eq!(values.value(key1), "alpha");
}

#[test]
fn missing_column_is_all_null_not_an_error() {
    let index = build_index();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_reader = &searcher.segment_readers()[0];

    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, true),
        Field::new("missing", DataType::Float64, true),
    ]));

    let doc_ids = [0u32, 1u32, 2u32];
    let batch = read_segment_columns(
        segment_reader,
        &projected_schema,
        DocSelection::Ids(&doc_ids),
        0,
        None,
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 3);
    let id_col = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(id_col.values(), &[10, 20, 30]);

    let missing_col = batch.column(1);
    assert_eq!(missing_col.null_count(), 3);
    assert_eq!(missing_col.data_type(), &DataType::Float64);
}

#[test]
fn all_selection_reads_every_alive_doc() {
    let index = build_index();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_reader = &searcher.segment_readers()[0];

    // Project every type, plus the synthetic _doc_id / _segment_ord columns.
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("_doc_id", DataType::UInt32, false),
        Field::new("_segment_ord", DataType::UInt32, false),
        Field::new("id", DataType::UInt64, true),
        Field::new("score", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch = read_segment_columns(
        segment_reader,
        &projected_schema,
        DocSelection::All { limit: None },
        0,
        None,
    )
    .unwrap();
    assert_eq!(batch.num_rows(), 3);

    // With a limit, only the first alive docs are returned.
    let limited = read_segment_columns(
        segment_reader,
        &projected_schema,
        DocSelection::All { limit: Some(2) },
        0,
        None,
    )
    .unwrap();
    assert_eq!(limited.num_rows(), 2);
}

#[test]
fn range_selection_reads_requested_window() {
    let index = build_index();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_reader = &searcher.segment_readers()[0];

    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("_doc_id", DataType::UInt32, false),
        Field::new("id", DataType::UInt64, true),
    ]));

    let batch = read_segment_columns(
        segment_reader,
        &projected_schema,
        DocSelection::Range(1..3),
        0,
        None,
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 2);
    let doc_id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(doc_id_col.values(), &[1, 2]);
    let id_col = batch.column(1).as_primitive::<UInt64Type>();
    assert_eq!(id_col.values(), &[20, 30]);
}
