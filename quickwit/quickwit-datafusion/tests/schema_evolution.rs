//! Tests for schema evolution across splits: missing columns get
//! NULL-filled, type mismatches error with a message telling the
//! user to CAST explicitly.

use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, UInt64Type};
use datafusion::prelude::*;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::search::SearchRequest;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{IndexOpener, full_text_udf};

use quickwit_datafusion::query_translator::build_search_plan;
use quickwit_datafusion::{SplitIndexOpener, SplitRegistry};

fn collect_batches(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// Split-1: old schema (id, price)
fn create_old_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let id_f = builder.add_u64_field("id", FAST | STORED);
    let price_f = builder.add_f64_field("price", FAST);
    let schema = builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_u64(id_f, 1);
    doc.add_f64(price_f, 1.5);
    writer.add_document(doc).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_u64(id_f, 2);
    doc.add_f64(price_f, 2.5);
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    index
}

/// Split-2: new schema (id, price, rating) — has an extra field
fn create_new_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let id_f = builder.add_u64_field("id", FAST | STORED);
    let price_f = builder.add_f64_field("price", FAST);
    let rating_f = builder.add_f64_field("rating", FAST);
    let schema = builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_u64(id_f, 3);
    doc.add_f64(price_f, 3.5);
    doc.add_f64(rating_f, 4.8);
    writer.add_document(doc).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_u64(id_f, 4);
    doc.add_f64(price_f, 4.5);
    doc.add_f64(rating_f, 3.2);
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    index
}

fn make_split_meta(split_id: &str) -> SplitMetadata {
    SplitMetadata {
        split_id: split_id.to_string(),
        ..Default::default()
    }
}

fn match_all_request() -> SearchRequest {
    SearchRequest {
        query_ast: serde_json::to_string(&serde_json::json!({"type": "match_all"})).unwrap(),
        ..Default::default()
    }
}

/// Missing column gets NULL-filled: old split lacks "rating",
/// new split has it. After union, old rows have rating=NULL.
#[tokio::test]
async fn test_missing_column_null_filled() {
    let registry = Arc::new(SplitRegistry::new());

    let old_idx = create_old_index();
    let new_idx = create_new_index();
    registry.insert("old-split".to_string(), old_idx);
    registry.insert("new-split".to_string(), new_idx);

    let splits = vec![make_split_meta("old-split"), make_split_meta("new-split")];

    let new_tantivy_schema = {
        let idx = registry.get("new-split").unwrap();
        idx.schema()
    };
    let reg = registry.clone();
    // Each opener uses the ACTUAL tantivy schema of its split, not the canonical one.
    let opener_factory: quickwit_datafusion::OpenerFactory =
        Arc::new(move |meta: &SplitMetadata| {
            let index = reg.get(&meta.split_id).unwrap().value().clone();
            let tantivy_schema = index.schema();
            let segment_sizes: Vec<u32> = index
                .reader()
                .map(|r| {
                    r.searcher()
                        .segment_readers()
                        .iter()
                        .map(|sr| sr.max_doc())
                        .collect()
                })
                .unwrap_or_default();
            Arc::new(SplitIndexOpener::new(
                meta.split_id.clone(),
                reg.clone(),
                tantivy_schema,
                segment_sizes,
            )) as Arc<dyn IndexOpener>
        });

    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());

    // Use the new schema as canonical (has the "rating" field).
    let canonical =
        Some(tantivy_datafusion::tantivy_schema_to_arrow(&new_tantivy_schema));

    let request = match_all_request();
    let df = build_search_plan(&ctx, &splits, &opener_factory, &request, canonical).unwrap();

    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // 4 rows total (2 from each split)
    assert_eq!(batch.num_rows(), 4);

    // Check that "rating" column exists
    let rating_idx = batch.schema().index_of("rating").unwrap();
    let rating_col = batch.column(rating_idx).as_primitive::<Float64Type>();

    // Old split rows should have NULL ratings, new split rows have values.
    // Rows are not guaranteed to be in order, so collect and check.
    let mut null_count = 0;
    let mut value_count = 0;
    for i in 0..batch.num_rows() {
        if rating_col.is_null(i) {
            null_count += 1;
        } else {
            value_count += 1;
        }
    }
    assert_eq!(null_count, 2, "old split rows should have NULL rating");
    assert_eq!(value_count, 2, "new split rows should have real rating");
}

/// Type mismatch errors with a clear message.
#[tokio::test]
async fn test_type_mismatch_errors() {
    let registry = Arc::new(SplitRegistry::new());

    // Split-1: id as u64
    let idx1 = {
        let mut builder = SchemaBuilder::new();
        let id_f = builder.add_u64_field("id", FAST | STORED);
        let schema = builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_f, 1);
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        index
    };
    registry.insert("split-1".to_string(), idx1);

    let splits = vec![make_split_meta("split-1")];

    let reg = registry.clone();
    let opener_factory: quickwit_datafusion::OpenerFactory =
        Arc::new(move |meta: &SplitMetadata| {
            let index = reg.get(&meta.split_id).unwrap().value().clone();
            let tantivy_schema = index.schema();
            let segment_sizes: Vec<u32> = index
                .reader()
                .map(|r| {
                    r.searcher()
                        .segment_readers()
                        .iter()
                        .map(|sr| sr.max_doc())
                        .collect()
                })
                .unwrap_or_default();
            Arc::new(SplitIndexOpener::new(
                meta.split_id.clone(),
                reg.clone(),
                tantivy_schema,
                segment_sizes,
            )) as Arc<dyn IndexOpener>
        });

    let ctx = SessionContext::new();

    // Create a canonical schema where "id" is Int64 instead of UInt64.
    // This simulates a type mismatch from schema evolution.
    let mismatched_canonical = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("_doc_id", arrow::datatypes::DataType::UInt32, false),
        arrow::datatypes::Field::new("_segment_ord", arrow::datatypes::DataType::UInt32, false),
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true), // mismatch: UInt64 vs Int64
    ]));

    let request = match_all_request();
    let result = build_search_plan(
        &ctx,
        &splits,
        &opener_factory,
        &request,
        Some(mismatched_canonical),
    );

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("type mismatch") && err.contains("id") && err.contains("CAST"),
        "error should mention type mismatch, field name, and CAST. Got: {err}"
    );
}

/// SQL CAST workaround: user can cast columns explicitly after the plan.
#[tokio::test]
async fn test_sql_cast_workaround() {
    let registry = Arc::new(SplitRegistry::new());
    let new_idx = create_new_index();
    registry.insert("split-1".to_string(), new_idx);

    let ctx = SessionContext::new();

    // Register the split as a table directly so we can run SQL on it.
    let index = registry.get("split-1").unwrap().value().clone();
    let opener = Arc::new(SplitIndexOpener::from_index(
        "split-1".to_string(),
        index,
        registry.clone(),
    ));
    ctx.register_table(
        "my_table",
        Arc::new(tantivy_datafusion::TantivyTableProvider::from_opener(
            opener as Arc<dyn IndexOpener>,
        )),
    )
    .unwrap();

    // User explicitly casts rating from f64 to integer — demonstrating
    // explicit type coercion via SQL CAST.
    let df = ctx
        .sql("SELECT id, CAST(rating AS BIGINT) as rating_int FROM my_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 3);
    assert_eq!(ids.value(1), 4);

    // The cast truncated: 4.8 → 4, 3.2 → 3
    let ratings = batch.column(1).as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(ratings.value(0), 4);
    assert_eq!(ratings.value(1), 3);
}
