//! Single-node integration tests for quickwit-datafusion.
//!
//! Tests the full flow: mock metastore → QuickwitTableProvider → tantivy-df
//! providers → query execution.

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion::prelude::*;
use quickwit_metastore::{ListSplitsResponseExt, SplitMetadata};
use quickwit_proto::metastore::MetastoreError;
use quickwit_proto::metastore::{
    ListSplitsResponse, MetastoreServiceClient, MockMetastoreService,
};
use quickwit_common::ServiceStream;
use quickwit_proto::types::IndexUid;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, TantivyInvertedIndexProvider, TantivyTableProvider,
};

use quickwit_datafusion::{SplitIndexOpener, SplitRegistry};

fn build_test_schema() -> (
    tantivy::schema::Schema,
    tantivy::schema::Field, // id
    tantivy::schema::Field, // score
    tantivy::schema::Field, // price
    tantivy::schema::Field, // category
) {
    let mut builder = SchemaBuilder::new();
    let id_field = builder.add_u64_field("id", FAST | STORED);
    let score_field = builder.add_i64_field("score", FAST);
    let price_field = builder.add_f64_field("price", FAST);
    let category_field = builder.add_text_field("category", TEXT | FAST | STORED);
    let schema = builder.build();
    (schema, id_field, score_field, price_field, category_field)
}

fn create_index(docs: &[(u64, i64, f64, &str)]) -> Index {
    let (schema, id_f, score_f, price_f, cat_f) = build_test_schema();
    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
    for &(id, score, price, category) in docs {
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_f, id);
        doc.add_i64(score_f, score);
        doc.add_f64(price_f, price);
        doc.add_text(cat_f, category);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();
    index
}

fn collect_batches(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// Create a mock metastore that returns the given split IDs.
fn mock_metastore(index_uid: &IndexUid, split_ids: &[&str]) -> MetastoreServiceClient {
    let index_uid = index_uid.clone();
    let split_ids: Vec<String> = split_ids.iter().map(|s| s.to_string()).collect();

    let mut mock = MockMetastoreService::new();
    mock.expect_list_splits().returning(move |_request| {
        let splits: Vec<quickwit_metastore::Split> = split_ids
            .iter()
            .map(|id| quickwit_metastore::Split {
                split_state: quickwit_metastore::SplitState::Published,
                split_metadata: SplitMetadata {
                    split_id: id.clone(),
                    index_uid: index_uid.clone(),
                    num_docs: 2,
                    ..Default::default()
                },
                update_timestamp: 0,
                publish_timestamp: None,
            })
            .collect();
        let response = ListSplitsResponse::try_from_splits(splits).unwrap();
        let items: Vec<Result<ListSplitsResponse, MetastoreError>> = vec![Ok(response)];
        Ok(ServiceStream::from(items))
    });

    MetastoreServiceClient::from_mock(mock)
}

// ── QuickwitTableProvider with mock metastore ───────────────────────

#[tokio::test]
async fn test_metastore_backed_provider() {
    let registry = Arc::new(SplitRegistry::new());
    let (tantivy_schema, ..) = build_test_schema();

    // Create indexes and register in the registry
    let idx1 = create_index(&[(1, 10, 1.5, "electronics"), (2, 20, 2.5, "electronics")]);
    let idx2 = create_index(&[(3, 30, 3.5, "books"), (4, 40, 4.5, "books")]);
    let idx3 = create_index(&[(5, 50, 5.5, "clothing"), (6, 60, 6.5, "clothing")]);
    registry.insert("split-1".to_string(), idx1);
    registry.insert("split-2".to_string(), idx2);
    registry.insert("split-3".to_string(), idx3);

    let index_uid = IndexUid::for_test("test-index", 0);
    let metastore = mock_metastore(&index_uid, &["split-1", "split-2", "split-3"]);

    // The opener factory creates a SplitIndexOpener from SplitMetadata.
    // It reads the tantivy schema from the registry (in production this
    // would come from the doc mapper or the split footer).
    let reg = registry.clone();
    let schema = tantivy_schema.clone();
    let opener_factory: quickwit_datafusion::OpenerFactory = Arc::new(move |meta: &SplitMetadata| {
        let index = reg.get(&meta.split_id).expect("split not in registry");
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
            schema.clone(),
            segment_sizes,
        )) as Arc<dyn IndexOpener>
    });

    let provider = quickwit_datafusion::QuickwitTableProvider::new(
        index_uid,
        metastore,
        opener_factory,
        &tantivy_schema,
    );

    let ctx = SessionContext::new();
    ctx.register_table("splits", Arc::new(provider)).unwrap();

    // SELECT all
    let df = ctx.sql("SELECT id, price FROM splits ORDER BY id").await.unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);
    assert_eq!(batch.num_rows(), 6);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = (0..6).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_metastore_backed_provider_group_by() {
    let registry = Arc::new(SplitRegistry::new());
    let (tantivy_schema, ..) = build_test_schema();

    let idx1 = create_index(&[(1, 10, 1.5, "electronics"), (2, 20, 2.5, "electronics")]);
    let idx2 = create_index(&[(3, 30, 3.5, "books"), (4, 40, 4.5, "books")]);
    registry.insert("split-1".to_string(), idx1);
    registry.insert("split-2".to_string(), idx2);

    let index_uid = IndexUid::for_test("test-index", 0);
    let metastore = mock_metastore(&index_uid, &["split-1", "split-2"]);

    let reg = registry.clone();
    let schema = tantivy_schema.clone();
    let opener_factory: quickwit_datafusion::OpenerFactory = Arc::new(move |meta: &SplitMetadata| {
        let index = reg.get(&meta.split_id).expect("split not in registry");
        let segment_sizes: Vec<u32> = index
            .reader()
            .map(|r| r.searcher().segment_readers().iter().map(|sr| sr.max_doc()).collect())
            .unwrap_or_default();
        Arc::new(SplitIndexOpener::new(
            meta.split_id.clone(),
            reg.clone(),
            schema.clone(),
            segment_sizes,
        )) as Arc<dyn IndexOpener>
    });

    let provider = quickwit_datafusion::QuickwitTableProvider::new(
        index_uid, metastore, opener_factory, &tantivy_schema,
    );

    let ctx = SessionContext::new();
    ctx.register_table("splits", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT category, COUNT(*) as cnt FROM splits GROUP BY category ORDER BY category")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let cat_col = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
    let categories = cat_col.as_string::<i32>();
    let counts = batch.column(1).as_primitive::<Int64Type>();
    assert_eq!(categories.value(0), "books");
    assert_eq!(counts.value(0), 2);
    assert_eq!(categories.value(1), "electronics");
    assert_eq!(counts.value(1), 2);
}

// ── Direct tantivy-df provider tests (no QuickwitTableProvider) ─────

#[tokio::test]
async fn test_full_text_join_single_split() {
    let registry = Arc::new(SplitRegistry::new());
    let idx = create_index(&[
        (1, 10, 1.5, "electronics"),
        (2, 20, 2.5, "electronics"),
        (3, 30, 3.5, "books"),
    ]);
    let opener = Arc::new(SplitIndexOpener::from_index(
        "split-1".to_string(), idx, registry.clone(),
    ));

    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "f",
        Arc::new(TantivyTableProvider::from_opener(opener.clone() as Arc<dyn IndexOpener>)),
    )
    .unwrap();
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::from_opener(opener as Arc<dyn IndexOpener>)),
    )
    .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}
