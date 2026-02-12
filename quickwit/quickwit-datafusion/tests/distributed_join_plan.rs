//! Distributed join plans showing the full tantivy-df decomposition
//! with stage boundaries, network shuffles, and segment-level
//! co-partitioned joins across workers.

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::UInt64Type;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use datafusion_distributed::{display_plan_ascii, DistributedExt};
use futures::TryStreamExt;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, OpenerMetadata, TantivyCodec, TantivyDocumentProvider,
    TantivyInvertedIndexProvider, TantivyTableProvider,
};

use quickwit_datafusion::{SplitIndexOpener, SplitRegistry};
use quickwit_datafusion::worker::build_worker_session_builder;

fn create_index(docs: &[(u64, i64, f64, &str)]) -> Index {
    let mut builder = SchemaBuilder::new();
    let id_f = builder.add_u64_field("id", FAST | STORED);
    let score_f = builder.add_i64_field("score", FAST);
    let price_f = builder.add_f64_field("price", FAST);
    let cat_f = builder.add_text_field("category", TEXT | FAST | STORED);
    let schema = builder.build();

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

fn register_split(ctx: &SessionContext, prefix: &str, opener: Arc<SplitIndexOpener>) {
    let o: Arc<dyn IndexOpener> = opener;
    ctx.register_table(
        &format!("{prefix}_f"),
        Arc::new(TantivyTableProvider::from_opener(o.clone())),
    )
    .unwrap();
    ctx.register_table(
        &format!("{prefix}_inv"),
        Arc::new(TantivyInvertedIndexProvider::from_opener(o.clone())),
    )
    .unwrap();
    ctx.register_table(
        &format!("{prefix}_d"),
        Arc::new(TantivyDocumentProvider::from_opener(o)),
    )
    .unwrap();
}

fn setup_registry() -> (Arc<SplitRegistry>, Vec<Arc<SplitIndexOpener>>) {
    let registry = Arc::new(SplitRegistry::new());

    let idx1 = create_index(&[
        (1, 10, 1.5, "electronics"),
        (2, 20, 2.5, "electronics"),
        (3, 30, 3.5, "books"),
    ]);
    let idx2 = create_index(&[
        (4, 40, 4.5, "books"),
        (5, 50, 5.5, "clothing"),
    ]);
    let idx3 = create_index(&[
        (6, 60, 6.5, "electronics"),
        (7, 70, 7.5, "clothing"),
    ]);

    let opener1 = Arc::new(SplitIndexOpener::from_index(
        "split-1".to_string(), idx1, registry.clone(),
    ));
    let opener2 = Arc::new(SplitIndexOpener::from_index(
        "split-2".to_string(), idx2, registry.clone(),
    ));
    let opener3 = Arc::new(SplitIndexOpener::from_index(
        "split-3".to_string(), idx3, registry.clone(),
    ));

    (registry, vec![opener1, opener2, opener3])
}

fn make_codec(registry: Arc<SplitRegistry>) -> TantivyCodec {
    TantivyCodec::new(move |meta: OpenerMetadata| {
        Arc::new(SplitIndexOpener::new(
            meta.identifier,
            registry.clone(),
            meta.tantivy_schema,
            meta.segment_sizes,
        )) as Arc<dyn IndexOpener>
    })
}

/// Full-text search across 3 splits: inv ⋈ f, distributed.
#[tokio::test]
async fn test_distributed_full_text_join_plan() {
    let (registry, openers) = setup_registry();

    let (mut ctx, _guard, _workers) =
        datafusion_distributed::test_utils::localhost::start_localhost_context(
            3,
            build_worker_session_builder(registry.clone()),
        )
        .await;

    ctx.set_distributed_user_codec(make_codec(registry.clone()));
    ctx.register_udf(full_text_udf());

    for (i, opener) in openers.iter().enumerate() {
        register_split(&ctx, &format!("s{}", i + 1), opener.clone());
    }

    let sql = "\
        SELECT id, price FROM ( \
          SELECT s1_f.id, s1_f.price \
          FROM s1_inv \
          JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id AND s1_f._segment_ord = s1_inv._segment_ord \
          WHERE full_text(s1_inv.category, 'electronics') \
          UNION ALL \
          SELECT s2_f.id, s2_f.price \
          FROM s2_inv \
          JOIN s2_f ON s2_f._doc_id = s2_inv._doc_id AND s2_f._segment_ord = s2_inv._segment_ord \
          WHERE full_text(s2_inv.category, 'electronics') \
          UNION ALL \
          SELECT s3_f.id, s3_f.price \
          FROM s3_inv \
          JOIN s3_f ON s3_f._doc_id = s3_inv._doc_id AND s3_f._segment_ord = s3_inv._segment_ord \
          WHERE full_text(s3_inv.category, 'electronics') \
        ) ORDER BY id";

    let df: DataFrame = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = display_plan_ascii(plan.as_ref(), false);

    println!("=== Distributed full-text join plan (inv ⋈ f, 3 splits) ===\n{plan_str}\n");

    assert!(
        plan_str.contains("InvertedIndexDataSource"),
        "should contain InvertedIndexDataSource\n\n{plan_str}"
    );
    assert!(
        plan_str.contains("FastFieldDataSource"),
        "should contain FastFieldDataSource\n\n{plan_str}"
    );

    // Execute and verify
    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics: ids {1,2} (split-1) + {6} (split-3)
    assert_eq!(batch.num_rows(), 3);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = (0..batch.num_rows()).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec![1, 2, 6]);
}

/// Three-way join across 3 splits: inv ⋈ f ⋈ d, distributed.
#[tokio::test]
async fn test_distributed_three_way_join_plan() {
    let (registry, openers) = setup_registry();

    let (mut ctx, _guard, _workers) =
        datafusion_distributed::test_utils::localhost::start_localhost_context(
            3,
            build_worker_session_builder(registry.clone()),
        )
        .await;

    ctx.set_distributed_user_codec(make_codec(registry.clone()));
    ctx.register_udf(full_text_udf());

    for (i, opener) in openers.iter().enumerate() {
        register_split(&ctx, &format!("s{}", i + 1), opener.clone());
    }

    let sql = "\
        SELECT id, price, doc FROM ( \
          SELECT s1_f.id, s1_f.price, s1_d._document as doc \
          FROM s1_inv \
          JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id AND s1_f._segment_ord = s1_inv._segment_ord \
          JOIN s1_d ON s1_d._doc_id = s1_f._doc_id AND s1_d._segment_ord = s1_f._segment_ord \
          WHERE full_text(s1_inv.category, 'books') \
          UNION ALL \
          SELECT s2_f.id, s2_f.price, s2_d._document as doc \
          FROM s2_inv \
          JOIN s2_f ON s2_f._doc_id = s2_inv._doc_id AND s2_f._segment_ord = s2_inv._segment_ord \
          JOIN s2_d ON s2_d._doc_id = s2_f._doc_id AND s2_d._segment_ord = s2_f._segment_ord \
          WHERE full_text(s2_inv.category, 'books') \
          UNION ALL \
          SELECT s3_f.id, s3_f.price, s3_d._document as doc \
          FROM s3_inv \
          JOIN s3_f ON s3_f._doc_id = s3_inv._doc_id AND s3_f._segment_ord = s3_inv._segment_ord \
          JOIN s3_d ON s3_d._doc_id = s3_f._doc_id AND s3_d._segment_ord = s3_f._segment_ord \
          WHERE full_text(s3_inv.category, 'books') \
        ) ORDER BY id";

    let df: DataFrame = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = display_plan_ascii(plan.as_ref(), false);

    println!("=== Distributed three-way join plan (inv ⋈ f ⋈ d, 3 splits) ===\n{plan_str}\n");

    assert!(plan_str.contains("InvertedIndexDataSource"), "{plan_str}");
    assert!(plan_str.contains("FastFieldDataSource"), "{plan_str}");
    assert!(plan_str.contains("DocumentDataSource"), "{plan_str}");

    // Execute and verify
    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batch = collect_batches(&batches);

    // books: ids {3} (split-1) + {4} (split-2)
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = (0..batch.num_rows()).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec![3, 4]);

    let docs = batch.column(2).as_string::<i32>();
    for i in 0..2 {
        let doc: serde_json::Value = serde_json::from_str(docs.value(i)).unwrap();
        assert_eq!(doc["category"][0], "books");
    }
}
