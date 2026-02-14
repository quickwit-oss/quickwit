//! Tests showing the full tantivy-df join plan with all three provider
//! types per split: InvertedIndex ⋈ FastField ⋈ Document, joined on
//! (_doc_id, _segment_ord) with segment-level co-partitioning.
//!
//! This is what a real Quickwit query looks like decomposed into tantivy nodes.

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, UInt64Type};
use datafusion::prelude::*;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, TantivyDocumentProvider, TantivyInvertedIndexProvider,
    TantivyTableProvider,
};

use quickwit_datafusion::{SplitIndexOpener, SplitRegistry};

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

fn plan_to_string(batches: &[RecordBatch]) -> String {
    let batch = arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap();
    let plan_col = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| plan_col.value(i))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Register all three tantivy-df providers for a single split.
///
/// This is the decomposition: one tantivy index → three DF table providers:
/// - `{prefix}_f`   — fast fields (columnar values)
/// - `{prefix}_inv` — inverted index (full-text search)
/// - `{prefix}_d`   — document store (stored JSON)
///
/// Joined on `(_doc_id, _segment_ord)` at query time.
fn register_split(
    ctx: &SessionContext,
    prefix: &str,
    opener: Arc<SplitIndexOpener>,
) {
    let opener_dyn: Arc<dyn IndexOpener> = opener;
    ctx.register_table(
        &format!("{prefix}_f"),
        Arc::new(TantivyTableProvider::from_opener(opener_dyn.clone())),
    )
    .unwrap();
    ctx.register_table(
        &format!("{prefix}_inv"),
        Arc::new(TantivyInvertedIndexProvider::from_opener(opener_dyn.clone())),
    )
    .unwrap();
    ctx.register_table(
        &format!("{prefix}_d"),
        Arc::new(TantivyDocumentProvider::from_opener(opener_dyn)),
    )
    .unwrap();
}

/// Set up 3 splits, each with all 3 providers registered.
fn setup() -> (SessionContext, Arc<SplitRegistry>) {
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

    // Use 1 target partition per split so segment partitioning is 1:1.
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());

    register_split(&ctx, "s1", opener1);
    register_split(&ctx, "s2", opener2);
    register_split(&ctx, "s3", opener3);

    (ctx, registry)
}

// ── Full-text search: inv ⋈ f ──────────────────────────────────────

#[tokio::test]
async fn test_full_text_join_plan() {
    let (ctx, _) = setup();

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT s1_f.id, s1_f.price \
             FROM s1_inv \
             JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id \
                      AND s1_f._segment_ord = s1_inv._segment_ord \
             WHERE full_text(s1_inv.category, 'electronics') \
             ORDER BY s1_f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    println!("=== Full-text join plan (single split) ===\n{plan}\n");

    // The physical plan should show:
    // - HashJoinExec on (_doc_id, _segment_ord) — segment-level co-partitioning
    // - InvertedIndexDataSource on the build side (with query=true)
    // - FastFieldDataSource on the probe side
    assert!(plan.contains("HashJoinExec"), "expected HashJoinExec\n\n{plan}");
    assert!(plan.contains("InvertedIndexDataSource"), "expected InvertedIndexDataSource\n\n{plan}");
    assert!(plan.contains("FastFieldDataSource"), "expected FastFieldDataSource\n\n{plan}");
}

#[tokio::test]
async fn test_full_text_join_results() {
    let (ctx, _) = setup();

    let df = ctx
        .sql(
            "SELECT s1_f.id, s1_f.price \
             FROM s1_inv \
             JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id \
                      AND s1_f._segment_ord = s1_inv._segment_ord \
             WHERE full_text(s1_inv.category, 'electronics') \
             ORDER BY s1_f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // split-1 has electronics at ids {1, 2}
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

// ── Three-way join: inv ⋈ f ⋈ d ────────────────────────────────────

#[tokio::test]
async fn test_three_way_join_plan() {
    let (ctx, _) = setup();

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT s1_f.id, s1_f.price, s1_d._document \
             FROM s1_inv \
             JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id \
                      AND s1_f._segment_ord = s1_inv._segment_ord \
             JOIN s1_d ON s1_d._doc_id = s1_f._doc_id \
                      AND s1_d._segment_ord = s1_f._segment_ord \
             WHERE full_text(s1_inv.category, 'electronics') \
             ORDER BY s1_f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    println!("=== Three-way join plan (inv ⋈ f ⋈ d, single split) ===\n{plan}\n");

    // All three DataSource types in one plan
    assert!(plan.contains("InvertedIndexDataSource"), "expected InvertedIndexDataSource\n\n{plan}");
    assert!(plan.contains("FastFieldDataSource"), "expected FastFieldDataSource\n\n{plan}");
    assert!(plan.contains("DocumentDataSource"), "expected DocumentDataSource\n\n{plan}");
    // Two joins
    let join_count = plan.matches("HashJoinExec").count();
    assert!(join_count >= 2, "expected 2 HashJoinExec, got {join_count}\n\n{plan}");
}

#[tokio::test]
async fn test_three_way_join_results() {
    let (ctx, _) = setup();

    let df = ctx
        .sql(
            "SELECT s1_f.id, s1_f.price, s1_d._document \
             FROM s1_inv \
             JOIN s1_f ON s1_f._doc_id = s1_inv._doc_id \
                      AND s1_f._segment_ord = s1_inv._segment_ord \
             JOIN s1_d ON s1_d._doc_id = s1_f._doc_id \
                      AND s1_d._segment_ord = s1_f._segment_ord \
             WHERE full_text(s1_inv.category, 'electronics') \
             ORDER BY s1_f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);

    // Documents should be JSON with id and category
    let docs = batch.column(2).as_string::<i32>();
    let doc0: serde_json::Value = serde_json::from_str(docs.value(0)).unwrap();
    assert_eq!(doc0["id"][0], 1);
    assert_eq!(doc0["category"][0], "electronics");
}

// ── Cross-split UNION of join plans ─────────────────────────────────

#[tokio::test]
async fn test_cross_split_full_text_plan() {
    let (ctx, _) = setup();

    // Search "electronics" across all 3 splits via UNION ALL of per-split joins.
    // This is how a real distributed Quickwit query decomposes.
    let df = ctx
        .sql(
            "EXPLAIN \
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
             ) ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    println!("=== Cross-split full-text search plan ===\n{plan}\n");

    // Should have 3 HashJoinExec (one per split), 3 InvertedIndexDataSource, 3 FastFieldDataSource
    let inv_count = plan.matches("InvertedIndexDataSource").count();
    let ff_count = plan.matches("FastFieldDataSource").count();
    let join_count = plan.matches("HashJoinExec").count();
    assert_eq!(inv_count, 3, "expected 3 InvertedIndexDataSource, got {inv_count}\n\n{plan}");
    assert_eq!(ff_count, 3, "expected 3 FastFieldDataSource, got {ff_count}\n\n{plan}");
    assert_eq!(join_count, 3, "expected 3 HashJoinExec, got {join_count}\n\n{plan}");
}

#[tokio::test]
async fn test_cross_split_full_text_results() {
    let (ctx, _) = setup();

    let df = ctx
        .sql(
            "SELECT id, price FROM ( \
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
             ) ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics across all splits: ids {1, 2} (split-1) + {} (split-2) + {6} (split-3)
    assert_eq!(batch.num_rows(), 3);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let prices = batch.column(1).as_primitive::<Float64Type>();
    assert_eq!(ids.value(0), 1);
    assert!((prices.value(0) - 1.5).abs() < 1e-10);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 6);
    assert!((prices.value(2) - 6.5).abs() < 1e-10);
}
