//! Integration test that exercises the real serving path:
//! Flight service on the same gRPC port, SearcherPool for worker
//! discovery, QuickwitSessionBuilder for distributed execution.
//!
//! Each simulated "searcher node" runs a tonic server with the Flight
//! service — the same way quickwit-serve/grpc.rs mounts it. Workers
//! discover each other via the SearcherPool, just like Chitchat would
//! populate it in production.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::UInt64Type;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use datafusion_distributed::display_plan_ascii;
use futures::TryStreamExt;
use quickwit_search::SearcherPool;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, TantivyInvertedIndexProvider, TantivyTableProvider,
};
use tokio::net::TcpListener;

use quickwit_datafusion::session::QuickwitSessionBuilder;
use quickwit_datafusion::{SplitIndexOpener, SplitRegistry, build_flight_service};

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

/// Start a tonic gRPC server with the Flight service on a random port.
/// This mirrors what `quickwit-serve/src/grpc.rs` does — same port,
/// same tonic server, Flight is just another service alongside search.
async fn start_searcher_node(registry: Arc<SplitRegistry>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let flight_service = build_flight_service(registry, quickwit_search::SearcherPool::default());

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight_service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Let the server start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

/// End-to-end: 3 searcher nodes on real TCP, SearcherPool for discovery,
/// distributed full-text join query across 3 splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_query_via_searcher_pool() {
    // ── Shared split registry (in production: each node has its own) ─
    let registry = Arc::new(SplitRegistry::new());

    let idx1 = create_index(&[
        (1, 10, 1.5, "electronics"),
        (2, 20, 2.5, "electronics"),
    ]);
    let idx2 = create_index(&[(3, 30, 3.5, "books"), (4, 40, 4.5, "books")]);
    let idx3 = create_index(&[
        (5, 50, 5.5, "clothing"),
        (6, 60, 6.5, "electronics"),
    ]);
    registry.insert("split-1".to_string(), idx1);
    registry.insert("split-2".to_string(), idx2);
    registry.insert("split-3".to_string(), idx3);

    // ── Start 3 searcher nodes (gRPC + Flight on same port) ─────────
    let node1_addr = start_searcher_node(registry.clone()).await;
    let node2_addr = start_searcher_node(registry.clone()).await;
    let node3_addr = start_searcher_node(registry.clone()).await;

    // ── Populate SearcherPool (as Chitchat would in production) ──────
    let searcher_pool = SearcherPool::default();
    for addr in [node1_addr, node2_addr, node3_addr] {
        searcher_pool.insert(
            addr,
            quickwit_search::SearchServiceClient::from_service(
                Arc::new(quickwit_search::MockSearchService::new()),
                addr,
            ),
        );
    }

    // ── Build DataFusion session via QuickwitSessionBuilder ──────────
    let mock_metastore = quickwit_proto::metastore::MetastoreServiceClient::from_mock(
        quickwit_proto::metastore::MockMetastoreService::new(),
    );
    let session_builder =
        QuickwitSessionBuilder::new(mock_metastore, searcher_pool, registry.clone());
    let ctx = session_builder.build_session();
    ctx.register_udf(full_text_udf());

    // ── Register per-split tantivy-df providers ─────────────────────
    for (i, split_id) in ["split-1", "split-2", "split-3"].iter().enumerate() {
        let index = registry.get(*split_id).unwrap().value().clone();
        let opener = Arc::new(SplitIndexOpener::from_index(
            split_id.to_string(),
            index,
            registry.clone(),
        ));
        let prefix = format!("s{}", i + 1);
        let o: Arc<dyn IndexOpener> = opener;
        ctx.register_table(
            &format!("{prefix}_f"),
            Arc::new(TantivyTableProvider::from_opener(o.clone())),
        )
        .unwrap();
        ctx.register_table(
            &format!("{prefix}_inv"),
            Arc::new(TantivyInvertedIndexProvider::from_opener(o)),
        )
        .unwrap();
    }

    // ── Execute distributed full-text join ───────────────────────────
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
    println!("=== Distributed plan (3 searcher nodes, same port) ===\n{plan_str}\n");

    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics: {1, 2} (split-1) + {6} (split-3) = 3 rows
    assert_eq!(batch.num_rows(), 3);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = (0..batch.num_rows()).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec![1, 2, 6]);
}
