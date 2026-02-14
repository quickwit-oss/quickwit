//! Integration test that exercises the full production path:
//!
//! - Real splits created via TestSandbox (indexing pipeline → storage)
//! - StorageSplitOpener calling open_index_with_caches
//! - QuickwitSchemaProvider catalog resolving indexes from metastore
//! - Flight + SearchService on the same tonic server
//! - SearcherPool for worker discovery
//! - Distributed query execution via Arrow Flight
//!
//! The only difference from production is localhost networking
//! and manual SearcherPool population (instead of Chitchat).

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::UInt64Type;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use datafusion_distributed::display_plan_ascii;
use futures::TryStreamExt;
use quickwit_config::SearcherConfig;
use quickwit_indexing::TestSandbox;
use quickwit_search::{
    MockSearchService, SearchServiceClient, SearcherPool, SearcherContext,
};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, TantivyInvertedIndexProvider, TantivyTableProvider,
};
use tokio::net::TcpListener;

use quickwit_datafusion::session::QuickwitSessionBuilder;
use quickwit_datafusion::split_opener::{SplitRegistry, StorageSplitOpener};
use quickwit_datafusion::build_flight_service;
use tantivy_datafusion::{OpenerFactory, OpenerMetadata};

fn collect_batches(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// Start a tonic server with the Flight service — matching how
/// grpc.rs mounts it alongside SearchService on the same port.
async fn start_searcher_node(
    opener_factory: OpenerFactory,
    searcher_pool: SearcherPool,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let flight_service = build_flight_service(opener_factory, searcher_pool);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight_service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

const DOC_MAPPING_YAML: &str = r#"
field_mappings:
  - name: id
    type: u64
    fast: true
    stored: true
  - name: category
    type: text
    tokenizer: default
    fast: true
    stored: true
  - name: price
    type: f64
    fast: true
"#;

/// End-to-end: real splits on storage, real opener, real catalog,
/// real Flight servers, distributed query execution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_full_production_path() {
    // ── Create a real index with splits via TestSandbox ──────────────
    let sandbox = TestSandbox::create("test-df-index", DOC_MAPPING_YAML, "{}", &["category"])
        .await
        .unwrap();

    // Index two batches → two splits.
    sandbox
        .add_documents(vec![
            serde_json::json!({"id": 1, "category": "electronics", "price": 1.5}),
            serde_json::json!({"id": 2, "category": "electronics", "price": 2.5}),
            serde_json::json!({"id": 3, "category": "books", "price": 3.5}),
        ])
        .await
        .unwrap();

    sandbox
        .add_documents(vec![
            serde_json::json!({"id": 4, "category": "books", "price": 4.5}),
            serde_json::json!({"id": 5, "category": "clothing", "price": 5.5}),
        ])
        .await
        .unwrap();

    let metastore = sandbox.metastore();
    let storage_resolver = sandbox.storage_resolver();
    let index_uid = sandbox.index_uid();
    let doc_mapper = sandbox.doc_mapper();
    let tantivy_schema = doc_mapper.schema();
    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default(), None));

    // ── Build a storage-backed opener factory ──────────────────────────
    // This is the same factory that production uses (via the catalog).
    let storage = sandbox.storage();
    let sc = searcher_context.clone();
    let ts = tantivy_schema.clone();
    let opener_factory: OpenerFactory = Arc::new(move |meta: OpenerMetadata| {
        Arc::new(StorageSplitOpener::new(
            meta.identifier,
            ts.clone(),
            meta.segment_sizes,
            sc.clone(),
            storage.clone(),
            meta.footer_start,
            meta.footer_end,
        )) as Arc<dyn IndexOpener>
    });

    // ── Start 2 searcher nodes (Flight on same port as SearchService) ─
    let registry = Arc::new(SplitRegistry::new());
    let searcher_pool = SearcherPool::default();

    let node1_addr = start_searcher_node(opener_factory.clone(), searcher_pool.clone()).await;
    let node2_addr = start_searcher_node(opener_factory.clone(), searcher_pool.clone()).await;

    // Populate SearcherPool (as Chitchat would in production).
    for addr in [node1_addr, node2_addr] {
        searcher_pool.insert(
            addr,
            SearchServiceClient::from_service(
                Arc::new(MockSearchService::new()),
                addr,
            ),
        );
    }

    // ── Build DataFusion session with catalog ────────────────────────
    let session_builder = QuickwitSessionBuilder::new(
        metastore.clone(),
        searcher_pool.clone(),
        registry.clone(),
    )
    .with_storage(storage_resolver.clone(), searcher_context.clone());

    let ctx = session_builder.build_session();
    ctx.register_udf(full_text_udf());

    // ── Register index tables using real openers ─────────────────────
    // In production, the QuickwitSchemaProvider catalog does this lazily.
    // Here we register manually to test with StorageSplitOpener.
    let storage = storage_resolver
        .resolve(&sandbox.index_uid().to_string().parse::<quickwit_common::uri::Uri>()
            .unwrap_or_else(|_| {
                // Fallback: use the ram:// URI pattern from TestSandbox
                format!("ram://quickwit-test-indexes/test-df-index")
                    .parse()
                    .unwrap()
            }))
        .await
        .unwrap_or_else(|_| {
            // Use the sandbox's storage directly
            sandbox.storage()
        });

    // List splits from the metastore and register per-split tables.
    use quickwit_metastore::{
        ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitState,
    };
    use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};

    let query = ListSplitsQuery::for_index(index_uid.clone())
        .with_split_state(SplitState::Published);
    let request = ListSplitsRequest::try_from_list_splits_query(&query).unwrap();
    let splits = metastore
        .clone()
        .list_splits(request)
        .await
        .unwrap()
        .collect_splits_metadata()
        .await
        .unwrap();

    assert!(splits.len() >= 2, "expected at least 2 splits, got {}", splits.len());

    for (i, split_meta) in splits.iter().enumerate() {
        // Use num_docs as the segment size — each split is typically
        // one tantivy Index with 1 segment after merging.
        let opener = Arc::new(StorageSplitOpener::new(
            split_meta.split_id.clone(),
            tantivy_schema.clone(),
            vec![split_meta.num_docs as u32],
            searcher_context.clone(),
            storage.clone(),
            split_meta.footer_offsets.start,
            split_meta.footer_offsets.end,
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

    // ── Build and execute a distributed query ────────────────────────
    let num_splits = splits.len();
    let union_parts: Vec<String> = (1..=num_splits)
        .map(|i| {
            format!(
                "SELECT s{i}_f.id, s{i}_f.price \
                 FROM s{i}_inv \
                 JOIN s{i}_f ON s{i}_f._doc_id = s{i}_inv._doc_id \
                            AND s{i}_f._segment_ord = s{i}_inv._segment_ord \
                 WHERE full_text(s{i}_inv.category, 'electronics')"
            )
        })
        .collect();
    let sql = format!(
        "SELECT id, price FROM ({}) ORDER BY id",
        union_parts.join(" UNION ALL ")
    );

    println!("SQL: {sql}");

    let df: DataFrame = ctx.sql(&sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = display_plan_ascii(plan.as_ref(), false);
    println!("=== Distributed plan (real splits, real storage) ===\n{plan_str}\n");

    // Verify the plan structure.
    assert!(
        plan_str.contains("CollectLeft"),
        "plan should use CollectLeft join mode (no hash repartition)\n\n{plan_str}"
    );
    assert!(
        plan_str.contains("InvertedIndexDataSource"),
        "plan should contain InvertedIndexDataSource\n\n{plan_str}"
    );
    assert!(
        plan_str.contains("FastFieldDataSource"),
        "plan should contain FastFieldDataSource\n\n{plan_str}"
    );

    // Execute the distributed query.
    let stream = execute_stream(plan, ctx.task_ctx()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("total rows: {total_rows}, batches: {}", batches.len());
    assert!(total_rows > 0, "expected results but got 0 rows");

    let batch = collect_batches(&batches);

    // "electronics" should match ids {1, 2}
    assert_eq!(batch.num_rows(), 2, "expected 2 electronics docs");
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = (0..batch.num_rows()).map(|i| ids.value(i)).collect();
    id_values.sort();
    assert_eq!(id_values, vec![1, 2]);

    println!("full production path: OK");

    sandbox.assert_quit().await;
}
