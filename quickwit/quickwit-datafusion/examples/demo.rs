//! Demo: Distributed DataFusion execution over Quickwit splits.
//!
//! Shows three query paths:
//! 1. Decomposed join plan (inv ⋈ f) — full visibility into tantivy nodes
//! 2. Unified table provider — hides join complexity, single table
//! 3. SearchRequest → DataFrame — Elasticsearch-compatible query path
//!
//! Run: cargo run -p quickwit-datafusion --example demo

use std::sync::Arc;

use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use datafusion_distributed::display_plan_ascii;
use futures::TryStreamExt;
use quickwit_config::SearcherConfig;
use quickwit_indexing::TestSandbox;
use quickwit_search::{MockSearchService, SearchServiceClient, SearcherPool, SearcherContext};
use tantivy_datafusion::{
    full_text_udf, IndexOpener, TantivyInvertedIndexProvider, TantivyTableProvider,
    UnifiedTantivyTableProvider,
};
use tokio::net::TcpListener;

use quickwit_datafusion::query_translator::build_search_plan;
use quickwit_datafusion::session::QuickwitSessionBuilder;
use quickwit_datafusion::split_opener::{SplitRegistry, StorageSplitOpener};
use quickwit_datafusion::build_flight_service;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitState,
};
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};

const DOC_MAPPING: &str = r#"
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
  - name: score
    type: i64
    fast: true
"#;

async fn start_worker(
    opener_factory: tantivy_datafusion::OpenerFactory,
    pool: SearcherPool,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let flight = build_flight_service(opener_factory, pool);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

fn section(title: &str) {
    println!("\n{}", "=".repeat(60));
    println!("  {title}");
    println!("{}\n", "=".repeat(60));
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    // ── Create index with real splits ────────────────────────────────
    println!("📦 Creating index with TestSandbox...");
    let sandbox = TestSandbox::create("demo-logs", DOC_MAPPING, "{}", &["category"]).await?;

    sandbox
        .add_documents(vec![
            serde_json::json!({"id": 1, "category": "electronics", "price": 9.99, "score": 85}),
            serde_json::json!({"id": 2, "category": "electronics", "price": 24.99, "score": 92}),
            serde_json::json!({"id": 3, "category": "books", "price": 14.99, "score": 78}),
            serde_json::json!({"id": 4, "category": "books", "price": 7.99, "score": 65}),
        ])
        .await?;

    sandbox
        .add_documents(vec![
            serde_json::json!({"id": 5, "category": "clothing", "price": 49.99, "score": 88}),
            serde_json::json!({"id": 6, "category": "electronics", "price": 199.99, "score": 95}),
            serde_json::json!({"id": 7, "category": "books", "price": 29.99, "score": 91}),
        ])
        .await?;

    let metastore = sandbox.metastore();
    let storage = sandbox.storage();
    let doc_mapper = sandbox.doc_mapper();
    let index_uid = sandbox.index_uid();
    let tantivy_schema = doc_mapper.schema();
    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default(), None));

    // List splits.
    let query = ListSplitsQuery::for_index(index_uid.clone())
        .with_split_state(SplitState::Published);
    let request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits = metastore
        .clone()
        .list_splits(request)
        .await?
        .collect_splits_metadata()
        .await?;

    println!(
        "   {} splits created ({} total docs)\n",
        splits.len(),
        splits.iter().map(|s| s.num_docs).sum::<usize>()
    );

    // ── Build opener factory + workers ───────────────────────────────
    let sc = searcher_context.clone();
    let st = storage.clone();
    let ts = tantivy_schema.clone();
    let opener_factory: tantivy_datafusion::OpenerFactory =
        Arc::new(move |meta: tantivy_datafusion::OpenerMetadata| {
            Arc::new(StorageSplitOpener::new(
                meta.identifier,
                ts.clone(),
                meta.segment_sizes,
                sc.clone(),
                st.clone(),
                meta.footer_start,
                meta.footer_end,
            )) as Arc<dyn IndexOpener>
        });

    let searcher_pool = SearcherPool::default();
    let addr1 = start_worker(opener_factory.clone(), searcher_pool.clone()).await;
    let addr2 = start_worker(opener_factory.clone(), searcher_pool.clone()).await;
    for addr in [addr1, addr2] {
        searcher_pool.insert(
            addr,
            SearchServiceClient::from_service(Arc::new(MockSearchService::new()), addr),
        );
    }
    println!("🚀 Started 2 Flight workers on :{}, :{}", addr1.port(), addr2.port());

    let session_builder = QuickwitSessionBuilder::new(
        metastore.clone(),
        searcher_pool.clone(),
        Arc::new(SplitRegistry::new()),
    );

    // ═══════════════════════════════════════════════════════════════
    //  DEMO 1: Decomposed join plan (inv ⋈ f)
    // ═══════════════════════════════════════════════════════════════
    section("DEMO 1: Decomposed Join Plan (inv ⋈ f)");
    {
        let ctx = session_builder.build_session();
        ctx.register_udf(full_text_udf());

        for (i, split) in splits.iter().enumerate() {
            let opener = Arc::new(StorageSplitOpener::new(
                split.split_id.clone(),
                tantivy_schema.clone(),
                vec![split.num_docs as u32],
                searcher_context.clone(),
                storage.clone(),
                split.footer_offsets.start,
                split.footer_offsets.end,
            ));
            let prefix = format!("s{}", i + 1);
            let o: Arc<dyn IndexOpener> = opener;
            ctx.register_table(
                &format!("{prefix}_f"),
                Arc::new(TantivyTableProvider::from_opener(o.clone())),
            )?;
            ctx.register_table(
                &format!("{prefix}_inv"),
                Arc::new(TantivyInvertedIndexProvider::from_opener(o)),
            )?;
        }

        let n = splits.len();
        let union_parts: Vec<String> = (1..=n)
            .map(|i| format!(
                "SELECT s{i}_f.id, s{i}_f.price \
                 FROM s{i}_inv \
                 JOIN s{i}_f ON s{i}_f._doc_id = s{i}_inv._doc_id \
                            AND s{i}_f._segment_ord = s{i}_inv._segment_ord \
                 WHERE full_text(s{i}_inv.category, 'electronics')"
            ))
            .collect();
        let sql = format!(
            "SELECT id, price FROM ({}) ORDER BY id",
            union_parts.join(" UNION ALL ")
        );

        println!("SQL:\n  {sql}\n");

        let df: DataFrame = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        println!("Distributed Plan:\n{}\n", display_plan_ascii(plan.as_ref(), false));

        let stream = execute_stream(plan, ctx.task_ctx())?;
        let batches: Vec<_> = stream.try_collect().await?;
        let formatted = datafusion::common::arrow::util::pretty::pretty_format_batches(&batches)?;
        println!("Results:\n{formatted}\n");
    }

    // ═══════════════════════════════════════════════════════════════
    //  DEMO 2: Unified table provider (hides join complexity)
    // ═══════════════════════════════════════════════════════════════
    section("DEMO 2: Unified Table Provider (distributed)");
    {
        let ctx = session_builder.build_session();

        // Register a single "logs" table backed by QuickwitTableProvider.
        // It queries the metastore, discovers splits, and unions them
        // automatically — the user just queries "logs".
        let st2 = storage.clone();
        let sc2 = searcher_context.clone();
        let ts2 = tantivy_schema.clone();
        let logs_opener_factory: quickwit_datafusion::OpenerFactory =
            Arc::new(move |meta: &quickwit_metastore::SplitMetadata| {
                Arc::new(StorageSplitOpener::new(
                    meta.split_id.clone(),
                    ts2.clone(),
                    vec![meta.num_docs as u32],
                    sc2.clone(),
                    st2.clone(),
                    meta.footer_offsets.start,
                    meta.footer_offsets.end,
                )) as Arc<dyn IndexOpener>
            });

        let logs_provider = quickwit_datafusion::QuickwitTableProvider::new(
            index_uid.clone(),
            metastore.clone(),
            logs_opener_factory,
            &tantivy_schema,
        );
        ctx.register_table("logs", Arc::new(logs_provider))?;

        let sql = "SELECT id, price FROM logs WHERE full_text(category, 'electronics') ORDER BY id";

        println!("SQL:\n  {sql}\n");

        let df: DataFrame = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        println!("Distributed Plan:\n{}\n", display_plan_ascii(plan.as_ref(), false));
        let batches: Vec<_> = execute_stream(plan, ctx.task_ctx())?
            .try_collect().await?;
        let formatted = datafusion::common::arrow::util::pretty::pretty_format_batches(&batches)?;
        println!("Results:\n{formatted}\n");
    }

    // ═══════════════════════════════════════════════════════════════
    //  DEMO 3: SearchRequest → DataFrame (ES-compatible path)
    // ═══════════════════════════════════════════════════════════════
    section("DEMO 3: SearchRequest → DataFrame");
    {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(1),
        );
        ctx.register_udf(full_text_udf());

        // Build a SearchRequest like Quickwit's REST handler would.
        let search_request = quickwit_proto::search::SearchRequest {
            query_ast: serde_json::to_string(&serde_json::json!({
                "type": "full_text",
                "field": "category",
                "text": "books",
                "params": {
                    "mode": { "type": "phrase_fallback_to_intersection" }
                },
                "lenient": false
            }))?,
            max_hits: 10,
            sort_fields: vec![quickwit_proto::search::SortField {
                field_name: "price".to_string(),
                sort_order: quickwit_proto::search::SortOrder::Desc as i32,
                ..Default::default()
            }],
            ..Default::default()
        };

        println!("SearchRequest:");
        println!("  query: full_text(category, 'books')");
        println!("  sort: price DESC");
        println!("  max_hits: 10\n");

        // For local execution (not distributed), use in-memory openers
        // that pre-open the indexes. The StorageSplitOpener + warmup
        // path is demonstrated in Demo 1 via the distributed workers.
        let demo3_registry = Arc::new(SplitRegistry::new());
        for split in &splits {
            let split_opener = StorageSplitOpener::new(
                split.split_id.clone(),
                tantivy_schema.clone(),
                vec![split.num_docs as u32],
                searcher_context.clone(),
                storage.clone(),
                split.footer_offsets.start,
                split.footer_offsets.end,
            );
            // Pre-open and cache in the registry for local execution.
            let index = IndexOpener::open(&split_opener).await
                .expect("failed to open split for demo 3");
            demo3_registry.insert(split.split_id.clone(), index);
        }
        let reg = demo3_registry.clone();
        let ts2 = tantivy_schema.clone();
        let opener_factory_for_plan: quickwit_datafusion::OpenerFactory =
            Arc::new(move |meta: &quickwit_metastore::SplitMetadata| {
                Arc::new(quickwit_datafusion::split_opener::SplitIndexOpener::new(
                    meta.split_id.clone(),
                    reg.clone(),
                    ts2.clone(),
                    vec![meta.num_docs as u32],
                )) as Arc<dyn IndexOpener>
            });

        println!("Building search plan...");
        match build_search_plan(
            &ctx,
            &splits,
            &opener_factory_for_plan,
            &search_request,
            None,
        ) {
            Ok(df) => {
                match df.create_physical_plan().await {
                    Ok(plan) => {
                        println!("Physical Plan:\n{}\n",
                            datafusion::physical_plan::displayable(plan.as_ref()).indent(true));
                        match execute_stream(plan, ctx.task_ctx()) {
                            Ok(stream) => {
                                let batches: Vec<_> = stream.try_collect().await?;
                                let formatted = datafusion::common::arrow::util::pretty::pretty_format_batches(&batches)?;
                                println!("Results:\n{formatted}\n");
                            }
                            Err(e) => println!("Execution error: {e}\n"),
                        }
                    }
                    Err(e) => println!("Plan error: {e}\n"),
                }
            }
            Err(e) => println!("Build error: {e}\n"),
        }
    }

    println!("✅ All demos complete.");

    // Exit immediately — TestSandbox's Universe panics on drop if
    // actors are still running, which is expected for a demo binary.
    #[allow(unreachable_code)]
    {
        std::process::exit(0);
        Ok(())
    }
}
