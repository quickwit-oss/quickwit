# TODO: Distributed DataFusion Execution

## Plan Quality

### Unnecessary RepartitionExec in join plans
The distributed plans show `RepartitionExec: Hash([_doc_id, _segment_ord], 3)` between every DataSource and its HashJoin. This is wrong — inv, f, and d within a single split are already co-partitioned by segment (they declare `Hash([_doc_id, _segment_ord], N)` partitioning). The optimizer shouldn't add shuffles for co-partitioned joins.

Likely cause: `target_partitions` on the distributed context doesn't match the segment count, so DF thinks it needs to repartition. With `target_partitions=1` locally the plan uses `CollectLeft` mode and no repartitions. Need to either:
- Set `target_partitions` = segment count on the distributed context
- Or teach the distributed planner to respect co-partitioned DataSource declarations

### Explicit UNION ALL vs plan-level decomposition
Currently each split's join plan is written as explicit SQL `UNION ALL`. This means the planner sees N copies of the same join pattern. A better approach:
- `QuickwitTableProvider` registers a single logical table backed by all splits
- The planner produces one join plan
- The distributed optimizer decomposes it by split at the physical level
- Each split's join subtree becomes a task assigned to a worker

This would let df-distributed's `DistributedPhysicalOptimizerRule` handle the split-to-worker mapping natively instead of the SQL manually encoding it.

### CollectLeft vs Partitioned join mode
With `target_partitions=1` the local plan uses `CollectLeft` (broadcast build side) which is correct for the inv⋈f pattern — the inverted index result is small. The distributed plan uses `Partitioned` mode because the context has higher target_partitions. Need to control this so the join strategy matches the data characteristics.

## Worker/Split/Segment Mapping

### How many workers per split? Per segment?
Currently: 1 worker per split. Each split may have multiple segments. Segments within a split are co-partitioned and joined locally on the worker.

Open questions:
- Should a split with many segments be split across multiple workers? (probably not — segments within a split share the same index and need co-partitioned joins)
- Should multiple small splits be assigned to the same worker? (yes — df-distributed's task assignment should batch them)
- How does the coordinator know how many workers are available? (WorkerResolver::get_urls() — need to implement for Quickwit cluster)

### Worker discovery
`start_localhost_context` hardcodes localhost workers. Production needs:
- `WorkerResolver` backed by Quickwit's cluster membership (Chitchat)
- Workers = Quickwit searcher nodes
- Split-to-worker affinity based on cache locality

## Metastore Integration

### QuickwitTableProvider doesn't query the metastore
Currently takes a hand-built `Vec<Arc<SplitIndexOpener>>`. Production path:
- `QuickwitTableProvider::new(metastore, index_id)`
- At `scan()` time, call `metastore.list_splits()` to discover splits
- Create `SplitIndexOpener` per split from split metadata
- Build per-split tantivy-df providers and compose the plan

### SplitIndexOpener doesn't open real splits
Currently backed by an in-memory `DashMap<String, Index>`. Production path:
- `open()` calls `open_index_with_caches()` from quickwit-search
- Downloads split bundle from object storage (S3/GCS/Azure)
- Opens tantivy index from the local cache or downloaded bundle
- Returns the opened `Index`

## Codec Gaps

### Pushed filters lost across serialization
`FastFieldDataSource` claims `PushedDown::Yes` for all filters, so DF removes the `FilterExec`. But the codec doesn't serialize pushed filters (they're `Arc<dyn PhysicalExpr>`, not trivially serializable). On the worker, the reconstructed DataSource has no filters.

Options:
- Serialize pushed filters via DF's PhysicalExpr proto support
- Change tantivy-df to return `PushedDown::No` so DF keeps FilterExec in the plan (it serializes fine as a built-in node)
- Encode the filter expressions as Expr in the proto and pass to scan()

### Aggregation pushdown not tested in distributed
tantivy-df has `AggPushdown` that replaces `AggregateExec` with `TantivyAggregateExec`. This is a custom ExecutionPlan node that the codec doesn't handle yet. Need to:
- Add `TantivyAggregateExec` encoding to `TantivyCodec`
- Or make it a DataSource variant the existing codec pattern handles
- Test partial aggregation on workers + final merge on coordinator

## Optimizer Rules in Distributed Context

### tantivy-df optimizer rules not registered on workers
`FastFieldFilterPushdown`, `TopKPushdown`, `AggPushdown`, `OrdinalGroupByOptimization` are registered on the coordinator's session but not on workers. Workers rebuild the plan from the codec, so they'd need these rules if the plan is further optimized on the worker side.

Currently this doesn't matter because the plan is fully optimized on the coordinator before distribution. But if df-distributed ever does worker-side re-optimization, the rules need to be registered in `build_worker_session_builder`.

## Testing

### Multi-segment splits
All current tests use single-segment splits (one commit). Need tests with multi-segment splits to verify:
- Segment-level co-partitioning in joins (N partitions per split)
- Correct partition mapping across inv/f/d providers
- Chunking behavior with target_partitions > segment count

### Real storage backend
All tests use `Index::create_in_ram`. Need integration tests with:
- Split bundles on local filesystem
- `open_index_with_caches()` in the opener
- Split download + cache warming
