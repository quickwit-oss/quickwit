# Phase 28: Root Split Planning Endpoint - Context

**Gathered:** 2026-01-22 (revised)
**Status:** Ready for planning

<vision>
## How This Should Work

This phase adds a **split planning endpoint** to the root search service. The endpoint takes an existing `SearchRequest` (with all its pruning capabilities) and returns the splits + node assignments — NOT the query results.

**Architecture:**

```
External Orchestrator (ClickHouse, custom client, etc.)
        │
        ├──► Step 1: Root "plan_splits" endpoint
        │         Input: SearchRequest (for time range, index, filters)
        │         Output: List of (Split, Node[]) assignments
        │
        └──► Step 2: Parallel calls to Leaf nodes
                  Input: SQL + Splits (to MetricsSqlLeafService from Phase 27)
                  Output: Arrow IPC results
                  └── Orchestrator aggregates these
```

The key insight: **Quickwit doesn't orchestrate the leaf calls** — it just provides:
1. A split planning endpoint (this phase)
2. A leaf execution endpoint (Phase 27, already done)

External orchestration decides how to parallelize, aggregate, and handle failures.

</vision>

<essential>
## What Must Be Nailed

- **Reuse existing SearchRequest** — Don't reinvent query parsing; leverage existing time range, index pattern, and filter pruning
- **Return splits + node assignments** — Each split maps to one or more nodes that can serve it
- **Same pruning logic as regular search** — Use `plan_splits_for_root_search` or equivalent
- **Simple response format** — Just splits and nodes, no Arrow IPC

</essential>

<specifics>
## Specific Ideas

### New RPC: PlanMetricsSplits

```protobuf
// Request reuses existing SearchRequest for pruning
message PlanMetricsSplitsRequest {
  SearchRequest search_request = 1;
}

// Response contains splits and their serving nodes
message PlanMetricsSplitsResponse {
  repeated SplitAssignment split_assignments = 1;
  uint64 elapsed_micros = 2;
}

message SplitAssignment {
  SplitIdAndFooterOffsets split = 1;
  string index_uri = 2;  // Storage URI for this index
  repeated string node_ids = 3;  // Nodes that can serve this split
}
```

### Implementation

```rust
impl SearchService for SearchServiceImpl {
    async fn plan_metrics_splits(
        &self,
        request: PlanMetricsSplitsRequest,
    ) -> crate::Result<PlanMetricsSplitsResponse> {
        let start = Instant::now();
        
        // Use existing split planning logic
        let (split_metadatas, indexes_meta) = plan_splits_for_root_search(
            &mut request.search_request,
            &mut self.metastore,
        ).await?;
        
        // Convert to SplitIdAndFooterOffsets + node assignments
        let assignments = self.assign_splits_to_nodes(split_metadatas, indexes_meta)?;
        
        Ok(PlanMetricsSplitsResponse {
            split_assignments: assignments,
            elapsed_micros: start.elapsed().as_micros() as u64,
        })
    }
}
```

### Node Assignment

Use existing ClusterClient logic to determine which nodes can serve each split:
- Split locality (which node has the split cached)
- Load balancing
- Availability

### REST Endpoint

```
POST /api/v1/metrics/plan-splits
Content-Type: application/json

{
  "index_id_patterns": ["otel-metrics-*"],
  "start_timestamp": 1705000000,
  "end_timestamp": 1705100000
}

Response:
{
  "split_assignments": [
    {
      "split": { "split_id": "...", "footer_offsets": {...} },
      "index_uri": "s3://bucket/indexes/otel-metrics-v1",
      "node_ids": ["node-1", "node-2"]
    },
    ...
  ],
  "elapsed_micros": 1234
}
```

</specifics>

<notes>
## Additional Context

**Depends on:** Phase 27 complete (MetricsSqlLeafService exists)

**Key existing files to leverage:**
- `quickwit/quickwit-search/src/root.rs` — `plan_splits_for_root_search`
- `quickwit/quickwit-search/src/cluster_client.rs` — Node selection logic
- `quickwit/quickwit-proto/protos/quickwit/search.proto` — SearchRequest definition

**New files to create:**
- Add to `quickwit/quickwit-proto/protos/quickwit/search.proto` — new messages
- Add to `quickwit/quickwit-search/src/service.rs` — new method
- `quickwit/quickwit-serve/src/metrics_sql_api/rest_handler.rs` — REST endpoint

**What this enables:**
- External orchestrator (ClickHouse, pandas, custom) calls plan-splits
- Orchestrator calls each node's MetricsSqlLeafService in parallel with SQL + assigned splits
- Orchestrator aggregates Arrow IPC results
- Quickwit doesn't need to understand SQL — it just provides splits

</notes>

---

*Phase: 28-rest-grpc-endpoints*
*Context revised: 2026-01-22*
