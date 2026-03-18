# Phase 29: gRPC Arrow IPC Extension - Context

**Gathered:** 2026-01-26 (revised)
**Status:** Ready for planning

<domain>
## Phase Boundary

Extend existing `MetricsSqlLeafService` gRPC service to optionally return Arrow IPC format in responses. This enables ClickHouse (via custom IStorage) and other Arrow-compatible clients to consume Quickwit metrics data through the existing gRPC infrastructure.

**Key scope change:** This phase does NOT implement Arrow Flight protocol. Instead, it adds Arrow IPC as an optional response format to the existing gRPC service.

</domain>

<decisions>
## Implementation Decisions

### Service Extension Approach
- Extend existing `MetricsSqlLeafService` (Phase 27) - do NOT create a new service
- Add optional `return_arrow: bool` field to `MetricsSqlLeafRequest`
- Add optional `arrow_ipc: bytes` field to `MetricsSqlLeafResponse`
- Backward compatible: existing clients that don't set the flag continue to work unchanged

### Request/Response Protocol
- Client sets `return_arrow: true` in request to receive Arrow IPC bytes
- When flag is true, service includes Arrow IPC serialized batches in response
- Arrow IPC field is populated alongside existing response fields (num_rows, elapsed_micros, failed_splits)
- Use existing Arrow IPC serialization from Phase 27 (`batches_to_ipc()`)

### Scope Limitation
- **Leaf service only** - Do NOT extend root service in this phase
- Root service extension can come later if needed
- Focus is on enabling ClickHouse IStorage to call leaf nodes directly

### ClickHouse Integration
- Phase 29 ensures gRPC service works correctly with Arrow IPC
- ClickHouse C++ IStorage implementation is separate (future work or external)
- No C++ example code in this phase - just ensure the gRPC interface is correct

### Authentication & Security
- Use existing Quickwit auth mechanisms (no changes)
- Use existing error handling patterns (no changes)
- Follow existing retry semantics (no changes)

### Testing Requirements
- Integration tests required (not just unit tests)
- Create end-to-end test with gRPC client calling leaf service with `return_arrow: true`
- Verify Arrow IPC bytes deserialize correctly to RecordBatches
- Test backward compatibility (requests without the flag still work)

### Claude's Discretion
- Exact test structure and mocking approach
- Whether to add helper functions for Arrow IPC validation
- Documentation format and examples

</decisions>

<specifics>
## Specific Ideas

### Protobuf Changes

Add to `quickwit-proto/protos/quickwit/metrics_sql.proto`:

```protobuf
message MetricsSqlLeafRequest {
  string sql = 1;
  repeated SplitIdAndFooterOffsets splits = 2;
  string index_uid = 3;
  string index_uri = 4;
  bool return_arrow = 5;  // NEW: Request Arrow IPC format
}

message MetricsSqlLeafResponse {
  bytes arrow_ipc = 1;     // Existing field
  uint64 num_rows = 2;
  uint64 elapsed_micros = 3;
  repeated MetricsSqlLeafError failed_splits = 4;
  // Arrow IPC is populated when return_arrow=true in request
}
```

### Service Implementation Pattern

```rust
// In MetricsSqlLeafService::execute()
pub async fn execute(
    &self,
    request: MetricsSqlLeafRequest,
) -> Result<MetricsSqlLeafResponse, MetricsSqlError> {
    // Execute SQL on splits (existing logic)
    let batches = self.execute_on_splits(...).await?;

    // Serialize to Arrow IPC if requested
    let arrow_ipc = if request.return_arrow {
        batches_to_ipc(&batches)?
    } else {
        Bytes::new()  // Empty bytes for backward compat
    };

    Ok(MetricsSqlLeafResponse {
        arrow_ipc,
        num_rows: count_rows(&batches),
        elapsed_micros: elapsed.as_micros() as u64,
        failed_splits: vec![],
    })
}
```

### Integration Test Example

```rust
#[tokio::test]
async fn test_leaf_service_arrow_ipc() {
    // Setup: Create test splits, start gRPC server
    let client = MetricsSqlClient::connect("http://localhost:9100").await?;

    // Request with Arrow flag
    let request = MetricsSqlLeafRequest {
        sql: "SELECT metric_name, AVG(value) FROM metrics GROUP BY metric_name".to_string(),
        splits: test_splits,
        index_uid: "test-index".to_string(),
        index_uri: "s3://test/".to_string(),
        return_arrow: true,  // Request Arrow IPC
    };

    let response = client.execute(request).await?;

    // Verify Arrow IPC bytes are present and valid
    assert!(!response.arrow_ipc.is_empty());

    // Deserialize Arrow IPC to RecordBatches
    let reader = StreamReader::try_new(&response.arrow_ipc[..])?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    assert_eq!(batches.len(), 1);
    assert_eq!(response.num_rows, batches[0].num_rows() as u64);
}
```

</specifics>

<deferred>
## Deferred Ideas

- **Arrow Flight protocol** - Decided against implementing Flight in this phase. Using gRPC extension instead.
- **Root service Arrow support** - Leaf only for now. Root extension can come later if needed.
- **ClickHouse C++ IStorage code** - Separate from this phase. This phase just ensures the gRPC interface works.
- **Streaming large results** - Current approach buffers in memory. True streaming would require Flight or chunked responses (future enhancement).

</deferred>

---

*Phase: 29-arrow-flight-service*
*Context revised: 2026-01-26*
*Major scope change: Arrow Flight → gRPC Arrow IPC extension*
