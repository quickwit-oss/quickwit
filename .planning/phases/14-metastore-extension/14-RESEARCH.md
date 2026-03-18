# Phase 14: Metastore Extension - Research

**Researched:** 2026-01-17
**Domain:** Quickwit metastore integration patterns (internal codebase)
**Confidence:** HIGH

<research_summary>
## Summary

Researched the existing Quickwit metastore architecture to understand how metrics splits should integrate. The metastore uses a pattern where metadata is JSON-serialized in protobuf messages, stored in Postgres with key fields duplicated as indexed columns, and operated on through stage/publish APIs with transactional guarantees.

Key finding: The existing `splits` table and `SplitMetadata` struct serve Tantivy splits. Metrics need a parallel system: new protobuf messages for `StageMetricsSplits`/`PublishMetricsSplits`, new methods on `MetastoreService`, and the existing `metrics_splits` table from Phase 13-02.

**Primary recommendation:** Add metrics-specific methods to `MetastoreService` trait that mirror the existing split lifecycle (stage → publish → mark for deletion) but operate on `metrics_splits` table with `MetricsSplitMetadata`.
</research_summary>

<standard_stack>
## Standard Stack

### Core Components (Already Exist)
| Component | Location | Purpose | Why Standard |
|-----------|----------|---------|--------------|
| `MetastoreService` trait | `quickwit-proto/src/metastore/` | gRPC service definition | Generated from protobuf, defines all metastore RPCs |
| `PostgresqlMetastore` | `quickwit-metastore/src/metastore/postgres/metastore.rs` | Implementation | 90KB file with all SQL operations |
| `metrics_splits` table | Migration 25 | Metrics-specific storage | Already created in Phase 13-02 |
| `PgMetricsSplit` | `quickwit-metrics-engine/src/split/postgres.rs` | Database model | Already created in Phase 13-02 |

### New Components Needed
| Component | Location | Purpose | Pattern Source |
|-----------|----------|---------|----------------|
| `StageMetricsSplitsRequest` | `quickwit-proto/protos/quickwit/metastore.proto` | gRPC request | Follow `StageSplitsRequest` |
| `PublishMetricsSplitsRequest` | `quickwit-proto/protos/quickwit/metastore.proto` | gRPC request | Follow `PublishSplitsRequest` |
| `ListMetricsSplitsRequest/Response` | `quickwit-proto/protos/quickwit/metastore.proto` | gRPC request/response | Follow `ListSplitsRequest/Response` |
| `stage_metrics_splits()` | `PostgresqlMetastore` | SQL implementation | Follow `stage_splits()` |
| `publish_metrics_splits()` | `PostgresqlMetastore` | SQL implementation | Follow `publish_splits()` |

### Libraries/Crates In Use
| Crate | Version | Purpose |
|-------|---------|---------|
| `sqlx` | workspace | Postgres async driver with compile-time query checking |
| `sea-query` | workspace | Type-safe SQL query builder (via `Iden` derive) |
| `serde_json` | workspace | JSON serialization for metadata |
| `prost` | workspace | Protobuf code generation |
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Pattern 1: JSON Metadata with Column Duplication

**What:** Store full metadata as JSON, duplicate key fields as indexed columns.

**Why:** JSON provides forward/backward compatibility; columns enable efficient pruning queries.

**Example from existing `splits` table:**
```sql
CREATE TABLE splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT,           -- Duplicated for indexing
    time_range_end BIGINT,             -- Duplicated for indexing
    tags TEXT[] NOT NULL,              -- Duplicated for GIN indexing
    split_metadata_json TEXT NOT NULL, -- Full metadata (authoritative)
    ...
);
```

**Already applied to `metrics_splits`:**
```sql
CREATE TABLE metrics_splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT NOT NULL,
    time_range_end BIGINT NOT NULL,
    metric_names TEXT[] NOT NULL,      -- GIN indexed
    tag_service TEXT[],                -- GIN indexed (low-cardinality tier)
    tag_env TEXT[],                    -- GIN indexed
    ...
    split_metadata_json TEXT NOT NULL, -- Full MetricsSplitMetadata
);
```

### Pattern 2: Protobuf with JSON Serialization

**What:** Protobuf messages carry JSON-serialized metadata, not native protobuf fields.

**Why:** Decouples protobuf schema evolution from metadata schema evolution.

**Example from `StageSplitsRequest`:**
```protobuf
message StageSplitsRequest {
  quickwit.common.IndexUid index_uid = 1;
  string split_metadata_list_serialized_json = 2;  // Vec<SplitMetadata> as JSON
}
```

**For metrics, follow same pattern:**
```protobuf
message StageMetricsSplitsRequest {
  string index_id = 1;                              // metrics use index_id, not index_uid
  string split_metadata_list_serialized_json = 2;  // Vec<MetricsSplitMetadata> as JSON
}
```

### Pattern 3: Batch Operations with UNNEST

**What:** Use PostgreSQL `UNNEST` for efficient bulk inserts.

**Why:** Single round-trip for multiple splits, atomic insert.

**Example from `stage_splits()`:**
```rust
let upserted_split_ids: Vec<String> = sqlx::query_scalar(r#"
    INSERT INTO splits (split_id, time_range_start, time_range_end, ...)
    SELECT split_id, time_range_start, time_range_end, ...
    FROM UNNEST($1, $2, $3, ...) AS staged_splits (split_id, time_range_start, ...)
    ON CONFLICT(index_uid, split_id) DO UPDATE
        SET time_range_start = excluded.time_range_start, ...
        WHERE splits.split_state = 'Staged'
    RETURNING split_id;
"#)
.bind(&split_ids)
.bind(time_range_start_list)
// ... bind all arrays
.fetch_all(tx.as_mut())
.await?;
```

### Pattern 4: CTE for Atomic Publish

**What:** Use Common Table Expressions (CTEs) for atomic multi-table updates.

**Why:** All-or-nothing semantics - either all splits publish or none do.

**Example from `publish_splits()`:**
```sql
WITH input_splits AS (
    -- Validate all splits exist with expected states
    SELECT split_id, expected_state, actual_state FROM ...
),
updated_index_metadata AS (
    -- Atomically update index metadata (checkpoint)
    UPDATE indexes SET ... WHERE NOT EXISTS (SELECT 1 FROM input_splits WHERE actual != expected)
),
updated_splits AS (
    -- Publish staged, mark replaced for deletion
    UPDATE splits SET split_state = CASE ... WHERE NOT EXISTS (...)
)
SELECT ... FROM input_splits;
```

### Pattern 5: Extension Methods on Request Types

**What:** Add helper methods to protobuf request types via extension traits.

**Why:** Keep serialization/deserialization logic close to the type.

**Example:**
```rust
// In quickwit-metastore/src/lib.rs
pub trait StageSplitsRequestExt {
    fn try_from_splits_metadata(
        index_uid: IndexUid,
        splits_metadata: Vec<SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest>;

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>>;
}

impl StageSplitsRequestExt for StageSplitsRequest {
    // Implementation...
}
```

### Recommended Project Structure

For Phase 14, add files to:
```
quickwit/
├── quickwit-proto/
│   └── protos/quickwit/
│       └── metastore.proto          # Add new messages + service RPCs
├── quickwit-metastore/
│   └── src/
│       ├── lib.rs                   # Add extension traits
│       ├── metastore/
│       │   ├── mod.rs               # Add to MetastoreServiceExt trait
│       │   └── postgres/
│       │       ├── metastore.rs     # Add stage/publish/list implementations
│       │       └── model.rs         # May need PgMetricsSplit integration
│       └── tests/                   # Add metrics split tests
└── quickwit-metrics-engine/
    └── src/split/
        └── postgres.rs              # Already has PgMetricsSplit (Phase 13-02)
```

### Anti-Patterns to Avoid

- **Sharing `splits` table:** Don't add metrics to existing splits table - schema differs significantly (metric_names, two-tier tags)
- **Native protobuf metadata fields:** Don't define `MetricsSplitMetadata` in protobuf - use JSON serialization for flexibility
- **Index-level coupling:** Metrics use `index_id` (string), not `IndexUid` (composite) - keep them separate
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SQL query building | String concatenation | `sqlx` with raw SQL or `sea-query` | SQL injection, type safety |
| Protobuf codegen | Manual message types | `prost` + `tonic` from .proto files | Consistency, gRPC integration |
| JSON serialization | Custom parsing | `serde_json` with derive macros | Correctness, maintainability |
| Transaction handling | Manual BEGIN/COMMIT | `run_with_tx!` macro | Rollback on error, connection pooling |
| Batch inserts | Loop with individual INSERTs | UNNEST pattern | Performance (single round-trip) |

**Key insight:** The existing metastore code has battle-tested patterns for all these operations. Follow `stage_splits()` / `publish_splits()` implementations exactly - they handle edge cases like partial failures, state validation, and atomic updates.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: State Transition Violations
**What goes wrong:** Trying to publish a split that isn't staged, or re-staging a published split.
**Why it happens:** Not checking current state before transition.
**How to avoid:** Follow the CTE pattern - validate all splits have expected state before updating any.
**Warning signs:** `MetastoreError::FailedPrecondition` with "splits are not staged".

### Pitfall 2: Partial Batch Failures
**What goes wrong:** Some splits in a batch succeed, others fail, leaving inconsistent state.
**Why it happens:** Not using transactions or not checking return counts.
**How to avoid:**
1. Use `run_with_tx!` macro for transactional operations
2. Compare `upserted_split_ids.len()` vs `split_ids.len()`
3. Return error if counts don't match
**Warning signs:** Missing splits after stage/publish, orphaned files in storage.

### Pitfall 3: Index Foreign Key Mismatch
**What goes wrong:** `INSERT` fails with foreign key violation.
**Why it happens:** Metrics splits reference `index_id` in `indexes` table, but index doesn't exist.
**How to avoid:** Ensure metrics index is created in `indexes` table before staging splits.
**Warning signs:** PostgreSQL error `violates foreign key constraint "metrics_splits_index_id_fkey"`.

### Pitfall 4: JSON Deserialization Version Skew
**What goes wrong:** Old metadata JSON fails to deserialize with new struct fields.
**Why it happens:** Adding required fields to `MetricsSplitMetadata` without defaults.
**How to avoid:** Use `#[serde(default)]` for new optional fields. See `SplitMetadataV0_8` versioning pattern.
**Warning signs:** Deserialization errors on older splits after code update.

### Pitfall 5: Missing GIN Index Usage
**What goes wrong:** Slow queries despite GIN indexes on tag arrays.
**Why it happens:** Using wrong SQL operator - `@>` (contains) vs `= ANY()` (element match).
**How to avoid:**
- Use `'value' = ANY(array_column)` for "does array contain value"
- Use `array_column @> ARRAY['value']` for "does array contain all values"
**Warning signs:** Sequential scans in EXPLAIN output instead of index scans.

### Pitfall 6: Timestamp Timezone Issues
**What goes wrong:** Time range queries miss splits due to timezone mismatch.
**Why it happens:** Mixing UTC timestamps with local time or vice versa.
**How to avoid:** Store all timestamps as UTC bigints (Unix seconds), use `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'` in SQL.
**Warning signs:** Off-by-hours errors in time range queries.
</common_pitfalls>

<code_examples>
## Code Examples

### Stage Metrics Splits (Proposed Pattern)
```rust
// Source: Follow existing stage_splits() in postgres/metastore.rs:585-687
async fn stage_metrics_splits(
    &self,
    request: StageMetricsSplitsRequest,
) -> MetastoreResult<EmptyResponse> {
    let splits_metadata: Vec<MetricsSplitMetadata> =
        serde_json::from_str(&request.split_metadata_list_serialized_json)?;

    if splits_metadata.is_empty() {
        return Ok(Default::default());
    }

    // Prepare batch arrays for UNNEST
    let mut split_ids = Vec::with_capacity(splits_metadata.len());
    let mut time_range_starts = Vec::with_capacity(splits_metadata.len());
    let mut time_range_ends = Vec::with_capacity(splits_metadata.len());
    let mut metric_names_list = Vec::with_capacity(splits_metadata.len());
    // ... more arrays for each column

    for metadata in &splits_metadata {
        let insertable = InsertableMetricsSplit::from_metadata(
            metadata,
            MetricsSplitState::Staged
        )?;
        split_ids.push(insertable.split_id);
        time_range_starts.push(insertable.time_range_start);
        time_range_ends.push(insertable.time_range_end);
        metric_names_list.push(insertable.metric_names);
        // ... extract all fields
    }

    run_with_tx!(self.connection_pool, tx, "stage metrics splits", {
        let upserted: Vec<String> = sqlx::query_scalar(r#"
            INSERT INTO metrics_splits
                (split_id, split_state, index_id, time_range_start, time_range_end,
                 metric_names, tag_service, tag_env, ..., split_metadata_json)
            SELECT split_id, $N as split_state, $M as index_id, ...
            FROM UNNEST($1, $2, $3, ...)
                AS staged(split_id, time_range_start, time_range_end, ...)
            ON CONFLICT(split_id) DO UPDATE
                SET split_state = excluded.split_state, ...
                WHERE metrics_splits.split_state = 'Staged'
            RETURNING split_id;
        "#)
        .bind(&split_ids)
        .bind(time_range_starts)
        // ... bind all arrays
        .fetch_all(tx.as_mut())
        .await?;

        if upserted.len() != split_ids.len() {
            let failed: Vec<_> = split_ids.into_iter()
                .filter(|id| !upserted.contains(id))
                .collect();
            return Err(MetastoreError::FailedPrecondition {
                entity: EntityKind::Splits { split_ids: failed },
                message: "metrics splits are not staged".into(),
            });
        }
        Ok(EmptyResponse {})
    })
}
```

### List Metrics Splits for Pruning
```rust
// Source: Query pattern for Tier 1 pruning
async fn list_metrics_splits(
    &self,
    request: ListMetricsSplitsRequest,
) -> MetastoreResult<ListMetricsSplitsResponse> {
    // Build dynamic WHERE clause based on request filters
    let mut query = String::from(r#"
        SELECT split_id, split_state, index_id, time_range_start, time_range_end,
               metric_names, tag_service, tag_env, tag_datacenter, tag_region, tag_host,
               high_cardinality_tag_keys, num_rows, size_bytes, split_metadata_json
        FROM metrics_splits
        WHERE split_state = 'Published'
          AND index_id = $1
    "#);

    let mut param_idx = 2;

    // Time range pruning (always applied)
    if let Some(time_range) = &request.time_range {
        query.push_str(&format!(
            " AND time_range_start <= ${} AND time_range_end >= ${}",
            param_idx, param_idx + 1
        ));
        param_idx += 2;
    }

    // Metric name pruning (100% of queries)
    if !request.metric_names.is_empty() {
        query.push_str(&format!(
            " AND metric_names && ${}", // && is array overlap operator
            param_idx
        ));
        param_idx += 1;
    }

    // Tag pruning (GIN index usage)
    if let Some(service) = &request.tag_service {
        query.push_str(&format!(
            " AND ${} = ANY(tag_service)",
            param_idx
        ));
        param_idx += 1;
    }

    // Execute and convert to response
    // ...
}
```

### Protobuf Message Definitions
```protobuf
// Source: Follow pattern from existing StageSplitsRequest/PublishSplitsRequest

// Stage metrics splits - first step after MetricsSplitWriter creates Parquet
message StageMetricsSplitsRequest {
  string index_id = 1;  // Note: index_id not IndexUid (metrics are simpler)
  string split_metadata_list_serialized_json = 2;
}

// Publish metrics splits - make staged splits queryable
message PublishMetricsSplitsRequest {
  string index_id = 1;
  repeated string staged_split_ids = 2;
  repeated string replaced_split_ids = 3;  // For merge operations
}

// List metrics splits - for query planning and pruning
message ListMetricsSplitsRequest {
  string index_id = 1;
  optional int64 time_range_start = 2;
  optional int64 time_range_end = 3;
  repeated string metric_names = 4;
  optional string tag_service = 5;
  optional string tag_env = 6;
  // ... other filters
  string split_state = 10;  // Usually "Published"
}

message ListMetricsSplitsResponse {
  string splits_serialized_json = 1;  // Vec<MetricsSplitMetadata> as JSON
}

// Mark for deletion - cleanup flow
message MarkMetricsSplitsForDeletionRequest {
  string index_id = 1;
  repeated string split_ids = 2;
}

// Delete - after storage cleanup complete
message DeleteMetricsSplitsRequest {
  string index_id = 1;
  repeated string split_ids = 2;
}
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `IndexUid` for all splits | `index_id` string for metrics | Phase 13 design | Simpler metrics split identity |
| Generic `tags` array | Two-tier tag storage | Phase 12 analysis | Better pruning performance |
| Single `splits` table | Separate `metrics_splits` | Phase 13-02 | Optimized schema per workload |

**New patterns being established:**
- GIN indexes for array containment queries (Phase 13-02)
- Low-cardinality tag columns with high-cardinality keys stored separately
- Parquet bloom filters for Tier 2 pruning (complementary to Postgres)

**Patterns from existing code that remain valid:**
- JSON serialization for metadata flexibility
- UNNEST for batch operations
- CTE for atomic multi-statement transactions
- Extension traits for request/response helpers
</sota_updates>

<open_questions>
## Open Questions

1. **Checkpoint Integration**
   - What we know: Tantivy splits use `IndexCheckpointDelta` for exactly-once ingestion
   - What's unclear: Do metrics need checkpointing? Metrics may be more tolerant of duplicates
   - Recommendation: Start without checkpoint for MVP, add if needed for exactly-once

2. **Index Registration**
   - What we know: Metrics splits reference `indexes.index_id` via foreign key
   - What's unclear: Should metrics indices be created via existing `CreateIndexRequest` or new API?
   - Recommendation: Use existing index creation - metrics index just needs an entry in `indexes` table

3. **Delete Task Integration**
   - What we know: Tantivy has delete tasks with `delete_opstamp` tracking
   - What's unclear: Do metrics need delete task support initially?
   - Recommendation: Skip for MVP (Phase 14-16), add later if needed for compliance
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- `quickwit-metastore/src/metastore/postgres/metastore.rs` - stage_splits (lines 585-687), publish_splits (lines 690-850)
- `quickwit-proto/protos/quickwit/metastore.proto` - service definition, message types
- `quickwit-metastore/migrations/postgresql/2_create-splits.up.sql` - existing splits schema
- `quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql` - metrics schema (Phase 13-02)

### Secondary (MEDIUM confidence)
- `quickwit-metastore/src/split_metadata.rs` - SplitMetadata struct and state machine
- `quickwit-indexing/src/actors/uploader.rs` - how indexing pipeline calls metastore
- `quickwit-metrics-engine/src/split/postgres.rs` - PgMetricsSplit model (Phase 13-02)

### Internal Context
- Phase 12 analysis: Two-tier pruning strategy decision
- Phase 13-01/13-02: MetricsSplitMetadata struct and Postgres model
- Phase 14 CONTEXT.md: User requirements for invisible integration
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Quickwit metastore (internal)
- Ecosystem: PostgreSQL, protobuf/gRPC, sqlx
- Patterns: stage/publish lifecycle, batch operations, transactional updates
- Pitfalls: state transitions, partial failures, timezone handling

**Confidence breakdown:**
- Standard stack: HIGH - internal codebase, patterns well-documented
- Architecture: HIGH - following existing implementations
- Pitfalls: HIGH - derived from existing error handling code
- Code examples: HIGH - adapted from working implementations

**Research date:** 2026-01-17
**Valid until:** N/A (internal patterns, stable)
</metadata>

---

*Phase: 14-metastore-extension*
*Research completed: 2026-01-17*
*Ready for planning: yes*
