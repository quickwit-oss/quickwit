# Phase 12: Metadata Analysis

**Created:** 2026-01-17
**Purpose:** Document metrics split metadata requirements for Phase 13 (Metadata Schema Design)

---

## Existing Pruning Infrastructure

This section documents the current Quickwit pruning infrastructure that serves as the foundation for metrics-specific pruning.

### SplitMetadata Structure

**Location:** `quickwit-metastore/src/split_metadata.rs`

The existing `SplitMetadata` struct provides the immutable metadata for each split:

| Field | Type | Purpose | Prunable |
|-------|------|---------|----------|
| `split_id` | `SplitId` | Unique identifier | No |
| `index_uid` | `IndexUid` | Index ownership | Yes |
| `partition_id` | `u64` | Tenant isolation | Yes |
| `source_id` | `SourceId` | Source tracking | No |
| `node_id` | `String` | Node tracking | No |
| `time_range` | `Option<RangeInclusive<i64>>` | Min/max timestamp (seconds) | **Yes** |
| `num_docs` | `usize` | Document count | No (planning) |
| `uncompressed_docs_size_in_bytes` | `u64` | Original size | No (planning) |
| `tags` | `BTreeSet<String>` | Tag set (`field:value` format) | **Yes** |
| `create_timestamp` | `i64` | Creation time | Yes |
| `delete_opstamp` | `u64` | Delete operation tracking | Yes |
| `footer_offsets` | `Range<u64>` | Parquet footer location | No |
| `maturity` | `SplitMaturity` | Merge eligibility | Yes |
| `num_merge_ops` | `usize` | Merge history | No |

**Key observations:**
- `time_range` enables temporal pruning via overlap detection
- `tags` uses a flat `BTreeSet<String>` with format `field:value`
- Presence indicator: `field!` means "field exists in split"
- Cardinality limit: Tags only stored when field cardinality <= MAX_VALUES_PER_TAG_FIELD

### ListSplitsQuery Pruning

**Location:** `quickwit-metastore/src/metastore/mod.rs` (line 633)

The `ListSplitsQuery` struct enables flexible pruning at query time:

```rust
pub struct ListSplitsQuery {
    pub index_uids: Option<Vec<IndexUid>>,      // Filter by index
    pub node_id: Option<NodeId>,                 // Filter by node
    pub split_states: Vec<SplitState>,           // Staged/Published/MarkedForDeletion
    pub tags: Option<TagFilterAst>,              // AST-based tag filtering
    pub time_range: FilterRange<i64>,            // Time overlap pruning
    pub max_time_range_end: Option<i64>,         // Upper bound constraint
    pub delete_opstamp: FilterRange<u64>,        // Delete tracking
    pub update_timestamp: FilterRange<i64>,      // Update time filtering
    pub create_timestamp: FilterRange<i64>,      // Creation time filtering
    pub mature: Bound<OffsetDateTime>,           // Maturity filtering
    pub sort_by: SortBy,                         // Result ordering
    pub limit: Option<usize>,                    // Pagination
    pub offset: Option<usize>,                   // Pagination
}
```

**Time Range Pruning:**
- Uses `FilterRange<T>` with flexible bounds (`Bound::Unbounded`, `Bound::Included(v)`, `Bound::Excluded(v)`)
- `overlaps_with()` method checks if query range overlaps split's time range
- Eliminates splits entirely outside the query's temporal window

**Tag Filtering:**
- Uses `TagFilterAst` for composable AND/OR expressions
- Supports both presence (`field!`) and value match (`field:value`)
- Evaluated in-memory after Postgres filtering

### Tag Filtering SQL Generation

**Location:** `quickwit-metastore/src/metastore/postgres/tags.rs`

The tag filtering system generates safe SQL for Postgres array matching:

```rust
pub(super) fn generate_sql_condition(tag_ast: &TagFilterAst) -> Cond {
    match tag_ast {
        TagFilterAst::And(child_asts) => {
            child_asts.iter()
                .map(generate_sql_condition)
                .fold(Cond::all(), |cond, child_cond| cond.add(child_cond))
        }
        TagFilterAst::Or(child_asts) => {
            child_asts.iter()
                .map(generate_sql_condition)
                .fold(Cond::any(), |cond, child_cond| cond.add(child_cond))
        }
        TagFilterAst::Tag { tag, is_present } => {
            let dollar_guard = generate_dollar_guard(tag);
            let expr_str = format!("${dollar_guard}${tag}${dollar_guard}$ = ANY(tags)");
            // ...
        }
    }
}
```

**Key SQL patterns:**
- Format: `$$tag_value$$ = ANY(tags)` for array containment check
- Dollar-quoting prevents SQL injection (guard generation for strings containing `$`)
- AND/OR composition via `sea_query::Cond`
- Negation supported: `NOT ($$tag:value$$ = ANY(tags))`

**Example SQL:**
```sql
-- Single tag filter
WHERE $$service:web$$ = ANY(tags)

-- AND composition
WHERE $$service:web$$ = ANY(tags)
  AND $$env:prod$$ = ANY(tags)

-- OR composition
WHERE ($$service:web$$ = ANY(tags)
   OR $$service:api$$ = ANY(tags))
```

### Current PostgreSQL Schema

**Location:** `quickwit-metastore/migrations/postgresql/2_create-splits.up.sql`

The existing `splits` table schema:

```sql
CREATE TABLE IF NOT EXISTS splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT,            -- Indexed for range queries
    time_range_end BIGINT,              -- Indexed for range queries
    tags TEXT[] NOT NULL,               -- Array for tag filtering
    split_metadata_json TEXT NOT NULL,  -- Full metadata as JSON
    index_id VARCHAR(50) NOT NULL,
    create_timestamp TIMESTAMP NOT NULL,
    update_timestamp TIMESTAMP NOT NULL
);

-- Indexes for efficient pruning
CREATE INDEX idx_splits_time ON splits (time_range_start, time_range_end);
CREATE INDEX idx_splits_tags ON splits USING GIN (tags);
```

**Observations:**
- Time range stored as separate columns (not JSON) for indexed queries
- Tags stored as `TEXT[]` with GIN index for efficient `= ANY()` queries
- Full metadata serialized as JSON for flexibility

---

## Metrics Query Pruning Requirements

This section documents what metrics queries need for efficient pruning.

### Query Patterns

Typical metrics query patterns that drive pruning requirements:

1. **Time Range (Always Present)**
   - "Metrics from last hour"
   - "Between T1 and T2"
   - Every metrics query filters by time, making this the highest priority pruning dimension

2. **Metric Name**
   - "cpu.usage", "memory.used", "disk.io.read"
   - Queries target specific metrics, rarely wildcards
   - Pruning by metric name eliminates splits that don't contain requested metrics

3. **Tag Filtering**
   - Service-level: `service=web`, `service=api`
   - Environment: `env=prod`, `env=staging`
   - Infrastructure: `host=server-001`, `datacenter=us-east-1`
   - Arbitrary attributes: Custom tags from VARIANT field

### Pruning Dimensions

| Dimension | Query Frequency | Cardinality | Pruning Level |
|-----------|-----------------|-------------|---------------|
| `time_range` | 100% | N/A | Postgres (indexed BIGINT) |
| `metric_name` | 100% | 100-1000 per split | Postgres (TEXT[] + GIN) |
| `tag_service` | 80% | Low (<100) | Postgres (TEXT[] + GIN) |
| `tag_env` | 50% | Very low (<10) | Postgres (TEXT[] + GIN) |
| `tag_datacenter` | 50% | Very low (<20) | Postgres (TEXT[] + GIN) |
| `tag_region` | 40% | Low (<50) | Postgres (TEXT[] + GIN) |
| `tag_host` | 40% | High (1000+) | Parquet bloom filter |
| `attributes` | 20% | Variable (potentially high) | Parquet bloom filter |

### Cardinality Threshold Decision

**Threshold: 1000 unique values per tag key**

The 1000 threshold determines whether tag values go in Postgres or Parquet bloom filters:

| Cardinality | Storage | Rationale |
|-------------|---------|-----------|
| < 1000 | Postgres TEXT[] | GIN index efficient, exact match queries fast |
| >= 1000 | Parquet bloom filter | Probabilistic filtering avoids metadata explosion |

**Why 1000?**
- Postgres arrays with GIN indexes perform well up to ~1000 elements
- Beyond 1000, array storage and query time degrade significantly
- Bloom filters provide O(1) lookup regardless of cardinality
- False positive rate of 1% (FPP=0.01, configured in Phase 11) acceptable for coarse pruning

**Example cardinality distribution:**

| Tag Key | Typical Cardinality | Storage |
|---------|---------------------|---------|
| `env` | 3-10 | Postgres |
| `service` | 10-100 | Postgres |
| `datacenter` | 5-20 | Postgres |
| `region` | 10-50 | Postgres |
| `host` | 100-10,000+ | Parquet bloom filter |
| `pod` | 1,000-100,000+ | Parquet bloom filter |
| `container_id` | 10,000+ | Parquet bloom filter |
| `trace_id` | Unbounded | Parquet bloom filter |

### Why Metric Names Are Special

Metric names deserve dedicated handling separate from tags:

1. **Always queried** - 100% of metrics queries filter by metric name
2. **Moderate cardinality** - Typically 100-1000 distinct metrics per split
3. **Well-known format** - Dotted notation (e.g., `cpu.usage`, `system.memory.used`)
4. **Efficient exact match** - Users query specific metrics, not patterns

Storing metric names in a dedicated `metric_names TEXT[]` column with GIN index enables:
- Fast `= ANY(metric_names)` queries
- Separate cardinality tracking from tags
- Clear semantic meaning in schema

---

## Two-Tier Pruning Strategy

This section documents the hybrid Postgres + Parquet pruning architecture.

### Tier 1: Postgres Metastore (Coarse Filtering)

**Purpose:** Eliminate 90%+ of splits before touching object storage.

**New `metrics_splits` Table Schema:**

```sql
CREATE TABLE IF NOT EXISTS metrics_splits (
    -- Identity
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    index_id VARCHAR(50) NOT NULL,

    -- Temporal pruning (always used)
    time_range_start BIGINT NOT NULL,
    time_range_end BIGINT NOT NULL,

    -- Metric name pruning (always used)
    metric_names TEXT[] NOT NULL,

    -- Low-cardinality tag pruning (<1000 unique values)
    tag_service TEXT[],
    tag_env TEXT[],
    tag_datacenter TEXT[],
    tag_region TEXT[],
    tag_host TEXT[],

    -- Planning metadata
    num_rows BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,

    -- Full metadata for flexibility
    split_metadata_json TEXT NOT NULL,

    -- Timestamps
    create_timestamp TIMESTAMP NOT NULL,
    update_timestamp TIMESTAMP NOT NULL
);

-- Indexes optimized for metrics queries
CREATE INDEX idx_metrics_splits_time ON metrics_splits (time_range_start, time_range_end);
CREATE INDEX idx_metrics_splits_metric_names ON metrics_splits USING GIN (metric_names);
CREATE INDEX idx_metrics_splits_tag_service ON metrics_splits USING GIN (tag_service);
CREATE INDEX idx_metrics_splits_tag_env ON metrics_splits USING GIN (tag_env);
CREATE INDEX idx_metrics_splits_tag_datacenter ON metrics_splits USING GIN (tag_datacenter);
CREATE INDEX idx_metrics_splits_tag_region ON metrics_splits USING GIN (tag_region);
CREATE INDEX idx_metrics_splits_tag_host ON metrics_splits USING GIN (tag_host);
```

**Query Pattern:**

```sql
SELECT * FROM metrics_splits
WHERE time_range_start <= :query_end
  AND time_range_end >= :query_start
  AND $$cpu.usage$$ = ANY(metric_names)
  AND $$web$$ = ANY(tag_service)
  AND $$prod$$ = ANY(tag_env);
```

**Expected outcome:** Query returns 10-100 splits from 10,000+ total (99% reduction).

### Tier 2: Parquet (Fine-Grained Filtering)

**Purpose:** Further reduce I/O within selected splits using embedded statistics.

**Bloom Filters (configured in Phase 11):**
- **Columns:** `tag_service`, `tag_env`, `tag_datacenter`, `tag_region`, `tag_host`, `attributes`
- **False positive probability:** 0.01 (1%)
- **NDV estimates:** Per-column based on expected cardinality
- **Query pattern:** Check bloom filter before reading row groups

**Row Group Statistics:**
- **timestamp column:** min/max for temporal pruning within split
- **Query pattern:** Skip row groups where timestamp range doesn't overlap query

**Expected outcome:** Read 10-50% of bytes within selected splits.

### Why Separate Table (metrics_splits)

**Decision:** Create a new `metrics_splits` table instead of extending the existing `splits` table.

**Rationale:**

1. **Different pruning needs**
   - Logs: Full-text search, field existence, boolean filters
   - Metrics: Metric names, numeric tags, time-series patterns
   - Schema optimizations differ significantly

2. **Independent schema evolution**
   - Add new tag columns without affecting log ingestion
   - Change cardinality thresholds independently
   - No backward compatibility constraints from logs

3. **Optimized indexes**
   - Logs: Generic `tags TEXT[]` with combined tag filtering
   - Metrics: Separate columns per common tag key with dedicated GIN indexes
   - Indexes match actual query patterns

4. **Clean separation of concerns**
   - Clear ownership: metrics engine owns `metrics_splits`
   - No coupling between log and metrics pipelines
   - Independent testing and optimization

### MetricsSplitMetadata Schema (For Phase 13)

**Target Rust struct:**

```rust
/// Metadata for a metrics split.
pub struct MetricsSplitMetadata {
    /// Unique split identifier.
    pub split_id: SplitId,

    /// Time range covered by this split (seconds since epoch).
    pub time_range: TimeRange,

    /// Number of data points in this split.
    pub num_rows: u64,

    /// Size of Parquet file(s) in bytes.
    pub size_bytes: u64,

    /// Distinct metric names in this split (for Postgres pruning).
    pub metric_names: HashSet<String>,

    /// Low-cardinality tag values by key (for Postgres pruning).
    /// Format: HashMap<tag_key, HashSet<tag_value>>
    /// Example: {"service": {"web", "api"}, "env": {"prod", "staging"}}
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,

    /// High-cardinality tag keys (for Parquet bloom filter).
    /// Only keys are stored; values are in Parquet bloom filters.
    pub high_cardinality_tag_keys: HashSet<String>,

    /// Parquet file path(s) relative to storage root.
    pub parquet_files: Vec<String>,
}
```

**Conversion to `metrics_splits` row:**

| Struct Field | Postgres Column | Transformation |
|--------------|-----------------|----------------|
| `split_id` | `split_id` | Direct |
| `time_range.start_secs` | `time_range_start` | Direct |
| `time_range.end_secs` | `time_range_end` | Direct |
| `metric_names` | `metric_names` | HashSet to TEXT[] |
| `low_cardinality_tags["service"]` | `tag_service` | HashSet to TEXT[] |
| `low_cardinality_tags["env"]` | `tag_env` | HashSet to TEXT[] |
| `low_cardinality_tags["datacenter"]` | `tag_datacenter` | HashSet to TEXT[] |
| `low_cardinality_tags["region"]` | `tag_region` | HashSet to TEXT[] |
| `low_cardinality_tags["host"]` | `tag_host` | HashSet to TEXT[] |
| `num_rows` | `num_rows` | Direct |
| `size_bytes` | `size_bytes` | Direct |
| Full struct | `split_metadata_json` | Serde JSON |

### Success Criteria for Phase 13

Phase 13 (Metadata Schema Design) must deliver:

1. **Finalize MetricsSplitMetadata struct**
   - All fields defined with appropriate types
   - Serde serialization for Postgres JSON column
   - Builder pattern for construction

2. **Create PostgreSQL migration**
   - `XX_create-metrics-splits.up.sql` with full schema
   - All indexes defined
   - Down migration for rollback

3. **Implement MetricsSplitMetadata to metrics_splits conversion**
   - Insert function for new splits
   - Update function for state changes
   - Query function matching ListSplitsQuery pattern

4. **Document pruning query patterns**
   - Example SQL for common queries
   - Performance expectations
   - Integration with existing metastore interface

### Pruning Strategy Summary

| Data | Storage | Query Method | When |
|------|---------|--------------|------|
| Time range | Postgres | `time_range_start/end` comparison | Every query |
| Metric names | Postgres | `= ANY(metric_names)` | Most queries |
| Low-cardinality tags | Postgres | `= ANY(tag_*)` | Filtered queries |
| High-cardinality tags | Parquet | Bloom filter check | Filtered queries |
| Row group data | Parquet | Min/max statistics | Execution time |

**End-to-end flow:**

```
Query: cpu.usage where service=web, env=prod, host=server-001
                        (last hour)

1. Postgres query (Tier 1):
   - Filter by time_range overlap with last hour
   - Filter by $$cpu.usage$$ = ANY(metric_names)
   - Filter by $$web$$ = ANY(tag_service)
   - Filter by $$prod$$ = ANY(tag_env)
   → Returns 50 splits from 10,000 total (99.5% pruned)

2. Parquet bloom filter (Tier 2):
   - Check bloom filter for host=server-001
   → 10 splits pass filter (80% pruned within Tier 1 results)

3. Row group statistics (Tier 2):
   - Check timestamp min/max per row group
   → Read 30% of bytes within remaining splits

4. Final read:
   - Read qualifying row groups
   - Apply predicate during scan
   → Return matching time series
```

---

*Phase: 12-metadata-analysis*
*Completed: 2026-01-17*
