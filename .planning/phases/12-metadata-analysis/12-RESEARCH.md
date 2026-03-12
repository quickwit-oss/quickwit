# Phase 12: Metadata Analysis - Research

**Researched:** 2026-01-17
**Domain:** Quickwit SplitMetadata and Postgres metastore pruning patterns
**Confidence:** HIGH

<research_summary>
## Summary

Researched the existing Quickwit SplitMetadata infrastructure to understand how to extend it for metrics pruning. The system has a clean architecture:

1. **SplitMetadata struct** (`quickwit-metastore/src/split_metadata.rs`) — immutable metadata per split including time range, tags, and counts
2. **ListSplitsQuery** (`quickwit-metastore/src/metastore/mod.rs:635`) — flexible query predicate with FilterRange-based pruning
3. **PostgreSQL schema** (`migrations/postgresql/2_create-splits.up.sql`) — columns for time range, tags array, and JSON-serialized metadata

The existing tag system stores strings as `TEXT[]` in Postgres and uses `= ANY(tags)` for filtering. For metrics, we need:
- Metric names as a dedicated prunable dimension
- Tag key-value pairs for low-cardinality tags (<1000 unique values)
- Parquet bloom filters for high-cardinality tag values (≥1000)

**Primary recommendation:** Use `MetricsSplitMetadata` as a **distinct type and table** rather than extending the existing log-oriented `SplitMetadata`. This provides:
- Clean separation of concerns (metrics vs logs have different pruning needs)
- Independent schema evolution without backward compatibility concerns
- Optimized table structure for metrics-specific queries
- No risk of breaking existing log ingestion pipelines

</research_summary>

<standard_stack>
## Existing Architecture

### SplitMetadata Struct
**Location:** `quickwit-metastore/src/split_metadata.rs`

| Field | Type | Purpose | Prunable |
|-------|------|---------|----------|
| split_id | SplitId | Unique identifier | No |
| index_uid | IndexUid | Index ownership | Yes |
| partition_id | u64 | Tenant isolation | Yes |
| time_range | Option<RangeInclusive<i64>> | Min/max timestamp (seconds) | **Yes** |
| num_docs | usize | Document count | No |
| uncompressed_docs_size_in_bytes | u64 | Original size | No |
| tags | BTreeSet<String> | Tag set (`field:value` format) | **Yes** |
| create_timestamp | i64 | Creation time | Yes |
| delete_opstamp | u64 | Delete operation tracking | Yes |
| footer_offsets | Range<u64> | Parquet footer location | No |

### PostgreSQL Schema
**Location:** `migrations/postgresql/2_create-splits.up.sql`

```sql
CREATE TABLE IF NOT EXISTS splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT,           -- Indexed for range queries
    time_range_end BIGINT,             -- Indexed for range queries
    tags TEXT[] NOT NULL,              -- Array for tag filtering
    split_metadata_json TEXT NOT NULL, -- Full metadata as JSON
    index_id VARCHAR(50) NOT NULL,
    create_timestamp TIMESTAMP NOT NULL,
    update_timestamp TIMESTAMP NOT NULL
);
```

### ListSplitsQuery
**Location:** `quickwit-metastore/src/metastore/mod.rs:635`

```rust
pub struct ListSplitsQuery {
    pub index_uids: Option<Vec<IndexUid>>,
    pub split_states: Vec<SplitState>,
    pub tags: Option<TagFilterAst>,         // AST-based tag filtering
    pub time_range: FilterRange<i64>,       // Time overlap pruning
    pub max_time_range_end: Option<i64>,
    pub delete_opstamp: FilterRange<u64>,
    pub mature: Bound<OffsetDateTime>,
    // ... pagination fields
}
```

### Tag Filtering SQL Generation
**Location:** `quickwit-metastore/src/metastore/postgres/tags.rs`

```rust
// Generates: $$tag_value$$ = ANY(tags)
fn generate_sql_condition(tag_ast: &TagFilterAst) -> Cond {
    match tag_ast {
        TagFilterAst::Tag { tag, is_present } => {
            let expr_str = format!("${guard}${tag}${guard}$ = ANY(tags)");
            // Handles AND/OR composition recursively
        }
    }
}
```

</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Pattern 1: Time Range Overlap Pruning
**What:** Splits store min/max timestamp, queries specify FilterRange, pruning uses overlap detection
**Current implementation:** Works well, already handles edge cases

```rust
// FilterRange supports flexible bounds
pub struct FilterRange<T> {
    pub start: Bound<T>,  // Unbounded, Included(v), Excluded(v)
    pub end: Bound<T>,
}

// Overlap check (not containment)
pub fn overlaps_with(&self, range: RangeInclusive<T>) -> bool {
    // split.time_range overlaps query.time_range
}
```

### Pattern 2: Tag Storage as String Array
**What:** Tags stored as `TEXT[]` with format `field:value`
**Limitation:** No separation of key vs value, no cardinality tracking

```rust
// Current tag format: "{field_name}:{field_value}"
// Example: ["service:web", "env:prod", "host:server-001"]

// SQL filtering uses = ANY()
WHERE $$service:web$$ = ANY(tags)
```

### Pattern 3: MetricsSplitMetadata (Recommended Approach)
**Location:** `quickwit-metrics-engine/src/split/metadata.rs`

```rust
pub struct MetricsSplitMetadata {
    pub split_id: SplitId,
    pub time_range: TimeRange,
    pub num_rows: u64,
    pub size_bytes: u64,
    pub metric_names: HashSet<String>,    // Distinct metrics
    pub service_names: HashSet<String>,   // Distinct services
    pub parquet_files: Vec<String>,
}
```

**Recommendation:** Use `MetricsSplitMetadata` as a **distinct type with its own Postgres table** (`metrics_splits`) rather than extending the log-oriented `splits` table. This approach:
- Keeps metrics metadata separate from logs metadata
- Allows metrics-specific schema optimizations
- Avoids backward compatibility issues with existing log splits
- Enables independent evolution of metrics vs logs pruning

### Recommended Project Structure

```
quickwit-metastore/
├── src/
│   ├── metrics_split_metadata.rs      # Dedicated MetricsSplitMetadata type
│   └── metastore/
│       ├── mod.rs                     # Add ListMetricsSplitsQuery
│       └── postgres/
│           ├── metrics_model.rs       # PgMetricsSplit model
│           └── metrics_tags.rs        # Metrics-specific tag filtering
│
migrations/postgresql/
├── XX_create-metrics-splits.up.sql    # New metrics_splits table
└── XX_create-metrics-splits.down.sql

quickwit-metrics-engine/
└── src/split/
    └── metadata.rs                    # MetricsSplitMetadata (no conversion needed)
```

</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Tag filtering SQL | Custom SQL strings | Adapt `generate_sql_condition()` pattern | SQL injection protection pattern |
| Time range overlap | Custom comparison | FilterRange::overlaps_with() pattern | Handles all edge cases |
| Split state management | Custom state machine | Adapt SplitState enum pattern | Staged/Published/MarkedForDeletion flow |
| Metadata versioning | Custom compatibility | Adopt versioning pattern | Handles backward compat |
| Streaming results | Custom batching | Adapt streaming pattern | Handles large result sets |

**Key insight:** The existing infrastructure patterns are well-designed. **Adopt the patterns** (FilterRange, tag filtering approach, SQL generation style) **but in a separate metrics domain** with dedicated `MetricsSplitMetadata` type and `metrics_splits` table. This avoids coupling metrics and logs while reusing proven architectural patterns.

</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Coupling Metrics and Logs Metadata
**What goes wrong:** Changes to metrics pruning affect log ingestion, or vice versa
**Why it happens:** Sharing a single `SplitMetadata` type and `splits` table for both domains
**How to avoid:**
- Use separate `MetricsSplitMetadata` type with dedicated `metrics_splits` table
- Keep schema evolution independent between metrics and logs
- No shared backward compatibility concerns
**Warning signs:** Feature requests blocked by compatibility with other domain

### Pitfall 2: Unbounded Tag Cardinality
**What goes wrong:** High-cardinality tags explode Postgres storage/query time
**Why it happens:** Storing all tag values in TEXT[] for high-cardinality dimensions
**How to avoid:**
- Cardinality threshold (1000) for Postgres vs Parquet bloom filter
- Track cardinality per tag key, not total
- Limit total tags per split (10k as discussed)
**Warning signs:** Slow ListSplits queries, large split_metadata_json

### Pitfall 3: Tag Key vs Value Conflation
**What goes wrong:** Can't prune by "tag key exists" separately from "tag key=value"
**Why it happens:** Current format `field:value` doesn't separate key presence from value match
**How to avoid:**
- Presence indicator: `field!` means "field exists in split"
- Value match: `field:value` means "field=value exists"
- Both already supported in tag system!
**Warning signs:** False negatives in pruning (splits that should match don't)

### Pitfall 4: Metric Name Explosion
**What goes wrong:** Metric names stored individually per split bloat metadata
**Why it happens:** Treating metric names like tags without cardinality limits
**How to avoid:**
- Store as array column in Postgres (like tags)
- Consider bloom filter for metric names if cardinality >10k
- Index the array column for efficient `= ANY()` queries
**Warning signs:** split_metadata_json grows large, slow metric name filtering

### Pitfall 5: Missing Index on New Columns
**What goes wrong:** ListSplits queries slow despite selective filters
**Why it happens:** New Postgres columns added without proper indexes
**How to avoid:**
- Add indexes in migration for frequently filtered columns
- Use GIN index for array columns (metric_names, tags)
- Test query plans with EXPLAIN ANALYZE
**Warning signs:** Sequential scans in Postgres query plans

</common_pitfalls>

<code_examples>
## Code Examples

### Current Tag Filtering (Postgres)
```rust
// Source: quickwit-metastore/src/metastore/postgres/tags.rs
pub(super) fn generate_sql_condition(tag_ast: &TagFilterAst) -> Cond {
    match tag_ast {
        TagFilterAst::And(child_asts) => {
            child_asts.iter()
                .map(generate_sql_condition)
                .fold(Cond::all(), |cond, child| cond.add(child))
        }
        TagFilterAst::Tag { tag, is_present } => {
            let expr = format!("$${}$$ = ANY(tags)", tag);
            if *is_present { all![Expr::cust(expr)] }
            else { all![Expr::cust(expr).not()] }
        }
    }
}
```

### Current Split Pruning Predicate
```rust
// Source: quickwit-metastore/src/metastore/file_backed/file_backed_index/mod.rs:697
fn split_query_predicate(split: &&Split, query: &ListSplitsQuery) -> bool {
    // 1. Tag filtering
    if !split_tag_filter(&split.split_metadata, query.tags.as_ref()) {
        return false;
    }

    // 2. Time range overlap
    if let Some(range) = &split.split_metadata.time_range {
        if !query.time_range.overlaps_with(range.clone()) {
            return false;
        }
    }

    // ... other filters
    true
}
```

### Metrics Split Writer (Current)
```rust
// Source: quickwit-metrics-engine/src/storage/split_writer.rs
pub fn write_split(&self, batch: &RecordBatch) -> Result<MetricsSplit, MetricsWriteError> {
    let split_id = SplitId::generate();
    let time_range = extract_time_range(batch)?;
    let metric_names = extract_metric_names(batch)?;
    let service_names = extract_service_names(batch)?;

    let metadata = MetricsSplitMetadata::builder()
        .split_id(split_id)
        .time_range(time_range)
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes)
        // ... metric_names, service_names
        .build();

    Ok(MetricsSplit::new(metadata))
}
```

### Example: Querying metrics_splits Table
```sql
-- Migration: XX_create-metrics-splits.up.sql
-- Creates dedicated metrics_splits table (see schema above)

-- Query pattern for metrics splits
SELECT * FROM metrics_splits
WHERE $$cpu.usage$$ = ANY(metric_names)
  AND time_range_start <= 1705500000
  AND time_range_end >= 1705400000
  AND $$web$$ = ANY(tag_service);
```

</code_examples>

<metrics_specific>
## Metrics-Specific Analysis

### Fields Needed for Pruning

| Field | Storage | Pruning Use | Cardinality |
|-------|---------|-------------|-------------|
| time_range | Postgres BIGINT x2 | Every query | N/A |
| metric_names | Postgres TEXT[] | Filter by metric | ~100-1000 per split |
| tag_service | Postgres TEXT[] | Filter by service | Low (<100) |
| tag_env | Postgres TEXT[] | Filter by env | Very low (<10) |
| tag_datacenter | Postgres TEXT[] | Filter by DC | Very low (<20) |
| tag_region | Postgres TEXT[] | Filter by region | Low (<50) |
| tag_host | Parquet bloom filter | Filter by host | High (1000+) |
| attributes | Parquet bloom filter | Filter by custom tags | Variable |

### Recommended Schema: Separate metrics_splits Table

```rust
// Dedicated MetricsSplitMetadata (not extending SplitMetadata)
pub struct MetricsSplitMetadata {
    /// Unique split identifier
    pub split_id: SplitId,

    /// Time range for temporal pruning
    pub time_range: TimeRange,

    /// Distinct metric names in this split
    pub metric_names: HashSet<String>,

    /// Low-cardinality tag values (stored in Postgres)
    /// Format: HashMap<tag_key, HashSet<tag_value>>
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,

    /// High-cardinality tag keys (bloom filter in Parquet)
    pub high_cardinality_tag_keys: HashSet<String>,

    /// Row count for query planning
    pub num_rows: u64,

    /// Byte size for query planning
    pub size_bytes: u64,

    /// Parquet file paths
    pub parquet_files: Vec<String>,
}
```

### Postgres Table: metrics_splits (New)

```sql
-- New dedicated table for metrics splits (not altering splits table)
CREATE TABLE IF NOT EXISTS metrics_splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT,
    time_range_end BIGINT,
    metric_names TEXT[] NOT NULL,
    tag_service TEXT[],
    tag_env TEXT[],
    tag_datacenter TEXT[],
    tag_region TEXT[],
    num_rows BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    split_metadata_json TEXT NOT NULL,
    index_id VARCHAR(50) NOT NULL,
    create_timestamp TIMESTAMP NOT NULL,
    update_timestamp TIMESTAMP NOT NULL
);

-- Indexes optimized for metrics queries
CREATE INDEX idx_metrics_splits_time ON metrics_splits (time_range_start, time_range_end);
CREATE INDEX idx_metrics_splits_metric_names ON metrics_splits USING GIN (metric_names);
CREATE INDEX idx_metrics_splits_tag_service ON metrics_splits USING GIN (tag_service);
CREATE INDEX idx_metrics_splits_tag_env ON metrics_splits USING GIN (tag_env);
```

**Benefits of separate table:**
- No migration risk to existing log splits
- Optimized indexes for metrics query patterns
- Independent schema evolution
- Clear ownership boundary between metrics and logs

### Two-Tier Pruning Strategy

**Tier 1: Postgres (`metrics_splits` table)**
- Time range: Always via `time_range_start`/`time_range_end` columns
- Metric names: Via `= ANY(metric_names)`
- Low-cardinality tags (service, env, dc, region): Via `= ANY(tag_xxx)`
- Result: Eliminate 90%+ of splits before touching storage

**Tier 2: Parquet (Query Time)**
- High-cardinality tags (host, custom attributes): Via bloom filter check on Parquet footer
- Row group statistics: Skip row groups via min/max predicates
- Result: Further reduce I/O within selected splits

</metrics_specific>

<open_questions>
## Open Questions

1. **Cardinality Detection**
   - What we know: Need to detect high vs low cardinality at write time
   - What's unclear: Should this be per-split or global tracking?
   - Recommendation: Per-split tracking with configurable threshold (1000)

2. **Tag Key Discovery**
   - What we know: Metrics have `attributes` VARIANT field with arbitrary tags
   - What's unclear: How to discover which tag keys exist across all splits?
   - Recommendation: Store tag key presence indicators in metadata (`tag_key!` format)

3. **MetricsSplitMetadata as Distinct Type**
   - What we know: Metrics have different pruning needs than logs
   - What's decided: Use `MetricsSplitMetadata` as a **separate type with dedicated `metrics_splits` table**
   - Rationale: Clean separation, independent evolution, no backward compatibility risk with logs

</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- `quickwit-metastore/src/split_metadata.rs` - SplitMetadata struct definition
- `quickwit-metastore/src/metastore/mod.rs` - ListSplitsQuery and FilterRange
- `quickwit-metastore/src/metastore/file_backed/file_backed_index/mod.rs` - split_query_predicate
- `quickwit-metastore/src/metastore/postgres/tags.rs` - SQL generation for tags
- `quickwit-metastore/migrations/postgresql/2_create-splits.up.sql` - PostgreSQL schema

### Secondary (HIGH confidence - internal codebase)
- `quickwit-metrics-engine/src/split/metadata.rs` - MetricsSplitMetadata current state
- `quickwit-metrics-engine/src/storage/split_writer.rs` - How metadata is extracted
- `quickwit-metrics-engine/src/schema/fields.rs` - Metrics field definitions

</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Quickwit metastore, PostgreSQL, Parquet
- Ecosystem: Existing split pruning infrastructure
- Patterns: Tag filtering, time range overlap, SQL generation
- Pitfalls: Cardinality explosion, backward compatibility, missing indexes

**Confidence breakdown:**
- Existing architecture: HIGH - direct codebase analysis
- Extension approach: HIGH - follows established patterns
- Cardinality thresholds: MEDIUM - may need tuning
- Performance: MEDIUM - needs benchmarking

**Research date:** 2026-01-17
**Valid until:** 2026-02-17 (30 days - internal patterns stable)

</metadata>

---

*Phase: 12-metadata-analysis*
*Research completed: 2026-01-17*
*Ready for planning: yes*
