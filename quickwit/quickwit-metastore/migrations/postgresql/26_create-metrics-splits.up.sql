-- Metrics splits table for two-tier pruning strategy.
-- Tier 1: Postgres with GIN indexes for low-cardinality tag filtering.
-- Tier 2: Parquet bloom filters for high-cardinality tags (not stored here).
CREATE TABLE IF NOT EXISTS metrics_splits (
    -- Identity
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    index_uid VARCHAR(282) NOT NULL,

    -- Temporal pruning (always used in metrics queries)
    time_range_start BIGINT NOT NULL,
    time_range_end BIGINT NOT NULL,

    -- Metric name pruning (100% of queries filter by metric name)
    metric_names TEXT[] NOT NULL,

    -- Low-cardinality tag pruning (<1000 unique values per key)
    -- Stored as TEXT[] for GIN indexing. NULL means tag not present in split.
    tag_service TEXT[],
    tag_env TEXT[],
    tag_datacenter TEXT[],
    tag_region TEXT[],
    tag_host TEXT[],

    -- High-cardinality tag keys (values stored in Parquet bloom filters)
    high_cardinality_tag_keys TEXT[] NOT NULL DEFAULT '{}',

    -- Planning metadata
    num_rows BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,

    -- Full metadata for flexibility and future-proofing
    split_metadata_json TEXT NOT NULL,

    -- Timestamps
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    publish_timestamp TIMESTAMP,

    -- Foreign key to indexes table (metrics indices are still registered there)
    FOREIGN KEY(index_uid) REFERENCES indexes(index_uid)
);

-- Indexes optimized for metrics query patterns

-- Time range: Composite index for range overlap queries
-- Query pattern: time_range_start <= :query_end AND time_range_end >= :query_start
CREATE INDEX idx_metrics_splits_time ON metrics_splits (time_range_start, time_range_end);

-- Metric names: GIN index for array containment
-- Query pattern: $$cpu.usage$$ = ANY(metric_names)
CREATE INDEX idx_metrics_splits_metric_names ON metrics_splits USING GIN (metric_names);

-- Per-tag GIN indexes for efficient filtering
-- Query pattern: $$web$$ = ANY(tag_service)
CREATE INDEX idx_metrics_splits_tag_service ON metrics_splits USING GIN (tag_service);
CREATE INDEX idx_metrics_splits_tag_env ON metrics_splits USING GIN (tag_env);
CREATE INDEX idx_metrics_splits_tag_datacenter ON metrics_splits USING GIN (tag_datacenter);
CREATE INDEX idx_metrics_splits_tag_region ON metrics_splits USING GIN (tag_region);
CREATE INDEX idx_metrics_splits_tag_host ON metrics_splits USING GIN (tag_host);
