-- Sketch splits table for DDSketch data.
-- Mirrors metrics_splits structure for two-tier pruning.
CREATE TABLE IF NOT EXISTS sketch_splits (
    -- Identity
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    index_uid VARCHAR(282) NOT NULL,

    -- Temporal pruning
    time_range_start BIGINT NOT NULL,
    time_range_end BIGINT NOT NULL,

    -- Metric name pruning
    metric_names TEXT[] NOT NULL,

    -- Low-cardinality tag pruning
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

    -- Full metadata
    split_metadata_json TEXT NOT NULL,

    -- Timestamps
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    publish_timestamp TIMESTAMP,

    FOREIGN KEY(index_uid) REFERENCES indexes(index_uid)
);

CREATE INDEX idx_sketch_splits_time ON sketch_splits (time_range_start, time_range_end);
CREATE INDEX idx_sketch_splits_metric_names ON sketch_splits USING GIN (metric_names);
CREATE INDEX idx_sketch_splits_tag_service ON sketch_splits USING GIN (tag_service);
CREATE INDEX idx_sketch_splits_tag_env ON sketch_splits USING GIN (tag_env);
CREATE INDEX idx_sketch_splits_tag_datacenter ON sketch_splits USING GIN (tag_datacenter);
CREATE INDEX idx_sketch_splits_tag_region ON sketch_splits USING GIN (tag_region);
CREATE INDEX idx_sketch_splits_tag_host ON sketch_splits USING GIN (tag_host);
