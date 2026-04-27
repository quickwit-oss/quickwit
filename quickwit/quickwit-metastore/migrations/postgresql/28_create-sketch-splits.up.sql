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

    -- Compaction metadata
    window_start BIGINT,
    window_duration_secs INTEGER,
    sort_fields TEXT NOT NULL DEFAULT '',
    num_merge_ops INTEGER NOT NULL DEFAULT 0,
    row_keys BYTEA,
    zonemap_regexes JSONB NOT NULL DEFAULT '{}',
    maturity_timestamp TIMESTAMP DEFAULT TO_TIMESTAMP(0),
    delete_opstamp BIGINT CHECK (delete_opstamp >= 0) DEFAULT 0,
    node_id VARCHAR(253),

    -- Full metadata
    split_metadata_json TEXT NOT NULL,

    -- Timestamps
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    publish_timestamp TIMESTAMP,

    FOREIGN KEY(index_uid) REFERENCES indexes(index_uid) ON DELETE CASCADE
);

-- Auto-set publish_timestamp when a split transitions Staged -> Published.
CREATE OR REPLACE FUNCTION set_publish_timestamp_for_sketch_split() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'UPDATE') AND (NEW.split_state = 'Published') AND (OLD.split_state = 'Staged') THEN
        NEW.publish_timestamp := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_publish_timestamp_on_sketch_split_publish
    BEFORE UPDATE ON sketch_splits
    FOR EACH ROW
    EXECUTE PROCEDURE set_publish_timestamp_for_sketch_split();

CREATE INDEX idx_sketch_splits_time ON sketch_splits (time_range_start, time_range_end);
CREATE INDEX idx_sketch_splits_metric_names ON sketch_splits USING GIN (metric_names);
CREATE INDEX idx_sketch_splits_tag_service ON sketch_splits USING GIN (tag_service);
CREATE INDEX idx_sketch_splits_tag_env ON sketch_splits USING GIN (tag_env);
CREATE INDEX idx_sketch_splits_tag_datacenter ON sketch_splits USING GIN (tag_datacenter);
CREATE INDEX idx_sketch_splits_tag_region ON sketch_splits USING GIN (tag_region);
CREATE INDEX idx_sketch_splits_tag_host ON sketch_splits USING GIN (tag_host);

-- Compaction scope index: supports the compaction planner's primary query pattern.
CREATE INDEX IF NOT EXISTS idx_sketch_splits_compaction_scope
    ON sketch_splits (index_uid, sort_fields, window_start)
    WHERE split_state = 'Published';
