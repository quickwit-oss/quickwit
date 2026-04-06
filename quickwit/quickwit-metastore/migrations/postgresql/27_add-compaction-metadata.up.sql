-- Phase 31: Add compaction metadata columns to metrics_splits.
-- These columns support time-windowed compaction planning and execution.
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS window_start BIGINT;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS window_duration_secs INTEGER;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS sort_fields TEXT NOT NULL DEFAULT '';
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS num_merge_ops INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS row_keys BYTEA;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS zonemap_regexes JSONB NOT NULL DEFAULT '{}';

-- Columns present on the `splits` table that were missing from `metrics_splits`.
-- maturity_timestamp: compaction planner needs this to restrict candidates to
-- Published-and-immature splits, matching the logic the log-side merge planner uses.
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS maturity_timestamp TIMESTAMP DEFAULT TO_TIMESTAMP(0);
-- delete_opstamp: tracks which delete tasks have been applied to a split.
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS delete_opstamp BIGINT CHECK (delete_opstamp >= 0) DEFAULT 0;
-- node_id: identifies which node produced the split.
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS node_id VARCHAR(253);

-- Auto-set publish_timestamp when a split transitions Staged → Published,
-- matching the trigger on the `splits` table (migration 3).
CREATE OR REPLACE FUNCTION set_publish_timestamp_for_metrics_split() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'UPDATE') AND (NEW.split_state = 'Published') AND (OLD.split_state = 'Staged') THEN
        NEW.publish_timestamp := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_publish_timestamp_on_metrics_split_publish ON metrics_splits CASCADE;
CREATE TRIGGER set_publish_timestamp_on_metrics_split_publish
    BEFORE UPDATE ON metrics_splits
    FOR EACH ROW
    EXECUTE PROCEDURE set_publish_timestamp_for_metrics_split();

-- Compaction scope index: supports the compaction planner's primary query pattern
-- "give me all Published splits for a given (index_uid, sort_fields, window_start) triple."
CREATE INDEX IF NOT EXISTS idx_metrics_splits_compaction_scope
    ON metrics_splits (index_uid, sort_fields, window_start)
    WHERE split_state = 'Published';
