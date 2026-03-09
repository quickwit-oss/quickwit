-- Phase 31: Add compaction metadata columns to metrics_splits.
-- These columns support time-windowed compaction planning and execution.
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS window_start BIGINT;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS window_duration_secs INTEGER;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS sort_fields TEXT NOT NULL DEFAULT '';
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS num_merge_ops INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS row_keys BYTEA;
ALTER TABLE metrics_splits ADD COLUMN IF NOT EXISTS zonemap_regexes JSONB NOT NULL DEFAULT '{}';

-- Compaction scope index: supports the compaction planner's primary query pattern
-- "give me all Published splits for a given (index_uid, sort_fields, window_start) triple."
CREATE INDEX IF NOT EXISTS idx_metrics_splits_compaction_scope
    ON metrics_splits (index_uid, sort_fields, window_start)
    WHERE split_state = 'Published';
