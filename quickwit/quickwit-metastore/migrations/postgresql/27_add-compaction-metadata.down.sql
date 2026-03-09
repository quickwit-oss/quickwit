-- Reverse Phase 31: Remove compaction metadata columns.
DROP INDEX IF EXISTS idx_metrics_splits_compaction_scope;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS zonemap_regexes;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS row_keys;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS num_merge_ops;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS sort_fields;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS window_duration_secs;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS window_start;
