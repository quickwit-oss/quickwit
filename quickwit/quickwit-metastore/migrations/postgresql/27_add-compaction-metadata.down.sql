-- Reverse Phase 31: Remove compaction metadata columns and triggers.
DROP TRIGGER IF EXISTS set_publish_timestamp_on_metrics_split_publish ON metrics_splits CASCADE;
DROP FUNCTION IF EXISTS set_publish_timestamp_for_metrics_split();
DROP INDEX IF EXISTS idx_metrics_splits_compaction_scope;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS node_id;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS delete_opstamp;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS maturity_timestamp;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS zonemap_regexes;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS row_keys;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS num_merge_ops;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS sort_fields;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS window_duration_secs;
ALTER TABLE metrics_splits DROP COLUMN IF EXISTS window_start;
