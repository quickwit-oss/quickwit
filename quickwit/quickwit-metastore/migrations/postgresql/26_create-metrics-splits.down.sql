-- Drop indexes first (implicitly dropped with table, but explicit for clarity)
DROP INDEX IF EXISTS idx_metrics_splits_published_time;
DROP INDEX IF EXISTS idx_metrics_splits_state;
DROP INDEX IF EXISTS idx_metrics_splits_tag_host;
DROP INDEX IF EXISTS idx_metrics_splits_tag_region;
DROP INDEX IF EXISTS idx_metrics_splits_tag_datacenter;
DROP INDEX IF EXISTS idx_metrics_splits_tag_env;
DROP INDEX IF EXISTS idx_metrics_splits_tag_service;
DROP INDEX IF EXISTS idx_metrics_splits_metric_names;
DROP INDEX IF EXISTS idx_metrics_splits_time;

-- Drop the table
DROP TABLE IF EXISTS metrics_splits;
