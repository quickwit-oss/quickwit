DROP INDEX IF EXISTS idx_splits_stats;

ALTER TABLE splits DROP COLUMN IF EXISTS split_size_bytes;
