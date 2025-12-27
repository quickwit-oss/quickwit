ALTER TABLE splits ADD COLUMN IF NOT EXISTS split_size_bytes BIGINT NOT NULL GENERATED ALWAYS AS ((split_metadata_json::json->'footer_offsets'->>'end')::bigint) STORED;

CREATE INDEX IF NOT EXISTS idx_splits_stats ON splits (index_uid, split_state) INCLUDE (split_size_bytes);
