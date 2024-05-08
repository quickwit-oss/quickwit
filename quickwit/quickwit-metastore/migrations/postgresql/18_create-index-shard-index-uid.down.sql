CREATE INDEX IF NOT EXISTS shards_index_uid_idx ON shards USING HASH(index_uid);
