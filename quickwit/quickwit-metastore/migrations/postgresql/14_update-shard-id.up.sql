ALTER TABLE shards
    ALTER COLUMN shard_id TYPE VARCHAR(255),
    ALTER COLUMN shard_id SET NOT NULL,
    ALTER COLUMN shard_state SET DEFAULT 'open',
    ALTER COLUMN publish_position_inclusive SET DEFAULT '',
    DROP CONSTRAINT shards_index_uid_fkey,
    ADD CONSTRAINT shards_index_uid_fkey FOREIGN KEY (index_uid) REFERENCES indexes(index_uid) ON DELETE CASCADE
