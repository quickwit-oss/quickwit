ALTER TABLE shards
    ALTER COLUMN shard_id TYPE BIGSERIAL,
    ALTER COLUMN shard_id DROP NOT NULL,
    ALTER COLUMN shard_state DROP DEFAULT,
    ALTER COLUMN publish_position_inclusive DROP DEFAULT,
    DROP CONSTRAINT shards_index_uid_fkey,
    ADD CONSTRAINT shards_index_uid_fkey FOREIGN KEY (index_uid) REFERENCES indexes(index_uid)
