ALTER TABLE splits
    ADD COLUMN node_id VARCHAR(253);

-- Split metadata has been stable for quite a while, so we allow ourselves to do this,
-- but please, reader of the future, do not reapply this pattern without careful consideration.
UPDATE
    splits
SET
    node_id = splits.split_metadata_json::json ->> 'node_id';

ALTER TABLE splits
    ALTER COLUMN node_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS splits_node_id_idx ON splits USING HASH (node_id);
