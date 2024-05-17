ALTER TABLE shards
    ADD COLUMN IF NOT EXISTS doc_mapping_json VARCHAR;

-- Index metadata has been stable for quite a while, so we allow ourselves to do this,
-- but please, reader of the future, do not reapply this pattern without careful consideration.
UPDATE
    shards
SET
    doc_mapping_json = (indexes.index_metadata_json::json ->> 'index_config')::json ->> 'doc_mapping'
FROM indexes
WHERE
    shards.index_uid = indexes.index_uid;

ALTER TABLE shards
    ALTER COLUMN doc_mapping_json SET NOT NULL;
