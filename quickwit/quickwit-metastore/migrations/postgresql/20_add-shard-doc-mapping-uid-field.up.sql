ALTER TABLE shards
    ADD COLUMN IF NOT EXISTS doc_mapping_uid VARCHAR(26);

-- Index metadata has been stable for quite a while, so we allow ourselves to do this,
-- but please, reader of the future, do not reapply this pattern without careful consideration.
UPDATE
    shards
SET
    doc_mapping_uid = '00000000000000000000000000';

ALTER TABLE shards
    ALTER COLUMN doc_mapping_uid SET NOT NULL;
