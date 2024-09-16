ALTER TABLE splits DROP CONSTRAINT splits_pkey, ADD PRIMARY KEY (index_uid, split_id);
DROP INDEX IF EXISTS splits_index_uid_idx;
