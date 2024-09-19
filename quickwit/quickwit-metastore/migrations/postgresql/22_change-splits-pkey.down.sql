CREATE INDEX IF NOT EXISTS splits_index_uid_idx ON splits USING HASH(index_uid);
ALTER TABLE splits DROP CONSTRAINT splits_pkey, ADD PRIMARY KEY (split_id);
