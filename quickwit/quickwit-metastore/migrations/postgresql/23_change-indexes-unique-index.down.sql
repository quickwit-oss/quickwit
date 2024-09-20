DROP INDEX IF EXISTS indexes_index_id_unique;
ALTER TABLE indexes ADD CONSTRAINT indexes_index_id_unique UNIQUE (index_id);
