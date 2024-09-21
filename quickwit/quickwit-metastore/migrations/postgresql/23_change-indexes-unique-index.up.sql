ALTER TABLE indexes DROP CONSTRAINT IF EXISTS indexes_index_id_unique;

CREATE UNIQUE INDEX IF NOT EXISTS indexes_index_id_unique
  ON indexes USING btree ("index_id" varchar_pattern_ops);
