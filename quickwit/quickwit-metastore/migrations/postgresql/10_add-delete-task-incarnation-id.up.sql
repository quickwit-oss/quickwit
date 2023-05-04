ALTER TABLE indexes ADD COLUMN incarnation_id VARCHAR(26) NOT NULL DEFAULT '00000000000000000000000000';
CREATE UNIQUE INDEX indexes_index_id_incarnation_id_unique ON indexes (index_id, incarnation_id);
ALTER TABLE indexes ADD CONSTRAINT indexes_index_id_incarnation_id_unique UNIQUE USING INDEX indexes_index_id_incarnation_id_unique;
ALTER TABLE splits DROP CONSTRAINT IF EXISTS splits_index_id_fkey;
ALTER TABLE splits ADD CONSTRAINT splits_index_uid_fkey FOREIGN KEY (index_id, incarnation_id) REFERENCES indexes (index_id, incarnation_id);
ALTER TABLE delete_tasks ADD COLUMN incarnation_id VARCHAR(26) NOT NULL DEFAULT '00000000000000000000000000';
ALTER TABLE delete_tasks DROP CONSTRAINT IF EXISTS delete_tasks_index_id_fkey;
ALTER TABLE delete_tasks ADD CONSTRAINT delete_tasks_index_uid_fkey FOREIGN KEY (index_id, incarnation_id) REFERENCES indexes (index_id, incarnation_id) ON DELETE CASCADE;
