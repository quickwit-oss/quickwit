ALTER TABLE delete_tasks DROP CONSTRAINT IF EXISTS delete_tasks_index_uid_fkey;
ALTER TABLE delete_tasks ADD CONSTRAINT delete_tasks_index_id_fkey FOREIGN KEY (index_id) REFERENCES indexes(index_id) ON DELETE CASCADE;
ALTER TABLE delete_tasks DROP COLUMN IF EXISTS incarnation_id;
ALTER TABLE splits DROP CONSTRAINT IF EXISTS splits_index_uid_fkey;
ALTER TABLE splits ADD CONSTRAINT splits_index_id_fkey FOREIGN KEY (index_id) REFERENCES indexes(index_id) ON DELETE CASCADE;
ALTER TABLE indexes DROP CONSTRAINT IF EXISTS indexes_index_id_incarnation_id_unique;
ALTER TABLE indexes DROP COLUMN IF EXISTS incarnation_id;
