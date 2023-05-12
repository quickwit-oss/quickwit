
ALTER TABLE delete_tasks DROP CONSTRAINT IF EXISTS delete_tasks_index_uid_fkey;
UPDATE delete_tasks set index_uid = split_part(index_uid,':',1);
ALTER TABLE delete_tasks ALTER COLUMN index_uid TYPE VARCHAR(50);
ALTER TABLE delete_tasks RENAME COLUMN index_uid TO index_id;

ALTER TABLE splits DROP CONSTRAINT IF EXISTS splits_index_uid_fkey;
ALTER TABLE splits ADD COLUMN incarnation_id VARCHAR(26) NOT NULL DEFAULT '00000000000000000000000000';
UPDATE splits set index_uid = split_part(index_uid,':',1);
ALTER TABLE splits ALTER COLUMN index_uid TYPE VARCHAR(50);
ALTER TABLE splits RENAME COLUMN index_uid TO index_id;

ALTER TABLE indexes DROP COLUMN index_id;
UPDATE indexes set index_uid = split_part(index_uid,':',1);
ALTER TABLE indexes ALTER COLUMN index_uid TYPE VARCHAR(50);
ALTER TABLE indexes RENAME COLUMN index_uid TO index_id;

ALTER TABLE delete_tasks ADD CONSTRAINT delete_tasks_index_id_fkey FOREIGN KEY (index_id) REFERENCES indexes(index_id) ON DELETE CASCADE;
ALTER TABLE splits ADD CONSTRAINT splits_index_id_fkey FOREIGN KEY (index_id) REFERENCES indexes(index_id) ON DELETE CASCADE;
