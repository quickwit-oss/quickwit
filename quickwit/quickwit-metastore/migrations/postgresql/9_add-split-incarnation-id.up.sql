ALTER TABLE indexes RENAME COLUMN index_id TO index_uid;
ALTER TABLE indexes ADD COLUMN index_id VARCHAR(50) NOT NULL DEFAULT '';
UPDATE indexes set index_id = index_uid;
ALTER TABLE indexes ADD CONSTRAINT indexes_index_id_unique UNIQUE (index_id);
ALTER TABLE indexes ALTER COLUMN index_uid TYPE VARCHAR(64);

ALTER TABLE splits DROP CONSTRAINT IF EXISTS splits_index_id_fkey;
ALTER TABLE splits RENAME COLUMN index_id TO index_uid;
ALTER TABLE splits ALTER COLUMN index_uid TYPE VARCHAR(64);
ALTER TABLE splits ADD CONSTRAINT splits_index_uid_fkey FOREIGN KEY (index_uid) REFERENCES indexes(index_uid) ON DELETE CASCADE;

ALTER TABLE delete_tasks DROP CONSTRAINT IF EXISTS delete_tasks_index_id_fkey;
ALTER TABLE delete_tasks RENAME COLUMN index_id TO index_uid;
ALTER TABLE delete_tasks ALTER COLUMN index_uid TYPE VARCHAR(64);
ALTER TABLE delete_tasks ADD CONSTRAINT delete_tasks_index_uid_fkey FOREIGN KEY (index_uid) REFERENCES indexes(index_uid) ON DELETE CASCADE;
