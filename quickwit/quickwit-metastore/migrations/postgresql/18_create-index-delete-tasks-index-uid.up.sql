CREATE INDEX IF NOT EXISTS delete_tasks_index_uid_idx ON delete_tasks USING HASH(index_uid);
