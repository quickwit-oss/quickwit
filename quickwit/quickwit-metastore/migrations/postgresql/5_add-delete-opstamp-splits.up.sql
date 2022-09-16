ALTER TABLE splits ADD COLUMN delete_opstamp BIGINT CHECK (delete_opstamp >= 0) DEFAULT 0;
