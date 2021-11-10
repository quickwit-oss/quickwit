-- This file should undo anything in `up.sql`

-- restore previous fields
ALTER TABLE splits
ADD update_timestamp BIGINT DEFAULT 0;

-- drop new fields
ALTER TABLE splits
DROP COLUMN created_at;
ALTER TABLE splits
DROP COLUMN updated_at;

-- remove diesel `set_updated_at` trigger
DROP TRIGGER IF EXISTS set_updated_at 
ON splits
