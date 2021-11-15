-- drop new fields
ALTER TABLE splits
DROP COLUMN create_timestamp;
ALTER TABLE splits
DROP COLUMN update_timestamp;

-- remove diesel `set_updated_at` trigger
DROP TRIGGER IF EXISTS set_updated_at 
ON splits

-- restore previous fields
ALTER TABLE splits
ADD update_timestamp BIGINT DEFAULT 0;
