-- remove diesel `set_update_timestamp` trigger
DROP TRIGGER IF EXISTS set_update_timestamp 
ON splits

DROP FUNCTION IF EXISTS quickwit_manage_update_timestamp(_tbl regclass);
DROP FUNCTION IF EXISTS quickwit_set_update_timestamp();

-- drop new fields
ALTER TABLE splits
DROP COLUMN create_timestamp;
ALTER TABLE splits
DROP COLUMN update_timestamp;

-- restore previous fields
ALTER TABLE splits
ADD update_timestamp BIGINT DEFAULT 0;
