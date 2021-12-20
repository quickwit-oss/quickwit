ALTER TABLE indexes
DROP COLUMN create_timestamp;
ALTER TABLE indexes
DROP COLUMN update_timestamp;

DROP TRIGGER IF EXISTS set_index_update_timestamp_for_split;

DROP FUNCTION IF EXISTS set_updated_at_for_index();


-- remove diesel `set_update_timestamp` trigger
DROP TRIGGER IF EXISTS set_update_timestamp 
ON indexes
