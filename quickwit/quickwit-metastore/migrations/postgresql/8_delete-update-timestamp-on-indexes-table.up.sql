ALTER TABLE indexes DROP COLUMN IF EXISTS update_timestamp;
DROP TRIGGER IF EXISTS set_update_timestamp ON indexes;