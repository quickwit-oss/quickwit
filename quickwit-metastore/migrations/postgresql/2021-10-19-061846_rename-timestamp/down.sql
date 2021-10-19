ALTER TABLE splits
RENAME COLUMN time_range_start TO start_time_range;
ALTER TABLE splits
RENAME COLUMN time_range_end TO end_time_range;

ALTER TABLE splits
DROP COLUMN update_timestamp;


