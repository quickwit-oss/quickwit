-- Renaming the start,end columns
ALTER TABLE splits
RENAME COLUMN start_time_range TO time_range_start;
ALTER TABLE splits
RENAME COLUMN end_time_range TO time_range_end;

-- Adding a column storage the last update timestamp
ALTER TABLE splits
ADD update_timestamp BIGINT DEFAULT 0;
