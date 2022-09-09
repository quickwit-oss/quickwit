ALTER TABLE splits
  DROP COLUMN publish_timestamp;


DROP FUNCTION IF EXISTS set_split_publish_timestamp_on_split_publish();
DROP TRIGGER IF EXISTS set_split_publish_timestamp_on_split_publish ON splits CASCADE;
DROP FUNCTION IF EXISTS set_split_publish_timestamp_for_split(); 
