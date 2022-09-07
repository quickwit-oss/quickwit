ALTER TABLE splits
    ADD COLUMN publish_timestamp TIMESTAMP DEFAULT NULL;

-- We want to update the split `publish_timestamp` field whenever the split
-- being is published.
CREATE OR REPLACE FUNCTION set_split_publish_timestamp_for_split() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'UPDATE') AND (NEW.split_state = 'Published') AND (OLD.split_state = 'Staged') THEN
        NEW.publish_timestamp := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
    END IF;
    RETURN NEW;
END;

$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_split_publish_timestamp_on_split_publish ON splits CASCADE;
CREATE TRIGGER set_split_publish_timestamp_on_split_publish
    BEFORE UPDATE ON splits
    FOR EACH ROW
    EXECUTE PROCEDURE set_split_publish_timestamp_for_split();
