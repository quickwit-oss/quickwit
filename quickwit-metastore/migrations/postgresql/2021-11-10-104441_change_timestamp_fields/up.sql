-- drop current timestamp field
ALTER TABLE splits
DROP COLUMN update_timestamp;

-- create new timestamp fields
ALTER TABLE splits
ADD create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
ALTER TABLE splits
ADD update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

-- replace diesel helper function to make sure it uses UTC timestamp
CREATE OR REPLACE FUNCTION diesel_set_updated_at() RETURNS trigger AS $$
BEGIN
    IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.update_timestamp IS NOT DISTINCT FROM OLD.update_timestamp
    ) THEN
        NEW.update_timestamp := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- apply update trigger
SELECT diesel_manage_updated_at('splits');
