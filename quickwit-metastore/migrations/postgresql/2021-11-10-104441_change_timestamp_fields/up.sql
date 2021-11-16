-- drop current timestamp field
ALTER TABLE splits
DROP COLUMN update_timestamp;

-- create new timestamp fields
ALTER TABLE splits
ADD create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
ALTER TABLE splits
ADD update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

-- Setup helper function and trigger to automatically set the `update_timestamp` field
-- Diesel already provides this in the setup migration, but we have created a new one here for:
--   Customising the field name
--   Making sure timestamp are always saved in UTC
CREATE OR REPLACE FUNCTION quickwit_manage_update_timestamp(_tbl regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE format('CREATE TRIGGER set_update_timestamp BEFORE UPDATE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE quickwit_set_update_timestamp()', _tbl);
END;
$$ LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION quickwit_set_update_timestamp() RETURNS trigger AS $$
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

-- apply the trigger to the `splits` table
SELECT quickwit_manage_update_timestamp('splits');
