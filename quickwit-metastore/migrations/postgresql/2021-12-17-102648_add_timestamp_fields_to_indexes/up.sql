-- create new timestamp fields
ALTER TABLE indexes
ADD create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
ALTER TABLE indexes
ADD update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');


-- Apply the `update_timestamp` trigger to the `indexes` table
SELECT quickwit_manage_update_timestamp('indexes');

-- We also want to update an index `update_timestamp` field whenever a related split
-- is modified.
CREATE OR REPLACE FUNCTION set_index_update_timestamp_for_split() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
        UPDATE indexes SET update_timestamp = NEW.update_timestamp
        WHERE indexes.index_id = NEW.index_id;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE indexes SET update_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') 
        WHERE indexes.index_id = OLD.index_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER quickwit_set_index_update_timestamp_on_split_change
    AFTER INSERT OR DELETE OR UPDATE ON splits
    FOR EACH ROW
    EXECUTE PROCEDURE set_index_update_timestamp_for_split();
