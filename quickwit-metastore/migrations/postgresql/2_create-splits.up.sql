CREATE TABLE IF NOT EXISTS splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT,
    time_range_end BIGINT,
    tags TEXT[] NOT NULL,
    split_metadata_json TEXT NOT NULL,
    index_id VARCHAR(50) NOT NULL,
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),

    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);

DROP TRIGGER IF EXISTS quickwit_set_index_update_timestamp_on_split_change ON splits CASCADE;
CREATE TRIGGER quickwit_set_index_update_timestamp_on_split_change
    AFTER INSERT OR DELETE OR UPDATE ON splits
    FOR EACH ROW
    EXECUTE PROCEDURE set_index_update_timestamp_for_split();

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


-- apply the trigger to the `splits` table
SELECT quickwit_manage_update_timestamp('splits');

