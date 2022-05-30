CREATE TABLE IF NOT EXISTS indexes (
    index_id VARCHAR(50) PRIMARY KEY,
    index_metadata_json TEXT NOT NULL,
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
);

CREATE OR REPLACE FUNCTION quickwit_manage_update_timestamp(_tbl regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE format('CREATE OR REPLACE TRIGGER set_update_timestamp BEFORE UPDATE ON %s
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

