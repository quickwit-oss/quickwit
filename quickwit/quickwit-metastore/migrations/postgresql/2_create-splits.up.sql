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

-- apply the trigger to the `splits` table
SELECT quickwit_manage_update_timestamp('splits');

