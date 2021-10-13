CREATE TABLE splits (
    split_id VARCHAR(50) PRIMARY KEY,
    split_state VARCHAR(30) NOT NULL,
    update_timestamp BIGINT DEFAULT 0,
    time_range_start BIGINT,
    time_range_end BIGINT,
    tags TEXT[] NOT NULL,
    split_metadata_json TEXT NOT NULL,
    index_id VARCHAR(50) NOT NULL,

    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);
