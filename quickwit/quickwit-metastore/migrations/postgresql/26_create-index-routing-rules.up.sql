CREATE TABLE IF NOT EXISTS index_routing_rules (
    routing_table_id VARCHAR(50) NOT NULL,
    rank INTEGER NOT NULL,
    filter TEXT NOT NULL,
    index_id VARCHAR(50) NOT NULL,
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),

    PRIMARY KEY (routing_table_id, rank),
    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);
