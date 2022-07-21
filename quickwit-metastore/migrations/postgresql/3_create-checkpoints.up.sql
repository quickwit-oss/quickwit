CREATE TABLE IF NOT EXISTS checkpoints (
    index_id VARCHAR(50) NOT NULL,
    source_id VARCHAR(50) NOT NULL,
    resource VARCHAR(255) NOT NULL,
    group_id VARCHAR(255),
    partition VARCHAR(255),
    position VARCHAR(50) DEFAULT '0',
    serial_number BIGINT DEFAULT 0,
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    update_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),

    PRIMARY KEY(index_id, source_id, resource, group_id, partition),
    FOREIGN KEY(index_id) REFERENCES indexes(index_id)
);

-- apply the update_timestamp trigger to the `checkpoints` table
SELECT quickwit_manage_update_timestamp('checkpoints');

