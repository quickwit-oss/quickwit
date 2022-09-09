CREATE TABLE IF NOT EXISTS delete_tasks (
    create_timestamp TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    opstamp BIGSERIAL PRIMARY KEY,
    index_id VARCHAR(50) NOT NULL,
    delete_query_json TEXT NOT NULL,

    FOREIGN KEY(index_id) REFERENCES indexes(index_id) ON DELETE CASCADE
);
