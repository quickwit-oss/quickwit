-- indexes table
CREATE TABLE indexes (
    index_uid VARCHAR(282) NOT NULL,
    index_id VARCHAR(255) NOT NULL,
    index_metadata_json TEXT NOT NULL,
    create_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (index_uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE UNIQUE INDEX indexes_index_id_unique ON indexes (index_id);

-- splits table
CREATE TABLE splits (
    index_uid VARCHAR(282) NOT NULL,
    split_id VARCHAR(50) NOT NULL,
    split_state VARCHAR(30) NOT NULL,
    time_range_start BIGINT NULL,
    time_range_end BIGINT NULL,
    tags JSON NOT NULL,
    split_metadata_json TEXT NOT NULL,
    create_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    publish_timestamp DATETIME NULL,
    delete_opstamp BIGINT NOT NULL DEFAULT 0 CHECK (delete_opstamp >= 0),
    maturity_timestamp DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00',
    node_id VARCHAR(253) NOT NULL,
    split_size_bytes BIGINT GENERATED ALWAYS AS (
        CAST(JSON_UNQUOTE(JSON_EXTRACT(split_metadata_json, '$.footer_offsets.end')) AS SIGNED)
    ) STORED,
    PRIMARY KEY (index_uid, split_id),
    FOREIGN KEY (index_uid) REFERENCES indexes (index_uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX splits_time_range_start_idx ON splits (time_range_start);
CREATE INDEX splits_time_range_end_idx ON splits (time_range_end);
CREATE INDEX splits_node_id_idx ON splits (node_id);
CREATE INDEX idx_splits_stats ON splits (index_uid, split_state, split_size_bytes);
CREATE INDEX idx_splits_tags ON splits ((CAST(tags->'$' AS CHAR(128) ARRAY)));

-- delete_tasks table
CREATE TABLE delete_tasks (
    create_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    opstamp BIGINT NOT NULL AUTO_INCREMENT,
    index_uid VARCHAR(282) NOT NULL,
    delete_query_json TEXT NOT NULL,
    PRIMARY KEY (opstamp),
    FOREIGN KEY (index_uid) REFERENCES indexes (index_uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- shards table
CREATE TABLE shards (
    index_uid VARCHAR(282) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    shard_id VARCHAR(128) NOT NULL,
    leader_id VARCHAR(255) NOT NULL,
    follower_id VARCHAR(255) NULL,
    shard_state ENUM('unspecified', 'open', 'unavailable', 'closed') NOT NULL DEFAULT 'open',
    publish_position_inclusive VARCHAR(255) NOT NULL DEFAULT '',
    publish_token VARCHAR(255) NULL,
    doc_mapping_uid VARCHAR(26) NOT NULL,
    update_timestamp DATETIME NOT NULL DEFAULT '2024-01-01 00:00:00',
    PRIMARY KEY (index_uid, source_id, shard_id),
    FOREIGN KEY (index_uid) REFERENCES indexes (index_uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- index_templates table
CREATE TABLE index_templates (
    template_id VARCHAR(255) NOT NULL,
    positive_index_id_patterns JSON NOT NULL,
    negative_index_id_patterns JSON NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    index_template_json TEXT NOT NULL,
    PRIMARY KEY (template_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- kv table
CREATE TABLE kv (
    `key` VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
