CREATE TYPE SHARD_STATE AS ENUM ('unspecified', 'open', 'unavailable', 'closed');

CREATE TABLE IF NOT EXISTS shards (
    index_uid VARCHAR(282) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    shard_id BIGSERIAL,
    leader_id VARCHAR(255) NOT NULL,
    follower_id VARCHAR(255),
    shard_state SHARD_STATE NOT NULL,
    publish_position_inclusive VARCHAR(255) NOT NULL,
    publish_token VARCHAR(255),
    PRIMARY KEY (index_uid, source_id, shard_id),
    FOREIGN KEY (index_uid) REFERENCES indexes (index_uid)
);