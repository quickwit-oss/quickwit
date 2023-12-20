INSERT INTO
    shards (
        index_uid,
        source_id,
        shard_id,
        shard_state,
        leader_id,
        follower_id,
        publish_position_inclusive,
        publish_token
    )
VALUES
    ($1, $2, $3, CAST($4 as SHARD_STATE), $5, $6, $7, $8)
