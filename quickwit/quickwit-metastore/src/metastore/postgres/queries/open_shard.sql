INSERT INTO
    shards (
        index_uid,
        source_id,
        shard_id,
        leader_id,
        follower_id
    )
VALUES
    ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
RETURNING
    *
