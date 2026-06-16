UPDATE
    shards
SET
    publish_token = $4
WHERE
    index_uid = $1
    AND source_id = $2
    AND shard_id = ANY ($3)
    -- Monotonic acquisition: the presented token ($4) must rank >= the recorded one.
    AND (
        publish_token IS NULL -- never acquired: free to take
        OR publish_token = '' -- empty placeholder: free to take
        OR publish_token LIKE '%/%' -- legacy pre-ULID token: ranks below any ULID, so superseded
        -- both are ULIDs: take it only if ours is newer-or-equal (and ours is itself a ULID)
        OR ($4 NOT LIKE '%/%' AND $4 >= publish_token)
    )
RETURNING
    *
