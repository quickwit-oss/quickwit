UPDATE
    shards
SET
    publish_token = $4
WHERE
    index_uid = $1
    AND source_id = $2
    AND shard_id = ANY($3)
RETURNING *
