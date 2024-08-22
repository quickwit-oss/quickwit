WITH recent_shards AS (
    SELECT shard_id
    FROM shards
    WHERE index_uid = $1
        AND source_id = $2
    ORDER BY update_timestamp DESC
    LIMIT $3
)
DELETE FROM shards
WHERE index_uid = $1
    AND source_id = $2
    AND shard_id NOT IN (SELECT shard_id FROM recent_shards)
