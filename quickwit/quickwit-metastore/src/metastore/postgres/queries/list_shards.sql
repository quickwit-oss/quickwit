SELECT
    *
FROM
    shards
WHERE
    index_uid = $1
    AND source_id = $2
    AND (
        $3 IS NULL
        OR shard_state = CAST($3 AS SHARD_STATE)
    )
