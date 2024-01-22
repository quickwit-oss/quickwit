DELETE FROM
    shards
WHERE
    index_uid = $1
    AND source_id = $2
    AND shard_id = ANY($3)
    AND (
        $4 = TRUE
        OR publish_position_inclusive LIKE '~%'
    )
