DELETE FROM shards
WHERE index_uid = $1
    AND source_id = $2
    AND update_timestamp < $3
