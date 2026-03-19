DELETE FROM shards
WHERE index_uid = ?
    AND source_id = ?
    AND update_timestamp < ?
