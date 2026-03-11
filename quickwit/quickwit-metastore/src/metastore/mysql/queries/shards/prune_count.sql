DELETE FROM shards
WHERE index_uid = ?
    AND source_id = ?
    AND shard_id NOT IN (
        SELECT shard_id FROM (
            SELECT shard_id
            FROM shards
            WHERE index_uid = ?
                AND source_id = ?
            ORDER BY update_timestamp DESC, shard_id DESC
            LIMIT ?
        ) AS recent
    )
