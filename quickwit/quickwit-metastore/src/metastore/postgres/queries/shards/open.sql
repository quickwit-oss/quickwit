INSERT INTO shards(index_uid, source_id, shard_id, leader_id, follower_id, doc_mapping_uid, publish_token, update_timestamp)
    VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
ON CONFLICT
    DO NOTHING
RETURNING
    *
