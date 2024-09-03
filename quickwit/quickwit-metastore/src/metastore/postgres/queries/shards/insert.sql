INSERT INTO shards(index_uid, source_id, shard_id, shard_state, leader_id, follower_id, doc_mapping_uid, publish_position_inclusive, publish_token, update_timestamp)
    VALUES ($1, $2, $3, CAST($4 AS SHARD_STATE), $5, $6, $7, $8, $9, $10)
