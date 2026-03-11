INSERT INTO shards(index_uid, source_id, shard_id, shard_state, leader_id, follower_id, doc_mapping_uid, publish_position_inclusive, publish_token, update_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
