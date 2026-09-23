UPDATE
    shards
SET
    publish_token = $4
WHERE
    index_uid = $1
    AND source_id = $2
    AND shard_id = ANY ($3)
    -- Acquisition is monotonic between ULIDs; a legacy presented token keeps pre-upgrade behavior.
    AND (
        $4 LIKE '%/%' -- presented token is legacy (pre-ULID): always takes, for rolling upgrades
        OR publish_token IS NULL -- never acquired: free to take
        OR publish_token = '' -- empty placeholder: free to take
        OR publish_token LIKE '%/%' -- recorded token is legacy: superseded by any ULID
        OR $4 >= publish_token -- both are ULIDs: take only if ours is newer-or-equal
    )
RETURNING
    *
