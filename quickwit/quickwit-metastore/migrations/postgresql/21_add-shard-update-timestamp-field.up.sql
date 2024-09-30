ALTER TABLE shards
    -- We prefer a fix value here because it makes  tests simpler. 
    -- Very few users use the shard API in versions <0.9 anyway.
    ADD COLUMN IF NOT EXISTS update_timestamp TIMESTAMP NOT NULL DEFAULT '2024-01-01 00:00:00+00';
