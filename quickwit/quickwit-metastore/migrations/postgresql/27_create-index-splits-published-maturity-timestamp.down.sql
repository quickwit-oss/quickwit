-- no-transaction
-- CONCURRENTLY matches the up migration: avoids blocking writes on `splits`
-- while the index is being dropped. Cannot run inside a transaction.
DROP INDEX CONCURRENTLY IF EXISTS splits_published_maturity_timestamp_idx;