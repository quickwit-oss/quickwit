-- no-transaction
-- Index for the compaction planner's scan, which on every tick reads up to
-- LIMIT splits matching:
--   split_state = 'Published'
--   maturity_timestamp > now()
-- ordered by maturity_timestamp ascending. The planner keeps a local set of
-- already-tracked splits and dedups against it, so re-reading the immature
-- set every tick is intentional -- it's how the planner recovers splits
-- whose merge timed out or failed.
--
-- The btree on (maturity_timestamp, split_id) lets postgres seek to the live
-- "still immature" range in index order, satisfying both the filter and the
-- ORDER BY without an extra sort. The split_id column is included as a
-- tiebreaker so postgres returns deterministic pages under LIMIT.
--
-- The partial predicate is restricted to split_state = 'Published' because
-- partial-index predicates must be IMMUTABLE; "now()" cannot appear here.
--
-- CONCURRENTLY avoids taking a SHARE lock on splits, which would block all
-- writes for the duration of the build. CONCURRENTLY cannot run inside a
-- transaction block, hence the `-- no-transaction` directive above.
CREATE INDEX CONCURRENTLY IF NOT EXISTS splits_published_maturity_timestamp_idx
    ON splits (maturity_timestamp, split_id)
    WHERE split_state = 'Published';