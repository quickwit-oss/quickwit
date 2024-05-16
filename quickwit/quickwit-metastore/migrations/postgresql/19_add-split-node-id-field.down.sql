DROP INDEX IF EXISTS splits_node_id_idx;

ALTER TABLE splits
    DROP IF EXISTS COLUMN node_id;
