SELECT
    *
FROM
    indexes
WHERE
    index_id = ANY ($1)
    OR index_uid = ANY ($2)
