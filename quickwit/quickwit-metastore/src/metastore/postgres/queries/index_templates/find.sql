SELECT DISTINCT ON (index_id)
    index_id,
    template_id,
    index_template_json
FROM
    unnest($1) AS index_ids(index_id)
    JOIN index_templates ON index_ids.index_id LIKE ANY (index_templates.positive_index_id_patterns)
        AND (cardinality(index_templates.negative_index_id_patterns) = 0
            OR index_ids.index_id NOT LIKE ANY (index_templates.negative_index_id_patterns))
    ORDER BY
        index_id,
        - priority,
        template_id ASC
