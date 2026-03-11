SELECT index_id, template_id, index_template_json
FROM (
    SELECT
        ids.index_id,
        t.template_id,
        t.index_template_json,
        ROW_NUMBER() OVER (PARTITION BY ids.index_id ORDER BY t.priority DESC, t.template_id ASC) AS rn
    FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(index_id VARCHAR(255) PATH '$')) AS ids
    CROSS JOIN index_templates t
    WHERE (
        SELECT COUNT(*) FROM JSON_TABLE(t.positive_index_id_patterns, '$[*]' COLUMNS(pat VARCHAR(255) PATH '$')) AS pp
        WHERE ids.index_id LIKE pp.pat
    ) > 0
    AND (
        SELECT COUNT(*) FROM JSON_TABLE(t.negative_index_id_patterns, '$[*]' COLUMNS(pat VARCHAR(255) PATH '$')) AS np
        WHERE ids.index_id LIKE np.pat
    ) = 0
) ranked
WHERE rn = 1
