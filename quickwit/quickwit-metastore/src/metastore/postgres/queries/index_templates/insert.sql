INSERT INTO index_templates(template_id, positive_index_id_patterns, negative_index_id_patterns, priority, index_template_json)
    VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (template_id)
    DO NOTHING
