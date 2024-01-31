INSERT INTO index_templates(template_id, positive_index_id_patterns, negative_index_id_patterns, priority, index_template_json)
    VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (template_id)
    DO UPDATE SET
        positive_index_id_patterns = $2, negative_index_id_patterns = $3, priority = $4, index_template_json = $5
