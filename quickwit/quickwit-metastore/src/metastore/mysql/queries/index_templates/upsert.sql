INSERT INTO index_templates(template_id, positive_index_id_patterns, negative_index_id_patterns, priority, index_template_json)
    VALUES (?, ?, ?, ?, ?)
    AS new
ON DUPLICATE KEY UPDATE
    positive_index_id_patterns = new.positive_index_id_patterns,
    negative_index_id_patterns = new.negative_index_id_patterns,
    priority = new.priority,
    index_template_json = new.index_template_json
