CREATE TABLE IF NOT EXISTS index_templates (
    template_id VARCHAR(255) NOT NULL,
    positive_index_id_patterns VARCHAR(255)[] NOT NULL,
    negative_index_id_patterns VARCHAR(255)[] NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    index_template_json TEXT NOT NULL,
    PRIMARY KEY (template_id)
);
