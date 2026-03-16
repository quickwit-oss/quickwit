ALTER TABLE splits
ADD COLUMN soft_deleted_doc_ids INTEGER[] NOT NULL DEFAULT '{}';
