-- create main table
CREATE TABLE documents (
    doc_id uuid PRIMARY KEY,
    object jsonb
);

-- creates index on documents for faster JSONPATH matching
CREATE INDEX doc_idx ON documents USING gin (object jsonb_path_ops);

-- optional: if you have fulltext searchable elements, you should probably create an index for that.
-- CREATE INDEX fts_idx ON documents USING gin (to_tsvector('english', (object ->> 'field'::text)));
