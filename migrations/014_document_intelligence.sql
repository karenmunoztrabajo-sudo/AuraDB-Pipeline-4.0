ALTER TABLE documents
ADD COLUMN IF NOT EXISTS original_name TEXT,
ADD COLUMN IF NOT EXISTS file_type TEXT,
ADD COLUMN IF NOT EXISTS processing_status TEXT NOT NULL DEFAULT 'uploaded',
ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE documents
SET original_name = COALESCE(original_name, logical_name),
    file_type = COALESCE(file_type, ''),
    uploaded_at = COALESCE(uploaded_at, created_at);

ALTER TABLE parsed_documents
ADD COLUMN IF NOT EXISTS extracted_text TEXT,
ADD COLUMN IF NOT EXISTS extraction_status TEXT NOT NULL DEFAULT 'completed',
ADD COLUMN IF NOT EXISTS parser_name TEXT,
ADD COLUMN IF NOT EXISTS file_type TEXT;

UPDATE parsed_documents
SET extracted_text = COALESCE(extracted_text, content);

ALTER TABLE chunks
ADD COLUMN IF NOT EXISTS document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS document_version_id UUID REFERENCES document_versions(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS chunk_index INTEGER,
ADD COLUMN IF NOT EXISTS page_number INTEGER;

CREATE INDEX IF NOT EXISTS idx_chunks_document_id_index ON chunks(document_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_chunks_document_page ON chunks(document_id, page_number);
