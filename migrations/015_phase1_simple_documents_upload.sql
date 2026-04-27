ALTER TYPE document_status_type ADD VALUE IF NOT EXISTS 'UPLOADED';

ALTER TABLE documents
ADD COLUMN IF NOT EXISTS original_name TEXT,
ADD COLUMN IF NOT EXISTS stored_name TEXT,
ADD COLUMN IF NOT EXISTS mime_type TEXT,
ADD COLUMN IF NOT EXISTS size BIGINT,
ADD COLUMN IF NOT EXISTS path TEXT,
ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE documents
SET original_name = COALESCE(original_name, logical_name),
    uploaded_at = COALESCE(uploaded_at, created_at);

CREATE INDEX IF NOT EXISTS idx_documents_owner_user_id ON documents(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_documents_uploaded_at ON documents(uploaded_at DESC);
