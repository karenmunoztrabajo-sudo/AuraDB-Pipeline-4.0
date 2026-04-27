ALTER TABLE chunks
ADD COLUMN IF NOT EXISTS document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS document_version_id UUID REFERENCES document_versions(id) ON DELETE CASCADE,
ADD COLUMN IF NOT EXISTS page_number INTEGER,
ADD COLUMN IF NOT EXISTS chunk_index INTEGER,
ADD COLUMN IF NOT EXISTS content TEXT,
ADD COLUMN IF NOT EXISTS token_count INTEGER,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chunks' AND column_name = 'ir_document_id'
    ) THEN
        ALTER TABLE chunks ALTER COLUMN ir_document_id DROP NOT NULL;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chunks' AND column_name = 'chunk_type'
    ) THEN
        ALTER TABLE chunks ALTER COLUMN chunk_type DROP NOT NULL;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chunks' AND column_name = 'content_hash'
    ) THEN
        ALTER TABLE chunks ALTER COLUMN content_hash DROP NOT NULL;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chunks' AND column_name = 'chunk_version'
    ) THEN
        ALTER TABLE chunks ALTER COLUMN chunk_version DROP NOT NULL;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_chunks_document_id_index ON chunks(document_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_chunks_document_page ON chunks(document_id, page_number);
