ALTER TABLE parsed_documents
ADD COLUMN IF NOT EXISTS extracted_text TEXT,
ADD COLUMN IF NOT EXISTS parser_name TEXT,
ADD COLUMN IF NOT EXISTS mime_type TEXT,
ADD COLUMN IF NOT EXISTS file_type TEXT,
ADD COLUMN IF NOT EXISTS text_length INTEGER,
ADD COLUMN IF NOT EXISTS quality_score INTEGER,
ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PARSED',
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS extraction_status TEXT NOT NULL DEFAULT 'completed';

ALTER TABLE parsed_documents ALTER COLUMN status SET DEFAULT 'PARSED';

UPDATE parsed_documents
SET extracted_text = COALESCE(extracted_text, content),
    text_length = COALESCE(text_length, length(content)),
    status = COALESCE(status, 'PARSED');
