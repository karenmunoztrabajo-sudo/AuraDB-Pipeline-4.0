CREATE TABLE chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    ir_document_id UUID NOT NULL REFERENCES ir_documents(id) ON DELETE CASCADE,
    chunk_type chunk_type_enum NOT NULL,
    page_number INTEGER,
    section_title TEXT,
    content TEXT NOT NULL,
    normalized_content TEXT,
    sensitivity_level sensitivity_level_type NOT NULL DEFAULT 'internal',
    quality_score NUMERIC(5,4),
    content_hash TEXT NOT NULL,
    chunk_version TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chunk_block_refs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    chunk_id UUID NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    ir_block_id UUID NOT NULL REFERENCES ir_blocks(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chunk_id, ir_block_id)
);

CREATE TABLE embeddings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    chunk_id UUID NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    embedding_model TEXT NOT NULL,
    embedding_version TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    provider_name TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    vector_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chunk_id, embedding_model, embedding_version)
);