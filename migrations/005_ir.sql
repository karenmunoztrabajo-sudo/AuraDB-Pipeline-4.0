CREATE TABLE ir_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
    ir_version TEXT NOT NULL,
    language TEXT,
    confidence_score NUMERIC(5,4),
    sensitivity_level sensitivity_level_type NOT NULL DEFAULT 'internal',
    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (document_version_id, ir_version)
);

CREATE TABLE ir_blocks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    ir_document_id UUID NOT NULL REFERENCES ir_documents(id) ON DELETE CASCADE,
    block_type TEXT NOT NULL,
    page_number INTEGER,
    section_title TEXT,
    block_order INTEGER NOT NULL,
    parent_block_id UUID REFERENCES ir_blocks(id) ON DELETE SET NULL,
    text_content TEXT,
    normalized_content TEXT,
    bbox JSONB,
    confidence_score NUMERIC(5,4),
    sensitivity_level sensitivity_level_type NOT NULL DEFAULT 'internal',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);