CREATE TABLE privacy_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_version_id UUID REFERENCES document_versions(id) ON DELETE CASCADE,
    ir_block_id UUID REFERENCES ir_blocks(id) ON DELETE CASCADE,
    policy_version TEXT NOT NULL,
    detected_entity_type TEXT,
    sensitivity_level sensitivity_level_type NOT NULL,
    action_taken privacy_action_type NOT NULL,
    confidence_score NUMERIC(5,4),
    decision_reason TEXT,
    decision_engine TEXT,
    is_manual_override BOOLEAN NOT NULL DEFAULT FALSE,
    decided_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE quality_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    document_version_id UUID REFERENCES document_versions(id) ON DELETE CASCADE,
    ir_document_id UUID REFERENCES ir_documents(id) ON DELETE CASCADE,
    score_type TEXT NOT NULL,
    score_value NUMERIC(5,4) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);