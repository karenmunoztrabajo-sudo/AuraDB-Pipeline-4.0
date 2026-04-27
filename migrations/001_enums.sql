DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'plan_type') THEN
        CREATE TYPE plan_type AS ENUM ('core', 'compliance', 'enterprise');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role_type') THEN
        CREATE TYPE user_role_type AS ENUM ('owner', 'admin', 'analyst', 'viewer', 'service');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'source_type') THEN
        CREATE TYPE source_type AS ENUM (
            'upload_api',
            's3',
            'google_drive',
            'dropbox',
            'slack',
            'email',
            'sharepoint',
            'onedrive',
            'notion',
            'internal_repository'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'document_status_type') THEN
        CREATE TYPE document_status_type AS ENUM (
            'active',
            'archived',
            'deleted',
            'quarantined'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status_type') THEN
        CREATE TYPE job_status_type AS ENUM (
            'received',
            'stored_raw',
            'queued_for_parse',
            'parsed',
            'ir_created',
            'privacy_scan_pending',
            'privacy_processed',
            'normalized',
            'quality_scored',
            'chunked',
            'embedding_requested',
            'embedded',
            'indexed',
            'verified',
            'failed_parse',
            'failed_partial',
            'failed_terminal',
            'quarantined'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'step_status_type') THEN
        CREATE TYPE step_status_type AS ENUM (
            'pending',
            'running',
            'completed',
            'failed',
            'compensated',
            'skipped'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sensitivity_level_type') THEN
        CREATE TYPE sensitivity_level_type AS ENUM (
            'public',
            'internal',
            'confidential',
            'restricted',
            'regulated'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'privacy_action_type') THEN
        CREATE TYPE privacy_action_type AS ENUM (
            'allow',
            'mask',
            'tokenize',
            'pseudonymize',
            'block',
            'review'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'chunk_type_enum') THEN
        CREATE TYPE chunk_type_enum AS ENUM (
            'heading_body',
            'paragraph',
            'table',
            'clause',
            'row_group',
            'faq',
            'email_thread',
            'other'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cost_type_enum') THEN
        CREATE TYPE cost_type_enum AS ENUM (
            'storage',
            'ocr',
            'embedding',
            'retrieval',
            'reranking',
            'egress',
            'compute',
            'other'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'deletion_status_type') THEN
        CREATE TYPE deletion_status_type AS ENUM (
            'requested',
            'in_progress',
            'completed',
            'failed'
        );
    END IF;
END$$;
