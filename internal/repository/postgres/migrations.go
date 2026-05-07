package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RunStartupMigrations(ctx context.Context, db *pgxpool.Pool) error {
	if err := EnsureCollections(ctx, db); err != nil {
		return err
	}
	if err := EnsureDocumentTags(ctx, db); err != nil {
		return err
	}
	if err := EnsureDocumentsColumns(ctx, db); err != nil {
		return err
	}
	if err := EnsureParsedDocumentsColumns(ctx, db); err != nil {
		return err
	}
	if err := EnsureChunksColumns(ctx, db); err != nil {
		return err
	}
	return nil
}

func EnsureDocumentsColumns(ctx context.Context, db *pgxpool.Pool) error {
	if err := ensureDocumentStatusEnumValue(ctx, db); err != nil {
		return err
	}

	columns := []struct {
		name       string
		definition string
	}{
		{name: "original_name", definition: "TEXT"},
		{name: "stored_name", definition: "TEXT"},
		{name: "mime_type", definition: "TEXT"},
		{name: "size", definition: "BIGINT"},
		{name: "path", definition: "TEXT"},
		{name: "status", definition: "TEXT DEFAULT 'UPLOADED'"},
		{name: "created_at", definition: "TIMESTAMP DEFAULT NOW()"},
		{name: "file_type", definition: "TEXT"},
		{name: "processing_status", definition: "TEXT NOT NULL DEFAULT 'uploaded'"},
		{name: "owner_user_id", definition: "UUID REFERENCES users(id) ON DELETE SET NULL"},
		{name: "uploaded_at", definition: "TIMESTAMPTZ NOT NULL DEFAULT NOW()"},
		{name: "collection_id", definition: "UUID REFERENCES collections(id) ON DELETE SET NULL"},
		{name: "is_favorite", definition: "BOOLEAN NOT NULL DEFAULT false"},
	}

	for _, column := range columns {
		exists, err := tableColumnExists(ctx, db, "documents", column.name)
		if err != nil {
			return fmt.Errorf("verificando columna documents.%s: %w", column.name, err)
		}
		if exists {
			continue
		}

		if _, err := db.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE documents ADD COLUMN %s %s",
			column.name,
			column.definition,
		)); err != nil {
			return fmt.Errorf("creando columna documents.%s: %w", column.name, err)
		}
	}

	if _, err := db.Exec(ctx, `ALTER TABLE documents ALTER COLUMN status SET DEFAULT 'UPLOADED'`); err != nil {
		return fmt.Errorf("actualizando default de documents.status: %w", err)
	}

	if _, err := db.Exec(ctx, `ALTER TABLE documents ADD COLUMN IF NOT EXISTS is_favorite boolean DEFAULT false`); err != nil {
		return fmt.Errorf("asegurando columna documents.is_favorite: %w", err)
	}
	if _, err := db.Exec(ctx, `UPDATE documents SET is_favorite = false WHERE is_favorite IS NULL`); err != nil {
		return fmt.Errorf("normalizando documents.is_favorite: %w", err)
	}
	if _, err := db.Exec(ctx, `ALTER TABLE documents ALTER COLUMN is_favorite SET DEFAULT false`); err != nil {
		return fmt.Errorf("actualizando default de documents.is_favorite: %w", err)
	}

	if _, err := db.Exec(ctx, `
		UPDATE documents
		SET original_name = COALESCE(original_name, logical_name),
		    file_type = COALESCE(file_type, ''),
		    uploaded_at = COALESCE(uploaded_at, created_at)
	`); err != nil {
		return fmt.Errorf("normalizando columnas de documents: %w", err)
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_documents_owner_user_id ON documents(owner_user_id)`); err != nil {
		return fmt.Errorf("creando indice idx_documents_owner_user_id: %w", err)
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_documents_uploaded_at ON documents(uploaded_at DESC)`); err != nil {
		return fmt.Errorf("creando indice idx_documents_uploaded_at: %w", err)
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_documents_collection_id ON documents(collection_id)`); err != nil {
		return fmt.Errorf("creando indice idx_documents_collection_id: %w", err)
	}

	return nil
}

func EnsureDocumentTags(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS document_tags (
			id UUID PRIMARY KEY,
			document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
			tag TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (document_id, tag)
		)
	`); err != nil {
		return fmt.Errorf("creando tabla document_tags: %w", err)
	}
	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_document_tags_document_id ON document_tags(document_id)`); err != nil {
		return fmt.Errorf("creando indice idx_document_tags_document_id: %w", err)
	}
	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_document_tags_tag ON document_tags(tag)`); err != nil {
		return fmt.Errorf("creando indice idx_document_tags_tag: %w", err)
	}
	return nil
}

func EnsureCollections(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS collections (
			id UUID PRIMARY KEY,
			tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, name)
		)
	`); err != nil {
		return fmt.Errorf("creando tabla collections: %w", err)
	}

	if _, err := db.Exec(ctx, `
		INSERT INTO collections (id, tenant_id, name, created_at)
		SELECT id, tenant_id, name, created_at
		FROM document_collections
		ON CONFLICT (id) DO NOTHING
	`); err != nil && !strings.Contains(err.Error(), `relation "document_collections" does not exist`) {
		return fmt.Errorf("migrando document_collections a collections: %w", err)
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_collections_tenant_id ON collections(tenant_id)`); err != nil {
		return fmt.Errorf("creando indice idx_collections_tenant_id: %w", err)
	}

	return nil
}

func EnsureParsedDocumentsColumns(ctx context.Context, db *pgxpool.Pool) error {
	columns := []struct {
		name       string
		definition string
	}{
		{name: "extracted_text", definition: "TEXT"},
		{name: "parser_name", definition: "TEXT"},
		{name: "mime_type", definition: "TEXT"},
		{name: "file_type", definition: "TEXT"},
		{name: "text_length", definition: "INTEGER"},
		{name: "quality_score", definition: "INTEGER"},
		{name: "status", definition: "TEXT DEFAULT 'PARSED'"},
		{name: "created_at", definition: "TIMESTAMP DEFAULT NOW()"},
		{name: "updated_at", definition: "TIMESTAMP DEFAULT NOW()"},
		{name: "extraction_status", definition: "TEXT NOT NULL DEFAULT 'completed'"},
	}

	for _, column := range columns {
		exists, err := tableColumnExists(ctx, db, "parsed_documents", column.name)
		if err != nil {
			return fmt.Errorf("verificando columna parsed_documents.%s: %w", column.name, err)
		}
		if exists {
			continue
		}

		if _, err := db.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE parsed_documents ADD COLUMN %s %s",
			column.name,
			column.definition,
		)); err != nil {
			return fmt.Errorf("creando columna parsed_documents.%s: %w", column.name, err)
		}
	}

	if _, err := db.Exec(ctx, `ALTER TABLE parsed_documents ALTER COLUMN status SET DEFAULT 'PARSED'`); err != nil {
		return fmt.Errorf("actualizando default de parsed_documents.status: %w", err)
	}

	if _, err := db.Exec(ctx, `
		UPDATE parsed_documents
		SET extracted_text = COALESCE(extracted_text, content),
		    text_length = COALESCE(text_length, length(content)),
		    status = COALESCE(status, 'PARSED')
	`); err != nil {
		return fmt.Errorf("normalizando columnas de parsed_documents: %w", err)
	}

	return nil
}

func EnsureChunksColumns(ctx context.Context, db *pgxpool.Pool) error {
	columns := []struct {
		name       string
		definition string
	}{
		{name: "document_id", definition: "UUID REFERENCES documents(id) ON DELETE CASCADE"},
		{name: "document_version_id", definition: "UUID REFERENCES document_versions(id) ON DELETE CASCADE"},
		{name: "page_number", definition: "INTEGER"},
		{name: "chunk_index", definition: "INTEGER"},
		{name: "content", definition: "TEXT"},
		{name: "token_count", definition: "INTEGER"},
		{name: "created_at", definition: "TIMESTAMP DEFAULT NOW()"},
		{name: "updated_at", definition: "TIMESTAMP DEFAULT NOW()"},
	}

	for _, column := range columns {
		exists, err := tableColumnExists(ctx, db, "chunks", column.name)
		if err != nil {
			return fmt.Errorf("verificando columna chunks.%s: %w", column.name, err)
		}
		if exists {
			continue
		}

		if _, err := db.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE chunks ADD COLUMN %s %s",
			column.name,
			column.definition,
		)); err != nil {
			return fmt.Errorf("creando columna chunks.%s: %w", column.name, err)
		}
	}

	legacyRequiredColumns := []string{"ir_document_id", "chunk_type", "content_hash", "chunk_version"}
	for _, columnName := range legacyRequiredColumns {
		exists, err := tableColumnExists(ctx, db, "chunks", columnName)
		if err != nil {
			return fmt.Errorf("verificando columna chunks.%s: %w", columnName, err)
		}
		if !exists {
			continue
		}

		if _, err := db.Exec(ctx, fmt.Sprintf("ALTER TABLE chunks ALTER COLUMN %s DROP NOT NULL", columnName)); err != nil {
			return fmt.Errorf("quitando NOT NULL de chunks.%s: %w", columnName, err)
		}
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_chunks_document_id_index ON chunks(document_id, chunk_index)`); err != nil {
		return fmt.Errorf("creando indice idx_chunks_document_id_index: %w", err)
	}

	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_chunks_document_page ON chunks(document_id, page_number)`); err != nil {
		return fmt.Errorf("creando indice idx_chunks_document_page: %w", err)
	}

	return nil
}

func tableColumnExists(ctx context.Context, db *pgxpool.Pool, tableName string, columnName string) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists)

	return exists, err
}

func ensureDocumentStatusEnumValue(ctx context.Context, db *pgxpool.Pool) error {
	var typeExists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'document_status_type')`).Scan(&typeExists); err != nil {
		return fmt.Errorf("verificando document_status_type: %w", err)
	}
	if !typeExists {
		return nil
	}

	var exists bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_type t
			JOIN pg_enum e ON e.enumtypid = t.oid
			WHERE t.typname = 'document_status_type'
			  AND e.enumlabel = 'UPLOADED'
		)
	`).Scan(&exists); err != nil {
		return fmt.Errorf("verificando document_status_type.UPLOADED: %w", err)
	}
	if exists {
		return nil
	}

	if _, err := db.Exec(ctx, `ALTER TYPE document_status_type ADD VALUE IF NOT EXISTS 'UPLOADED'`); err != nil {
		return fmt.Errorf("agregando document_status_type.UPLOADED: %w", err)
	}

	return nil
}
