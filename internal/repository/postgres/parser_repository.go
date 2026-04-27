package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ParserRepository struct {
	db *pgxpool.Pool
}

func NewParserRepository(db *pgxpool.Pool) *ParserRepository {
	return &ParserRepository{db: db}
}

func (r *ParserRepository) SaveParsedDocument(
	ctx context.Context,
	tenantID string,
	documentID string,
	documentVersionID string,
	content string,
) error {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO parsed_documents (
			id, tenant_id, document_id, document_version_id, content, created_at
		)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, id, tenantID, documentID, documentVersionID, content)

	return err
}