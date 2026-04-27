package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ChunkRepository struct {
	db *pgxpool.Pool
}

func NewChunkRepository(db *pgxpool.Pool) *ChunkRepository {
	return &ChunkRepository{db: db}
}

func (r *ChunkRepository) SaveChunk(
	ctx context.Context,
	tenantID string,
	documentID string,
	documentVersionID string,
	chunkIndex int,
	content string,
) (string, error) {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO chunks (
			id, tenant_id, document_id, document_version_id, chunk_index, content, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, id, tenantID, documentID, documentVersionID, chunkIndex, content)
	if err != nil {
		return "", err
	}

	return id, nil
}