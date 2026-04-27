package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EmbeddingRepository struct {
	db *pgxpool.Pool
}

func NewEmbeddingRepository(db *pgxpool.Pool) *EmbeddingRepository {
	return &EmbeddingRepository{db: db}
}

func (r *EmbeddingRepository) SaveEmbedding(
	ctx context.Context,
	tenantID string,
	chunkID string,
	vector []float64,
) error {
	id := uuid.New().String()

	data, _ := json.Marshal(vector)

	_, err := r.db.Exec(ctx, `
		INSERT INTO embeddings (
			id, tenant_id, chunk_id, embedding, created_at
		)
		VALUES ($1, $2, $3, $4::jsonb, NOW())
	`, id, tenantID, chunkID, string(data))

	return err
}