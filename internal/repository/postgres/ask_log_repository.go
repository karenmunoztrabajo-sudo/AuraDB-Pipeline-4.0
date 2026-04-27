package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AskLogRepository struct {
	db *pgxpool.Pool
}

type AskLogItem struct {
	ID         string    `json:"id"`
	DocumentID string    `json:"document_id"`
	Question   string    `json:"question"`
	Answer     string    `json:"answer"`
	CreatedAt  time.Time `json:"created_at"`
}

func NewAskLogRepository(db *pgxpool.Pool) *AskLogRepository {
	return &AskLogRepository{db: db}
}

func (r *AskLogRepository) Create(
	ctx context.Context,
	tenantID string,
	userID string,
	documentID string,
	question string,
	answer string,
	contextText string,
	sources any,
) error {
	id := uuid.New().String()

	sourcesJSON, err := json.Marshal(sources)
	if err != nil {
		return err
	}

	var nullableDocumentID any
	if documentID != "" {
		nullableDocumentID = documentID
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO ask_logs (
			id, tenant_id, user_id, document_id,
			question, answer, context, sources_json, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, NOW())
	`, id, tenantID, userID, nullableDocumentID, question, answer, contextText, string(sourcesJSON))

	return err
}

func (r *AskLogRepository) ListLatestByTenant(ctx context.Context, tenantID string, limit int) ([]AskLogItem, error) {
	if limit <= 0 || limit > 20 {
		limit = 20
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			id::text,
			COALESCE(document_id::text, ''),
			question,
			answer,
			created_at
		FROM ask_logs
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]AskLogItem, 0)
	for rows.Next() {
		var item AskLogItem
		if err := rows.Scan(
			&item.ID,
			&item.DocumentID,
			&item.Question,
			&item.Answer,
			&item.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func (r *AskLogRepository) DeleteByIDAndTenant(ctx context.Context, tenantID string, id string) (bool, error) {
	result, err := r.db.Exec(ctx, `
		DELETE FROM ask_logs
		WHERE id = $1
		  AND tenant_id = $2
	`, id, tenantID)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() > 0, nil
}
