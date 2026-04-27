package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AuditRepository struct {
	db *pgxpool.Pool
}

func NewAuditRepository(db *pgxpool.Pool) *AuditRepository {
	return &AuditRepository{db: db}
}

func (r *AuditRepository) Create(
	ctx context.Context,
	tenantID string,
	userID string,
	jobID string,
	requestID string,
	auditNonce string,
	actionType string,
	resourceType string,
	resourceID string,
	details map[string]interface{},
) error {
	id := uuid.New().String()

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO audit_logs (
			id, tenant_id, user_id, job_id, request_id, audit_nonce,
			action_type, resource_type, resource_id, details, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, NOW())
	`, id, tenantID, userID, jobID, requestID, auditNonce, actionType, resourceType, resourceID, string(detailsJSON))

	return err
}