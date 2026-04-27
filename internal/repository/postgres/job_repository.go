package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	db *pgxpool.Pool
}

func NewJobRepository(db *pgxpool.Pool) *JobRepository {
	return &JobRepository{db: db}
}

func (r *JobRepository) CreateJob(
	ctx context.Context,
	tenantID string,
	documentID string,
	documentVersionID string,
	userID string,
	requestID string,
	auditNonce string,
) (string, error) {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO jobs (
			id, tenant_id, document_id, document_version_id,
			job_type, status, current_step,
			request_id, audit_nonce, initiated_by_user_id,
			priority, cumulative_cost, created_at, updated_at
		)
		VALUES (
			$1, $2, $3, $4,
			'upload', 'received', 'received',
			$5, $6, $7,
			100, 0, NOW(), NOW()
		)
	`, id, tenantID, documentID, documentVersionID, requestID, auditNonce, userID)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (r *JobRepository) CreateJobStep(
	ctx context.Context,
	tenantID string,
	jobID string,
	stepName string,
	stepOrder int,
	status string,
) error {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO job_steps (
			id, tenant_id, job_id, step_name, step_order,
			status, was_compensated, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, false, NOW())
	`, id, tenantID, jobID, stepName, stepOrder, status)

	return err
}
func (r *JobRepository) UpdateJobStatus(
	ctx context.Context,
	jobID string,
	status string,
	currentStep string,
) error {
	_, err := r.db.Exec(ctx, `
		UPDATE jobs
		SET status = $1,
		    current_step = $2,
		    updated_at = NOW()
		WHERE id = $3
	`, status, currentStep, jobID)

	return err
}

func (r *JobRepository) AddJobStep(
	ctx context.Context,
	tenantID string,
	jobID string,
	stepName string,
	stepOrder int,
	status string,
) error {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO job_steps (
			id, tenant_id, job_id, step_name, step_order,
			status, was_compensated, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, false, NOW())
	`, id, tenantID, jobID, stepName, stepOrder, status)

	return err
}