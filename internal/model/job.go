package model

import "time"

type Job struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	DocumentID        *string   `json:"document_id,omitempty"`
	DocumentVersionID *string   `json:"document_version_id,omitempty"`
	JobType           string    `json:"job_type"`
	Status            string    `json:"status"`
	CurrentStep       string    `json:"current_step"`
	RequestID         *string   `json:"request_id,omitempty"`
	AuditNonce        *string   `json:"audit_nonce,omitempty"`
	InitiatedByUserID *string   `json:"initiated_by_user_id,omitempty"`
	IdempotencyKey    *string   `json:"idempotency_key,omitempty"`
	Priority          int       `json:"priority"`
	CumulativeCost    float64   `json:"cumulative_cost"`
	ErrorMessage      *string   `json:"error_message,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type JobStep struct {
	ID                 string     `json:"id"`
	TenantID           string     `json:"tenant_id"`
	JobID              string     `json:"job_id"`
	StepName           string     `json:"step_name"`
	StepOrder          int        `json:"step_order"`
	Status             string     `json:"status"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	ErrorMessage       *string    `json:"error_message,omitempty"`
	CompensationAction *string    `json:"compensation_action,omitempty"`
	WasCompensated     bool       `json:"was_compensated"`
	CreatedAt          time.Time  `json:"created_at"`
}