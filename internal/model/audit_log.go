package model

import "time"

type AuditLog struct {
	ID           string    `json:"id"`
	TenantID     *string   `json:"tenant_id,omitempty"`
	UserID       *string   `json:"user_id,omitempty"`
	JobID        *string   `json:"job_id,omitempty"`
	RequestID    *string   `json:"request_id,omitempty"`
	AuditNonce   *string   `json:"audit_nonce,omitempty"`
	ActionType   string    `json:"action_type"`
	ResourceType string    `json:"resource_type"`
	ResourceID   *string   `json:"resource_id,omitempty"`
	Details      string    `json:"details"`
	CreatedAt    time.Time `json:"created_at"`
}