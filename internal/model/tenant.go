package model

import "time"

type Tenant struct {
	ID                   string    `json:"id"`
	Name                 string    `json:"name"`
	Slug                 string    `json:"slug"`
	Plan                 string    `json:"plan"`
	IsActive             bool      `json:"is_active"`
	DefaultPolicyVersion string    `json:"default_policy_version"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}