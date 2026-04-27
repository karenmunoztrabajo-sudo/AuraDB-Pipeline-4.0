package model

import "time"

type Document struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	LogicalName      string    `json:"logical_name"`
	CurrentVersionID *string   `json:"current_version_id,omitempty"`
	Status           string    `json:"status"`
	SensitivityLevel string    `json:"sensitivity_level"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type DocumentVersion struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	DocumentID      string    `json:"document_id"`
	VersionNumber   int       `json:"version_number"`
	ContentHash     string    `json:"content_hash"`
	MimeType        string    `json:"mime_type"`
	SizeBytes       int64     `json:"size_bytes"`
	ObjectKey       string    `json:"object_key"`
	ExtractionReady bool      `json:"extraction_ready"`
	CreatedAt       time.Time `json:"created_at"`
}

type RawObject struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	DocumentVersionID string    `json:"document_version_id"`
	ObjectKey         string    `json:"object_key"`
	BucketName        string    `json:"bucket_name"`
	ContentHash       string    `json:"content_hash"`
	MimeType          string    `json:"mime_type"`
	SizeBytes         int64     `json:"size_bytes"`
	StorageClass      *string   `json:"storage_class,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}