package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DocumentRepository struct {
	db *pgxpool.Pool
}

func NewDocumentRepository(db *pgxpool.Pool) *DocumentRepository {
	return &DocumentRepository{db: db}
}

func (r *DocumentRepository) CreateDocument(
	ctx context.Context,
	tenantID string,
	logicalName string,
) (string, error) {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO documents (
			id, tenant_id, logical_name, original_name, file_type, processing_status, uploaded_at
		)
		VALUES ($1, $2, $3, $3, '', 'uploaded', NOW())
	`, id, tenantID, logicalName)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (r *DocumentRepository) CreateDocumentVersion(
	ctx context.Context,
	tenantID string,
	documentID string,
	versionNumber int,
	contentHash string,
	mimeType string,
	sizeBytes int64,
	objectKey string,
) (string, error) {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO document_versions (
			id, tenant_id, document_id, version_number, content_hash, mime_type,
			size_bytes, object_key
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, id, tenantID, documentID, versionNumber, contentHash, mimeType, sizeBytes, objectKey)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (r *DocumentRepository) CreateRawObject(
	ctx context.Context,
	tenantID string,
	documentVersionID string,
	objectKey string,
	bucketName string,
	contentHash string,
	mimeType string,
	sizeBytes int64,
) error {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO raw_objects (
			id, tenant_id, document_version_id, object_key, bucket_name,
			content_hash, mime_type, size_bytes
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, id, tenantID, documentVersionID, objectKey, bucketName, contentHash, mimeType, sizeBytes)

	return err
}

func (r *DocumentRepository) CreateUploadedDocument(
	ctx context.Context,
	tenantID string,
	ownerUserID string,
	originalName string,
	storedName string,
	mimeType string,
	sizeBytes int64,
	path string,
) (string, error) {
	id := uuid.New().String()

	_, err := r.db.Exec(ctx, `
		INSERT INTO documents (
			id, tenant_id, logical_name, original_name, stored_name,
			mime_type, size, path, status, owner_user_id, uploaded_at
		)
		VALUES ($1, $2, $3, $3, $4, $5, $6, $7, 'UPLOADED', $8, NOW())
	`, id, tenantID, originalName, storedName, mimeType, sizeBytes, path, ownerUserID)
	if err != nil {
		return "", err
	}

	return id, nil
}
