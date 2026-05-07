package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DocumentRepository struct {
	db *pgxpool.Pool
}

type FavoriteDocument struct {
	ID           string    `json:"id"`
	OriginalName string    `json:"original_name"`
	CollectionID string    `json:"collection_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type DocumentTag struct {
	ID         string `json:"id"`
	DocumentID string `json:"document_id"`
	Tag        string `json:"tag"`
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

func (r *DocumentRepository) SetFavorite(ctx context.Context, tenantID string, documentID string, favorite bool) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE documents
		SET is_favorite = $3
		WHERE tenant_id = $1 AND id = $2
	`, tenantID, documentID, favorite)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *DocumentRepository) ListFavorites(ctx context.Context, tenantID string) ([]FavoriteDocument, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			id,
			COALESCE(NULLIF(original_name, ''), NULLIF(logical_name, ''), NULLIF(stored_name, ''), 'Documento cargado') AS original_name,
			COALESCE(collection_id::text, '') AS collection_id,
			COALESCE(uploaded_at, created_at, NOW()) AS created_at
		FROM documents
		WHERE tenant_id = $1 AND is_favorite = true
		ORDER BY COALESCE(uploaded_at, created_at, NOW()) DESC, id ASC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	favorites := make([]FavoriteDocument, 0)
	for rows.Next() {
		var item FavoriteDocument
		if err := rows.Scan(&item.ID, &item.OriginalName, &item.CollectionID, &item.CreatedAt); err != nil {
			return nil, err
		}
		favorites = append(favorites, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return favorites, nil
}

func (r *DocumentRepository) AddDocumentTag(ctx context.Context, tenantID string, documentID string, tag string) (DocumentTag, error) {
	id := uuid.New().String()
	var item DocumentTag
	err := r.db.QueryRow(ctx, `
		INSERT INTO document_tags (id, document_id, tag)
		SELECT $1, d.id, $3
		FROM documents d
		WHERE d.tenant_id = $2 AND d.id = $4
		ON CONFLICT (document_id, tag) DO UPDATE SET tag = EXCLUDED.tag
		RETURNING id, document_id, tag
	`, id, tenantID, tag, documentID).Scan(&item.ID, &item.DocumentID, &item.Tag)
	if err != nil {
		return DocumentTag{}, err
	}
	return item, nil
}

func (r *DocumentRepository) DeleteDocumentTag(ctx context.Context, tenantID string, documentID string, tag string) error {
	commandTag, err := r.db.Exec(ctx, `
		DELETE FROM document_tags dt
		USING documents d
		WHERE d.id = dt.document_id
		  AND d.tenant_id = $1
		  AND d.id = $2
		  AND dt.tag = $3
	`, tenantID, documentID, tag)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *DocumentRepository) ListDocumentTags(ctx context.Context, tenantID string, documentID string) ([]DocumentTag, error) {
	rows, err := r.db.Query(ctx, `
		SELECT dt.id, dt.document_id, dt.tag
		FROM document_tags dt
		INNER JOIN documents d ON d.id = dt.document_id
		WHERE d.tenant_id = $1 AND d.id = $2
		ORDER BY dt.tag ASC
	`, tenantID, documentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := make([]DocumentTag, 0)
	for rows.Next() {
		var item DocumentTag
		if err := rows.Scan(&item.ID, &item.DocumentID, &item.Tag); err != nil {
			return nil, err
		}
		tags = append(tags, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}

func (r *DocumentRepository) ListDocumentsByTag(ctx context.Context, tenantID string, tag string) ([]FavoriteDocument, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			d.id,
			COALESCE(NULLIF(d.original_name, ''), NULLIF(d.logical_name, ''), NULLIF(d.stored_name, ''), 'Documento cargado') AS original_name,
			COALESCE(d.collection_id::text, '') AS collection_id,
			COALESCE(d.uploaded_at, d.created_at, NOW()) AS created_at
		FROM document_tags dt
		INNER JOIN documents d ON d.id = dt.document_id
		WHERE d.tenant_id = $1 AND dt.tag = $2
		ORDER BY COALESCE(d.uploaded_at, d.created_at, NOW()) DESC, d.id ASC
	`, tenantID, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documents := make([]FavoriteDocument, 0)
	for rows.Next() {
		var item FavoriteDocument
		if err := rows.Scan(&item.ID, &item.OriginalName, &item.CollectionID, &item.CreatedAt); err != nil {
			return nil, err
		}
		documents = append(documents, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return documents, nil
}
