package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DocumentCollection struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id,omitempty"`
	Name          string    `json:"name"`
	DocumentCount int       `json:"document_count"`
	CreatedAt     time.Time `json:"created_at"`
}

type CollectionDocument struct {
	ID           string `json:"id"`
	OriginalName string `json:"original_name"`
	Name         string `json:"name"`
	IsFavorite   bool   `json:"is_favorite"`
}

func (r *DocumentRepository) CreateCollection(ctx context.Context, tenantID string, name string) (DocumentCollection, error) {
	name = strings.TrimSpace(name)
	id := uuid.New().String()

	var collection DocumentCollection
	err := r.db.QueryRow(ctx, `
		INSERT INTO collections (id, tenant_id, name)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, tenant_id, name, created_at
	`, id, tenantID, name).Scan(&collection.ID, &collection.TenantID, &collection.Name, &collection.CreatedAt)
	if err != nil {
		return DocumentCollection{}, err
	}
	if err := r.syncLegacyCollection(ctx, collection); err != nil {
		return DocumentCollection{}, err
	}

	return collection, nil
}

func (r *DocumentRepository) syncLegacyCollection(ctx context.Context, collection DocumentCollection) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO document_collections (id, tenant_id, name, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
	`, collection.ID, collection.TenantID, collection.Name, collection.CreatedAt)
	if err != nil && strings.Contains(err.Error(), `relation "document_collections" does not exist`) {
		return nil
	}
	return err
}

func (r *DocumentRepository) ListCollections(ctx context.Context, tenantID string) ([]DocumentCollection, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			c.id,
			c.tenant_id,
			c.name,
			c.created_at,
			COUNT(d.id)::int AS document_count
		FROM collections c
		LEFT JOIN documents d ON d.collection_id = c.id AND d.tenant_id = c.tenant_id
		WHERE c.tenant_id = $1
		GROUP BY c.id, c.tenant_id, c.name, c.created_at
		ORDER BY c.name ASC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	collections := make([]DocumentCollection, 0)
	for rows.Next() {
		var item DocumentCollection
		if err := rows.Scan(&item.ID, &item.TenantID, &item.Name, &item.CreatedAt, &item.DocumentCount); err != nil {
			return nil, err
		}
		collections = append(collections, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return collections, nil
}

func (r *DocumentRepository) AssignDocumentToCollection(ctx context.Context, tenantID string, documentID string, collectionID string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE documents
		SET collection_id = $3
		WHERE tenant_id = $1
		  AND id = $2
		  AND EXISTS (
			  SELECT 1
			  FROM collections
			  WHERE id = $3 AND tenant_id = $1
		  )
	`, tenantID, documentID, collectionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *DocumentRepository) CollectionExists(ctx context.Context, tenantID string, collectionID string) (bool, error) {
	if strings.TrimSpace(collectionID) == "" {
		return false, nil
	}

	var exists bool
	err := r.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM collections
			WHERE tenant_id = $1 AND id = $2
		)
	`, tenantID, collectionID).Scan(&exists)
	return exists, err
}

func (r *DocumentRepository) ListCollectionDocuments(ctx context.Context, tenantID string, collectionID string) ([]CollectionDocument, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			id,
			COALESCE(NULLIF(original_name, ''), NULLIF(logical_name, ''), NULLIF(stored_name, ''), 'Documento cargado') AS original_name,
			COALESCE(is_favorite, false) AS is_favorite
		FROM documents
		WHERE tenant_id = $1 AND collection_id = $2
		ORDER BY COALESCE(uploaded_at, created_at, NOW()) DESC, id ASC
	`, tenantID, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	documents := make([]CollectionDocument, 0)
	for rows.Next() {
		var item CollectionDocument
		if err := rows.Scan(&item.ID, &item.OriginalName, &item.IsFavorite); err != nil {
			return nil, err
		}
		item.Name = item.OriginalName
		documents = append(documents, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return documents, nil
}
