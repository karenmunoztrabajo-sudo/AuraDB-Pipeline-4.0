package postgres

import (
	"context"

	"auradb-pipeline/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TenantRepository struct {
	db *pgxpool.Pool
}

func NewTenantRepository(db *pgxpool.Pool) *TenantRepository {
	return &TenantRepository{db: db}
}

func (r *TenantRepository) Create(ctx context.Context, name, slug, plan string) (*model.Tenant, error) {
	id := uuid.New().String()

	query := `
		INSERT INTO tenants (id, name, slug, plan)
		VALUES ($1, $2, $3, $4)
		RETURNING id, name, slug, plan, is_active, default_policy_version, created_at, updated_at
	`

	var t model.Tenant
	err := r.db.QueryRow(ctx, query, id, name, slug, plan).Scan(
		&t.ID,
		&t.Name,
		&t.Slug,
		&t.Plan,
		&t.IsActive,
		&t.DefaultPolicyVersion,
		&t.CreatedAt,
		&t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &t, nil
}