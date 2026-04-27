package postgres

import (
	"context"

	"auradb-pipeline/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, tenantID, email, passwordHash, role, fullName string) (*model.User, error) {
	id := uuid.New().String()

	query := `
		INSERT INTO users (id, tenant_id, email, password_hash, role, full_name)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, tenant_id, email, password_hash, role, full_name, is_active, created_at, updated_at
	`

	var u model.User
	err := r.db.QueryRow(ctx, query, id, tenantID, email, passwordHash, role, fullName).Scan(
		&u.ID,
		&u.TenantID,
		&u.Email,
		&u.PasswordHash,
		&u.Role,
		&u.FullName,
		&u.IsActive,
		&u.CreatedAt,
		&u.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &u, nil
}

func (r *UserRepository) FindByEmail(ctx context.Context, tenantID, email string) (*model.User, error) {
	query := `
		SELECT id, tenant_id, email, password_hash, role, full_name, is_active, created_at, updated_at
		FROM users
		WHERE tenant_id = $1 AND LOWER(email) = LOWER($2)
		ORDER BY is_active DESC, updated_at DESC, created_at DESC, id DESC
		LIMIT 1
	`

	var u model.User
	err := r.db.QueryRow(ctx, query, tenantID, email).Scan(
		&u.ID,
		&u.TenantID,
		&u.Email,
		&u.PasswordHash,
		&u.Role,
		&u.FullName,
		&u.IsActive,
		&u.CreatedAt,
		&u.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &u, nil
}
